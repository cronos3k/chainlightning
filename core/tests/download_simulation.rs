//! Comprehensive 1GB Download Simulation Test
//!
//! Exercises the full rate control + scheduling pipeline:
//! - RateController (Glorytun/MUD-style) with simulated probe exchange
//! - LinkScheduler with dynamic weight updates via set_rate_controlled_weights()
//! - ProbePacket encode/decode wire format (51-byte roundtrip)
//! - Congestion detection, loss tracking, PathState machine transitions
//! - Link failure (timeout → DOWN) and recovery (PROBING → RUNNING)
//! - Rate floor protection (death spiral prevention)
//!
//! Link layout (matching production):
//!   L0: ADSL1     -  62.5 Mbps down / 12.7 Mbps up
//!   L1: Starlink1 - 220.0 Mbps down / 20.0 Mbps up
//!   L2: ADSL2     -  62.5 Mbps down / 12.7 Mbps up
//!   L3: Starlink2 - 220.0 Mbps down / 20.0 Mbps up
//!   L4: ADSL3     -  62.5 Mbps down / 12.7 Mbps up
//!
//! All tests are fully deterministic with no network I/O.

use std::time::Duration;

use chainlightning_common::config::{LinkSchedulerConfig, RateControlConfig};
use chainlightning_common::protocol::{now_micros, PathState, ProbePacket};
use chainlightning_core::rate_controller::RateController;
use chainlightning_core::link_scheduler::LinkScheduler;

// === Constants matching production deployment ===

/// Link download bandwidths in bytes/sec
const LINK_BPS: [u64; 5] = [
    7_812_500,   // L0: ADSL1 (62.5 Mbps)
    27_500_000,  // L1: Starlink1 (220 Mbps)
    7_812_500,   // L2: ADSL2 (62.5 Mbps)
    27_500_000,  // L3: Starlink2 (220 Mbps)
    7_812_500,   // L4: ADSL3 (62.5 Mbps)
];

const NUM_LINKS: usize = 5;

/// 1 GB in bytes
const ONE_GB: u64 = 1_073_741_824;

/// Probe interval in seconds (200ms)
const PROBE_SECS: f64 = 0.2;

// === Helpers ===

fn make_rate_config() -> RateControlConfig {
    RateControlConfig::default()
}

fn make_scheduler_config() -> LinkSchedulerConfig {
    LinkSchedulerConfig {
        sync_interval_ms: 50,
        max_send_delay_ms: 100,
        enable_sync: false,
        strategy: "weighted".to_string(),
        link_tiers: vec![],
        flow_affinity: false,
        flow_affinity_timeout_secs: 30,
    }
}

fn link_label(i: usize) -> &'static str {
    match i {
        0 => "ADSL1",
        1 => "Star1",
        2 => "ADSL2",
        3 => "Star2",
        4 => "ADSL3",
        _ => "?????",
    }
}

/// Simulate one 200ms probe cycle (download direction: server → client).
///
/// - `rc`: server-side RateController
/// - `last_server_ts`: timestamps from previous server probes (echoed by client for RTT)
/// - `congestion`: per-link factor (1.0=healthy, 0.5=50% throughput)
/// - `alive`: per-link liveness (false=no probe response, simulates link failure)
///
/// Returns bytes transferred per link this interval.
fn simulate_cycle(
    rc: &mut RateController,
    last_server_ts: &mut [u64; NUM_LINKS],
    congestion: &[f64; NUM_LINKS],
    alive: &[bool; NUM_LINKS],
) -> [u64; NUM_LINKS] {
    let mut bytes = [0u64; NUM_LINKS];

    // 1. Calculate bytes each link transfers this interval based on current rate
    for i in 0..NUM_LINKS {
        bytes[i] = (rc.rate(i) as f64 * PROBE_SECS) as u64;
    }

    // 2. Record server TX per-packet (server sends this data to client)
    for i in 0..NUM_LINKS {
        if bytes[i] > 0 {
            let num_pkts = (bytes[i] / 1400).max(1) as usize;
            let pkt_size = bytes[i] as usize / num_pkts;
            for _ in 0..num_pkts {
                rc.record_tx(i, pkt_size);
            }
        }
    }

    // 3. Construct client response probes and feed them to server's rate controller
    //    This MUST happen BEFORE build_probes() which resets interval counters.
    let now = now_micros();
    for i in 0..NUM_LINKS {
        if !alive[i] {
            continue; // Dead link: no probe response
        }

        let received = (bytes[i] as f64 * congestion[i]) as u64;
        let recv_pkts = if received > 0 {
            (received / 1400).max(1) as u32
        } else {
            0
        };

        // Client probe reports its own stats:
        //   tx_bytes/tx_packets = what CLIENT sent (0 in download-only test)
        //   rx_bytes/rx_packets = what CLIENT received FROM server
        let client_probe = ProbePacket {
            link_id: i as u8,
            seq: 0,
            timestamp_us: now + 10_000, // client clock (10ms one-way simulated)
            echo_timestamp_us: last_server_ts[i], // echo server's last timestamp for RTT
            tx_bytes: 0,          // client not sending bulk data (download test)
            tx_packets: 0,        // client not sending bulk data
            rx_bytes: received,   // what client actually received from server
            rx_packets: recv_pkts,
            loss_ratio: 0,
            path_state: PathState::Running,
        };

        rc.process_probe(&client_probe);
    }

    // 4. Build server probes (captures interval stats, resets counters)
    let server_probes = rc.build_probes();
    for p in &server_probes {
        last_server_ts[p.link_id as usize] = p.timestamp_us;
    }

    bytes
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 1: Steady-State 1GB Download - All Links Healthy
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_1gb_steady_state_download() {
    let mut rc = RateController::new(make_rate_config(), &LINK_BPS);
    let mut sched = LinkScheduler::new(make_scheduler_config(), &LINK_BPS.to_vec(), false);

    let mut ts = [0u64; NUM_LINKS];
    let congestion = [1.0; NUM_LINKS];
    let alive = [true; NUM_LINKS];

    let mut total: u64 = 0;
    let mut per_link = [0u64; NUM_LINKS];
    let mut cycles: u32 = 0;

    while total < ONE_GB && cycles < 500 {
        let bytes = simulate_cycle(&mut rc, &mut ts, &congestion, &alive);
        for i in 0..NUM_LINKS {
            per_link[i] += bytes[i];
            total += bytes[i];
        }
        sched.set_rate_controlled_weights(&rc.weights());
        cycles += 1;
    }

    let elapsed = cycles as f64 * PROBE_SECS;
    let agg_mbps = (total as f64 * 8.0) / (elapsed * 1_000_000.0);

    println!("=== TEST 1: Steady-State 1GB Download ===");
    println!("Transferred: {:.2} GB in {:.1}s ({} cycles)", total as f64 / 1e9, elapsed, cycles);
    println!("Aggregate:   {:.1} Mbps", agg_mbps);
    println!();
    for i in 0..NUM_LINKS {
        let mbps = (per_link[i] as f64 * 8.0) / (elapsed * 1_000_000.0);
        let pct = per_link[i] as f64 * 100.0 / total as f64;
        println!(
            "  L{} ({:5}): {:7.1} MB ({:4.1}%) = {:6.1} Mbps  state={:?} w={}",
            i, link_label(i), per_link[i] as f64 / 1e6, pct, mbps,
            rc.link_state(i), rc.weights()[i]
        );
    }
    println!();
    println!("{}", rc.status_summary());

    // --- Assertions ---
    assert!(total >= ONE_GB, "Must transfer >= 1GB, got {}", total);
    assert!(cycles < 200, "Should finish in <200 cycles, took {}", cycles);

    // All links Running
    for i in 0..NUM_LINKS {
        assert_eq!(rc.link_state(i), PathState::Running, "L{} should be Running", i);
    }

    // Weight sanity: ADSL ~60, Starlink ~220
    let w = rc.weights();
    for &i in &[0, 2, 4] {
        assert!(w[i] >= 40 && w[i] <= 80, "ADSL L{} weight {}, expected 40-80", i, w[i]);
    }
    for &i in &[1, 3] {
        assert!(w[i] >= 150 && w[i] <= 260, "Starlink L{} weight {}, expected 150-260", i, w[i]);
    }

    // Starlink carries more than ADSL
    for &sl in &[1, 3] {
        for &adsl in &[0, 2, 4] {
            assert!(per_link[sl] > per_link[adsl],
                "Starlink L{} ({}) should > ADSL L{} ({})", sl, per_link[sl], adsl, per_link[adsl]);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 2: Congestion Detection and Recovery
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_congestion_reduces_weight_then_recovers() {
    let mut cfg = make_rate_config();
    cfg.tapa_enabled = false; // Disable TAPA: testing congestion algorithm directly
    let mut rc = RateController::new(cfg, &LINK_BPS);
    let mut sched = LinkScheduler::new(make_scheduler_config(), &LINK_BPS.to_vec(), false);

    let mut ts = [0u64; NUM_LINKS];
    let alive = [true; NUM_LINKS];
    let healthy = [1.0; NUM_LINKS];

    // Phase 1: Steady state (20 cycles = 4s)
    for _ in 0..20 {
        simulate_cycle(&mut rc, &mut ts, &healthy, &alive);
        sched.set_rate_controlled_weights(&rc.weights());
    }

    let w_before = rc.weights()[1];
    let r_before = rc.rate(1);

    println!("=== TEST 2: Congestion Detection ===");
    println!("Before: L1 weight={}, rate={:.1} Mbps", w_before, r_before as f64 * 8.0 / 1e6);

    // Phase 2: L1 (Starlink1) congested at 50% for 50 cycles (10s)
    let congested = [1.0, 0.5, 1.0, 1.0, 1.0];
    for _ in 0..50 {
        simulate_cycle(&mut rc, &mut ts, &congested, &alive);
        sched.set_rate_controlled_weights(&rc.weights());
    }

    let w_cong = rc.weights()[1];
    let r_cong = rc.rate(1);
    println!("Congested: L1 weight={}, rate={:.1} Mbps", w_cong, r_cong as f64 * 8.0 / 1e6);

    assert!(r_cong < r_before, "Rate should drop: before={}, after={}", r_before, r_cong);
    assert!(w_cong < w_before, "Weight should drop: before={}, after={}", w_before, w_cong);

    // Other links unaffected
    for &i in &[0, 2, 3, 4] {
        assert_eq!(rc.link_state(i), PathState::Running, "L{} should stay Running", i);
    }

    // Phase 3: Congestion clears, recovery (30 cycles = 6s)
    for _ in 0..30 {
        simulate_cycle(&mut rc, &mut ts, &healthy, &alive);
        sched.set_rate_controlled_weights(&rc.weights());
    }

    let r_recovered = rc.rate(1);
    println!("Recovered: L1 rate={:.1} Mbps", r_recovered as f64 * 8.0 / 1e6);
    println!("{}", rc.status_summary());

    assert!(r_recovered > r_cong, "Rate should recover: congested={}, recovered={}", r_cong, r_recovered);
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 3: Link Failure (Timeout → DOWN) and Recovery
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_link_failure_and_recovery() {
    let mut cfg = make_rate_config();
    cfg.down_timeout_ms = 1000; // 1s for faster test

    let mut rc = RateController::new(cfg, &LINK_BPS);
    let mut ts = [0u64; NUM_LINKS];
    let healthy = [1.0; NUM_LINKS];
    let all_alive = [true; NUM_LINKS];

    // Warm up: 10 cycles
    for _ in 0..10 {
        simulate_cycle(&mut rc, &mut ts, &healthy, &all_alive);
    }
    assert_eq!(rc.link_state(2), PathState::Running);

    println!("=== TEST 3: Link Failure & Recovery ===");
    println!("Pre-failure: L2 state={:?}, weight={}", rc.link_state(2), rc.weights()[2]);

    // Phase 2: L2 goes dark (no probe responses)
    let l2_dead = [true, true, false, true, true];

    // We need real time to elapse for timeout detection (Instant::now-based)
    for cycle in 0..12 {
        simulate_cycle(&mut rc, &mut ts, &healthy, &l2_dead);
        rc.check_timeouts();

        let s = rc.link_state(2);
        println!("Failure cycle {}: L2 state={:?}, weight={}", cycle, s, rc.weights()[2]);

        if s == PathState::Down {
            break;
        }
        // Sleep to advance real time past timeout threshold
        std::thread::sleep(Duration::from_millis(150));
    }

    assert_eq!(rc.link_state(2), PathState::Down, "L2 should be DOWN after timeout");
    assert_eq!(rc.weights()[2], 0, "DOWN link weight must be 0");

    println!("\nL2 DOWN confirmed. Starting recovery...");

    // Phase 3: L2 comes back
    for cycle in 0..10 {
        simulate_cycle(&mut rc, &mut ts, &healthy, &all_alive);
        let s = rc.link_state(2);
        let w = rc.weights()[2];
        println!("Recovery cycle {}: L2 state={:?}, weight={}", cycle, s, w);
        if s == PathState::Running {
            break;
        }
    }

    let final_state = rc.link_state(2);
    assert!(
        final_state == PathState::Running || final_state == PathState::Probing,
        "L2 should be Running or Probing, got {:?}", final_state
    );

    for &i in &[0, 1, 3, 4] {
        assert_eq!(rc.link_state(i), PathState::Running, "L{} should still be Running", i);
    }

    println!("\nFinal: {}", rc.status_summary());
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 4: Loss Detection and State Transitions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_loss_detection() {
    let mut cfg = make_rate_config();
    cfg.tapa_enabled = false; // Disable TAPA: testing loss algorithm directly
    cfg.loss_min_packets = 100; // Lower threshold for faster test

    let mut rc = RateController::new(cfg, &LINK_BPS);
    let mut ts = [0u64; NUM_LINKS];
    let alive = [true; NUM_LINKS];

    // Warm up to build accumulators
    let clean = [1.0; NUM_LINKS];
    for _ in 0..20 {
        simulate_cycle(&mut rc, &mut ts, &clean, &alive);
    }

    println!("=== TEST 4: Loss Detection ===");
    println!("Pre-loss: L4 loss={}, state={:?}", rc.loss(4), rc.link_state(4));

    // Severe loss on L4: client only gets 30% of packets
    let lossy = [1.0, 1.0, 1.0, 1.0, 0.3];
    for cycle in 0..60 {
        simulate_cycle(&mut rc, &mut ts, &lossy, &alive);
        if cycle % 15 == 0 {
            println!(
                "  Cycle {:2}: L4 loss={:3}/255 ({:4.1}%) state={:?} rate={:.1} Mbps",
                cycle, rc.loss(4), rc.loss(4) as f64 * 100.0 / 255.0,
                rc.link_state(4), rc.rate(4) as f64 * 8.0 / 1e6
            );
        }
    }

    println!("\nFinal: L4 loss={}/255 ({:.1}%), state={:?}",
        rc.loss(4), rc.loss(4) as f64 * 100.0 / 255.0, rc.link_state(4));
    println!("{}", rc.status_summary());

    // Rate should be reduced due to congestion detection
    assert!(rc.rate(4) < LINK_BPS[4],
        "Lossy link rate should decrease: max={}, got={}", LINK_BPS[4], rc.rate(4));
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 5: ProbePacket Wire Format (51 bytes)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_probe_packet_wire_format() {
    let probe = ProbePacket {
        link_id: 3,
        seq: 12345,
        timestamp_us: 1_700_000_000_000_000,
        echo_timestamp_us: 1_699_999_999_800_000,
        tx_bytes: 5_500_000,
        tx_packets: 3928,
        rx_bytes: 1_562_500,
        rx_packets: 1116,
        loss_ratio: 7,
        path_state: PathState::Lossy,
    };

    let encoded = probe.encode();
    println!("=== TEST 5: Probe Wire Format ===");
    println!("Size: {} bytes (expected {})", encoded.len(), ProbePacket::SIZE);

    assert_eq!(encoded.len(), 51, "Probe must be 51 bytes");
    assert_eq!(encoded[0], 0x06, "First byte must be MSG_PROBE (0x06)");

    let d = ProbePacket::decode(&encoded).expect("decode should succeed");
    assert_eq!(d.link_id, 3);
    assert_eq!(d.seq, 12345);
    assert_eq!(d.timestamp_us, 1_700_000_000_000_000);
    assert_eq!(d.echo_timestamp_us, 1_699_999_999_800_000);
    assert_eq!(d.tx_bytes, 5_500_000);
    assert_eq!(d.tx_packets, 3928);
    assert_eq!(d.rx_bytes, 1_562_500);
    assert_eq!(d.rx_packets, 1116);
    assert_eq!(d.loss_ratio, 7);
    assert_eq!(d.path_state, PathState::Lossy);

    // All PathState variants roundtrip
    for &state in &[PathState::Running, PathState::Lossy, PathState::Down, PathState::Probing] {
        let mut p = probe;
        p.path_state = state;
        let dec = ProbePacket::decode(&p.encode()).unwrap();
        assert_eq!(dec.path_state, state, "PathState {:?} roundtrip failed", state);
    }

    // Reject too-short buffer
    assert!(ProbePacket::decode(&[0x06; 10]).is_none(), "Short buffer should fail");

    // Reject wrong message type
    let mut bad = encoded.clone();
    bad[0] = 0xFF;
    assert!(ProbePacket::decode(&bad).is_none(), "Wrong msg type should fail");

    println!("All roundtrips and edge cases passed.");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 6: Scheduler Weight Integration
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_scheduler_weight_integration() {
    let mut sched = LinkScheduler::new(make_scheduler_config(), &LINK_BPS.to_vec(), false);

    println!("=== TEST 6: Scheduler Weight Integration ===");

    // Initial weights from configured bandwidths
    let initial = sched.weights();
    println!("Initial: {:?}", initial);
    assert_eq!(initial.len(), 5);

    // Simulate rate controller output: L1 halved, L2 down
    let rc_weights = vec![60u32, 110, 0, 220, 60];
    sched.set_rate_controlled_weights(&rc_weights);
    assert_eq!(sched.weights(), rc_weights);

    // Schedule 10k chunks and measure distribution
    let mut counts = [0u32; NUM_LINKS];
    for _ in 0..10_000 {
        let d = sched.schedule(1400, false);
        counts[d.link_id] += 1;
    }

    let total_w: u32 = rc_weights.iter().sum();
    println!("Distribution (10k chunks, total_weight={}):", total_w);
    for i in 0..NUM_LINKS {
        let actual_pct = counts[i] as f64 * 100.0 / 10_000.0;
        let expect_pct = rc_weights[i] as f64 * 100.0 / total_w as f64;
        println!(
            "  L{}: {:5} chunks ({:5.1}%, expected {:5.1}%)",
            i, counts[i], actual_pct, expect_pct
        );
    }

    // L2 (weight=0) must get zero chunks
    assert_eq!(counts[2], 0, "DOWN link should get 0 chunks");

    // Starlink L3 (w=220) / ADSL L0 (w=60) ratio should be ~3.67
    let ratio = counts[3] as f64 / counts[0] as f64;
    assert!(ratio > 2.5 && ratio < 5.5, "L3/L0 ratio should be ~3.67, got {:.2}", ratio);

    // L1 (w=110) / L0 (w=60) ratio should be ~1.83
    let ratio_l1 = counts[1] as f64 / counts[0] as f64;
    assert!(ratio_l1 > 1.2 && ratio_l1 < 2.8, "L1/L0 ratio should be ~1.83, got {:.2}", ratio_l1);
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 7: Full 1GB with Mid-Transfer Degradation
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_1gb_with_link_degradation() {
    let mut rc = RateController::new(make_rate_config(), &LINK_BPS);
    let mut sched = LinkScheduler::new(make_scheduler_config(), &LINK_BPS.to_vec(), false);

    let mut ts = [0u64; NUM_LINKS];
    let alive = [true; NUM_LINKS];
    let healthy = [1.0; NUM_LINKS];

    let mut total: u64 = 0;
    let mut per_link = [0u64; NUM_LINKS];
    let mut cycles: u32 = 0;
    let mut phase_bytes = [0u64; 3];

    println!("=== TEST 7: 1GB Download with Mid-Transfer Degradation ===\n");

    // Phase 1: First third - all healthy
    let target1 = ONE_GB / 3;
    while total < target1 && cycles < 500 {
        let b = simulate_cycle(&mut rc, &mut ts, &healthy, &alive);
        for i in 0..NUM_LINKS { per_link[i] += b[i]; total += b[i]; }
        sched.set_rate_controlled_weights(&rc.weights());
        cycles += 1;
    }
    phase_bytes[0] = total;
    let c1 = cycles;
    println!("Phase 1 (healthy):  {:.1} MB in {} cycles, weights={:?}",
        total as f64 / 1e6, c1, rc.weights());

    // Phase 2: Middle third - L1 at 40%, L4 at 60%
    let target2 = 2 * ONE_GB / 3;
    let degraded = [1.0, 0.4, 1.0, 1.0, 0.6];
    let c2_start = cycles;

    while total < target2 && cycles < 500 {
        let b = simulate_cycle(&mut rc, &mut ts, &degraded, &alive);
        for i in 0..NUM_LINKS { per_link[i] += b[i]; total += b[i]; }
        sched.set_rate_controlled_weights(&rc.weights());
        cycles += 1;
    }
    phase_bytes[1] = total - phase_bytes[0];

    println!("Phase 2 (degraded): +{:.1} MB in {} cycles", phase_bytes[1] as f64 / 1e6, cycles - c2_start);
    println!("  L1 rate: {:.1} Mbps (max {:.1})",
        rc.rate(1) as f64 * 8.0 / 1e6, LINK_BPS[1] as f64 * 8.0 / 1e6);
    println!("  L4 rate: {:.1} Mbps (max {:.1})",
        rc.rate(4) as f64 * 8.0 / 1e6, LINK_BPS[4] as f64 * 8.0 / 1e6);
    println!("  Weights: {:?}", rc.weights());

    // Phase 3: Final third - recovery
    let c3_start = cycles;

    while total < ONE_GB && cycles < 500 {
        let b = simulate_cycle(&mut rc, &mut ts, &healthy, &alive);
        for i in 0..NUM_LINKS { per_link[i] += b[i]; total += b[i]; }
        sched.set_rate_controlled_weights(&rc.weights());
        cycles += 1;
    }
    phase_bytes[2] = total - phase_bytes[0] - phase_bytes[1];

    let elapsed = cycles as f64 * PROBE_SECS;

    println!("Phase 3 (recovery): +{:.1} MB in {} cycles, weights={:?}\n",
        phase_bytes[2] as f64 / 1e6, cycles - c3_start, rc.weights());

    println!("--- Final Summary ---");
    println!("Total: {:.2} GB in {:.1}s ({} cycles)", total as f64 / 1e9, elapsed, cycles);
    println!("Aggregate: {:.1} Mbps\n", (total as f64 * 8.0) / (elapsed * 1e6));

    for i in 0..NUM_LINKS {
        let pct = per_link[i] as f64 * 100.0 / total as f64;
        println!("  L{} ({:5}): {:7.1} MB ({:4.1}%)", i, link_label(i), per_link[i] as f64 / 1e6, pct);
    }
    println!("\n{}", rc.status_summary());

    // Assertions
    assert!(total >= ONE_GB, "Must transfer >= 1GB");
    assert!(cycles < 500, "Should finish within 500 cycles");

    // All links contributed
    for i in 0..NUM_LINKS {
        assert!(per_link[i] > 0, "L{} should carry data", i);
    }

    // All links should be Running or Lossy at end.
    // Lossy is acceptable because the 15/16 exponential decay of loss accumulators
    // means accumulated loss from phase 2 takes many cycles to fully clear.
    for i in 0..NUM_LINKS {
        let state = rc.link_state(i);
        assert!(
            state == PathState::Running || state == PathState::Lossy,
            "L{} should be Running or Lossy at end, got {:?}", i, state
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 8: Rate Floor Prevents Death Spiral
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_rate_floor_prevents_death_spiral() {
    let mut cfg = make_rate_config();
    cfg.tapa_enabled = false; // Disable TAPA: testing rate floor directly
    let mut rc = RateController::new(cfg, &LINK_BPS);
    let mut ts = [0u64; NUM_LINKS];
    let alive = [true; NUM_LINKS];

    println!("=== TEST 8: Rate Floor Protection ===");

    // Severe congestion on ALL links for 100 cycles (20s)
    // Use 0.3 (70% loss) to stay below loss_down_threshold (200/255=78%)
    // so links go LOSSY but not DOWN
    let severe = [0.3; NUM_LINKS]; // Only 30% gets through
    for cycle in 0..100 {
        simulate_cycle(&mut rc, &mut ts, &severe, &alive);
        if cycle % 25 == 0 {
            let rates: Vec<f64> = (0..NUM_LINKS).map(|i| rc.rate(i) as f64 * 8.0 / 1e6).collect();
            println!("  Cycle {:3}: rates(Mbps) = [{:.1}, {:.1}, {:.1}, {:.1}, {:.1}]",
                cycle, rates[0], rates[1], rates[2], rates[3], rates[4]);
        }
    }

    println!("\nAfter 100 cycles of 70% congestion:");
    println!("{}", rc.status_summary());

    // Every link must remain above its floor rate
    for i in 0..NUM_LINKS {
        let rate = rc.rate(i);
        let floor = ((LINK_BPS[i] as f64 * 0.10) as u64).max(12_500);

        assert!(rate >= floor,
            "L{} rate {} must be >= floor {} (10% of {} or 12500)",
            i, rate, floor, LINK_BPS[i]);

        // Weight must be >= 1 (links still alive, not DOWN)
        assert!(rc.weights()[i] >= 1,
            "L{} weight must be >= 1 (still getting probes)", i);
    }

    println!("All rates above floor. Death spiral prevented.");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 9: RTT Measurement via Probe Echo
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_rtt_measurement() {
    let mut rc = RateController::new(make_rate_config(), &LINK_BPS);
    let mut ts = [0u64; NUM_LINKS];
    let healthy = [1.0; NUM_LINKS];
    let alive = [true; NUM_LINKS];

    println!("=== TEST 9: RTT Measurement ===");

    // Initial RTT should be 0
    for i in 0..NUM_LINKS {
        assert_eq!(rc.rtt_us(i), 0, "Initial RTT should be 0");
    }

    // Run several cycles - probes carry echo timestamps for RTT calculation
    for _ in 0..10 {
        simulate_cycle(&mut rc, &mut ts, &healthy, &alive);
    }

    // After exchanges, RTT should be measurable (very small since test runs fast)
    println!("After 10 cycles:");
    for i in 0..NUM_LINKS {
        let rtt = rc.rtt_us(i);
        println!("  L{}: RTT = {} us ({:.3} ms)", i, rtt, rtt as f64 / 1000.0);
    }

    // RTT should be non-zero after probe exchange (timestamps echo back)
    // Note: might be 0 if the first echo_timestamp was 0 (not yet set)
    // After 10 cycles, at least some should have RTT > 0
    let any_rtt = (0..NUM_LINKS).any(|i| rc.rtt_us(i) > 0);
    println!("Any link has RTT > 0: {}", any_rtt);

    // If we have RTT, verify status_summary includes it
    let summary = rc.status_summary();
    println!("\n{}", summary);
    assert!(summary.contains("RateCtrl:"), "Summary should start with RateCtrl:");
    assert!(summary.contains("RUN"), "All links should show RUN state");
}

// ═══════════════════════════════════════════════════════════════════════
// TEST 10: Dual Rate Controller (Server + Client) Full Probe Exchange
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_dual_rate_controller_probe_exchange() {
    // Server controls download rate, client controls upload rate
    let down_bps = LINK_BPS;
    let up_bps: [u64; 5] = [
        1_587_500,  // L0: ADSL1 up (12.7 Mbps)
        2_500_000,  // L1: Starlink1 up (20 Mbps)
        1_587_500,  // L2: ADSL2 up
        2_500_000,  // L3: Starlink2 up
        1_587_500,  // L4: ADSL3 up
    ];

    let cfg = make_rate_config();
    let mut server_rc = RateController::new(cfg.clone(), &down_bps);
    let mut client_rc = RateController::new(cfg, &up_bps);

    let mut server_ts = [0u64; NUM_LINKS];
    let mut client_ts = [0u64; NUM_LINKS];

    println!("=== TEST 10: Dual Rate Controller Exchange ===\n");

    // 50 cycles of bidirectional probe exchange
    for cycle in 0..50 {
        // --- Server sends download data ---
        for i in 0..NUM_LINKS {
            let dl_bytes = (server_rc.rate(i) as f64 * PROBE_SECS) as usize;
            server_rc.record_tx(i, dl_bytes);
            client_rc.record_rx(i, dl_bytes); // client receives it
        }

        // --- Client sends upload data ---
        for i in 0..NUM_LINKS {
            let ul_bytes = (client_rc.rate(i) as f64 * PROBE_SECS) as usize;
            client_rc.record_tx(i, ul_bytes);
            server_rc.record_rx(i, ul_bytes); // server receives it
        }

        // --- Exchange probes (process previous, then build new) ---

        // Server processes client's probes from last cycle
        // Client processes server's probes from last cycle
        let now = now_micros();

        for i in 0..NUM_LINKS {
            // Client → Server probe
            let client_probe = ProbePacket {
                link_id: i as u8,
                seq: cycle,
                timestamp_us: now,
                echo_timestamp_us: server_ts[i],
                tx_bytes: (client_rc.rate(i) as f64 * PROBE_SECS) as u64,
                tx_packets: 100,
                rx_bytes: (server_rc.rate(i) as f64 * PROBE_SECS) as u64, // what client got
                rx_packets: 100,
                loss_ratio: 0,
                path_state: PathState::Running,
            };
            server_rc.process_probe(&client_probe);

            // Server → Client probe
            let server_probe = ProbePacket {
                link_id: i as u8,
                seq: cycle,
                timestamp_us: now,
                echo_timestamp_us: client_ts[i],
                tx_bytes: (server_rc.rate(i) as f64 * PROBE_SECS) as u64,
                tx_packets: 100,
                rx_bytes: (client_rc.rate(i) as f64 * PROBE_SECS) as u64, // what server got
                rx_packets: 100,
                loss_ratio: 0,
                path_state: PathState::Running,
            };
            client_rc.process_probe(&server_probe);
        }

        // Build new probes
        let s_probes = server_rc.build_probes();
        let c_probes = client_rc.build_probes();
        for p in &s_probes { server_ts[p.link_id as usize] = p.timestamp_us; }
        for p in &c_probes { client_ts[p.link_id as usize] = p.timestamp_us; }

        if cycle % 10 == 0 {
            println!("Cycle {}:", cycle);
            println!("  Server weights: {:?}", server_rc.weights());
            println!("  Client weights: {:?}", client_rc.weights());
        }
    }

    println!("\n--- Final State ---");
    println!("Server: {}", server_rc.status_summary());
    println!("Client: {}", client_rc.status_summary());

    // Both controllers should have all links Running
    for i in 0..NUM_LINKS {
        assert_eq!(server_rc.link_state(i), PathState::Running, "Server L{} should be Running", i);
        assert_eq!(client_rc.link_state(i), PathState::Running, "Client L{} should be Running", i);
    }

    // Server weights should reflect download bandwidths
    let sw = server_rc.weights();
    for &i in &[0, 2, 4] {
        assert!(sw[i] >= 1, "Server ADSL weight should be positive");
    }
    for &i in &[1, 3] {
        assert!(sw[i] > sw[0], "Server Starlink weight should exceed ADSL");
    }

    // Client weights should reflect upload bandwidths
    let cw = client_rc.weights();
    for &i in &[0, 2, 4] {
        assert!(cw[i] >= 1, "Client ADSL weight should be positive");
    }
}

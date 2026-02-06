//! ChainLightning v4 Client
//!
//! Client-side of the multi-WAN bonding system.
//! Runs on the router with multiple WAN connections.
//!
//! Direct UDP mode - binds to each WAN interface, sends to datacenter.
//! This binary only works on Linux due to TUN device requirements.

use std::net::UdpSocket;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn, debug};
use tracing_subscriber::EnvFilter;

use chainlightning_common::{
    ConfigManager, MetricsCollector,
    NUM_LINKS, LINKS, SERVER_PUBLIC_IP, TUN_NAME, TUN_SERVER_IP, TUN_CLIENT_IP, TUN_MTU,
    protocol::{Packet, ChunkPacket, AckPacket, KeepalivePacket, now_micros},
};
use chainlightning_core::{
    FlowClassifier, FlowMode,
    ChunkAggregator, Chunk,
    LinkScheduler,
    RateController,
    ChunkReceiver,
    StatsCollector,
    ThroughputOptimizer, OptimizerSnapshot,
    tun::{TunDevice, configure_tun, delete_tun},
};
use chainlightning_testing::{TestSuite, create_standard_tests};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("chainlightning=info".parse()?)
            .add_directive("info".parse()?))
        .init();

    info!("ChainLightning v4 Client starting...");

    // Load or create configuration
    let config_manager = ConfigManager::new();
    let config = config_manager.get();

    info!("Configuration loaded:");
    info!("  Flow classifier: single_link={:.0}%, multi_link={:.0}%",
        config.flow_classifier.single_link_threshold * 100.0,
        config.flow_classifier.multi_link_threshold * 100.0);
    info!("  Chunk size: {}-{} bytes, BDP mult={:.1}",
        config.chunk_aggregator.min_chunk_size,
        config.chunk_aggregator.max_chunk_size,
        config.chunk_aggregator.bdp_multiplier);
    info!("  Receiver timeout: {}ms", config.receiver.reorder_timeout_ms);

    // Clean up any existing TUN device
    let _ = delete_tun(TUN_NAME);

    // Create TUN device
    info!("Creating TUN device {}...", TUN_NAME);
    let tun = TunDevice::create(TUN_NAME)?;
    configure_tun(TUN_NAME, TUN_CLIENT_IP, TUN_SERVER_IP, TUN_MTU)?;
    info!("TUN device {} created with IP {}", TUN_NAME, TUN_CLIENT_IP);

    // Create UDP sockets for each link
    // Direct UDP: bind to each WAN interface's local IP, send to datacenter
    let mut sockets = Vec::new();
    for link in LINKS.iter() {
        // Bind to the WAN interface's local IP
        let bind_addr = format!("{}:0", link.client_bind_ip);  // Let OS pick port
        let socket = UdpSocket::bind(&bind_addr)?;
        socket.set_nonblocking(true)?;

        // Set large socket buffers for high-throughput bonding
        #[cfg(target_os = "linux")]
        {
            let buf_size: libc::c_int = 10 * 1024 * 1024; // 10 MB
            unsafe {
                libc::setsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    &buf_size as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
                libc::setsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_SNDBUF,
                    &buf_size as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        // Bind to specific interface to ensure packets go out the right WAN
        #[cfg(target_os = "linux")]
        {
            use std::ffi::CString;
            let iface = CString::new(link.interface).unwrap();
            unsafe {
                let ret = libc::setsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_BINDTODEVICE,
                    iface.as_ptr() as *const libc::c_void,
                    iface.as_bytes_with_nul().len() as libc::socklen_t,
                );
                if ret != 0 {
                    warn!("Link {}: SO_BINDTODEVICE failed for {}: {}",
                        link.id, link.interface, std::io::Error::last_os_error());
                }
            }
        }

        // Connect to datacenter server (for send/recv instead of send_to/recv_from)
        let peer_addr = format!("{}:{}", SERVER_PUBLIC_IP, link.port);
        socket.connect(&peer_addr)?;

        info!("Link {}: {} ({}) -> {} ({})",
            link.id,
            link.client_bind_ip,
            link.interface,
            peer_addr,
            if link.link_type == chainlightning_common::LinkType::Starlink { "Starlink" } else { "ADSL" });
        sockets.push(socket);
    }

    // Initialize components with expected UPLOAD bandwidths (client sends upstream)
    let link_bandwidths: Vec<u64> = LINKS.iter()
        .map(|l| l.expected_bandwidth_up)
        .collect();

    let flow_classifier = Arc::new(Mutex::new(
        FlowClassifier::new(
            config.flow_classifier.clone(),
            config.realtime.clone(),
            config.link_scheduler.link_tiers.clone(),
            link_bandwidths.clone(),
        )
    ));

    let chunk_aggregator = Arc::new(Mutex::new(
        ChunkAggregator::new(config.chunk_aggregator.clone(), NUM_LINKS)
    ));

    // Scheduler uses expected upload bandwidths for proportional weighting
    // Client sends uploads, so is_upload = true
    let link_scheduler = Arc::new(Mutex::new(
        LinkScheduler::new(config.link_scheduler.clone(), &link_bandwidths, true)
    ));

    info!("Link weights (upload): {:?}", link_bandwidths.iter()
        .map(|&bw| format!("{:.0} Mbps", bw as f64 * 8.0 / 1_000_000.0))
        .collect::<Vec<_>>());

    let chunk_receiver = Arc::new(Mutex::new(
        ChunkReceiver::new(config.receiver.clone())
    ));

    let stats_collector = Arc::new(Mutex::new(
        StatsCollector::new(NUM_LINKS, config.stats.ewma_alpha, config.stats.bandwidth_window_ms)
    ));

    // Lock-free atomic counters for send/receive hot path (avoids stats/rc mutex contention)
    let atomic_counters = stats_collector.lock().unwrap().atomic_counters();

    let metrics_collector = Arc::new(MetricsCollector::new());

    // Initialize rate controller with upload bandwidths
    let rate_controller = Arc::new(Mutex::new(
        RateController::new(config.rate_control.clone(), &link_bandwidths)
    ));
    info!("Rate control: {}", if config.rate_control.enabled { "ENABLED" } else { "DISABLED (static weights)" });

    // Initialize throughput optimizer
    let optimizer = Arc::new(Mutex::new(
        ThroughputOptimizer::new(config.optimizer.clone(), NUM_LINKS)
    ));
    info!("Throughput optimizer: {}", if config.optimizer.enabled { "ENABLED" } else { "DISABLED (opt-in)" });

    // Initialize A/B testing if enabled
    let test_suite = if config.testing.ab_testing_enabled {
        let mut suite = TestSuite::new();
        for test in create_standard_tests() {
            suite.add_test(test);
        }
        Some(Arc::new(Mutex::new(suite)))
    } else {
        None
    };

    info!("All components initialized");
    info!("Client ready - connecting to server...");

    // Per-link sender channels — each link gets its own send queue for parallel sending
    let mut link_txs: Vec<mpsc::Sender<Chunk>> = Vec::new();
    let mut link_rxs: Vec<Option<mpsc::Receiver<Chunk>>> = Vec::new();
    for _ in 0..NUM_LINKS {
        let (tx, rx) = mpsc::channel::<Chunk>(4000);
        link_txs.push(tx);
        link_rxs.push(Some(rx));
    }
    // Channel for reassembled chunks going to TUN writer (large buffer absorbs bursts)
    let (tx_from_links, mut rx_from_links) = mpsc::channel::<(Chunk, FlowMode)>(8000);

    // Spawn TUN reader task (Linux only)
    #[cfg(target_os = "linux")]
    {
        let tun_read_flow_classifier = flow_classifier.clone();
        let tun_read_aggregator = chunk_aggregator.clone();
        let tun_read_scheduler = link_scheduler.clone();
        let tun_read_metrics = metrics_collector.clone();
        let tun_fd = tun.raw_fd();
        let tun_link_txs = link_txs.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 2000];
            loop {
                // Non-blocking read from TUN
                let n = unsafe {
                    libc::read(tun_fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
                };

                if n > 0 {
                    let packet = &buf[..n as usize];

                    // Classify flow — now used for routing decisions
                    let mode = tun_read_flow_classifier.lock().unwrap().classify(packet);

                    // Get flow ID
                    let flow_id = chainlightning_common::protocol::FlowId::from_packet(packet);

                    // Aggregate into chunk (mode-aware: Realtime flushes immediately)
                    let chunk = tun_read_aggregator.lock().unwrap().add_packet(packet, flow_id, mode);
                    if let Some(chunk) = chunk {
                        // Schedule here (1 lock) and route to per-link channel
                        let decision = tun_read_scheduler.lock().unwrap().schedule(chunk.size(), &chunk.flow_mode);
                        let _ = tun_link_txs[decision.link_id].send(chunk).await;
                    }

                    // Record metrics
                    tun_read_metrics.record_packet_tx(n as usize);
                } else {
                    // No data, brief sleep
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            }
        });
    }

    // Spawn TUN writer task (Linux only)
    #[cfg(target_os = "linux")]
    {
        let tun_write_metrics = metrics_collector.clone();
        let tun_fd = tun.raw_fd();

        tokio::spawn(async move {
            let mut rx = rx_from_links;
            while let Some((chunk, _flow_mode)) = rx.recv().await {
                // Extract packets from chunk and write to TUN
                for packet in chunk.extract_packets() {
                    unsafe {
                        libc::write(tun_fd, packet.as_ptr() as *const libc::c_void, packet.len());
                    }
                    tun_write_metrics.record_packet_rx(packet.len());
                }
            }
        });
    }

    // Spawn link receiver tasks (one per link)
    for (link_id, socket) in sockets.iter().enumerate() {
        let socket = socket.try_clone()?;
        let receiver = chunk_receiver.clone();
        let counters = atomic_counters.clone();
        let metrics = metrics_collector.clone();
        let stats = stats_collector.clone();
        let rc = rate_controller.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 65536];
            loop {
                // Using recv() since socket is connected
                match socket.recv(&mut buf) {
                    Ok(n) => {
                        if let Some(packet) = Packet::decode(&buf[..n]) {
                            match packet {
                                Packet::Data(chunk_pkt) => {
                                    // ACK FIRST — before any locks or processing.
                                    // Delayed ACKs cause rate controller false-loss → death spiral.
                                    let ack = AckPacket {
                                        chunk_id: chunk_pkt.chunk_id,
                                        link_id: link_id as u8,
                                        echo_timestamp_us: 0,
                                        recv_timestamp_us: now_micros(),
                                    };
                                    let _ = socket.send(&ack.encode());

                                    // Record stats (atomic — no mutex)
                                    counters.record_rx(link_id, n);
                                    metrics.record_chunk_rx(chunk_pkt.payload.len());

                                    // Decode flow mode from wire
                                    let flow_mode = FlowMode::from_wire(chunk_pkt.flow_mode, link_id);

                                    // Convert to Chunk and store in receiver
                                    let mut chunk = Chunk::new(chunk_pkt.chunk_id);
                                    chunk.data = chunk_pkt.payload;
                                    chunk.flow_mode = flow_mode;

                                    // Store only — drain happens in dedicated task
                                    receiver.lock().unwrap().handle_chunk(chunk, flow_mode);
                                }
                                Packet::Announce(ann) => {
                                    receiver.lock().unwrap().handle_announcement(ann);
                                }
                                Packet::Ack(ack) => {
                                    // Calculate RTT
                                    let now = now_micros();
                                    if ack.echo_timestamp_us > 0 && now > ack.echo_timestamp_us {
                                        let rtt = now - ack.echo_timestamp_us;
                                        stats.lock().unwrap().record_rtt(link_id, rtt);
                                    }
                                }
                                Packet::Probe(probe) => {
                                    // Process probe for rate control
                                    rc.lock().unwrap().process_probe(&probe);
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        tokio::time::sleep(Duration::from_micros(100)).await;
                    }
                    Err(e) => {
                        warn!("Link {} recv error: {}", link_id, e);
                    }
                }
            }
        });
    }

    // Spawn receiver drain task — decoupled from link receivers to prevent ACK delays.
    // Link receivers now only store chunks; this task drains them to the TUN writer.
    {
        let drain_receiver = chunk_receiver.clone();
        let drain_tx = tx_from_links.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_micros(100));
            loop {
                interval.tick().await;
                let ready = drain_receiver.lock().unwrap().drain();
                for chunk in ready {
                    let mode = chunk.flow_mode;
                    let _ = drain_tx.send((chunk, mode)).await;
                }
            }
        });
    }

    // Spawn per-link sender tasks — one sender per link for parallel sending
    // Scheduling already happened in TUN reader; each sender just encodes and sends
    // Uses atomic counters instead of stats/rc mutex locks on hot path
    for link_id in 0..NUM_LINKS {
        let socket = sockets[link_id].try_clone()?;
        let mut rx = link_rxs[link_id].take().expect("link_rx already taken");
        let counters = atomic_counters.clone();
        let metrics = metrics_collector.clone();

        tokio::spawn(async move {
            while let Some(chunk) = rx.recv().await {
                // Encode chunk — move data instead of clone
                let chunk_size = chunk.size();
                let flow_wire = chunk.flow_mode.to_wire();
                let chunk_pkt = ChunkPacket {
                    chunk_id: chunk.id,
                    link_id: link_id as u8,
                    flow_mode: flow_wire,
                    total_size: chunk_size as u32,
                    offset: 0,
                    payload: chunk.data,
                };
                let encoded = chunk_pkt.encode();

                // Send on connected socket
                if let Err(e) = socket.send(&encoded) {
                    warn!("Link {} send error: {}", link_id, e);
                } else {
                    // Atomic — no mutex, no contention
                    counters.record_tx(link_id, encoded.len());
                    metrics.record_chunk_tx(chunk_size);
                }
            }
        });
    }

    // Spawn stats update task
    let stats_updater = stats_collector.clone();
    let metrics_updater = metrics_collector.clone();
    let scheduler_updater = link_scheduler.clone();
    let aggregator_updater = chunk_aggregator.clone();
    let classifier_updater = flow_classifier.clone();
    let rc_updater = rate_controller.clone();
    let receiver_updater = chunk_receiver.clone();
    let optimizer_updater = optimizer.clone();
    let rc_enabled = config.rate_control.enabled;
    let optimizer_enabled = config.optimizer.enabled;

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut log_counter = 0u32;
        loop {
            interval.tick().await;
            log_counter += 1;

            // Update stats — tick() drains atomics and returns deltas for rc
            let deltas = stats_updater.lock().unwrap().tick();

            // Fold deltas into rate controller
            {
                let mut rc = rc_updater.lock().unwrap();
                for (i, &(tx_bytes, rx_bytes)) in deltas.iter().enumerate() {
                    if tx_bytes > 0 { rc.record_tx(i, tx_bytes as usize); }
                    if rx_bytes > 0 { rc.record_rx(i, rx_bytes as usize); }
                }
            }

            // Get current stats
            let stats = stats_updater.lock().unwrap();
            let bandwidths = stats.bandwidths();
            let rtts = stats.rtts();
            let _total_bw = stats.total_bandwidth();
            let health = stats.health();

            info!("{}", stats.summary());

            // Use rate controller RTTs (from probes) — stats collector RTTs may be zeros
            let rc_rtts: Vec<u64> = if rc_enabled {
                let rc = rc_updater.lock().unwrap();
                (0..bandwidths.len()).map(|i| rc.rtt_us(i)).collect()
            } else {
                rtts.clone()
            };

            // Update scheduler with bandwidths and rate-controller RTTs
            drop(stats);
            for (i, &bw) in bandwidths.iter().enumerate() {
                let rtt = rc_rtts.get(i).copied().unwrap_or(0);
                scheduler_updater.lock().unwrap().update_link(i, bw, rtt, true);
                aggregator_updater.lock().unwrap().update_link_stats(i, bw, (rtt / 1000) as u32);
            }

            // Update flow classifier RTTs (for realtime link selection).
            // NOTE: We do NOT call fc.update_bandwidths() here. The classifier
            // uses configured link CAPACITIES (from config) for flow hashing and
            // Bulk detection, not measured throughput from stats.
            {
                let mut fc = classifier_updater.lock().unwrap();
                fc.update_rtts(rc_rtts.clone());

                // Log flow classification stats
                let fc_stats = fc.stats();
                if fc_stats.total_flows > 0 {
                    info!("Flows: {} total (RT={}, SL={}, Bulk={}) rt_link=L{} fast_link=L{}",
                        fc_stats.total_flows, fc_stats.realtime_flows,
                        fc_stats.single_link_flows, fc_stats.bulk_flows,
                        fc_stats.realtime_link, fc_stats.fastest_link);
                }

                // Expire old flows periodically
                fc.expire_flows();
            }

            // Rate control: check timeouts and push weights
            if rc_enabled {
                let mut rc = rc_updater.lock().unwrap();
                rc.check_timeouts();
                let weights = rc.weights();
                drop(rc);
                scheduler_updater.lock().unwrap().set_rate_controlled_weights(&weights);
            }

            // Throughput optimizer: perturb weight factors, measure, accept/revert
            if optimizer_enabled {
                let per_link_loss: Vec<f64> = if rc_enabled {
                    let rc = rc_updater.lock().unwrap();
                    (0..NUM_LINKS).map(|i| rc.loss(i) as f64 / 255.0).collect()
                } else {
                    vec![0.0; NUM_LINKS]
                };

                let fc_stats = classifier_updater.lock().unwrap().stats();
                let opt_snapshot = OptimizerSnapshot {
                    per_link_bandwidth_bps: bandwidths.clone(),
                    per_link_loss_ratio: per_link_loss,
                    per_link_healthy: health.clone(),
                    active_flow_count: fc_stats.total_flows,
                };

                if let Some(new_factors) = optimizer_updater.lock().unwrap().tick(opt_snapshot) {
                    classifier_updater.lock().unwrap().set_weight_factors(new_factors);
                }
            }

            // Dynamic reorder timeout: 1.5x the one-way RTT spread of bulk-eligible links
            {
                let spread_ms = scheduler_updater.lock().unwrap().bulk_rtt_spread_ms();
                if spread_ms > 0 {
                    let timeout_ms = (spread_ms * 3 / 2).max(5).min(200);
                    receiver_updater.lock().unwrap().set_reorder_timeout_ms(timeout_ms);
                }
            }

            // Log status every 5 seconds
            if log_counter % 5 == 0 {
                let scheduler = scheduler_updater.lock().unwrap();
                info!("Scheduler: {}", scheduler.status_summary());
                drop(scheduler);
                if rc_enabled {
                    let rc = rc_updater.lock().unwrap();
                    info!("{}", rc.status_summary());
                }
                if optimizer_enabled {
                    let opt = optimizer_updater.lock().unwrap();
                    info!("{}", opt.status_summary());
                }
            }

            // Take metrics snapshot
            let snapshot = metrics_updater.snapshot();
            debug!(
                "Metrics: {:.1} Mbps down, {:.1} Mbps up, p50={:.1}ms",
                snapshot.throughput_down_mbps,
                snapshot.throughput_up_mbps,
                snapshot.latency_p50_us as f64 / 1000.0
            );
        }
    });

    // Spawn aggregation timeout checker
    let timeout_aggregator = chunk_aggregator.clone();
    let timeout_scheduler = link_scheduler.clone();
    let timeout_link_txs = link_txs;

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        loop {
            interval.tick().await;
            let chunk = timeout_aggregator.lock().unwrap().check_timeout();
            if let Some(chunk) = chunk {
                let decision = timeout_scheduler.lock().unwrap().schedule(chunk.size(), &chunk.flow_mode);
                let _ = timeout_link_txs[decision.link_id].send(chunk).await;
            }
        }
    });

    // Spawn probe sender task (rate control)
    if config.rate_control.enabled {
        let probe_sockets: Vec<_> = sockets.iter().map(|s| s.try_clone().unwrap()).collect();
        let probe_rc = rate_controller.clone();
        let probe_interval = config.rate_control.probe_interval_ms;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(probe_interval));
            loop {
                interval.tick().await;
                let probes = probe_rc.lock().unwrap().build_probes();
                for probe in &probes {
                    let link_id = probe.link_id as usize;
                    if let Some(socket) = probe_sockets.get(link_id) {
                        let _ = socket.send(&probe.encode());
                    }
                }
            }
        });
    }

    // Spawn keepalive task to establish and maintain NAT mappings
    let keepalive_sockets: Vec<_> = sockets.iter().map(|s| s.try_clone().unwrap()).collect();

    tokio::spawn(async move {
        // Send initial keepalives immediately to establish NAT mappings
        info!("Sending initial keepalives to establish NAT mappings...");
        for (link_id, socket) in keepalive_sockets.iter().enumerate() {
            let keepalive = KeepalivePacket {
                link_id: link_id as u8,
                timestamp_us: now_micros(),
            };
            if let Err(e) = socket.send(&keepalive.encode()) {
                warn!("Link {} initial keepalive failed: {}", link_id, e);
            } else {
                debug!("Link {} initial keepalive sent", link_id);
            }
        }

        // Then send periodic keepalives to maintain NAT mappings (every 30 seconds)
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            for (link_id, socket) in keepalive_sockets.iter().enumerate() {
                let keepalive = KeepalivePacket {
                    link_id: link_id as u8,
                    timestamp_us: now_micros(),
                };
                if let Err(e) = socket.send(&keepalive.encode()) {
                    warn!("Link {} keepalive failed: {}", link_id, e);
                }
            }
            debug!("Keepalives sent on all links");
        }
    });

    // Wait forever
    info!("Client running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;

    info!("Shutting down...");
    let _ = delete_tun(TUN_NAME);

    Ok(())
}

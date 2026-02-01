//! Glorytun/MUD-Style Adaptive Rate Controller with TAPA
//!
//! Timing-based congestion detection + loss tracking with exponential decay.
//! Key insight: compare how fast we SEND vs how fast the remote RECEIVES.
//!
//! TAPA (Traffic-Aware Probe Attenuation) adds intelligent probe trust
//! scaling based on traffic load. When links are under heavy data load,
//! probe measurements become unreliable - TAPA detects this and shifts
//! to throughput-based health assessment.
//!
//! Also fixes the directional loss bug: loss is now tracked per-direction
//! (send vs receive) using the correct counter pairs.

use std::time::Instant;
use chainlightning_common::config::RateControlConfig;
use chainlightning_common::protocol::{PathState, ProbePacket, now_micros};

/// Per-link rate state
struct RateState {
    /// Link index
    link_id: usize,

    /// Current allowed rate (bytes/sec) - THE OUTPUT
    tx_rate: u64,
    /// Configured ceiling (bytes/sec)
    tx_max_rate: u64,
    /// Floor: min_rate_fraction * tx_max_rate, NEVER zero
    tx_min_rate: u64,

    /// Effective loss ratio (0-255) = max(send_loss, recv_loss)
    loss: u8,
    /// Loss in our->remote direction (0-255)
    send_loss: u8,
    /// Loss in remote->our direction (0-255)
    recv_loss: u8,

    // === Directional loss accumulators (with 15/16 decay) ===
    /// SENDING dir: what we sent (accumulated from our own tx counts)
    send_tx_acc: u64,
    /// SENDING dir: what remote received from us (from probe.rx_packets)
    send_rx_acc: u64,
    /// RECEIVING dir: what remote sent to us (from probe.tx_packets)
    recv_tx_acc: u64,
    /// RECEIVING dir: what we received (accumulated from our own rx counts)
    recv_rx_acc: u64,

    /// Smoothed RTT (microseconds) - 7:1 EWMA
    srtt_us: u64,
    /// RTT variance (microseconds) - 3:1 EWMA
    rtt_var_us: u64,

    /// Current path state
    state: PathState,

    // === Cumulative counters (never reset) ===
    total_tx_bytes: u64,
    total_tx_packets: u64,
    total_rx_bytes: u64,
    total_rx_packets: u64,

    // === Snapshots for build_probe() delta calculation ===
    last_probe_tx_bytes: u64,
    last_probe_tx_packets: u64,
    last_probe_rx_bytes: u64,
    last_probe_rx_packets: u64,

    // === Snapshots for process_probe() loss calculation ===
    last_loss_tx_packets: u64,
    last_loss_rx_packets: u64,

    /// Probe sequence counter
    probe_seq: u32,
    /// Last timestamp we received from remote (echoed back for RTT)
    last_remote_timestamp_us: u64,
    /// When we received the probe whose timestamp we're echoing (for echo_delay_us)
    last_remote_recv_us: u64,

    /// Timestamp of last sent probe
    last_probe_sent_us: u64,
    /// Timestamp of last received probe
    last_probe_recv: Instant,

    /// Timestamp of last process_probe call (for aligned interval calculation)
    last_process_probe_us: u64,

    /// Recovery probe counter (for DOWN->PROBING->RUNNING)
    recovery_count: u32,

    // === TAPA fields ===
    /// Current probe confidence (0.0 = unreliable, 1.0 = fully trusted)
    probe_confidence: f64,
    /// Previous cycle's probe confidence (for detecting transitions)
    prev_probe_confidence: f64,
    /// Probe confidence when link entered DOWN state (for fast recovery)
    entered_down_confidence: f64,
}

impl RateState {
    fn new(link_id: usize, max_rate: u64, min_rate_fraction: f64) -> Self {
        let min_rate = ((max_rate as f64) * min_rate_fraction) as u64;
        let min_rate = min_rate.max(12_500); // At least 100 Kbps
        Self {
            link_id,
            tx_rate: max_rate,
            tx_max_rate: max_rate,
            tx_min_rate: min_rate,
            loss: 0,
            send_loss: 0,
            recv_loss: 0,
            send_tx_acc: 0,
            send_rx_acc: 0,
            recv_tx_acc: 0,
            recv_rx_acc: 0,
            srtt_us: 0,
            rtt_var_us: 0,
            state: PathState::Running,
            total_tx_bytes: 0,
            total_tx_packets: 0,
            total_rx_bytes: 0,
            total_rx_packets: 0,
            last_probe_tx_bytes: 0,
            last_probe_tx_packets: 0,
            last_probe_rx_bytes: 0,
            last_probe_rx_packets: 0,
            last_loss_tx_packets: 0,
            last_loss_rx_packets: 0,
            probe_seq: 0,
            last_remote_timestamp_us: 0,
            last_remote_recv_us: 0,
            last_probe_sent_us: 0,
            last_probe_recv: Instant::now(),
            last_process_probe_us: 0,
            recovery_count: 0,
            probe_confidence: 1.0,
            prev_probe_confidence: 1.0,
            entered_down_confidence: 1.0,
        }
    }

    /// Compute weight from current rate and state
    fn weight(&self) -> u32 {
        match self.state {
            PathState::Down | PathState::Probing => 0,
            _ => (self.tx_rate / 125_000).max(1) as u32, // Rate in ~Mbps units
        }
    }

    /// Build outgoing probe packet using delta from cumulative counters
    fn build_probe(&mut self) -> ProbePacket {
        self.probe_seq = self.probe_seq.wrapping_add(1);
        let now = now_micros();
        self.last_probe_sent_us = now;

        // Compute deltas since last probe
        let tx_bytes = self.total_tx_bytes - self.last_probe_tx_bytes;
        let tx_packets = (self.total_tx_packets - self.last_probe_tx_packets) as u32;
        let rx_bytes = self.total_rx_bytes - self.last_probe_rx_bytes;
        let rx_packets = (self.total_rx_packets - self.last_probe_rx_packets) as u32;

        // Update snapshots
        self.last_probe_tx_bytes = self.total_tx_bytes;
        self.last_probe_tx_packets = self.total_tx_packets;
        self.last_probe_rx_bytes = self.total_rx_bytes;
        self.last_probe_rx_packets = self.total_rx_packets;

        // How long we held the echoed timestamp before sending this probe
        let echo_delay = if self.last_remote_recv_us > 0 {
            now.saturating_sub(self.last_remote_recv_us)
        } else {
            0
        };

        ProbePacket {
            link_id: self.link_id as u8,
            seq: self.probe_seq,
            timestamp_us: now,
            echo_timestamp_us: self.last_remote_timestamp_us,
            tx_bytes,
            tx_packets,
            rx_bytes,
            rx_packets,
            loss_ratio: self.loss,
            path_state: self.state,
            echo_delay_us: echo_delay,
        }
    }

    /// Calculate traffic load as fraction of capacity (0.0 - 1.0+)
    fn traffic_load(&self, probe_interval_us: u64) -> f64 {
        if probe_interval_us == 0 || self.tx_max_rate == 0 {
            return 0.0;
        }
        // Bytes in last probe interval
        let tx_delta = self.total_tx_bytes - self.last_probe_tx_bytes;
        let rx_delta = self.total_rx_bytes - self.last_probe_rx_bytes;
        let max_delta = tx_delta.max(rx_delta);

        // Convert to rate: bytes_in_interval * 1_000_000 / interval_us
        let rate = max_delta * 1_000_000 / probe_interval_us.max(1);
        rate as f64 / self.tx_max_rate as f64
    }

    fn status_label(&self) -> &'static str {
        match self.state {
            PathState::Running => "RUN",
            PathState::Lossy => "LOSSY",
            PathState::Down => "DOWN",
            PathState::Probing => "PROBE",
        }
    }
}

/// Rate Controller managing all links
pub struct RateController {
    states: Vec<RateState>,
    config: RateControlConfig,
}

impl RateController {
    /// Create rate controller with configured max rates (bytes/sec) per link
    pub fn new(config: RateControlConfig, max_rates: &[u64]) -> Self {
        let states = max_rates
            .iter()
            .enumerate()
            .map(|(id, &rate)| RateState::new(id, rate, config.min_rate_fraction))
            .collect();
        Self { states, config }
    }

    /// Is rate control enabled?
    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get current weights for all links
    pub fn weights(&self) -> Vec<u32> {
        self.states.iter().map(|s| s.weight()).collect()
    }

    /// Record bytes transmitted on a link (called from chunk sender)
    pub fn record_tx(&mut self, link_id: usize, bytes: usize) {
        if let Some(state) = self.states.get_mut(link_id) {
            state.total_tx_bytes += bytes as u64;
            state.total_tx_packets += 1;
        }
    }

    /// Record bytes received on a link (called from link receiver)
    pub fn record_rx(&mut self, link_id: usize, bytes: usize) {
        if let Some(state) = self.states.get_mut(link_id) {
            state.total_rx_bytes += bytes as u64;
            state.total_rx_packets += 1;
        }
    }

    /// Build probe packets for all links (called every probe_interval_ms)
    pub fn build_probes(&mut self) -> Vec<ProbePacket> {
        self.states.iter_mut().map(|s| s.build_probe()).collect()
    }

    /// Calculate probe confidence for a link based on traffic load
    fn calculate_probe_confidence(&self, link_id: usize) -> f64 {
        if !self.config.tapa_enabled {
            return 1.0;
        }
        let state = match self.states.get(link_id) {
            Some(s) => s,
            None => return 1.0,
        };

        let probe_interval_us = self.config.probe_interval_ms * 1000;
        let load = state.traffic_load(probe_interval_us);

        let idle = self.config.tapa_idle_threshold;
        let heavy = self.config.tapa_heavy_threshold;
        let min_conf = self.config.tapa_min_confidence;

        if load <= idle {
            1.0
        } else if load >= heavy {
            min_conf
        } else {
            // Linear interpolation: 1.0 at idle, min_conf at heavy
            let range = heavy - idle;
            let t = (load - idle) / range;
            1.0 - t * (1.0 - min_conf)
        }
    }

    /// Check if all links have phantom loss (cross-link correlation)
    fn all_links_phantom_loss(&self) -> bool {
        if !self.config.tapa_enabled || !self.config.tapa_cross_link_check {
            return false;
        }
        let link_count = self.states.len();
        if link_count < 2 {
            return false;
        }

        let high_loss_count = self.states.iter()
            .filter(|s| s.loss > self.config.loss_threshold)
            .count();
        let heavy_traffic_count = self.states.iter()
            .filter(|s| s.probe_confidence < 0.3)
            .count();

        // If >80% of links show high loss AND >80% have heavy traffic
        high_loss_count * 5 > link_count * 4 && heavy_traffic_count * 5 > link_count * 4
    }

    /// Process received probe from remote side
    /// This is the CORE ALGORITHM - called when we receive a probe packet
    pub fn process_probe(&mut self, probe: &ProbePacket) {
        let link_id = probe.link_id as usize;

        // Calculate probe confidence BEFORE mutating state
        let confidence = self.calculate_probe_confidence(link_id);

        // Check cross-link correlation
        let suppress_loss_transitions = self.all_links_phantom_loss();

        let state = match self.states.get_mut(link_id) {
            Some(s) => s,
            None => return,
        };

        let now = now_micros();
        state.last_probe_recv = Instant::now();
        state.last_remote_timestamp_us = probe.timestamp_us;
        state.last_remote_recv_us = now;

        // Update TAPA confidence
        state.prev_probe_confidence = state.probe_confidence;
        state.probe_confidence = confidence;

        // === 1. RTT measurement (unaffected by TAPA) ===
        // Subtract echo_delay_us: the time the remote held our timestamp before echoing it.
        // This removes the scheduling delay from the measured RTT.
        if probe.echo_timestamp_us > 0 && now > probe.echo_timestamp_us {
            let rtt = (now - probe.echo_timestamp_us).saturating_sub(probe.echo_delay_us);
            if state.srtt_us == 0 {
                state.srtt_us = rtt;
                state.rtt_var_us = rtt / 2;
            } else {
                let alpha = self.config.rtt_ewma_alpha;
                state.srtt_us = ((alpha * rtt as f64) + ((1.0 - alpha) * state.srtt_us as f64)) as u64;
                let var_alpha = self.config.rtt_var_alpha;
                let diff = if rtt > state.srtt_us { rtt - state.srtt_us } else { state.srtt_us - rtt };
                state.rtt_var_us = ((var_alpha * diff as f64) + ((1.0 - var_alpha) * state.rtt_var_us as f64)) as u64;
            }
        }

        // === 2. Congestion Detection (with TAPA) ===
        let tx_dt = self.config.probe_interval_ms * 1000; // interval in microseconds

        if confidence > 0.5 || !self.config.tapa_enabled {
            // Normal probe-based congestion detection
            let remote_rx_bytes = probe.rx_bytes;

            // Compute our ACTUAL TX bytes in current interval (from cumulative counters)
            // CRITICAL: Only use real data delta. When delta==0, no data was sent,
            // so congestion detection is meaningless. Previously, falling back to
            // state.tx_rate caused false congestion detection on idle links because
            // the "allowed rate" was compared against tiny probe-only rx_bytes.
            let tx_delta = state.total_tx_bytes.saturating_sub(state.last_probe_tx_bytes);
            let our_tx_rate = if tx_dt > 0 && tx_delta > 0 {
                tx_delta * 1_000_000 / tx_dt
            } else {
                0 // No actual data sent → cannot measure congestion
            };

            let remote_rx_rate = if tx_dt > 0 {
                remote_rx_bytes * 1_000_000 / tx_dt
            } else {
                0
            };

            if our_tx_rate > 0 && remote_rx_rate > 0 {
                let threshold = (our_tx_rate as f64 * self.config.congestion_threshold) as u64;
                if remote_rx_rate + threshold < our_tx_rate {
                    let new_rate = (remote_rx_rate as f64 * self.config.congestion_reduction) as u64;
                    apply_rate_change(state, new_rate, &self.config);
                } else {
                    let growth = (state.tx_rate as f64 * self.config.growth_factor) as u64;
                    let new_rate = state.tx_rate.saturating_add(growth);
                    apply_rate_change(state, new_rate, &self.config);
                }
            } else {
                // No data traffic (or one-sided) - grow towards max
                let growth = (state.tx_rate as f64 * self.config.growth_factor) as u64;
                let new_rate = state.tx_rate.saturating_add(growth);
                apply_rate_change(state, new_rate, &self.config);
            }
        } else {
            // Low confidence: use throughput-based health instead
            let our_tx_delta = state.total_tx_packets - state.last_loss_tx_packets;
            let our_rx_delta = state.total_rx_packets - state.last_loss_rx_packets;
            if our_tx_delta > 0 || our_rx_delta > 0 {
                // Data is flowing through this link - it's working. Gentle growth.
                let growth = (state.tx_rate as f64 * self.config.growth_factor * 0.5) as u64;
                let new_rate = state.tx_rate.saturating_add(growth);
                apply_rate_change(state, new_rate, &self.config);
            }
            // If no data flowing AND low confidence, hold rate steady
        }

        // === 3. Directional Loss Detection (with time-normalized deltas + TAPA gating) ===
        //
        // KEY FIX: The remote's packet deltas cover [remote_last_build, remote_this_build]
        // (~probe_interval_ms), while our deltas cover [our_last_process, our_this_process]
        // (variable, depends on probe arrival timing). At high throughput this mismatch
        // causes phantom loss. Fix: scale remote's deltas to match our measurement window.

        // Snapshot our own counters
        let our_tx_delta = state.total_tx_packets - state.last_loss_tx_packets;
        let our_rx_delta = state.total_rx_packets - state.last_loss_rx_packets;
        state.last_loss_tx_packets = state.total_tx_packets;
        state.last_loss_rx_packets = state.total_rx_packets;

        // Compute our actual interval since last process_probe (microseconds)
        let our_interval_us = if state.last_process_probe_us > 0 {
            now.saturating_sub(state.last_process_probe_us)
        } else {
            0
        };
        state.last_process_probe_us = now;

        // Remote's nominal probe interval
        let remote_interval_us = self.config.probe_interval_ms * 1000;

        // Gate factor: scale accumulation by probe confidence
        let gate = if self.config.tapa_enabled { confidence } else { 1.0 };

        // Only compute loss when we have valid intervals (skip first probe)
        if our_interval_us > 10_000 && remote_interval_us > 0 {
            // Scale factor: adjust remote's deltas to cover our interval duration
            // e.g., if our interval is 150ms and remote's is 200ms, scale = 0.75
            let scale = (our_interval_us as f64 / remote_interval_us as f64).clamp(0.2, 5.0);

            // SENDING direction (us -> remote):
            //   What we sent: our_tx_delta (covers our_interval)
            //   What remote received from us: probe.rx_packets (covers remote_interval, scale to ours)
            let scaled_remote_rx = (probe.rx_packets as f64 * scale) as u64;
            state.send_tx_acc += (our_tx_delta as f64 * gate) as u64;
            state.send_rx_acc += (scaled_remote_rx as f64 * gate) as u64;

            // RECEIVING direction (remote -> us):
            //   What remote sent to us: probe.tx_packets (covers remote_interval, scale to ours)
            //   What we received: our_rx_delta (covers our_interval)
            let scaled_remote_tx = (probe.tx_packets as f64 * scale) as u64;
            state.recv_tx_acc += (scaled_remote_tx as f64 * gate) as u64;
            state.recv_rx_acc += (our_rx_delta as f64 * gate) as u64;
        }

        // Calculate SENDING direction loss
        if state.send_tx_acc > self.config.loss_min_packets {
            state.send_loss = if state.send_tx_acc > state.send_rx_acc {
                (((state.send_tx_acc - state.send_rx_acc) * 255) / state.send_tx_acc) as u8
            } else {
                0
            };
            // 7/8 decay (faster recovery than 15/16)
            state.send_tx_acc -= state.send_tx_acc / 8;
            state.send_rx_acc -= state.send_rx_acc / 8;
        }

        // Calculate RECEIVING direction loss
        if state.recv_tx_acc > self.config.loss_min_packets {
            state.recv_loss = if state.recv_tx_acc > state.recv_rx_acc {
                (((state.recv_tx_acc - state.recv_rx_acc) * 255) / state.recv_tx_acc) as u8
            } else {
                0
            };
            // 7/8 decay (faster recovery than 15/16)
            state.recv_tx_acc -= state.recv_tx_acc / 8;
            state.recv_rx_acc -= state.recv_rx_acc / 8;
        }

        // Effective loss = worst direction
        state.loss = state.send_loss.max(state.recv_loss);

        // === 4. Accelerated Recovery (TAPA) ===
        if self.config.tapa_enabled && self.config.tapa_accelerated_recovery {
            // When traffic transitions from heavy to light, accelerate loss decay
            if state.prev_probe_confidence < 0.3 && state.probe_confidence > 0.7 {
                let decay = self.config.tapa_recovery_decay;
                state.send_tx_acc = (state.send_tx_acc as f64 * decay) as u64;
                state.send_rx_acc = (state.send_rx_acc as f64 * decay) as u64;
                state.recv_tx_acc = (state.recv_tx_acc as f64 * decay) as u64;
                state.recv_rx_acc = (state.recv_rx_acc as f64 * decay) as u64;
            }
        }

        // === 5. State Transitions (with TAPA damping) ===
        // When confidence is low, require higher thresholds to change state
        let (loss_thresh, down_thresh) = if self.config.tapa_enabled && confidence < 0.5 {
            let mult = self.config.tapa_loss_threshold_multiplier;
            (
                self.config.loss_threshold.saturating_mul(mult).min(200),
                self.config.loss_down_threshold.saturating_add(
                    (255 - self.config.loss_down_threshold) / 2
                ),
            )
        } else {
            (self.config.loss_threshold, self.config.loss_down_threshold)
        };

        // Cross-link suppression: if all links show phantom loss, skip transitions
        let suppress = suppress_loss_transitions;

        match state.state {
            PathState::Running => {
                if !suppress {
                    if state.loss > down_thresh {
                        state.state = PathState::Down;
                        state.recovery_count = 0;
                        state.entered_down_confidence = confidence;
                    } else if state.loss > loss_thresh {
                        state.state = PathState::Lossy;
                    }
                }
            }
            PathState::Lossy => {
                if !suppress {
                    if state.loss > down_thresh {
                        state.state = PathState::Down;
                        state.recovery_count = 0;
                        state.entered_down_confidence = confidence;
                    } else if state.loss <= loss_thresh {
                        state.state = PathState::Running;
                    }
                } else if state.loss <= self.config.loss_threshold {
                    // Even with suppression, allow recovery to Running
                    state.state = PathState::Running;
                }
            }
            PathState::Down => {
                // Got a probe response - start recovery
                if state.entered_down_confidence < 0.3 && self.config.tapa_enabled {
                    // Likely phantom DOWN from heavy traffic - fast-track recovery
                    state.state = PathState::Probing;
                    state.recovery_count = self.config.probe_recovery_count.saturating_sub(1);
                    // Start at 50% of max instead of minimum
                    state.tx_rate = state.tx_max_rate / 2;
                    // Accelerate loss decay
                    state.send_tx_acc = state.send_tx_acc / 2;
                    state.send_rx_acc = state.send_rx_acc / 2;
                    state.recv_tx_acc = state.recv_tx_acc / 2;
                    state.recv_rx_acc = state.recv_rx_acc / 2;
                } else {
                    // Normal recovery path
                    state.state = PathState::Probing;
                    state.recovery_count = 1;
                    state.tx_rate = state.tx_min_rate;
                }
            }
            PathState::Probing => {
                state.recovery_count += 1;
                if state.recovery_count >= self.config.probe_recovery_count {
                    state.state = PathState::Running;
                    // If recovering from phantom DOWN, keep current rate
                    if state.entered_down_confidence >= 0.3 {
                        state.tx_rate = state.tx_min_rate;
                    }
                    // Otherwise keep the tx_rate set during fast recovery
                }
            }
        }
    }

    /// Check for timeouts - call periodically (every 1s)
    pub fn check_timeouts(&mut self) {
        let timeout = std::time::Duration::from_millis(self.config.down_timeout_ms);
        for state in &mut self.states {
            if state.state != PathState::Down && state.last_probe_recv.elapsed() > timeout {
                state.state = PathState::Down;
                state.recovery_count = 0;
                state.entered_down_confidence = state.probe_confidence;
            }
        }
    }

    /// Get human-readable status summary
    pub fn status_summary(&self) -> String {
        let parts: Vec<String> = self.states.iter().map(|s| {
            let rate_mbps = s.tx_rate as f64 * 8.0 / 1_000_000.0;
            let max_mbps = s.tx_max_rate as f64 * 8.0 / 1_000_000.0;
            let rtt_ms = s.srtt_us as f64 / 1000.0;
            let send_loss_pct = s.send_loss as f64 * 100.0 / 255.0;
            let recv_loss_pct = s.recv_loss as f64 * 100.0 / 255.0;
            format!(
                "L{}[{:.0}/{:.0}Mbps|{:.1}ms|SL{:.1}%|RL{:.1}%|c{:.2}|{}|w:{}]",
                s.link_id, rate_mbps, max_mbps, rtt_ms,
                send_loss_pct, recv_loss_pct, s.probe_confidence,
                s.status_label(), s.weight()
            )
        }).collect();
        format!("RateCtrl: {}", parts.join(" "))
    }

    /// Get number of links
    pub fn num_links(&self) -> usize {
        self.states.len()
    }

    /// Get current rate for a link (bytes/sec)
    pub fn rate(&self, link_id: usize) -> u64 {
        self.states.get(link_id).map(|s| s.tx_rate).unwrap_or(0)
    }

    /// Get state for a link
    pub fn link_state(&self, link_id: usize) -> PathState {
        self.states.get(link_id).map(|s| s.state).unwrap_or(PathState::Down)
    }

    /// Get RTT for a link (microseconds)
    pub fn rtt_us(&self, link_id: usize) -> u64 {
        self.states.get(link_id).map(|s| s.srtt_us).unwrap_or(0)
    }

    /// Get loss for a link (0-255)
    pub fn loss(&self, link_id: usize) -> u8 {
        self.states.get(link_id).map(|s| s.loss).unwrap_or(255)
    }
}

/// Apply a rate change with safeguards
fn apply_rate_change(state: &mut RateState, desired_rate: u64, config: &RateControlConfig) {
    let current = state.tx_rate;
    let max_change = (current as f64 * config.max_rate_change_fraction) as u64;
    let max_change = max_change.max(12_500); // At least 100 Kbps change possible

    // Clamp change to +/- max_change
    let new_rate = if desired_rate > current {
        current + (desired_rate - current).min(max_change)
    } else {
        current - (current - desired_rate).min(max_change)
    };

    // Enforce floor and ceiling
    state.tx_rate = new_rate.clamp(state.tx_min_rate, state.tx_max_rate);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> RateControlConfig {
        RateControlConfig::default()
    }

    #[test]
    fn test_initial_state() {
        let config = test_config();
        let rates = vec![7_500_000u64, 27_500_000]; // 60 Mbps, 220 Mbps
        let rc = RateController::new(config, &rates);

        assert_eq!(rc.rate(0), 7_500_000);
        assert_eq!(rc.rate(1), 27_500_000);

        let weights = rc.weights();
        assert_eq!(weights[0], 60);
        assert_eq!(weights[1], 220);

        assert_eq!(rc.link_state(0), PathState::Running);
        assert_eq!(rc.link_state(1), PathState::Running);
    }

    #[test]
    fn test_min_rate_never_zero() {
        let config = test_config();
        let rates = vec![100_000u64];
        let rc = RateController::new(config, &rates);

        assert!(rc.rate(0) > 0);
        let weights = rc.weights();
        assert!(weights[0] >= 1);
    }

    #[test]
    fn test_rate_change_clamping() {
        let config = test_config();
        let mut state = RateState::new(0, 10_000_000, 0.10);

        apply_rate_change(&mut state, 20_000_000, &config);
        assert!(state.tx_rate <= 11_000_000);
        assert!(state.tx_rate >= 10_000_000);

        apply_rate_change(&mut state, 0, &config);
        assert!(state.tx_rate >= state.tx_min_rate);
    }

    #[test]
    fn test_weight_when_down() {
        let config = test_config();
        let rates = vec![7_500_000u64];
        let mut rc = RateController::new(config, &rates);

        rc.states[0].state = PathState::Down;
        assert_eq!(rc.weights()[0], 0);

        rc.states[0].state = PathState::Probing;
        assert_eq!(rc.weights()[0], 0);
    }

    #[test]
    fn test_directional_loss_no_phantom_on_asymmetric_traffic() {
        // KEY TEST: Upload-only traffic should NOT cause phantom loss
        let mut config = test_config();
        config.probe_interval_ms = 20; // Match test sleep cadence (~15ms)
        let rates = vec![7_500_000u64]; // 60 Mbps
        let mut rc = RateController::new(config, &rates);

        // Baseline probe to initialize timing
        let probe0 = ProbePacket {
            link_id: 0, seq: 0, timestamp_us: now_micros() - 400_000,
            echo_timestamp_us: 0, tx_bytes: 0, tx_packets: 0,
            rx_bytes: 0, rx_packets: 0, loss_ratio: 0,
            path_state: PathState::Running, echo_delay_us: 0,
        };
        rc.process_probe(&probe0);

        // Sleep to create valid interval for loss computation
        std::thread::sleep(std::time::Duration::from_millis(15));

        // Simulate heavy upload: lots of TX, minimal RX
        for _ in 0..100 {
            rc.record_tx(0, 10_000); // 10KB per packet, heavy upload
        }
        // Only a few RX (probe responses, ACKs)
        for _ in 0..5 {
            rc.record_rx(0, 51); // probe packets only
        }

        // Remote sends probe: it reports its own TX/RX
        // Remote's TX = what remote sent TO us (small, no download traffic)
        // Remote's RX = what remote received FROM us (should match our TX)
        let probe = ProbePacket {
            link_id: 0,
            seq: 1,
            timestamp_us: now_micros() - 200_000,
            echo_timestamp_us: 0,
            tx_bytes: 500,        // Remote sent very little to us
            tx_packets: 5,        // Remote sent 5 packets
            rx_bytes: 900_000,    // Remote received ~900KB from us (our upload data)
            rx_packets: 95,       // Remote received 95 of our 100 packets
            loss_ratio: 0,
            path_state: PathState::Running,
            echo_delay_us: 0,
        };

        rc.process_probe(&probe);

        // With directional loss:
        // SEND dir: we sent 100 packets, remote received 95 -> 5% loss (acceptable)
        // RECV dir: remote sent 5 packets, we received 5 -> 0% loss
        // Should NOT be in DOWN state
        assert_ne!(rc.link_state(0), PathState::Down,
            "Asymmetric traffic should not cause DOWN state");

        // Loss should be low (send_loss from 100 sent / 95 received = ~5%)
        // With gating from TAPA, even lower
        assert!(rc.loss(0) < 50,
            "Loss should be low for asymmetric traffic, got: {}", rc.loss(0));
    }

    #[test]
    fn test_probe_confidence_idle() {
        let config = test_config();
        let rates = vec![7_500_000u64];
        let rc = RateController::new(config, &rates);

        // No traffic = high confidence
        let conf = rc.calculate_probe_confidence(0);
        assert!((conf - 1.0).abs() < 0.001, "Idle link should have confidence 1.0");
    }

    #[test]
    fn test_probe_confidence_heavy_traffic() {
        let config = test_config();
        let rates = vec![1_000_000u64]; // 8 Mbps
        let mut rc = RateController::new(config, &rates);

        // Build initial probe to set snapshots
        let _ = rc.build_probes();

        // Send 80% of capacity in one probe interval (200ms)
        // 1_000_000 bytes/sec * 0.2 sec * 0.8 = 160_000 bytes
        for _ in 0..160 {
            rc.record_tx(0, 1000);
        }

        let conf = rc.calculate_probe_confidence(0);
        assert!(conf < 0.5, "Heavy traffic should reduce confidence, got: {}", conf);
    }

    #[test]
    fn test_tapa_loss_gating() {
        // When confidence is low, loss accumulation should be slowed
        let mut config = test_config();
        config.loss_min_packets = 10; // Lower threshold for test
        config.probe_interval_ms = 20; // Match test sleep cadence (~15ms)
        let rates = vec![1_000_000u64];
        let mut rc = RateController::new(config, &rates);

        // === Cycle 0: Establish baseline snapshots ===
        // Small initial traffic
        for _ in 0..5 {
            rc.record_tx(0, 100);
        }
        let _ = rc.build_probes();
        // Process a neutral probe to set last_loss_tx_packets and timing baseline
        let probe0 = ProbePacket {
            link_id: 0,
            seq: 0,
            timestamp_us: now_micros() - 200_000,
            echo_timestamp_us: 0,
            tx_bytes: 0,
            tx_packets: 0,
            rx_bytes: 500,
            rx_packets: 5,
            loss_ratio: 0,
            path_state: PathState::Running,
            echo_delay_us: 0,
        };
        rc.process_probe(&probe0);
        assert_eq!(rc.link_state(0), PathState::Running, "Baseline should be Running");

        // Sleep to create valid interval for loss computation
        std::thread::sleep(std::time::Duration::from_millis(15));

        // === Cycle 1: Heavy traffic ===
        let _ = rc.build_probes(); // Reset probe snapshots
        for _ in 0..200 {
            rc.record_tx(0, 5000); // 1MB total - heavy load on 1MB/s link
        }

        // Remote probe: reports receiving most of our 200 packets
        let probe1 = ProbePacket {
            link_id: 0,
            seq: 1,
            timestamp_us: now_micros() - 200_000,
            echo_timestamp_us: 0,
            tx_bytes: 100,
            tx_packets: 2,
            rx_bytes: 950_000,
            rx_packets: 195,  // Received 195 of our 200
            loss_ratio: 0,
            path_state: PathState::Running,
            echo_delay_us: 0,
        };

        rc.process_probe(&probe1);

        // With TAPA: confidence is very low (load >> capacity), so loss is gated
        // Even the small 5/200 gap is scaled down by confidence ~0.1
        // Should remain Running (not falsely go to Lossy/Down)
        assert_eq!(rc.link_state(0), PathState::Running,
            "Heavy traffic with good delivery should stay Running with TAPA gating");
    }

    #[test]
    fn test_cross_link_phantom_detection() {
        let config = test_config();
        let rates = vec![7_500_000u64; 5]; // 5 links
        let mut rc = RateController::new(config, &rates);

        // Set all links to low confidence and high loss
        for state in &mut rc.states {
            state.probe_confidence = 0.2;
            state.loss = 50; // Above threshold
        }

        assert!(rc.all_links_phantom_loss(),
            "All links with high loss + low confidence should be detected as phantom");
    }

    #[test]
    fn test_fast_recovery_from_phantom_down() {
        let config = test_config();
        let rates = vec![7_500_000u64];
        let mut rc = RateController::new(config, &rates);

        // Simulate link going DOWN during low confidence (phantom)
        rc.states[0].state = PathState::Down;
        rc.states[0].entered_down_confidence = 0.1; // Was heavy traffic
        rc.states[0].recovery_count = 0;

        // Receive a probe (triggers recovery)
        let probe = ProbePacket {
            link_id: 0,
            seq: 1,
            timestamp_us: now_micros() - 200_000,
            echo_timestamp_us: 0,
            tx_bytes: 0,
            tx_packets: 0,
            rx_bytes: 0,
            rx_packets: 0,
            loss_ratio: 0,
            path_state: PathState::Running,
            echo_delay_us: 0,
        };

        rc.process_probe(&probe);

        // Should fast-track to near-recovery
        assert_eq!(rc.link_state(0), PathState::Probing);
        // Recovery count should be near the threshold (fast-tracked)
        assert!(rc.states[0].recovery_count >= rc.config.probe_recovery_count - 1,
            "Phantom DOWN should fast-track recovery");
        // Rate should be 50% of max, not minimum
        assert!(rc.states[0].tx_rate > rc.states[0].tx_min_rate,
            "Phantom recovery should start at 50% max, not min");
    }

    #[test]
    fn test_real_loss_still_detected() {
        // Ensure TAPA doesn't mask real loss on idle links
        let mut config = test_config();
        config.loss_min_packets = 50;
        config.probe_interval_ms = 20; // Match test sleep cadence (~15ms)
        let rates = vec![7_500_000u64];
        let mut rc = RateController::new(config, &rates);

        // Simulate real loss on idle link: we send packets, remote receives few
        // Run multiple probe cycles with real intervals for time-normalized loss detection
        for cycle in 0..20 {
            // Sleep to create valid interval between probes
            std::thread::sleep(std::time::Duration::from_millis(15));

            // We send some data each cycle
            for _ in 0..10 {
                rc.record_tx(0, 1000);
            }

            let probe = ProbePacket {
                link_id: 0,
                seq: cycle + 1,
                timestamp_us: now_micros() - 200_000,
                echo_timestamp_us: 0,
                tx_bytes: 0,
                tx_packets: 0,
                rx_bytes: 2000,
                rx_packets: 2,   // Remote only received 2 of our 10 (80% loss)
                loss_ratio: 0,
                path_state: PathState::Running,
                echo_delay_us: 0,
            };

            rc.process_probe(&probe);
        }

        // On an idle link, confidence should be high, so real loss should be detected
        assert!(rc.loss(0) > 20,
            "Real loss on idle link should be detected, got: {}", rc.loss(0));
    }

    #[test]
    fn test_tapa_disabled_behaves_like_original() {
        let mut config = test_config();
        config.tapa_enabled = false;
        config.loss_min_packets = 10;
        let rates = vec![7_500_000u64];
        let mut rc = RateController::new(config, &rates);

        // Verify confidence is always 1.0 when TAPA disabled
        let conf = rc.calculate_probe_confidence(0);
        assert!((conf - 1.0).abs() < 0.001);
    }
}

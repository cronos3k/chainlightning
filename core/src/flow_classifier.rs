//! Flow Classifier
//!
//! Classifies flows as single-link or multi-link based on bandwidth demands.
//! - Below single_link_threshold (66%): Use fastest single link
//! - Above multi_link_threshold (90%) sustained: Use multi-link aggregation
//! - Between: Monitor and decide

use std::collections::HashMap;
use std::time::{Duration, Instant};
use chainlightning_common::protocol::FlowId;
use chainlightning_common::config::FlowClassifierConfig;

/// Flow routing mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowMode {
    /// Use fastest single link only
    SingleLink { link_id: usize },
    /// Scatter across multiple links
    MultiLink,
}

/// State of a tracked flow
#[derive(Debug, Clone)]
pub struct FlowState {
    pub flow_id: FlowId,
    pub mode: FlowMode,
    pub bytes_seen: u64,
    pub packets_seen: u64,
    pub first_seen: Instant,
    pub last_seen: Instant,
    /// Bandwidth samples (bytes/sec) in sliding window
    pub bandwidth_samples: Vec<(Instant, u64)>,
    /// Time flow has been above single_link_threshold
    pub time_above_threshold: Duration,
    /// When flow started exceeding threshold
    pub exceeded_threshold_at: Option<Instant>,
}

impl FlowState {
    pub fn new(flow_id: FlowId, fastest_link: usize) -> Self {
        Self {
            flow_id,
            mode: FlowMode::SingleLink { link_id: fastest_link },
            bytes_seen: 0,
            packets_seen: 0,
            first_seen: Instant::now(),
            last_seen: Instant::now(),
            bandwidth_samples: Vec::new(),
            time_above_threshold: Duration::ZERO,
            exceeded_threshold_at: None,
        }
    }

    /// Estimate current bandwidth (bytes/sec)
    pub fn estimated_bandwidth(&self, window: Duration) -> u64 {
        let now = Instant::now();
        let cutoff = now.checked_sub(window).unwrap_or(now);

        let recent_bytes: u64 = self.bandwidth_samples.iter()
            .filter(|(t, _)| *t > cutoff)
            .map(|(_, b)| b)
            .sum();

        let elapsed = now.duration_since(cutoff);
        if elapsed.as_secs_f64() > 0.0 {
            (recent_bytes as f64 / elapsed.as_secs_f64()) as u64
        } else {
            0
        }
    }

    /// Record bytes seen
    pub fn record_bytes(&mut self, bytes: usize) {
        self.bytes_seen += bytes as u64;
        self.packets_seen += 1;
        self.last_seen = Instant::now();
        self.bandwidth_samples.push((Instant::now(), bytes as u64));

        // Prune old samples (keep last 5 seconds worth)
        let cutoff = Instant::now() - Duration::from_secs(5);
        self.bandwidth_samples.retain(|(t, _)| *t > cutoff);
    }
}

/// Flow classifier for routing decisions
pub struct FlowClassifier {
    /// Active flows
    flows: HashMap<FlowId, FlowState>,
    /// Configuration
    config: FlowClassifierConfig,
    /// Link bandwidths (bytes/sec)
    link_bandwidths: Vec<u64>,
    /// Fastest link ID
    fastest_link: usize,
    /// Total available bandwidth
    total_bandwidth: u64,
}

impl FlowClassifier {
    pub fn new(config: FlowClassifierConfig, link_bandwidths: Vec<u64>) -> Self {
        let fastest_link = link_bandwidths.iter()
            .enumerate()
            .max_by_key(|(_, &bw)| bw)
            .map(|(i, _)| i)
            .unwrap_or(0);

        let total_bandwidth: u64 = link_bandwidths.iter().sum();

        Self {
            flows: HashMap::new(),
            config,
            link_bandwidths,
            fastest_link,
            total_bandwidth,
        }
    }

    /// Update link bandwidths (from stats collector)
    pub fn update_bandwidths(&mut self, bandwidths: Vec<u64>) {
        self.link_bandwidths = bandwidths;
        self.fastest_link = self.link_bandwidths.iter()
            .enumerate()
            .max_by_key(|(_, &bw)| bw)
            .map(|(i, _)| i)
            .unwrap_or(0);
        self.total_bandwidth = self.link_bandwidths.iter().sum();
    }

    /// Classify a packet and return routing decision
    pub fn classify(&mut self, packet: &[u8]) -> FlowMode {
        let flow_id = match FlowId::from_packet(packet) {
            Some(id) => id,
            None => {
                // Unknown protocol - use multi-link
                return FlowMode::MultiLink;
            }
        };

        let packet_len = packet.len();
        let fastest_link = self.fastest_link;
        let fastest_bw = self.link_bandwidths.get(fastest_link).copied().unwrap_or(1);

        // Get or create flow state
        let flow = self.flows.entry(flow_id).or_insert_with(|| {
            FlowState::new(flow_id, fastest_link)
        });

        // Record the packet
        flow.record_bytes(packet_len);

        // Calculate flow bandwidth
        let window = Duration::from_millis(self.config.flow_window_ms);
        let flow_bw = flow.estimated_bandwidth(window);

        // Calculate ratio to fastest link
        let ratio = flow_bw as f64 / fastest_bw.max(1) as f64;

        // Decision logic
        match flow.mode {
            FlowMode::SingleLink { link_id } => {
                // Currently single-link - check if should switch to multi
                if ratio > self.config.single_link_threshold {
                    // Above threshold - start/continue monitoring
                    if flow.exceeded_threshold_at.is_none() {
                        flow.exceeded_threshold_at = Some(Instant::now());
                    }

                    let exceeded_duration = flow.exceeded_threshold_at
                        .map(|t| t.elapsed())
                        .unwrap_or(Duration::ZERO);

                    // If sustained above multi_link_threshold for monitor_duration
                    if ratio > self.config.multi_link_threshold &&
                        exceeded_duration >= Duration::from_millis(self.config.monitor_duration_ms) {
                        // Switch to multi-link
                        flow.mode = FlowMode::MultiLink;
                        flow.exceeded_threshold_at = None;
                        tracing::debug!(
                            flow_id = ?flow_id,
                            ratio = ratio,
                            "Flow switched to multi-link"
                        );
                    }
                } else {
                    // Below threshold - reset monitoring
                    flow.exceeded_threshold_at = None;
                }

                flow.mode
            }
            FlowMode::MultiLink => {
                // Currently multi-link - check if should switch back to single
                if ratio < self.config.single_link_threshold {
                    // Below threshold for a while - switch back
                    if flow.exceeded_threshold_at.is_none() {
                        flow.exceeded_threshold_at = Some(Instant::now());
                    }

                    let below_duration = flow.exceeded_threshold_at
                        .map(|t| t.elapsed())
                        .unwrap_or(Duration::ZERO);

                    if below_duration >= Duration::from_millis(self.config.monitor_duration_ms) {
                        // Switch back to single-link
                        flow.mode = FlowMode::SingleLink { link_id: fastest_link };
                        flow.exceeded_threshold_at = None;
                        tracing::debug!(
                            flow_id = ?flow_id,
                            ratio = ratio,
                            "Flow switched to single-link"
                        );
                    }
                } else {
                    flow.exceeded_threshold_at = None;
                }

                flow.mode
            }
        }
    }

    /// Get current mode for a flow (without updating)
    pub fn get_mode(&self, flow_id: FlowId) -> Option<FlowMode> {
        self.flows.get(&flow_id).map(|f| f.mode)
    }

    /// Expire old flows
    pub fn expire_flows(&mut self) {
        let expiry = Duration::from_millis(self.config.flow_expiry_ms);
        let now = Instant::now();

        self.flows.retain(|_, flow| {
            now.duration_since(flow.last_seen) < expiry
        });
    }

    /// Get flow statistics
    pub fn stats(&self) -> FlowClassifierStats {
        let single_link_flows = self.flows.values()
            .filter(|f| matches!(f.mode, FlowMode::SingleLink { .. }))
            .count();
        let multi_link_flows = self.flows.values()
            .filter(|f| matches!(f.mode, FlowMode::MultiLink))
            .count();

        FlowClassifierStats {
            total_flows: self.flows.len(),
            single_link_flows,
            multi_link_flows,
            fastest_link: self.fastest_link,
        }
    }
}

/// Flow classifier statistics
#[derive(Debug, Clone)]
pub struct FlowClassifierStats {
    pub total_flows: usize,
    pub single_link_flows: usize,
    pub multi_link_flows: usize,
    pub fastest_link: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_flow_is_single_link() {
        let config = FlowClassifierConfig {
            single_link_threshold: 0.66,
            multi_link_threshold: 0.90,
            monitor_duration_ms: 2000,
            flow_window_ms: 1000,
            flow_expiry_ms: 30000,
        };

        // 5 links with Starlink being fastest
        let bandwidths = vec![
            7_500_000,   // ADSL
            27_500_000,  // Starlink (fastest)
            7_500_000,   // ADSL
            27_500_000,  // Starlink
            7_500_000,   // ADSL
        ];

        let mut classifier = FlowClassifier::new(config, bandwidths);

        // Create a small packet
        let mut packet = vec![0u8; 100];
        packet[0] = 0x45; // IPv4
        packet[9] = 6;    // TCP

        let mode = classifier.classify(&packet);
        // max_by_key returns the last element when ties exist.
        // L1 and L3 both have 27.5M, so fastest_link = 3 (last of equals).
        assert!(matches!(mode, FlowMode::SingleLink { link_id: 3 }));
    }
}

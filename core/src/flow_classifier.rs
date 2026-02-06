//! Flow Classifier
//!
//! Classifies flows into three modes:
//! - Realtime: VoIP, gaming, SSH — pinned to lowest-latency eligible link
//! - SingleLink: Default moderate traffic — fastest single link
//! - Bulk: Large sustained transfers — multi-link aggregation with sync delay

use std::collections::HashMap;
use std::time::{Duration, Instant};
use chainlightning_common::protocol::FlowId;
use chainlightning_common::config::{FlowClassifierConfig, RealtimeConfig, LinkTierConfig};

/// Flow routing mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowMode {
    /// Pin to lowest-latency eligible link (VoIP, gaming, SSH)
    Realtime { link_id: usize },
    /// Use fastest single link only
    SingleLink { link_id: usize },
    /// Scatter across multiple links with sync delay (rename of MultiLink)
    Bulk,
}

impl FlowMode {
    /// Convert to wire format byte
    pub fn to_wire(&self) -> u8 {
        match self {
            FlowMode::Realtime { .. } => 0,
            FlowMode::SingleLink { .. } => 1,
            FlowMode::Bulk => 2,
        }
    }

    /// Convert from wire format byte (needs link_id context for Realtime/SingleLink)
    pub fn from_wire(value: u8, link_id: usize) -> Self {
        match value {
            0 => FlowMode::Realtime { link_id },
            1 => FlowMode::SingleLink { link_id },
            2 => FlowMode::Bulk,
            _ => FlowMode::SingleLink { link_id },
        }
    }
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
    /// Packet timestamps for cadence detection (realtime heuristic)
    pub packet_timestamps: Vec<Instant>,
    /// Sticky flag: once classified as realtime, stays realtime
    pub is_realtime: bool,
    /// Cached protocol number from IP header
    pub protocol: u8,
    /// Cached dst port
    pub dst_port: u16,
    /// Cached src port
    pub src_port: u16,
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
            packet_timestamps: Vec::new(),
            is_realtime: false,
            protocol: 0,
            dst_port: 0,
            src_port: 0,
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
        let now = Instant::now();
        self.last_seen = now;
        self.bandwidth_samples.push((now, bytes as u64));
        self.packet_timestamps.push(now);

        // Prune old samples (keep last 5 seconds worth)
        let cutoff = now - Duration::from_secs(5);
        self.bandwidth_samples.retain(|(t, _)| *t > cutoff);

        // Prune old packet timestamps (keep last 2 seconds for cadence)
        let cadence_cutoff = now - Duration::from_secs(2);
        self.packet_timestamps.retain(|t| *t > cadence_cutoff);
    }

    /// Calculate packets per second in a given window
    pub fn packets_per_second(&self, window: Duration) -> f64 {
        let now = Instant::now();
        let cutoff = now.checked_sub(window).unwrap_or(now);
        let count = self.packet_timestamps.iter()
            .filter(|t| **t > cutoff)
            .count();
        let elapsed = window.as_secs_f64();
        if elapsed > 0.0 {
            count as f64 / elapsed
        } else {
            0.0
        }
    }
}

/// Flow classifier for routing decisions
pub struct FlowClassifier {
    /// Active flows
    flows: HashMap<FlowId, FlowState>,
    /// Flow classifier configuration
    config: FlowClassifierConfig,
    /// Realtime detection configuration
    realtime_config: RealtimeConfig,
    /// Link tier configuration (for finding realtime-eligible links)
    link_tiers: Vec<LinkTierConfig>,
    /// Link bandwidths (bytes/sec)
    link_bandwidths: Vec<u64>,
    /// Link RTTs in microseconds
    link_rtts: Vec<u64>,
    /// Per-link weight factors from optimizer [0.5 - 2.0], default 1.0
    /// Multiplied with bandwidth in weighted hash bucketing
    weight_factors: Vec<f64>,
    /// Fastest link ID (by bandwidth)
    fastest_link: usize,
    /// Lowest-latency realtime-eligible link ID
    realtime_link: usize,
    /// Total available bandwidth
    total_bandwidth: u64,
}

impl FlowClassifier {
    pub fn new(
        config: FlowClassifierConfig,
        realtime_config: RealtimeConfig,
        link_tiers: Vec<LinkTierConfig>,
        link_bandwidths: Vec<u64>,
    ) -> Self {
        let fastest_link = link_bandwidths.iter()
            .enumerate()
            .max_by_key(|(_, &bw)| bw)
            .map(|(i, _)| i)
            .unwrap_or(0);

        let total_bandwidth: u64 = link_bandwidths.iter().sum();

        let realtime_link = Self::find_realtime_link_static(&link_tiers, &link_bandwidths);

        let num_links = link_bandwidths.len();
        Self {
            flows: HashMap::new(),
            config,
            realtime_config,
            link_tiers,
            link_bandwidths,
            link_rtts: vec![0; num_links],
            weight_factors: vec![1.0; num_links],
            fastest_link,
            realtime_link,
            total_bandwidth,
        }
    }

    /// Find the lowest-latency realtime-eligible link (static helper for constructor)
    fn find_realtime_link_static(
        link_tiers: &[LinkTierConfig],
        link_bandwidths: &[u64],
    ) -> usize {
        // Find eligible links sorted by priority (lower = better latency)
        let mut eligible: Vec<_> = link_tiers.iter()
            .filter(|t| t.realtime_eligible && t.link_id < link_bandwidths.len())
            .collect();
        eligible.sort_by_key(|t| t.priority);

        eligible.first()
            .map(|t| t.link_id)
            .unwrap_or(0)
    }

    /// Find the lowest-latency realtime-eligible link using current RTT data
    fn find_lowest_latency_eligible(&self) -> usize {
        let eligible: Vec<_> = self.link_tiers.iter()
            .filter(|t| t.realtime_eligible && t.link_id < self.link_bandwidths.len())
            .collect();

        if eligible.is_empty() {
            return 0;
        }

        // If we have RTT data, use it; otherwise fall back to priority order
        let has_rtt_data = eligible.iter()
            .any(|t| self.link_rtts.get(t.link_id).copied().unwrap_or(0) > 0);

        if has_rtt_data {
            eligible.iter()
                .min_by_key(|t| {
                    let rtt = self.link_rtts.get(t.link_id).copied().unwrap_or(u64::MAX);
                    if rtt == 0 { u64::MAX } else { rtt }
                })
                .map(|t| t.link_id)
                .unwrap_or(0)
        } else {
            // No RTT data yet — use priority (lower = better)
            eligible.iter()
                .min_by_key(|t| t.priority)
                .map(|t| t.link_id)
                .unwrap_or(0)
        }
    }

    /// Update link bandwidths (from stats collector)
    pub fn update_bandwidths(&mut self, bandwidths: Vec<u64>) {
        // Don't overwrite with all-zero values — keep configured/last-known bandwidths.
        // This prevents cold-start issues where measured bandwidths haven't been
        // computed yet (first stats window), which would cause weighted_flow_link()
        // to assign all flows to link 0.
        let new_total: u64 = bandwidths.iter().sum();
        if new_total == 0 {
            return;
        }

        self.link_bandwidths = bandwidths;
        self.fastest_link = self.link_bandwidths.iter()
            .enumerate()
            .max_by_key(|(_, &bw)| bw)
            .map(|(i, _)| i)
            .unwrap_or(0);
        self.total_bandwidth = self.link_bandwidths.iter().sum();
    }

    /// Update link RTTs (from stats collector)
    pub fn update_rtts(&mut self, rtts: Vec<u64>) {
        self.link_rtts = rtts;
        self.realtime_link = self.find_lowest_latency_eligible();
    }

    /// Set optimizer weight factors (multiplied with bandwidth in hash bucketing)
    pub fn set_weight_factors(&mut self, factors: Vec<f64>) {
        self.weight_factors = factors;
    }

    /// Get current weight factors
    pub fn weight_factors(&self) -> &[f64] {
        &self.weight_factors
    }

    /// Assign a link to a flow using weighted hashing based on link bandwidths.
    /// Flows are distributed proportionally: a 220 Mbps Starlink link gets ~3.7x
    /// more flows than a 60 Mbps ADSL link.
    fn weighted_flow_link(&self, flow_id: FlowId) -> usize {
        if self.link_bandwidths.is_empty() {
            return 0;
        }

        // Apply weight factors to bandwidths for proportional hashing
        let total: u64 = self.link_bandwidths.iter().enumerate()
            .map(|(i, &bw)| {
                let factor = self.weight_factors.get(i).copied().unwrap_or(1.0);
                (bw as f64 * factor) as u64
            })
            .sum();
        if total == 0 {
            return 0;
        }

        // Use the flow_id's inner u64 directly — it's already a hash of the 5-tuple
        let bucket = flow_id.0 % total;
        let mut cumulative = 0u64;
        for (i, &bw) in self.link_bandwidths.iter().enumerate() {
            let factor = self.weight_factors.get(i).copied().unwrap_or(1.0);
            cumulative += (bw as f64 * factor) as u64;
            if bucket < cumulative {
                return i;
            }
        }

        self.link_bandwidths.len() - 1
    }

    /// Extract protocol and ports from an IP packet
    fn extract_tuple(packet: &[u8]) -> (u8, u16, u16) {
        if packet.len() < 20 {
            return (0, 0, 0);
        }
        let ihl = (packet[0] & 0x0F) as usize * 4;
        let protocol = packet[9];

        let (src_port, dst_port) = if packet.len() >= ihl + 4 && (protocol == 6 || protocol == 17) {
            let src = u16::from_be_bytes([packet[ihl], packet[ihl + 1]]);
            let dst = u16::from_be_bytes([packet[ihl + 2], packet[ihl + 3]]);
            (src, dst)
        } else {
            (0, 0)
        };

        (protocol, src_port, dst_port)
    }

    /// Check if a packet matches realtime heuristics (port-based only)
    fn is_realtime_packet(&self, protocol: u8, src_port: u16, dst_port: u16, _packet_len: usize) -> bool {
        // Check known realtime UDP ports
        if protocol == 17 { // UDP
            if self.realtime_config.realtime_udp_ports.contains(&dst_port)
                || self.realtime_config.realtime_udp_ports.contains(&src_port) {
                return true;
            }
            // NOTE: Removed standalone "UDP + small packet" check.
            // With TUN MTU=1400 and max_realtime_packet_size=1400, ALL UDP traffic
            // was being classified as Realtime, including iperf3 bulk transfers.
            // Small-packet realtime detection is now handled by cadence detection only.
        }

        // Check known realtime TCP ports
        if protocol == 6 { // TCP
            if self.realtime_config.realtime_tcp_ports.contains(&dst_port)
                || self.realtime_config.realtime_tcp_ports.contains(&src_port) {
                return true;
            }
        }

        false
    }

    /// Classify a packet and return routing decision
    pub fn classify(&mut self, packet: &[u8]) -> FlowMode {
        let flow_id = match FlowId::from_packet(packet) {
            Some(id) => id,
            None => {
                // Unknown protocol - use bulk
                return FlowMode::Bulk;
            }
        };

        let packet_len = packet.len();
        let fastest_link = self.fastest_link;
        let realtime_link = self.realtime_link;
        let fastest_bw = self.link_bandwidths.get(fastest_link).copied().unwrap_or(1);

        let (protocol, src_port, dst_port) = Self::extract_tuple(packet);

        // Pre-compute realtime check BEFORE borrowing self.flows mutably
        let is_rt_packet = self.is_realtime_packet(protocol, src_port, dst_port, packet_len);
        let cadence_window = Duration::from_millis(self.config.realtime_window_ms);
        let pps_threshold = self.config.realtime_pps_threshold as f64;
        let max_rt_size = self.realtime_config.max_realtime_packet_size;
        let flow_window = Duration::from_millis(self.config.flow_window_ms);
        let single_threshold = self.config.single_link_threshold;
        let multi_threshold = self.config.multi_link_threshold;
        let monitor_dur = Duration::from_millis(self.config.monitor_duration_ms);

        // Get or create flow state — assign to weighted link (not always fastest)
        let assigned_link = self.weighted_flow_link(flow_id);
        let flow = self.flows.entry(flow_id).or_insert_with(|| {
            FlowState::new(flow_id, assigned_link)
        });

        // Cache protocol info on first packet
        if flow.packets_seen == 0 {
            flow.protocol = protocol;
            flow.src_port = src_port;
            flow.dst_port = dst_port;
        }

        // Record the packet
        flow.record_bytes(packet_len);

        // ── Step 1: Realtime detection (sticky) ──
        if flow.is_realtime {
            flow.mode = FlowMode::Realtime { link_id: realtime_link };
            return flow.mode;
        }

        // Check realtime heuristics on the packet
        if is_rt_packet {
            flow.is_realtime = true;
            flow.mode = FlowMode::Realtime { link_id: realtime_link };
            tracing::debug!(
                flow_id = ?flow_id,
                protocol = protocol,
                dst_port = dst_port,
                packet_len = packet_len,
                link_id = realtime_link,
                "Flow classified as Realtime"
            );
            return flow.mode;
        }

        // Check cadence: high pps of small packets → realtime
        // BUT: only if flow bandwidth is low (<1 Mbps). Real VoIP is ~100 Kbps,
        // gaming ~50-500 Kbps. Anything above 1 Mbps is bulk/transfer traffic.
        let pps = flow.packets_per_second(cadence_window);
        let flow_bw_early = flow.estimated_bandwidth(cadence_window);
        let is_low_bandwidth = flow_bw_early < 125_000; // 1 Mbps in bytes/sec
        if pps > pps_threshold && packet_len <= max_rt_size && is_low_bandwidth {
            flow.is_realtime = true;
            flow.mode = FlowMode::Realtime { link_id: realtime_link };
            tracing::debug!(
                flow_id = ?flow_id,
                pps = pps,
                bw_bps = flow_bw_early * 8,
                "Flow classified as Realtime (cadence)"
            );
            return flow.mode;
        }

        // ── Step 2: Bandwidth-based Bulk detection ──
        let flow_bw = flow.estimated_bandwidth(flow_window);
        let ratio = flow_bw as f64 / fastest_bw.max(1) as f64;

        match flow.mode {
            FlowMode::SingleLink { link_id: _ } => {
                // Currently single-link - check if should switch to bulk
                if ratio > single_threshold {
                    if flow.exceeded_threshold_at.is_none() {
                        flow.exceeded_threshold_at = Some(Instant::now());
                    }

                    let exceeded_duration = flow.exceeded_threshold_at
                        .map(|t| t.elapsed())
                        .unwrap_or(Duration::ZERO);

                    if ratio > multi_threshold && exceeded_duration >= monitor_dur {
                        flow.mode = FlowMode::Bulk;
                        flow.exceeded_threshold_at = None;
                        tracing::debug!(
                            flow_id = ?flow_id,
                            ratio = ratio,
                            "Flow switched to Bulk"
                        );
                    }
                } else {
                    flow.exceeded_threshold_at = None;
                }

                flow.mode
            }
            FlowMode::Bulk => {
                // Currently bulk - check if should switch back to single-link
                if ratio < single_threshold {
                    if flow.exceeded_threshold_at.is_none() {
                        flow.exceeded_threshold_at = Some(Instant::now());
                    }

                    let below_duration = flow.exceeded_threshold_at
                        .map(|t| t.elapsed())
                        .unwrap_or(Duration::ZERO);

                    if below_duration >= monitor_dur {
                        flow.mode = FlowMode::SingleLink { link_id: assigned_link };
                        flow.exceeded_threshold_at = None;
                        tracing::debug!(
                            flow_id = ?flow_id,
                            ratio = ratio,
                            link_id = assigned_link,
                            "Flow switched to SingleLink"
                        );
                    }
                } else {
                    flow.exceeded_threshold_at = None;
                }

                flow.mode
            }
            FlowMode::Realtime { .. } => {
                // Should not reach here (handled above), but be safe
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
        let bulk_flows = self.flows.values()
            .filter(|f| matches!(f.mode, FlowMode::Bulk))
            .count();
        let realtime_flows = self.flows.values()
            .filter(|f| matches!(f.mode, FlowMode::Realtime { .. }))
            .count();

        FlowClassifierStats {
            total_flows: self.flows.len(),
            single_link_flows,
            bulk_flows,
            realtime_flows,
            fastest_link: self.fastest_link,
            realtime_link: self.realtime_link,
        }
    }
}

/// Flow classifier statistics
#[derive(Debug, Clone)]
pub struct FlowClassifierStats {
    pub total_flows: usize,
    pub single_link_flows: usize,
    pub bulk_flows: usize,
    pub realtime_flows: usize,
    pub fastest_link: usize,
    pub realtime_link: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chainlightning_common::config::{LinkTierConfig, RealtimeConfig};

    fn test_realtime_config() -> RealtimeConfig {
        RealtimeConfig {
            realtime_udp_ports: vec![5060, 5061, 3478, 27015],
            realtime_tcp_ports: vec![22],
            max_realtime_packet_size: 600,
            force_adsl_only: true,
        }
    }

    fn test_link_tiers() -> Vec<LinkTierConfig> {
        vec![
            LinkTierConfig {
                link_id: 2,
                priority: 1,
                capacity_down_bps: 7_812_500,
                capacity_up_bps: 1_587_500,
                utilization_threshold: 0.90,
                realtime_eligible: true,
                link_type: "adsl".to_string(),
            },
            LinkTierConfig {
                link_id: 0,
                priority: 2,
                capacity_down_bps: 7_812_500,
                capacity_up_bps: 1_587_500,
                utilization_threshold: 0.90,
                realtime_eligible: true,
                link_type: "adsl".to_string(),
            },
            LinkTierConfig {
                link_id: 1,
                priority: 4,
                capacity_down_bps: 27_500_000,
                capacity_up_bps: 2_500_000,
                utilization_threshold: 0.95,
                realtime_eligible: false,
                link_type: "starlink".to_string(),
            },
            LinkTierConfig {
                link_id: 3,
                priority: 5,
                capacity_down_bps: 27_500_000,
                capacity_up_bps: 2_500_000,
                utilization_threshold: 0.95,
                realtime_eligible: false,
                link_type: "starlink".to_string(),
            },
            LinkTierConfig {
                link_id: 4,
                priority: 3,
                capacity_down_bps: 7_812_500,
                capacity_up_bps: 1_587_500,
                utilization_threshold: 0.90,
                realtime_eligible: true,
                link_type: "adsl".to_string(),
            },
        ]
    }

    #[test]
    fn test_new_flow_is_single_link() {
        let config = FlowClassifierConfig {
            single_link_threshold: 0.66,
            multi_link_threshold: 0.90,
            monitor_duration_ms: 2000,
            flow_window_ms: 1000,
            flow_expiry_ms: 30000,
            realtime_pps_threshold: 10,
            realtime_window_ms: 1000,
        };

        let bandwidths = vec![
            7_500_000,   // ADSL
            27_500_000,  // Starlink (fastest)
            7_500_000,   // ADSL
            27_500_000,  // Starlink
            7_500_000,   // ADSL
        ];

        let mut classifier = FlowClassifier::new(
            config,
            test_realtime_config(),
            test_link_tiers(),
            bandwidths,
        );

        // Create a TCP packet on port 8080 (non-realtime, non-small)
        let mut packet = vec![0u8; 1500];
        packet[0] = 0x45; // IPv4
        packet[9] = 6;    // TCP
        packet[12..16].copy_from_slice(&[192, 168, 1, 1]);
        packet[16..20].copy_from_slice(&[10, 0, 0, 1]);
        packet[20..22].copy_from_slice(&[0x1F, 0x90]); // src port 8080
        packet[22..24].copy_from_slice(&[0x00, 0x50]); // dst port 80

        let mode = classifier.classify(&packet);
        // Weighted hash distributes flows across links proportionally to bandwidth
        assert!(matches!(mode, FlowMode::SingleLink { .. }));
    }

    #[test]
    fn test_ssh_classified_as_realtime() {
        let config = FlowClassifierConfig {
            single_link_threshold: 0.66,
            multi_link_threshold: 0.90,
            monitor_duration_ms: 2000,
            flow_window_ms: 1000,
            flow_expiry_ms: 30000,
            realtime_pps_threshold: 10,
            realtime_window_ms: 1000,
        };

        let bandwidths = vec![7_500_000, 27_500_000, 7_500_000, 27_500_000, 7_500_000];

        let mut classifier = FlowClassifier::new(
            config,
            test_realtime_config(),
            test_link_tiers(),
            bandwidths,
        );

        // TCP packet to port 22 (SSH)
        let mut packet = vec![0u8; 100];
        packet[0] = 0x45; // IPv4
        packet[9] = 6;    // TCP
        packet[12..16].copy_from_slice(&[192, 168, 1, 1]);
        packet[16..20].copy_from_slice(&[10, 0, 0, 1]);
        packet[20..22].copy_from_slice(&[0xC0, 0x00]); // src port 49152
        packet[22..24].copy_from_slice(&[0x00, 0x16]); // dst port 22

        let mode = classifier.classify(&packet);
        // Should be realtime on link 2 (highest priority realtime-eligible)
        assert!(matches!(mode, FlowMode::Realtime { link_id: 2 }));
    }

    #[test]
    fn test_small_udp_non_realtime_port_is_single_link() {
        let config = FlowClassifierConfig {
            single_link_threshold: 0.66,
            multi_link_threshold: 0.90,
            monitor_duration_ms: 2000,
            flow_window_ms: 1000,
            flow_expiry_ms: 30000,
            realtime_pps_threshold: 10,
            realtime_window_ms: 1000,
        };

        let bandwidths = vec![7_500_000, 27_500_000, 7_500_000, 27_500_000, 7_500_000];

        let mut classifier = FlowClassifier::new(
            config,
            test_realtime_config(),
            test_link_tiers(),
            bandwidths,
        );

        // Small UDP packet (DNS-like, 60 bytes) to port 53 (NOT in realtime ports)
        // Should be SingleLink — size alone doesn't trigger Realtime anymore
        let mut packet = vec![0u8; 60];
        packet[0] = 0x45; // IPv4
        packet[9] = 17;   // UDP
        packet[12..16].copy_from_slice(&[192, 168, 1, 1]);
        packet[16..20].copy_from_slice(&[8, 8, 8, 8]);
        packet[20..22].copy_from_slice(&[0xC0, 0x01]); // src port 49153
        packet[22..24].copy_from_slice(&[0x00, 0x35]); // dst port 53

        let mode = classifier.classify(&packet);
        assert!(matches!(mode, FlowMode::SingleLink { .. }));
    }

    #[test]
    fn test_known_realtime_udp_port_classified_as_realtime() {
        let config = FlowClassifierConfig {
            single_link_threshold: 0.66,
            multi_link_threshold: 0.90,
            monitor_duration_ms: 2000,
            flow_window_ms: 1000,
            flow_expiry_ms: 30000,
            realtime_pps_threshold: 10,
            realtime_window_ms: 1000,
        };

        let bandwidths = vec![7_500_000, 27_500_000, 7_500_000, 27_500_000, 7_500_000];

        let mut classifier = FlowClassifier::new(
            config,
            test_realtime_config(),
            test_link_tiers(),
            bandwidths,
        );

        // SIP packet on known realtime port 5060
        let mut packet = vec![0u8; 200];
        packet[0] = 0x45; // IPv4
        packet[9] = 17;   // UDP
        packet[12..16].copy_from_slice(&[192, 168, 1, 1]);
        packet[16..20].copy_from_slice(&[10, 0, 0, 1]);
        packet[20..22].copy_from_slice(&[0xC0, 0x01]); // src port 49153
        packet[22..24].copy_from_slice(&[0x13, 0xC4]); // dst port 5060

        let mode = classifier.classify(&packet);
        assert!(matches!(mode, FlowMode::Realtime { link_id: 2 }));
    }

    #[test]
    fn test_realtime_is_sticky() {
        let config = FlowClassifierConfig {
            single_link_threshold: 0.66,
            multi_link_threshold: 0.90,
            monitor_duration_ms: 2000,
            flow_window_ms: 1000,
            flow_expiry_ms: 30000,
            realtime_pps_threshold: 10,
            realtime_window_ms: 1000,
        };

        let bandwidths = vec![7_500_000, 27_500_000, 7_500_000, 27_500_000, 7_500_000];

        let mut classifier = FlowClassifier::new(
            config,
            test_realtime_config(),
            test_link_tiers(),
            bandwidths,
        );

        // SSH packet
        let mut packet = vec![0u8; 100];
        packet[0] = 0x45;
        packet[9] = 6;
        packet[12..16].copy_from_slice(&[192, 168, 1, 1]);
        packet[16..20].copy_from_slice(&[10, 0, 0, 1]);
        packet[20..22].copy_from_slice(&[0xC0, 0x00]);
        packet[22..24].copy_from_slice(&[0x00, 0x16]); // port 22

        let mode1 = classifier.classify(&packet);
        assert!(matches!(mode1, FlowMode::Realtime { .. }));

        // Classify same flow again — should stay Realtime even with large packet
        let mut large_packet = vec![0u8; 1500];
        large_packet[0] = 0x45;
        large_packet[9] = 6;
        large_packet[12..16].copy_from_slice(&[192, 168, 1, 1]);
        large_packet[16..20].copy_from_slice(&[10, 0, 0, 1]);
        large_packet[20..22].copy_from_slice(&[0xC0, 0x00]);
        large_packet[22..24].copy_from_slice(&[0x00, 0x16]);

        let mode2 = classifier.classify(&large_packet);
        assert!(matches!(mode2, FlowMode::Realtime { .. }));
    }
}

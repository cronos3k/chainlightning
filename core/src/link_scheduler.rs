//! Link Scheduler - Adaptive Weighted Round Robin
//!
//! Automatically measures actual link capacity and adjusts weights accordingly.
//! No reliance on static configuration - learns from real throughput.
//!
//! Key insight: We can measure what each link actually delivers and use that
//! to set weights, rather than trusting pre-configured values.

use std::time::{Duration, Instant};
use chainlightning_common::config::LinkSchedulerConfig;
use crate::flow_classifier::FlowMode;

/// Link health state
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LinkHealth {
    Normal,
    Degraded,  // Delivering less than expected
}

/// Scheduling decision
#[derive(Debug, Clone)]
pub struct ScheduleDecision {
    pub link_id: usize,
    pub delay: Duration,
    pub participating_links: Vec<usize>,
}

/// Per-link state with throughput tracking
struct LinkState {
    id: usize,
    /// Configured expected bandwidth (bytes/sec) - starting point
    configured_bandwidth_bps: u64,
    /// Measured actual bandwidth (bytes/sec) - what we observe
    measured_bandwidth_bps: u64,
    /// Current weight for scheduling
    weight: u32,
    /// Bytes sent through this link in current measurement window
    bytes_sent: u64,
    /// Bytes acknowledged/received in current measurement window
    bytes_delivered: u64,
    /// RTT in microseconds (smoothed)
    rtt_us: u64,
    /// Health state
    health: LinkHealth,
    /// Last measurement update time
    last_update: Instant,
    /// Measurement window start
    window_start: Instant,
}

impl LinkState {
    fn new(id: usize, configured_bandwidth_bps: u64) -> Self {
        let weight = Self::bandwidth_to_weight(configured_bandwidth_bps);
        Self {
            id,
            configured_bandwidth_bps,
            measured_bandwidth_bps: configured_bandwidth_bps, // Start with configured
            weight,
            bytes_sent: 0,
            bytes_delivered: 0,
            rtt_us: 0,
            health: LinkHealth::Normal,
            last_update: Instant::now(),
            window_start: Instant::now(),
        }
    }

    /// Convert bandwidth to weight (1 Mbps = 1 weight unit)
    fn bandwidth_to_weight(bps: u64) -> u32 {
        // 125,000 bytes/sec = 1 Mbps
        (bps / 125_000).max(1) as u32
    }

    /// Get effective bandwidth for weight calculation
    /// Uses measured if we have data, otherwise configured
    fn effective_bandwidth(&self) -> u64 {
        if self.measured_bandwidth_bps > 0 {
            self.measured_bandwidth_bps
        } else {
            self.configured_bandwidth_bps
        }
    }

    /// Update RTT with EWMA smoothing
    fn update_rtt(&mut self, rtt_us: u64) {
        if rtt_us == 0 {
            return;
        }
        const ALPHA: f64 = 0.125;
        if self.rtt_us == 0 {
            self.rtt_us = rtt_us;
        } else {
            self.rtt_us = ((ALPHA * rtt_us as f64) + ((1.0 - ALPHA) * self.rtt_us as f64)) as u64;
        }
    }

    /// Record bytes sent through this link
    fn record_sent(&mut self, bytes: u64) {
        self.bytes_sent += bytes;
    }

    /// Record bytes that were successfully delivered (from ACK or stats update)
    fn record_delivered(&mut self, bytes: u64) {
        self.bytes_delivered += bytes;
    }

    /// Calculate delivery ratio (0.0 to 1.0)
    fn delivery_ratio(&self) -> f64 {
        if self.bytes_sent == 0 {
            return 1.0; // No data to judge
        }
        (self.bytes_delivered as f64 / self.bytes_sent as f64).min(1.0)
    }

    /// Drain time estimate for scheduling
    fn drain_time(&self, pending_bytes: u64) -> Duration {
        let bw = self.effective_bandwidth();
        if bw == 0 {
            return Duration::MAX;
        }
        Duration::from_secs_f64(pending_bytes as f64 / bw as f64)
    }

    /// One-way latency estimate
    fn one_way_latency(&self) -> Duration {
        Duration::from_micros(self.rtt_us / 2)
    }
}

/// Link Scheduler with automatic capacity measurement
pub struct LinkScheduler {
    config: LinkSchedulerConfig,
    links: Vec<LinkState>,
    cursor: u64,
    total_weight: u32,
    /// Measurement window duration
    measurement_window: Duration,
    /// Minimum time between weight adjustments
    adjustment_interval: Duration,
    /// Last adjustment time
    last_adjustment: Instant,
    /// Preferred link for single-link mode (sticky selection)
    preferred_link: Option<usize>,
    /// Last time we changed the preferred link
    last_link_change: Instant,
    /// Direction: true = upload (client to server), false = download (server to client)
    is_upload: bool,
    /// Bulk-eligible link IDs (excluding RTT outliers)
    bulk_eligible: Vec<usize>,
    /// Total weight of bulk-eligible links
    bulk_total_weight: u32,
}

impl LinkScheduler {
    /// Create scheduler with configured bandwidths (bytes/sec)
    /// is_upload: true for client (sending uploads), false for server (sending downloads)
    pub fn new(config: LinkSchedulerConfig, configured_bandwidths: &[u64], is_upload: bool) -> Self {
        let links: Vec<LinkState> = configured_bandwidths
            .iter()
            .enumerate()
            .map(|(id, &bw)| LinkState::new(id, bw))
            .collect();

        let total_weight = links.iter().map(|l| l.weight).sum::<u32>().max(1);

        // Pick the link with highest configured bandwidth as initial preferred
        let initial_preferred = configured_bandwidths
            .iter()
            .enumerate()
            .max_by_key(|(_, &bw)| bw)
            .map(|(id, _)| id);

        let all_links: Vec<usize> = links.iter().map(|l| l.id).collect();
        Self {
            config,
            cursor: 0,
            total_weight,
            measurement_window: Duration::from_secs(2),
            adjustment_interval: Duration::from_secs(1),
            last_adjustment: Instant::now(),
            preferred_link: initial_preferred,
            last_link_change: Instant::now(),
            is_upload,
            bulk_eligible: all_links,
            bulk_total_weight: total_weight,
            links,
        }
    }

    /// Create with default equal weights
    pub fn new_default(config: LinkSchedulerConfig, num_links: usize, is_upload: bool) -> Self {
        let default_bw = 10_000_000 / 8; // 10 Mbps
        let configured: Vec<u64> = (0..num_links).map(|_| default_bw).collect();
        Self::new(config, &configured, is_upload)
    }

    /// Update link with measured performance data
    /// Called periodically (every second) with actual measured bandwidth
    pub fn update_link(&mut self, link_id: usize, measured_bandwidth_bps: u64, rtt_us: u64, _healthy: bool) {
        if let Some(link) = self.links.get_mut(link_id) {
            // Update RTT
            link.update_rtt(rtt_us);

            // Update measured bandwidth - this is the KEY DATA
            // This tells us what the link actually achieved
            if measured_bandwidth_bps > 0 {
                // EWMA smoothing for bandwidth
                const BW_ALPHA: f64 = 0.3;
                if link.measured_bandwidth_bps == 0 {
                    link.measured_bandwidth_bps = measured_bandwidth_bps;
                } else {
                    link.measured_bandwidth_bps = ((BW_ALPHA * measured_bandwidth_bps as f64)
                        + ((1.0 - BW_ALPHA) * link.measured_bandwidth_bps as f64)) as u64;
                }

                // Track as delivered bytes
                link.record_delivered(measured_bandwidth_bps);
            }

            link.last_update = Instant::now();
        }

        // Check if it's time to adjust weights based on measurements
        self.maybe_adjust_weights();

        // Recalculate bulk-eligible links (RTT outlier exclusion)
        self.update_bulk_eligible();
    }

    /// Adjust weights - use CONFIGURED values, not measured
    /// Measured throughput is unreliable during transfers due to TCP behavior.
    /// Previous working version used static weights and achieved 30+ Mbps uploads.
    fn maybe_adjust_weights(&mut self) {
        if self.last_adjustment.elapsed() < self.adjustment_interval {
            return;
        }
        self.last_adjustment = Instant::now();

        // Use configured weights - don't adjust based on measured throughput
        // The "adaptive" approach was causing constant link switching and degradation
        for link in &mut self.links {
            // Keep weight at configured value
            link.weight = LinkState::bandwidth_to_weight(link.configured_bandwidth_bps);
            // Don't mark links as degraded - TCP throughput varies, that's normal
            link.health = LinkHealth::Normal;
        }

        self.recalculate_total_weight();
    }

    fn recalculate_total_weight(&mut self) {
        self.total_weight = self.links.iter().map(|l| l.weight).sum::<u32>().max(1);
    }

    /// Schedule a chunk based on flow mode
    pub fn schedule(&mut self, chunk_size: usize, flow_mode: &FlowMode) -> ScheduleDecision {
        match flow_mode {
            FlowMode::Realtime { link_id } => {
                // Force the specific low-latency link, zero delay
                let lid = *link_id;
                if let Some(link) = self.links.get_mut(lid) {
                    link.record_sent(chunk_size as u64);
                }
                ScheduleDecision {
                    link_id: lid,
                    delay: Duration::ZERO,
                    participating_links: vec![lid],
                }
            }
            FlowMode::SingleLink { link_id } => {
                // Honor the assigned link_id — pin flow to one link to avoid
                // cross-link reordering that destroys TCP throughput
                let lid = *link_id;
                if let Some(link) = self.links.get_mut(lid) {
                    link.record_sent(chunk_size as u64);
                }
                ScheduleDecision {
                    link_id: lid,
                    delay: Duration::ZERO,
                    participating_links: vec![lid],
                }
            }
            FlowMode::Bulk => {
                // Multi-link with sync delay
                self.schedule_multi_link(chunk_size)
            }
        }
    }

    /// Legacy schedule method for backward compatibility (tests, etc.)
    pub fn schedule_bool(&mut self, chunk_size: usize, multi_link: bool) -> ScheduleDecision {
        if multi_link {
            self.schedule_multi_link(chunk_size)
        } else {
            self.schedule_single_link(chunk_size)
        }
    }

    /// Single link scheduling - simple weighted round-robin across ALL links
    /// This distributes traffic proportionally by configured capacity.
    /// No complex health checks - just distribute and let it flow.
    fn schedule_single_link(&mut self, chunk_size: usize) -> ScheduleDecision {
        // Simple weighted round-robin - the approach that previously achieved 30+ Mbps uploads
        // Use configured weights, not measured (measured is unreliable during transfers)

        let link_id = self.weighted_select();

        if let Some(link) = self.links.get_mut(link_id) {
            link.record_sent(chunk_size as u64);
        }

        ScheduleDecision {
            link_id,
            delay: Duration::ZERO,
            participating_links: (0..self.links.len()).collect(),
        }
    }

    /// Tiered fill scheduling - spread across all links in highest available tier
    /// For bulk transfers: round-robin within same-latency tier (all ADSL links)
    /// Only use lower-priority tiers (Starlink) when higher tier is saturated
    /// Uses capacity_up_bps for uploads, capacity_down_bps for downloads
    fn schedule_tiered_fill(&mut self, chunk_size: usize) -> ScheduleDecision {
        // Sort tiers by priority (lower = higher priority)
        let mut tiers = self.config.link_tiers.clone();
        tiers.sort_by_key(|t| t.priority);

        // Helper to get correct capacity based on direction
        let get_capacity = |t: &chainlightning_common::config::LinkTierConfig| -> u64 {
            if self.is_upload {
                t.capacity_up_bps
            } else {
                t.capacity_down_bps
            }
        };

        // Group links by link_type (same latency tier)
        // ADSL links should be used together, Starlink together
        let adsl_links: Vec<_> = tiers.iter()
            .filter(|t| t.link_type == "adsl" && t.link_id < self.links.len())
            .collect();
        let starlink_links: Vec<_> = tiers.iter()
            .filter(|t| t.link_type == "starlink" && t.link_id < self.links.len())
            .collect();

        // Check if ADSL tier has aggregate capacity
        // Only overflow to Starlink when TOTAL ADSL usage exceeds threshold
        let adsl_total_capacity: u64 = adsl_links.iter()
            .map(|t| get_capacity(t))
            .sum();
        let adsl_total_load: u64 = adsl_links.iter()
            .map(|t| self.links[t.link_id].measured_bandwidth_bps)
            .sum();
        // Use 90% threshold on AGGREGATE capacity
        let adsl_threshold = (adsl_total_capacity as f64 * 0.90) as u64;
        let adsl_has_capacity = adsl_total_load < adsl_threshold;

        // If ADSL has capacity, round-robin across all ADSL links
        if adsl_has_capacity && !adsl_links.is_empty() {
            // Use weighted round-robin across ADSL links based on capacity
            // This ensures proper load distribution even when stats haven't updated yet
            if adsl_total_capacity > 0 {
                // Use cursor to distribute across ADSL links proportionally
                let pos = (self.cursor % adsl_total_capacity) as u64;
                self.cursor = self.cursor.wrapping_add(chunk_size as u64);

                let mut cumulative = 0u64;
                for tier in &adsl_links {
                    cumulative += get_capacity(tier);
                    if pos < cumulative {
                        let link_id = tier.link_id;
                        if let Some(link) = self.links.get_mut(link_id) {
                            link.record_sent(chunk_size as u64);
                        }
                        return ScheduleDecision {
                            link_id,
                            delay: Duration::ZERO,
                            participating_links: adsl_links.iter().map(|t| t.link_id).collect(),
                        };
                    }
                }
            }

            // Fallback: first ADSL link
            let link_id = adsl_links[0].link_id;
            if let Some(link) = self.links.get_mut(link_id) {
                link.record_sent(chunk_size as u64);
            }
            return ScheduleDecision {
                link_id,
                delay: Duration::ZERO,
                participating_links: adsl_links.iter().map(|t| t.link_id).collect(),
            };
        }

        // ADSL saturated - use Starlink overflow with weighted round-robin
        if !starlink_links.is_empty() {
            let starlink_total_capacity: u64 = starlink_links.iter()
                .map(|t| get_capacity(t))
                .sum();

            if starlink_total_capacity > 0 {
                let pos = (self.cursor % starlink_total_capacity) as u64;
                self.cursor = self.cursor.wrapping_add(chunk_size as u64);

                let mut cumulative = 0u64;
                for tier in &starlink_links {
                    cumulative += get_capacity(tier);
                    if pos < cumulative {
                        let link_id = tier.link_id;
                        if let Some(link) = self.links.get_mut(link_id) {
                            link.record_sent(chunk_size as u64);
                        }
                        return ScheduleDecision {
                            link_id,
                            delay: Duration::ZERO,
                            participating_links: starlink_links.iter().map(|t| t.link_id).collect(),
                        };
                    }
                }
            }

            let link_id = starlink_links[0].link_id;
            if let Some(link) = self.links.get_mut(link_id) {
                link.record_sent(chunk_size as u64);
            }
            return ScheduleDecision {
                link_id,
                delay: Duration::ZERO,
                participating_links: starlink_links.iter().map(|t| t.link_id).collect(),
            };
        }

        // Fallback: first available link
        let fallback = tiers.first().map(|t| t.link_id).unwrap_or(0);
        if let Some(link) = self.links.get_mut(fallback) {
            link.record_sent(chunk_size as u64);
        }
        ScheduleDecision {
            link_id: fallback,
            delay: Duration::ZERO,
            participating_links: vec![fallback],
        }
    }

    /// Multi-link scheduling - weighted round robin among bulk-eligible links
    /// Links with RTT > 2.5x median are excluded to prevent reorder buffer overflow
    fn schedule_multi_link(&mut self, chunk_size: usize) -> ScheduleDecision {
        let link_id = if self.bulk_eligible.len() >= 2 {
            self.weighted_select_bulk()
        } else {
            self.weighted_select()
        };

        let delay = if self.config.enable_sync {
            self.calculate_sync_delay(link_id)
        } else {
            Duration::ZERO
        };

        if let Some(link) = self.links.get_mut(link_id) {
            link.record_sent(chunk_size as u64);
        }

        ScheduleDecision {
            link_id,
            delay,
            participating_links: self.bulk_eligible.clone(),
        }
    }

    /// Recalculate which links are eligible for bulk (multi-link) scheduling.
    /// Excludes links whose RTT is more than 2.5x the median RTT of all links,
    /// since they'd cause excessive reorder buffer delays.
    fn update_bulk_eligible(&mut self) {
        let mut link_rtts: Vec<(usize, u64)> = self.links.iter()
            .filter(|l| l.weight > 0 && l.rtt_us > 0)
            .map(|l| (l.id, l.rtt_us))
            .collect();

        if link_rtts.len() <= 1 {
            // Not enough RTT data — use all links with weight > 0
            self.bulk_eligible = self.links.iter()
                .filter(|l| l.weight > 0)
                .map(|l| l.id)
                .collect();
        } else {
            link_rtts.sort_by_key(|(_, rtt)| *rtt);
            let median_rtt = link_rtts[link_rtts.len() / 2].1;
            let threshold = (median_rtt as f64 * 2.5) as u64;

            let eligible: Vec<usize> = link_rtts.iter()
                .filter(|(_, rtt)| *rtt <= threshold)
                .map(|(id, _)| *id)
                .collect();

            // Log when a link is excluded
            let excluded: Vec<(usize, u64)> = link_rtts.iter()
                .filter(|(_, rtt)| *rtt > threshold)
                .copied()
                .collect();
            if !excluded.is_empty() {
                for (id, rtt) in &excluded {
                    tracing::info!(
                        "L{} excluded from Bulk: RTT {}ms > threshold {}ms (median {}ms * 2.5)",
                        id, rtt / 1000, threshold / 1000, median_rtt / 1000
                    );
                }
            }

            self.bulk_eligible = if eligible.is_empty() {
                // Don't exclude ALL links — keep at least the ones with data
                link_rtts.iter().map(|(id, _)| *id).collect()
            } else {
                eligible
            };
        }

        self.bulk_total_weight = self.bulk_eligible.iter()
            .filter_map(|&id| self.links.get(id))
            .map(|l| l.weight)
            .sum::<u32>()
            .max(1);
    }

    /// Weighted round-robin selection among bulk-eligible links only
    fn weighted_select_bulk(&mut self) -> usize {
        self.cursor = self.cursor.wrapping_add(1);
        let pos = (self.cursor % self.bulk_total_weight as u64) as u32;
        let mut cumulative = 0u32;

        for &id in &self.bulk_eligible {
            if let Some(link) = self.links.get(id) {
                if link.weight == 0 { continue; }
                cumulative += link.weight;
                if pos < cumulative {
                    return id;
                }
            }
        }

        self.bulk_eligible.first().copied().unwrap_or(0)
    }

    /// Get the one-way RTT spread in milliseconds among bulk-eligible links.
    /// Used by the receiver to set its reorder timeout.
    pub fn bulk_rtt_spread_ms(&self) -> u64 {
        if self.bulk_eligible.len() < 2 {
            return 0;
        }

        let rtts: Vec<u64> = self.bulk_eligible.iter()
            .filter_map(|&id| self.links.get(id))
            .filter(|l| l.rtt_us > 0)
            .map(|l| l.rtt_us)
            .collect();

        if rtts.len() < 2 {
            return 0;
        }

        let min = rtts.iter().copied().min().unwrap_or(0);
        let max = rtts.iter().copied().max().unwrap_or(0);

        // One-way spread (RTT/2 difference)
        (max - min) / 2000
    }

    /// Weighted round-robin selection
    fn weighted_select(&mut self) -> usize {
        self.cursor = self.cursor.wrapping_add(1);
        let pos = (self.cursor % self.total_weight as u64) as u32;
        let mut cumulative = 0u32;

        for link in &self.links {
            if link.weight == 0 {
                continue;
            }
            cumulative += link.weight;
            if pos < cumulative {
                return link.id;
            }
        }

        // Fallback
        self.links.iter().find(|l| l.weight > 0).map(|l| l.id).unwrap_or(0)
    }

    /// Calculate delay for arrival synchronization
    fn calculate_sync_delay(&self, target_link: usize) -> Duration {
        let target = match self.links.get(target_link) {
            Some(l) => l,
            None => return Duration::ZERO,
        };

        let max_latency = self.links
            .iter()
            .filter(|l| l.weight > 0)
            .map(|l| l.one_way_latency())
            .max()
            .unwrap_or(Duration::ZERO);

        let target_latency = target.one_way_latency();
        if max_latency > target_latency {
            (max_latency - target_latency).min(Duration::from_millis(self.config.max_send_delay_ms))
        } else {
            Duration::ZERO
        }
    }

    /// Acknowledge bytes (for delivery tracking)
    pub fn ack_bytes(&mut self, link_id: usize, bytes: u64) {
        if let Some(link) = self.links.get_mut(link_id) {
            link.record_delivered(bytes);
        }
    }

    /// Set explicit weights (for testing)
    pub fn set_weights(&mut self, weights: &[u32]) {
        for (i, &w) in weights.iter().enumerate() {
            if let Some(link) = self.links.get_mut(i) {
                link.weight = w;
            }
        }
        self.recalculate_total_weight();
    }

    /// Set weights from rate controller (Glorytun/MUD-style adaptive)
    /// Called periodically when rate control is enabled
    pub fn set_rate_controlled_weights(&mut self, weights: &[u32]) {
        for (i, &w) in weights.iter().enumerate() {
            if let Some(link) = self.links.get_mut(i) {
                link.weight = w;
                link.health = if w > 0 { LinkHealth::Normal } else { LinkHealth::Degraded };
            }
        }
        self.recalculate_total_weight();
        self.update_bulk_eligible();
    }

    /// Force reset to configured values
    pub fn force_recalculate(&mut self) {
        for link in &mut self.links {
            link.weight = LinkState::bandwidth_to_weight(link.configured_bandwidth_bps);
            link.measured_bandwidth_bps = 0; // Reset measurements
        }
        self.recalculate_total_weight();
    }

    // === Getters ===

    pub fn weights(&self) -> Vec<u32> {
        self.links.iter().map(|l| l.weight).collect()
    }

    pub fn target_weights(&self) -> Vec<u32> {
        self.links.iter()
            .map(|l| LinkState::bandwidth_to_weight(l.configured_bandwidth_bps))
            .collect()
    }

    pub fn health_states(&self) -> Vec<LinkHealth> {
        self.links.iter().map(|l| l.health).collect()
    }

    pub fn health(&self) -> Vec<bool> {
        self.links.iter().map(|l| l.weight > 0).collect()
    }

    pub fn effective_capacities(&self) -> Vec<u64> {
        self.links.iter().map(|l| l.effective_bandwidth()).collect()
    }

    pub fn measured_bandwidths(&self) -> Vec<u64> {
        self.links.iter().map(|l| l.measured_bandwidth_bps).collect()
    }

    pub fn historical_bandwidths(&self) -> Vec<u64> {
        self.links.iter().map(|l| l.configured_bandwidth_bps).collect()
    }

    /// Status summary for logging
    pub fn status_summary(&self) -> String {
        let mut parts = Vec::new();
        for link in &self.links {
            let health = match link.health {
                LinkHealth::Normal => "OK",
                LinkHealth::Degraded => "DEG",
            };
            let configured_weight = LinkState::bandwidth_to_weight(link.configured_bandwidth_bps);
            let measured_mbps = link.measured_bandwidth_bps as f64 * 8.0 / 1_000_000.0;
            let rtt_ms = link.rtt_us as f64 / 1000.0;
            parts.push(format!(
                "L{}[w:{}/{}|meas:{:.0}Mbps|{:.1}ms|{}]",
                link.id, link.weight, configured_weight, measured_mbps, rtt_ms, health
            ));
        }
        parts.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_weights_not_affected_by_measured() {
        // maybe_adjust_weights() intentionally uses STATIC configured weights.
        // Adaptive weight reduction was removed because it caused a death spiral:
        // low traffic → low measured throughput → lower weight → even less traffic.
        // Dynamic adaptation is now handled by the RateController (Glorytun/MUD-style)
        // which pushes weights via set_rate_controlled_weights().
        let config = LinkSchedulerConfig {
            sync_interval_ms: 50,
            max_send_delay_ms: 100,
            enable_sync: false,
            strategy: "tiered_fill".to_string(),
            link_tiers: vec![],
            flow_affinity: false,
            flow_affinity_timeout_secs: 30,
        };

        let configured = vec![7_500_000u64, 27_500_000];
        let mut scheduler = LinkScheduler::new(config, &configured, false);

        // Initial weights from configured bandwidths
        assert_eq!(scheduler.weights(), vec![60, 220]);

        // Feed degraded measurements - weights should NOT change
        scheduler.update_link(0, 625_000, 100_000, true); // 5 Mbps measured
        scheduler.update_link(1, 27_500_000, 40_000, true);

        scheduler.last_adjustment = Instant::now() - Duration::from_secs(2);
        scheduler.maybe_adjust_weights();

        // Weights remain at configured values (static scheduling)
        let weights = scheduler.weights();
        assert_eq!(weights[0], 60, "ADSL weight should stay at configured 60");
        assert_eq!(weights[1], 220, "Starlink weight should stay at configured 220");

        // Dynamic adaptation comes from rate controller instead
        scheduler.set_rate_controlled_weights(&[30, 220]);
        assert_eq!(scheduler.weights(), vec![30, 220]);
    }

    #[test]
    fn test_weighted_distribution() {
        let config = LinkSchedulerConfig {
            sync_interval_ms: 50,
            max_send_delay_ms: 100,
            enable_sync: false,
            strategy: "tiered_fill".to_string(),
            link_tiers: vec![],
            flow_affinity: false,
            flow_affinity_timeout_secs: 30,
        };

        let configured = vec![7_500_000u64, 27_500_000, 7_500_000, 27_500_000, 7_500_000];
        let scheduler = LinkScheduler::new(config, &configured, false);

        let weights = scheduler.weights();
        assert_eq!(weights[0], 60);
        assert_eq!(weights[1], 220);
        assert_eq!(weights[2], 60);
        assert_eq!(weights[3], 220);
        assert_eq!(weights[4], 60);
    }
}

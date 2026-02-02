//! Statistics Collector
//!
//! Collects per-link statistics for bandwidth estimation, RTT measurement,
//! and health monitoring.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::warn;
use chainlightning_common::NUM_LINKS;

/// Lock-free counters for the hot path (per-link senders/receivers)
pub struct AtomicCounters {
    tx_bytes: Vec<AtomicU64>,
    tx_packets: Vec<AtomicU64>,
    rx_bytes: Vec<AtomicU64>,
    rx_packets: Vec<AtomicU64>,
}

impl AtomicCounters {
    pub fn new(num_links: usize) -> Self {
        Self {
            tx_bytes: (0..num_links).map(|_| AtomicU64::new(0)).collect(),
            tx_packets: (0..num_links).map(|_| AtomicU64::new(0)).collect(),
            rx_bytes: (0..num_links).map(|_| AtomicU64::new(0)).collect(),
            rx_packets: (0..num_links).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// Record TX (called from per-link sender, no lock needed)
    pub fn record_tx(&self, link_id: usize, bytes: usize) {
        if link_id < self.tx_bytes.len() {
            self.tx_bytes[link_id].fetch_add(bytes as u64, Ordering::Relaxed);
            self.tx_packets[link_id].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record RX (called from per-link receiver, no lock needed)
    pub fn record_rx(&self, link_id: usize, bytes: usize) {
        if link_id < self.rx_bytes.len() {
            self.rx_bytes[link_id].fetch_add(bytes as u64, Ordering::Relaxed);
            self.rx_packets[link_id].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Drain accumulated TX counters for a link (called from 1/sec tick)
    pub fn drain_tx(&self, link_id: usize) -> (u64, u64) {
        let bytes = self.tx_bytes[link_id].swap(0, Ordering::Relaxed);
        let packets = self.tx_packets[link_id].swap(0, Ordering::Relaxed);
        (bytes, packets)
    }

    /// Drain accumulated RX counters for a link (called from 1/sec tick)
    pub fn drain_rx(&self, link_id: usize) -> (u64, u64) {
        let bytes = self.rx_bytes[link_id].swap(0, Ordering::Relaxed);
        let packets = self.rx_packets[link_id].swap(0, Ordering::Relaxed);
        (bytes, packets)
    }
}

/// Per-link statistics
#[derive(Debug, Clone)]
pub struct LinkStats {
    /// Link ID
    pub id: usize,
    /// TX bytes in current window
    pub tx_bytes: u64,
    /// TX packets in current window
    pub tx_packets: u64,
    /// RX bytes in current window
    pub rx_bytes: u64,
    /// RX packets in current window
    pub rx_packets: u64,
    /// RTT samples (microseconds)
    pub rtt_samples: VecDeque<u64>,
    /// Smoothed RTT (EWMA, microseconds)
    pub smoothed_rtt_us: u64,
    /// Estimated bandwidth (bytes/sec)
    pub bandwidth_bps: u64,
    /// Packet loss ratio (0.0 - 1.0)
    pub loss_ratio: f32,
    /// Link health state
    pub healthy: bool,
    /// Pending bytes awaiting ACK
    pub pending_bytes: u64,
    /// Last activity timestamp
    pub last_activity: Instant,
    /// Window start time
    window_start: Instant,
}

impl LinkStats {
    pub fn new(id: usize) -> Self {
        Self {
            id,
            tx_bytes: 0,
            tx_packets: 0,
            rx_bytes: 0,
            rx_packets: 0,
            rtt_samples: VecDeque::with_capacity(100),
            smoothed_rtt_us: 0,
            bandwidth_bps: 0,
            loss_ratio: 0.0,
            healthy: true,
            pending_bytes: 0,
            last_activity: Instant::now(),
            window_start: Instant::now(),
        }
    }

    /// Record transmitted bytes
    pub fn record_tx(&mut self, bytes: usize) {
        self.tx_bytes += bytes as u64;
        self.tx_packets += 1;
        self.pending_bytes += bytes as u64;
        self.last_activity = Instant::now();
    }

    /// Record received bytes
    pub fn record_rx(&mut self, bytes: usize) {
        self.rx_bytes += bytes as u64;
        self.rx_packets += 1;
        self.last_activity = Instant::now();
    }

    /// Record RTT sample
    pub fn record_rtt(&mut self, rtt_us: u64, alpha: f64) {
        // Add to samples
        self.rtt_samples.push_back(rtt_us);
        if self.rtt_samples.len() > 100 {
            self.rtt_samples.pop_front();
        }

        // EWMA update
        if self.smoothed_rtt_us == 0 {
            self.smoothed_rtt_us = rtt_us;
        } else {
            self.smoothed_rtt_us = ((1.0 - alpha) * self.smoothed_rtt_us as f64
                + alpha * rtt_us as f64) as u64;
        }
    }

    /// Record ACK (for pending bytes tracking)
    pub fn record_ack(&mut self, bytes: u64) {
        self.pending_bytes = self.pending_bytes.saturating_sub(bytes);
    }

    /// Update bandwidth estimate
    pub fn update_bandwidth(&mut self, window: Duration) {
        let elapsed = self.window_start.elapsed();
        if elapsed >= window {
            // Calculate bandwidth from TX in this window
            let secs = elapsed.as_secs_f64();
            if secs > 0.0 {
                self.bandwidth_bps = (self.tx_bytes as f64 / secs) as u64;
            }

            // Reset window
            self.tx_bytes = 0;
            self.tx_packets = 0;
            self.rx_bytes = 0;
            self.rx_packets = 0;
            self.window_start = Instant::now();
        }
    }

    /// Update health status
    pub fn update_health(&mut self, _timeout: Duration) {
        // Only mark unhealthy if we have evidence of problems
        // Don't mark unhealthy purely due to inactivity (chicken-and-egg problem)
        let high_loss = self.loss_ratio > 0.5;
        let very_high_rtt = self.smoothed_rtt_us > 500_000; // 500ms

        // If we've never sent on this link, assume it's healthy
        // Only mark unhealthy if we have actual problem indicators
        self.healthy = !high_loss && !very_high_rtt;
    }

    /// Get current bandwidth in Mbps
    pub fn bandwidth_mbps(&self) -> f64 {
        self.bandwidth_bps as f64 * 8.0 / 1_000_000.0
    }

    /// Get smoothed RTT in milliseconds
    pub fn rtt_ms(&self) -> f64 {
        self.smoothed_rtt_us as f64 / 1000.0
    }
}

/// Statistics collector for all links
pub struct StatsCollector {
    /// Per-link stats
    pub links: Vec<LinkStats>,
    /// EWMA alpha for RTT smoothing
    ewma_alpha: f64,
    /// Bandwidth calculation window
    bandwidth_window: Duration,
    /// Health check timeout
    health_timeout: Duration,
    /// Lock-free counters shared with hot path tasks
    pub atomic: Arc<AtomicCounters>,
}

impl StatsCollector {
    pub fn new(num_links: usize, ewma_alpha: f64, bandwidth_window_ms: u64) -> Self {
        Self {
            links: (0..num_links).map(LinkStats::new).collect(),
            ewma_alpha,
            bandwidth_window: Duration::from_millis(bandwidth_window_ms),
            health_timeout: Duration::from_secs(5),
            atomic: Arc::new(AtomicCounters::new(num_links)),
        }
    }

    /// Get shared atomic counters for hot path tasks
    pub fn atomic_counters(&self) -> Arc<AtomicCounters> {
        self.atomic.clone()
    }

    /// Record TX on a link
    pub fn record_tx(&mut self, link_id: usize, bytes: usize) {
        if let Some(stats) = self.links.get_mut(link_id) {
            stats.record_tx(bytes);
        }
    }

    /// Record RX on a link
    pub fn record_rx(&mut self, link_id: usize, bytes: usize) {
        if let Some(stats) = self.links.get_mut(link_id) {
            stats.record_rx(bytes);
        }
    }

    /// Record RTT measurement
    pub fn record_rtt(&mut self, link_id: usize, rtt_us: u64) {
        if let Some(stats) = self.links.get_mut(link_id) {
            stats.record_rtt(rtt_us, self.ewma_alpha);
        }
    }

    /// Record ACK
    pub fn record_ack(&mut self, link_id: usize, bytes: u64) {
        if let Some(stats) = self.links.get_mut(link_id) {
            stats.record_ack(bytes);
        }
    }

    /// Periodic update (call every second or so).
    /// Folds atomic counters from hot path into per-link stats.
    /// Returns per-link deltas (tx_bytes, rx_bytes) for the caller to fold into rate controller.
    pub fn tick(&mut self) -> Vec<(u64, u64)> {
        let mut deltas = Vec::with_capacity(self.links.len());

        // Drain atomic counters (accumulated by per-link sender/receiver tasks)
        for (i, stats) in self.links.iter_mut().enumerate() {
            let (tx_bytes, tx_packets) = self.atomic.drain_tx(i);
            let (rx_bytes, rx_packets) = self.atomic.drain_rx(i);

            if tx_bytes > 0 || rx_bytes > 0 {
                warn!("DRAIN L{}: tx={}B/{}pkt rx={}B/{}pkt accum_tx={} bw_bps={}",
                    i, tx_bytes, tx_packets, rx_bytes, rx_packets,
                    stats.tx_bytes + tx_bytes, stats.bandwidth_bps);
                stats.last_activity = Instant::now();
            }
            stats.tx_bytes += tx_bytes;
            stats.tx_packets += tx_packets;
            stats.pending_bytes += tx_bytes;
            stats.rx_bytes += rx_bytes;
            stats.rx_packets += rx_packets;

            deltas.push((tx_bytes, rx_bytes));
        }

        for stats in &mut self.links {
            let pre_bw = stats.bandwidth_bps;
            stats.update_bandwidth(self.bandwidth_window);
            if pre_bw != stats.bandwidth_bps {
                warn!("BW_UPDATE L{}: {}bps -> {}bps (window={:?})",
                    stats.id, pre_bw, stats.bandwidth_bps, self.bandwidth_window);
            }
            stats.update_health(self.health_timeout);
        }

        deltas
    }

    /// Get bandwidths for all links (bytes/sec)
    pub fn bandwidths(&self) -> Vec<u64> {
        self.links.iter().map(|l| l.bandwidth_bps).collect()
    }

    /// Get RTTs for all links (microseconds)
    pub fn rtts(&self) -> Vec<u64> {
        self.links.iter().map(|l| l.smoothed_rtt_us).collect()
    }

    /// Get health status for all links
    pub fn health(&self) -> Vec<bool> {
        self.links.iter().map(|l| l.healthy).collect()
    }

    /// Get total bandwidth (all links, bytes/sec)
    pub fn total_bandwidth(&self) -> u64 {
        self.links.iter()
            .filter(|l| l.healthy)
            .map(|l| l.bandwidth_bps)
            .sum()
    }

    /// Generate summary for logging
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        for stats in &self.links {
            let status = if stats.healthy { "OK" } else { "DOWN" };
            parts.push(format!(
                "L{}[{:.1}Mbps/{:.1}ms/{}]",
                stats.id,
                stats.bandwidth_mbps(),
                stats.rtt_ms(),
                status
            ));
        }
        parts.join(" ")
    }
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self::new(NUM_LINKS, 0.2, 1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rtt_ewma() {
        let mut stats = LinkStats::new(0);

        // First sample sets the value
        stats.record_rtt(100_000, 0.2);
        assert_eq!(stats.smoothed_rtt_us, 100_000);

        // Subsequent samples are smoothed
        stats.record_rtt(200_000, 0.2);
        // 0.8 * 100_000 + 0.2 * 200_000 = 120_000
        assert_eq!(stats.smoothed_rtt_us, 120_000);
    }

    #[test]
    fn test_stats_collector() {
        let mut collector = StatsCollector::new(5, 0.2, 100);

        collector.record_tx(0, 1000);
        collector.record_tx(1, 2000);
        collector.record_rtt(0, 20_000);
        collector.record_rtt(1, 40_000);

        // Check RTT was recorded
        assert_eq!(collector.links[0].smoothed_rtt_us, 20_000);
        assert_eq!(collector.links[1].smoothed_rtt_us, 40_000);
    }
}

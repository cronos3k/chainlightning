//! Metrics collection for performance measurement and A/B testing.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use chrono::{DateTime, Utc};

use crate::NUM_LINKS;

/// Snapshot of system metrics at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metrics {
    pub timestamp: DateTime<Utc>,

    // Throughput (bytes/sec)
    pub throughput_down: f64,
    pub throughput_up: f64,

    // Throughput (Mbps) - convenience fields
    pub throughput_down_mbps: f64,
    pub throughput_up_mbps: f64,

    // Raw bytes
    pub bytes_sent: u64,
    pub bytes_received: u64,

    // Latency (milliseconds)
    pub latency_avg_ms: f64,
    pub latency_min_ms: f64,
    pub latency_max_ms: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,

    // Latency (microseconds) - for precise measurements
    pub latency_p50_us: u64,
    pub latency_p99_us: u64,

    // Packet statistics
    pub packets_sent: u64,
    pub packets_received: u64,
    pub packets_lost: u64,
    pub loss_ratio: f64,

    // Chunk statistics
    pub chunks_sent: u64,
    pub chunks_received: u64,
    pub chunks_reordered: u64,
    pub reorder_ratio: f64,

    // Gap statistics (missing sequences)
    pub gaps_detected: u64,
    pub gap_ratio: f64,

    // Resource usage
    pub cpu_usage_percent: f64,
    pub memory_usage_bytes: u64,

    // Per-link metrics
    pub links: Vec<LinkMetrics>,

    // Flow statistics
    pub active_flows: usize,
    pub single_link_flows: usize,
    pub multi_link_flows: usize,
}

/// Per-link metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkMetrics {
    pub link_id: usize,

    // Throughput
    pub throughput_down: f64,
    pub throughput_up: f64,

    // Latency
    pub rtt_avg_ms: f64,
    pub rtt_min_ms: f64,
    pub rtt_max_ms: f64,
    pub jitter_ms: f64,

    // Reliability
    pub packets_sent: u64,
    pub packets_acked: u64,
    pub loss_ratio: f64,

    // State
    pub is_active: bool,
    pub bandwidth_estimate: f64,  // bytes/sec
}

impl Default for LinkMetrics {
    fn default() -> Self {
        Self {
            link_id: 0,
            throughput_down: 0.0,
            throughput_up: 0.0,
            rtt_avg_ms: 0.0,
            rtt_min_ms: 0.0,
            rtt_max_ms: 0.0,
            jitter_ms: 0.0,
            packets_sent: 0,
            packets_acked: 0,
            loss_ratio: 0.0,
            is_active: true,
            bandwidth_estimate: 0.0,
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            timestamp: Utc::now(),
            throughput_down: 0.0,
            throughput_up: 0.0,
            throughput_down_mbps: 0.0,
            throughput_up_mbps: 0.0,
            bytes_sent: 0,
            bytes_received: 0,
            latency_avg_ms: 0.0,
            latency_min_ms: 0.0,
            latency_max_ms: 0.0,
            latency_p50_ms: 0.0,
            latency_p95_ms: 0.0,
            latency_p99_ms: 0.0,
            latency_p50_us: 0,
            latency_p99_us: 0,
            packets_sent: 0,
            packets_received: 0,
            packets_lost: 0,
            loss_ratio: 0.0,
            chunks_sent: 0,
            chunks_received: 0,
            chunks_reordered: 0,
            reorder_ratio: 0.0,
            gaps_detected: 0,
            gap_ratio: 0.0,
            cpu_usage_percent: 0.0,
            memory_usage_bytes: 0,
            links: (0..NUM_LINKS).map(|i| {
                let mut m = LinkMetrics::default();
                m.link_id = i;
                m
            }).collect(),
            active_flows: 0,
            single_link_flows: 0,
            multi_link_flows: 0,
        }
    }
}

/// Raw counters that get converted to metrics
#[derive(Debug)]
struct RawCounters {
    // Bytes transferred in current window
    bytes_down: u64,
    bytes_up: u64,

    // Packet counts
    packets_sent: u64,
    packets_received: u64,
    packets_lost: u64,

    // Chunk counts
    chunks_sent: u64,
    chunks_received: u64,
    chunks_reordered: u64,
    gaps_detected: u64,

    // Latency samples (microseconds)
    latency_samples: Vec<u64>,

    // Per-link counters
    link_bytes_down: [u64; NUM_LINKS],
    link_bytes_up: [u64; NUM_LINKS],
    link_packets_sent: [u64; NUM_LINKS],
    link_packets_acked: [u64; NUM_LINKS],
    link_rtt_samples: [Vec<u64>; NUM_LINKS],  // microseconds

    // Flow counts
    active_flows: usize,
    single_link_flows: usize,
    multi_link_flows: usize,

    // Window start time
    window_start: Instant,
}

impl RawCounters {
    fn new() -> Self {
        Self {
            bytes_down: 0,
            bytes_up: 0,
            packets_sent: 0,
            packets_received: 0,
            packets_lost: 0,
            chunks_sent: 0,
            chunks_received: 0,
            chunks_reordered: 0,
            gaps_detected: 0,
            latency_samples: Vec::new(),
            link_bytes_down: [0; NUM_LINKS],
            link_bytes_up: [0; NUM_LINKS],
            link_packets_sent: [0; NUM_LINKS],
            link_packets_acked: [0; NUM_LINKS],
            link_rtt_samples: Default::default(),
            active_flows: 0,
            single_link_flows: 0,
            multi_link_flows: 0,
            window_start: Instant::now(),
        }
    }

    fn reset(&mut self) {
        self.bytes_down = 0;
        self.bytes_up = 0;
        self.latency_samples.clear();
        self.link_bytes_down = [0; NUM_LINKS];
        self.link_bytes_up = [0; NUM_LINKS];
        for samples in &mut self.link_rtt_samples {
            samples.clear();
        }
        self.window_start = Instant::now();
    }
}

/// Metrics collector - thread-safe accumulator
pub struct MetricsCollector {
    counters: Arc<RwLock<RawCounters>>,
    history: Arc<RwLock<VecDeque<Metrics>>>,
    max_history: usize,
}

impl MetricsCollector {
    /// Create a new metrics collector with default history size
    pub fn new() -> Self {
        Self::with_history(1000)
    }

    /// Create a new metrics collector with custom history size
    pub fn with_history(max_history: usize) -> Self {
        Self {
            counters: Arc::new(RwLock::new(RawCounters::new())),
            history: Arc::new(RwLock::new(VecDeque::with_capacity(max_history))),
            max_history,
        }
    }

    // === Convenience methods for common operations ===

    /// Record a packet transmitted (includes bytes)
    pub fn record_packet_tx(&self, bytes: usize) {
        if let Ok(mut c) = self.counters.write() {
            c.packets_sent += 1;
            c.bytes_up += bytes as u64;
        }
    }

    /// Record a packet received (includes bytes)
    pub fn record_packet_rx(&self, bytes: usize) {
        if let Ok(mut c) = self.counters.write() {
            c.packets_received += 1;
            c.bytes_down += bytes as u64;
        }
    }

    /// Record a chunk transmitted
    pub fn record_chunk_tx(&self, bytes: usize) {
        if let Ok(mut c) = self.counters.write() {
            c.chunks_sent += 1;
            c.bytes_up += bytes as u64;
        }
    }

    /// Record a chunk received
    pub fn record_chunk_rx(&self, bytes: usize) {
        if let Ok(mut c) = self.counters.write() {
            c.chunks_received += 1;
            c.bytes_down += bytes as u64;
        }
    }

    // === Recording methods (called from hot paths) ===

    pub fn record_bytes_down(&self, bytes: u64) {
        if let Ok(mut c) = self.counters.write() {
            c.bytes_down += bytes;
        }
    }

    pub fn record_bytes_up(&self, bytes: u64) {
        if let Ok(mut c) = self.counters.write() {
            c.bytes_up += bytes;
        }
    }

    pub fn record_packet_sent(&self, link_id: usize) {
        if let Ok(mut c) = self.counters.write() {
            c.packets_sent += 1;
            if link_id < NUM_LINKS {
                c.link_packets_sent[link_id] += 1;
            }
        }
    }

    pub fn record_packet_received(&self) {
        if let Ok(mut c) = self.counters.write() {
            c.packets_received += 1;
        }
    }

    pub fn record_packet_lost(&self) {
        if let Ok(mut c) = self.counters.write() {
            c.packets_lost += 1;
        }
    }

    pub fn record_packet_acked(&self, link_id: usize) {
        if let Ok(mut c) = self.counters.write() {
            if link_id < NUM_LINKS {
                c.link_packets_acked[link_id] += 1;
            }
        }
    }

    pub fn record_chunk_sent(&self) {
        if let Ok(mut c) = self.counters.write() {
            c.chunks_sent += 1;
        }
    }

    pub fn record_chunk_received(&self) {
        if let Ok(mut c) = self.counters.write() {
            c.chunks_received += 1;
        }
    }

    pub fn record_chunk_reordered(&self) {
        if let Ok(mut c) = self.counters.write() {
            c.chunks_reordered += 1;
        }
    }

    pub fn record_gap(&self) {
        if let Ok(mut c) = self.counters.write() {
            c.gaps_detected += 1;
        }
    }

    pub fn record_latency_us(&self, latency_us: u64) {
        if let Ok(mut c) = self.counters.write() {
            c.latency_samples.push(latency_us);
        }
    }

    pub fn record_link_rtt_us(&self, link_id: usize, rtt_us: u64) {
        if let Ok(mut c) = self.counters.write() {
            if link_id < NUM_LINKS {
                c.link_rtt_samples[link_id].push(rtt_us);
            }
        }
    }

    pub fn record_link_bytes_down(&self, link_id: usize, bytes: u64) {
        if let Ok(mut c) = self.counters.write() {
            if link_id < NUM_LINKS {
                c.link_bytes_down[link_id] += bytes;
            }
        }
    }

    pub fn record_link_bytes_up(&self, link_id: usize, bytes: u64) {
        if let Ok(mut c) = self.counters.write() {
            if link_id < NUM_LINKS {
                c.link_bytes_up[link_id] += bytes;
            }
        }
    }

    pub fn record_flow_counts(&self, active: usize, single: usize, multi: usize) {
        if let Ok(mut c) = self.counters.write() {
            c.active_flows = active;
            c.single_link_flows = single;
            c.multi_link_flows = multi;
        }
    }

    // === Snapshot and analysis ===

    /// Take a snapshot of current metrics and reset counters
    pub fn snapshot(&self) -> Metrics {
        let (counters, elapsed) = {
            let mut c = self.counters.write().unwrap();
            let elapsed = c.window_start.elapsed();
            let counters = std::mem::replace(&mut *c, RawCounters::new());
            (counters, elapsed)
        };

        let elapsed_secs = elapsed.as_secs_f64().max(0.001);

        // Calculate throughput
        let throughput_down = counters.bytes_down as f64 / elapsed_secs;
        let throughput_up = counters.bytes_up as f64 / elapsed_secs;

        // Calculate latency percentiles
        let (latency_avg, latency_min, latency_max, latency_p50, latency_p95, latency_p99) =
            calculate_percentiles(&counters.latency_samples);

        // Calculate loss ratio
        let total_packets = counters.packets_sent.max(1);
        let loss_ratio = counters.packets_lost as f64 / total_packets as f64;

        // Calculate reorder and gap ratios
        let total_chunks = counters.chunks_received.max(1);
        let reorder_ratio = counters.chunks_reordered as f64 / total_chunks as f64;
        let gap_ratio = counters.gaps_detected as f64 / total_chunks as f64;

        // Per-link metrics
        let links: Vec<LinkMetrics> = (0..NUM_LINKS).map(|i| {
            let (rtt_avg, rtt_min, rtt_max, _, _, _) =
                calculate_percentiles(&counters.link_rtt_samples[i]);

            let jitter = if counters.link_rtt_samples[i].len() > 1 {
                calculate_jitter(&counters.link_rtt_samples[i])
            } else {
                0.0
            };

            let sent = counters.link_packets_sent[i].max(1);
            let link_loss = 1.0 - (counters.link_packets_acked[i] as f64 / sent as f64);

            LinkMetrics {
                link_id: i,
                throughput_down: counters.link_bytes_down[i] as f64 / elapsed_secs,
                throughput_up: counters.link_bytes_up[i] as f64 / elapsed_secs,
                rtt_avg_ms: rtt_avg,
                rtt_min_ms: rtt_min,
                rtt_max_ms: rtt_max,
                jitter_ms: jitter,
                packets_sent: counters.link_packets_sent[i],
                packets_acked: counters.link_packets_acked[i],
                loss_ratio: link_loss.max(0.0),
                is_active: counters.link_packets_sent[i] > 0,
                bandwidth_estimate: counters.link_bytes_up[i] as f64 / elapsed_secs,
            }
        }).collect();

        // Get CPU and memory (platform-specific)
        let (cpu_usage, memory_usage) = get_resource_usage();

        // Get latency in microseconds for precise measurements
        let latency_p50_us = counters.latency_samples.get(counters.latency_samples.len() / 2).copied().unwrap_or(0);
        let latency_p99_us = counters.latency_samples.get((counters.latency_samples.len() as f64 * 0.99) as usize).copied().unwrap_or(0);

        let metrics = Metrics {
            timestamp: Utc::now(),
            throughput_down,
            throughput_up,
            throughput_down_mbps: throughput_down * 8.0 / 1_000_000.0,
            throughput_up_mbps: throughput_up * 8.0 / 1_000_000.0,
            bytes_sent: counters.bytes_up,
            bytes_received: counters.bytes_down,
            latency_avg_ms: latency_avg,
            latency_min_ms: latency_min,
            latency_max_ms: latency_max,
            latency_p50_ms: latency_p50,
            latency_p95_ms: latency_p95,
            latency_p99_ms: latency_p99,
            latency_p50_us,
            latency_p99_us,
            packets_sent: counters.packets_sent,
            packets_received: counters.packets_received,
            packets_lost: counters.packets_lost,
            loss_ratio,
            chunks_sent: counters.chunks_sent,
            chunks_received: counters.chunks_received,
            chunks_reordered: counters.chunks_reordered,
            reorder_ratio,
            gaps_detected: counters.gaps_detected,
            gap_ratio,
            cpu_usage_percent: cpu_usage,
            memory_usage_bytes: memory_usage,
            links,
            active_flows: counters.active_flows,
            single_link_flows: counters.single_link_flows,
            multi_link_flows: counters.multi_link_flows,
        };

        // Store in history
        {
            let mut history = self.history.write().unwrap();
            if history.len() >= self.max_history {
                history.pop_front();
            }
            history.push_back(metrics.clone());
        }

        metrics
    }

    /// Get metrics history
    pub fn history(&self) -> Vec<Metrics> {
        self.history.read().unwrap().iter().cloned().collect()
    }

    /// Clear history
    pub fn clear_history(&self) {
        self.history.write().unwrap().clear();
    }

    /// Calculate summary statistics over history
    pub fn summarize(&self) -> MetricsSummary {
        let history = self.history.read().unwrap();

        if history.is_empty() {
            return MetricsSummary::default();
        }

        let n = history.len() as f64;

        let avg_throughput_down = history.iter().map(|m| m.throughput_down).sum::<f64>() / n;
        let avg_throughput_up = history.iter().map(|m| m.throughput_up).sum::<f64>() / n;
        let avg_latency = history.iter().map(|m| m.latency_avg_ms).sum::<f64>() / n;
        let avg_loss = history.iter().map(|m| m.loss_ratio).sum::<f64>() / n;
        let avg_reorder = history.iter().map(|m| m.reorder_ratio).sum::<f64>() / n;
        let avg_cpu = history.iter().map(|m| m.cpu_usage_percent).sum::<f64>() / n;

        let max_throughput_down = history.iter().map(|m| m.throughput_down).fold(0.0f64, f64::max);
        let max_throughput_up = history.iter().map(|m| m.throughput_up).fold(0.0f64, f64::max);
        let max_latency = history.iter().map(|m| m.latency_max_ms).fold(0.0f64, f64::max);

        MetricsSummary {
            sample_count: history.len(),
            avg_throughput_down_mbps: avg_throughput_down * 8.0 / 1_000_000.0,
            avg_throughput_up_mbps: avg_throughput_up * 8.0 / 1_000_000.0,
            max_throughput_down_mbps: max_throughput_down * 8.0 / 1_000_000.0,
            max_throughput_up_mbps: max_throughput_up * 8.0 / 1_000_000.0,
            avg_latency_ms: avg_latency,
            max_latency_ms: max_latency,
            avg_loss_ratio: avg_loss,
            avg_reorder_ratio: avg_reorder,
            avg_cpu_percent: avg_cpu,
        }
    }
}

/// Summary statistics from metrics history
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsSummary {
    pub sample_count: usize,
    pub avg_throughput_down_mbps: f64,
    pub avg_throughput_up_mbps: f64,
    pub max_throughput_down_mbps: f64,
    pub max_throughput_up_mbps: f64,
    pub avg_latency_ms: f64,
    pub max_latency_ms: f64,
    pub avg_loss_ratio: f64,
    pub avg_reorder_ratio: f64,
    pub avg_cpu_percent: f64,
}

// === Helper functions ===

fn calculate_percentiles(samples: &[u64]) -> (f64, f64, f64, f64, f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    }

    let mut sorted: Vec<f64> = samples.iter().map(|&s| s as f64 / 1000.0).collect(); // us -> ms
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let n = sorted.len();
    let avg = sorted.iter().sum::<f64>() / n as f64;
    let min = sorted[0];
    let max = sorted[n - 1];
    let p50 = sorted[((n - 1) as f64 * 0.50) as usize];
    let p95 = sorted[((n - 1) as f64 * 0.95) as usize];
    let p99 = sorted[((n - 1) as f64 * 0.99) as usize];

    (avg, min, max, p50, p95, p99)
}

fn calculate_jitter(samples: &[u64]) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }

    let mut diffs = Vec::with_capacity(samples.len() - 1);
    for i in 1..samples.len() {
        let diff = (samples[i] as i64 - samples[i-1] as i64).abs() as f64;
        diffs.push(diff);
    }

    let avg_diff = diffs.iter().sum::<f64>() / diffs.len() as f64;
    avg_diff / 1000.0  // us -> ms
}

#[cfg(target_os = "linux")]
fn get_resource_usage() -> (f64, u64) {
    use std::fs;

    // CPU usage from /proc/stat - simplified, returns 0 for now
    // In production, would track delta between readings
    let cpu = 0.0;

    // Memory from /proc/self/status
    let memory = fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|content| {
            content.lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(|line| {
                    line.split_whitespace().nth(1)?.parse::<u64>().ok()
                })
        })
        .map(|kb| kb * 1024)
        .unwrap_or(0);

    (cpu, memory)
}

#[cfg(not(target_os = "linux"))]
fn get_resource_usage() -> (f64, u64) {
    (0.0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector() {
        let collector = MetricsCollector::with_history(100);

        collector.record_bytes_down(1000);
        collector.record_bytes_up(500);
        collector.record_packet_sent(0);
        collector.record_packet_received();
        collector.record_latency_us(10000);  // 10ms

        let metrics = collector.snapshot();

        assert!(metrics.throughput_down > 0.0);
        assert!(metrics.throughput_up > 0.0);
        assert!(metrics.packets_sent == 1);
        assert!(metrics.packets_received == 1);
    }

    #[test]
    fn test_percentiles() {
        let samples: Vec<u64> = (1..=100).map(|i| i * 1000).collect();  // 1ms to 100ms
        let (avg, min, max, p50, p95, p99) = calculate_percentiles(&samples);

        assert!((avg - 50.5).abs() < 0.1);
        assert!((min - 1.0).abs() < 0.01);
        assert!((max - 100.0).abs() < 0.01);
        assert!((p50 - 50.0).abs() < 1.0);
        assert!((p95 - 95.0).abs() < 1.0);
    }
}

//! Chunk Aggregator
//!
//! Aggregates IP packets into chunks for efficient multi-link transmission.
//! Chunk size is dynamic based on Bandwidth-Delay Product (BDP).

use std::collections::VecDeque;
use std::time::{Duration, Instant};
use chainlightning_common::config::ChunkAggregatorConfig;
use chainlightning_common::protocol::{ChunkId, FlowId};

/// A chunk of aggregated packets
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Unique chunk identifier
    pub id: ChunkId,
    /// Flow ID (if single-flow chunk) or None for aggregated
    pub flow_id: Option<FlowId>,
    /// Aggregated packet data
    pub data: Vec<u8>,
    /// Individual packet boundaries (offsets within data)
    pub packet_offsets: Vec<usize>,
    /// Creation timestamp
    pub created_at: Instant,
    /// Target link (None = distribute across all)
    pub target_link: Option<usize>,
}

impl Chunk {
    /// Create a new empty chunk
    pub fn new(id: ChunkId) -> Self {
        Self {
            id,
            flow_id: None,
            data: Vec::new(),
            packet_offsets: Vec::new(),
            created_at: Instant::now(),
            target_link: None,
        }
    }

    /// Add a packet to the chunk
    pub fn add_packet(&mut self, packet: &[u8]) {
        self.packet_offsets.push(self.data.len());
        // Prepend packet length (2 bytes)
        let len = packet.len() as u16;
        self.data.extend_from_slice(&len.to_be_bytes());
        self.data.extend_from_slice(packet);
    }

    /// Get number of packets in chunk
    pub fn packet_count(&self) -> usize {
        self.packet_offsets.len()
    }

    /// Get total chunk size
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Check if chunk is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Extract individual packets from chunk data
    pub fn extract_packets(&self) -> Vec<Vec<u8>> {
        let mut packets = Vec::new();
        let mut offset = 0;

        while offset + 2 <= self.data.len() {
            let len = u16::from_be_bytes([self.data[offset], self.data[offset + 1]]) as usize;
            offset += 2;

            if offset + len <= self.data.len() {
                packets.push(self.data[offset..offset + len].to_vec());
                offset += len;
            } else {
                break;
            }
        }

        packets
    }
}

/// Chunk aggregator
pub struct ChunkAggregator {
    /// Configuration
    config: ChunkAggregatorConfig,
    /// Current chunk being built
    current_chunk: Chunk,
    /// Next chunk ID
    next_chunk_id: ChunkId,
    /// Ready chunks queue
    ready_chunks: VecDeque<Chunk>,
    /// Link stats for BDP calculation (bandwidth_bps, rtt_ms per link)
    link_stats: Vec<(u64, u32)>,
    /// Computed optimal chunk size
    optimal_chunk_size: usize,
}

impl ChunkAggregator {
    pub fn new(config: ChunkAggregatorConfig, num_links: usize) -> Self {
        let min_chunk_size = config.min_chunk_size;
        Self {
            config,
            current_chunk: Chunk::new(ChunkId(0)),
            next_chunk_id: ChunkId(1),
            ready_chunks: VecDeque::new(),
            link_stats: vec![(0, 0); num_links],
            optimal_chunk_size: min_chunk_size,
        }
    }

    /// Update link statistics for BDP calculation
    pub fn update_link_stats(&mut self, link_id: usize, bandwidth_bps: u64, rtt_ms: u32) {
        if link_id < self.link_stats.len() {
            self.link_stats[link_id] = (bandwidth_bps, rtt_ms);
            self.recalculate_chunk_size();
        }
    }

    /// Recalculate optimal chunk size based on BDP
    fn recalculate_chunk_size(&mut self) {
        // Calculate BDP for each link and use the maximum
        let mut max_bdp: usize = 0;

        for (bandwidth_bps, rtt_ms) in &self.link_stats {
            if *bandwidth_bps > 0 && *rtt_ms > 0 {
                // BDP = bandwidth * RTT
                // bandwidth is bytes/sec, RTT is ms
                // BDP in bytes = (bandwidth_bps * rtt_ms) / 1000
                let bdp = (*bandwidth_bps as f64 * *rtt_ms as f64 / 1000.0) as usize;
                max_bdp = max_bdp.max(bdp);
            }
        }

        if max_bdp > 0 {
            // Apply multiplier and clamp to configured bounds
            let target = (max_bdp as f64 * self.config.bdp_multiplier) as usize;
            self.optimal_chunk_size = target
                .max(self.config.min_chunk_size)
                .min(self.config.max_chunk_size);

            tracing::trace!(
                bdp = max_bdp,
                optimal_size = self.optimal_chunk_size,
                "Recalculated chunk size"
            );
        }
    }

    /// Add a packet to be aggregated
    /// Returns Some(Chunk) if a chunk is ready to send
    pub fn add_packet(&mut self, packet: &[u8], flow_id: Option<FlowId>) -> Option<Chunk> {
        // Check if adding this packet would exceed chunk size
        let new_size = self.current_chunk.size() + 2 + packet.len(); // 2 bytes for length prefix

        if new_size > self.optimal_chunk_size && !self.current_chunk.is_empty() {
            // Current chunk is full - finalize and start new one
            let ready_chunk = self.finalize_current_chunk();
            self.current_chunk.add_packet(packet);
            if flow_id.is_some() {
                self.current_chunk.flow_id = flow_id;
            }
            return Some(ready_chunk);
        }

        // Add to current chunk
        self.current_chunk.add_packet(packet);
        if flow_id.is_some() && self.current_chunk.flow_id.is_none() {
            self.current_chunk.flow_id = flow_id;
        }

        // Check if chunk has reached optimal size
        if self.current_chunk.size() >= self.optimal_chunk_size {
            return Some(self.finalize_current_chunk());
        }

        None
    }

    /// Force flush current chunk (due to timeout or explicit request)
    pub fn flush(&mut self) -> Option<Chunk> {
        if self.current_chunk.is_empty() {
            return None;
        }
        Some(self.finalize_current_chunk())
    }

    /// Check if timeout has elapsed and flush if needed
    pub fn check_timeout(&mut self) -> Option<Chunk> {
        if self.current_chunk.is_empty() {
            return None;
        }

        let elapsed = self.current_chunk.created_at.elapsed();
        if elapsed >= Duration::from_millis(self.config.aggregation_timeout_ms) {
            return self.flush();
        }

        None
    }

    /// Finalize current chunk and prepare new one
    fn finalize_current_chunk(&mut self) -> Chunk {
        let mut ready = std::mem::replace(
            &mut self.current_chunk,
            Chunk::new(self.next_chunk_id)
        );
        ready.id = ChunkId(self.next_chunk_id.0.wrapping_sub(1));
        self.next_chunk_id = self.next_chunk_id.next();
        ready
    }

    /// Get current optimal chunk size
    pub fn optimal_chunk_size(&self) -> usize {
        self.optimal_chunk_size
    }

    /// Get pending data size
    pub fn pending_size(&self) -> usize {
        self.current_chunk.size()
    }

    /// Get next chunk ID (for announcements)
    pub fn next_id(&self) -> ChunkId {
        self.current_chunk.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_packet_roundtrip() {
        let mut chunk = Chunk::new(ChunkId(1));

        let packets = vec![
            vec![1, 2, 3, 4, 5],
            vec![10, 20, 30],
            vec![100; 100],
        ];

        for p in &packets {
            chunk.add_packet(p);
        }

        let extracted = chunk.extract_packets();
        assert_eq!(extracted.len(), packets.len());
        for (orig, ext) in packets.iter().zip(extracted.iter()) {
            assert_eq!(orig, ext);
        }
    }

    #[test]
    fn test_aggregator_flush_on_size() {
        let config = ChunkAggregatorConfig {
            min_chunk_size: 100,
            max_chunk_size: 1000,
            bdp_multiplier: 1.0,
            aggregation_timeout_ms: 50,
        };

        let mut agg = ChunkAggregator::new(config, 5);

        // Add small packets - should accumulate
        for _ in 0..5 {
            let result = agg.add_packet(&[0u8; 10], None);
            assert!(result.is_none());
        }

        // Add large packet to trigger flush
        let result = agg.add_packet(&[0u8; 80], None);
        assert!(result.is_some());

        let chunk = result.unwrap();
        assert_eq!(chunk.packet_count(), 5);
    }
}

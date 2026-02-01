//! Chunk Receiver
//!
//! Receives chunks from multiple links and reassembles them.
//! Uses a brief timeout-based reorder buffer to handle out-of-order arrival.
//! Key principle: NO head-of-line blocking - forward packets ASAP.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};
use chainlightning_common::config::ReceiverConfig;
use chainlightning_common::protocol::AnnouncePacket;
use crate::chunk_aggregator::Chunk;
use crate::flow_classifier::FlowMode;

/// Reassembly buffer slot
#[derive(Debug)]
struct BufferSlot {
    /// Chunk data (if received)
    chunk: Option<Chunk>,
    /// Announcement (if received before data)
    announcement: Option<AnnouncePacket>,
    /// Expected link
    expected_link: Option<usize>,
    /// Flow mode from wire (determines forwarding strategy)
    flow_mode: Option<FlowMode>,
    /// When slot was created
    created_at: Instant,
    /// Has been forwarded
    forwarded: bool,
}

impl BufferSlot {
    fn new() -> Self {
        Self {
            chunk: None,
            announcement: None,
            expected_link: None,
            flow_mode: None,
            created_at: Instant::now(),
            forwarded: false,
        }
    }

    fn is_ready(&self) -> bool {
        self.chunk.is_some() && !self.forwarded
    }

    fn is_timed_out(&self, timeout: Duration) -> bool {
        self.created_at.elapsed() > timeout
    }
}

/// Reassembly buffer for chunks
pub struct ReassemblyBuffer {
    /// Configuration
    config: ReceiverConfig,
    /// Buffer slots indexed by chunk ID
    slots: BTreeMap<u64, BufferSlot>,
    /// Next expected chunk ID (for in-order tracking)
    next_expected: u64,
    /// Highest chunk ID seen
    highest_seen: u64,
    /// Statistics
    chunks_received: u64,
    chunks_forwarded: u64,
    chunks_timeout: u64,
}

impl ReassemblyBuffer {
    pub fn new(config: ReceiverConfig) -> Self {
        Self {
            config,
            slots: BTreeMap::new(),
            next_expected: 0,
            highest_seen: 0,
            chunks_received: 0,
            chunks_forwarded: 0,
            chunks_timeout: 0,
        }
    }

    /// Record announcement (pre-allocates slot)
    pub fn record_announcement(&mut self, ann: AnnouncePacket) {
        let id = ann.chunk_id.0;

        // Update highest seen
        if id > self.highest_seen {
            self.highest_seen = id;
        }

        let slot = self.slots.entry(id).or_insert_with(BufferSlot::new);
        slot.announcement = Some(ann);
        slot.expected_link = Some(ann.link_id as usize);
    }

    /// Store received chunk with flow mode from wire
    pub fn store_chunk(&mut self, chunk: Chunk, flow_mode: FlowMode) {
        let id = chunk.id.0;

        // Update tracking
        if id > self.highest_seen {
            self.highest_seen = id;
        }
        self.chunks_received += 1;

        let slot = self.slots.entry(id).or_insert_with(BufferSlot::new);
        slot.flow_mode = Some(flow_mode);
        slot.chunk = Some(chunk);
    }

    /// Get all ready chunks using per-chunk mode-aware forwarding:
    /// - immediate_forward=true: forward ALL chunks immediately (bypass reorder buffer entirely)
    /// - Realtime/SingleLink chunks: immediate forward (bypass reorder buffer)
    /// - Late-arriving chunks (id < next_expected): immediate forward (better late than lost)
    /// - Bulk chunks: ordered delivery with reorder timeout
    pub fn drain_ready(&mut self) -> Vec<Chunk> {
        let mut ready = Vec::new();

        // Fast path: when immediate_forward is enabled, forward ALL ready chunks
        // regardless of flow_mode. This avoids reorder buffer head-of-line blocking
        // that kills throughput when Bulk-tagged chunks arrive out of order.
        if self.config.immediate_forward {
            let all_ready_ids: Vec<u64> = self.slots.iter()
                .filter(|(_, slot)| slot.is_ready())
                .map(|(id, _)| *id)
                .collect();

            for id in all_ready_ids {
                if let Some(mut slot) = self.slots.remove(&id) {
                    if let Some(chunk) = slot.chunk.take() {
                        ready.push(chunk);
                        self.chunks_forwarded += 1;
                    }
                }
                if id >= self.next_expected {
                    self.next_expected = id + 1;
                }
            }

            self.prune_old_slots();
            return ready;
        }

        let timeout = Duration::from_millis(self.config.reorder_timeout_ms);

        // Pass 1: Immediately forward chunks that bypass reorder:
        //   a) Realtime/SingleLink tagged chunks
        //   b) Late-arriving chunks (id < next_expected) — deliver rather than drop
        let bypass_ids: Vec<u64> = self.slots.iter()
            .filter(|(id, slot)| {
                if !slot.is_ready() { return false; }
                // Bypass for Realtime/SingleLink
                match slot.flow_mode {
                    Some(FlowMode::Realtime { .. }) | Some(FlowMode::SingleLink { .. }) => return true,
                    _ => {}
                }
                // Bypass for late arrivals (already past next_expected)
                if **id < self.next_expected {
                    return true;
                }
                false
            })
            .map(|(id, _)| *id)
            .collect();

        for id in bypass_ids {
            if let Some(mut slot) = self.slots.remove(&id) {
                if let Some(chunk) = slot.chunk.take() {
                    ready.push(chunk);
                    self.chunks_forwarded += 1;
                }
            }
            // Only advance next_expected for sequential bypass, never backward
            if id == self.next_expected {
                self.next_expected = id + 1;
            }
        }

        // Pass 2: Ordered delivery for Bulk chunks
        while let Some((&id, slot)) = self.slots.first_key_value() {
            // Safety: skip any chunks behind next_expected (shouldn't happen after Pass 1, but be safe)
            if id < self.next_expected {
                if let Some(mut slot) = self.slots.remove(&id) {
                    if let Some(chunk) = slot.chunk.take() {
                        ready.push(chunk);
                        self.chunks_forwarded += 1;
                    }
                } else {
                    self.slots.remove(&id);
                }
                continue;
            }

            // Gap detection: id > next_expected means chunks are missing
            if id > self.next_expected {
                if slot.is_timed_out(timeout) {
                    let skipped = id - self.next_expected;
                    self.chunks_timeout += skipped;
                    self.next_expected = id;
                    tracing::debug!(
                        gap_start = self.next_expected - skipped,
                        gap_end = id,
                        "Gap timeout, skipping {} chunks", skipped
                    );
                } else {
                    break;
                }
            }

            if slot.is_ready() {
                if let Some(mut slot) = self.slots.remove(&id) {
                    if let Some(chunk) = slot.chunk.take() {
                        ready.push(chunk);
                        self.chunks_forwarded += 1;
                    }
                }
                self.next_expected = id + 1;
            } else if slot.is_timed_out(timeout) {
                self.slots.remove(&id);
                self.chunks_timeout += 1;
                self.next_expected = id + 1;
                tracing::debug!(chunk_id = id, "Chunk timeout, skipping");
            } else {
                break;
            }
        }

        // Prune old slots
        self.prune_old_slots();

        ready
    }

    /// Prune slots that are way past expected
    fn prune_old_slots(&mut self) {
        // Keep at most max_buffer_chunks slots
        while self.slots.len() > self.config.max_buffer_chunks {
            if let Some((&id, _)) = self.slots.first_key_value() {
                self.slots.remove(&id);
                self.chunks_timeout += 1;
            }
        }

        // Remove very old slots
        let timeout = Duration::from_millis(self.config.reorder_timeout_ms * 10);
        let old_ids: Vec<u64> = self.slots.iter()
            .filter(|(_, slot)| slot.is_timed_out(timeout))
            .map(|(id, _)| *id)
            .collect();

        for id in old_ids {
            self.slots.remove(&id);
        }
    }

    /// Get buffer statistics
    pub fn stats(&self) -> ReassemblyStats {
        let buffered = self.slots.len();
        let ready = self.slots.values().filter(|s| s.is_ready()).count();

        ReassemblyStats {
            buffered,
            ready,
            next_expected: self.next_expected,
            highest_seen: self.highest_seen,
            chunks_received: self.chunks_received,
            chunks_forwarded: self.chunks_forwarded,
            chunks_timeout: self.chunks_timeout,
        }
    }
}

/// Reassembly statistics
#[derive(Debug, Clone)]
pub struct ReassemblyStats {
    pub buffered: usize,
    pub ready: usize,
    pub next_expected: u64,
    pub highest_seen: u64,
    pub chunks_received: u64,
    pub chunks_forwarded: u64,
    pub chunks_timeout: u64,
}

/// Chunk receiver combining multiple links
pub struct ChunkReceiver {
    /// Per-link buffers (for announcement tracking)
    announcement_link: HashMap<u64, usize>,
    /// Reassembly buffer
    buffer: ReassemblyBuffer,
}

impl ChunkReceiver {
    pub fn new(config: ReceiverConfig) -> Self {
        Self {
            announcement_link: HashMap::new(),
            buffer: ReassemblyBuffer::new(config),
        }
    }

    /// Handle announcement
    pub fn handle_announcement(&mut self, ann: AnnouncePacket) {
        self.announcement_link.insert(ann.chunk_id.0, ann.link_id as usize);
        self.buffer.record_announcement(ann);
    }

    /// Handle chunk data with flow mode from wire
    pub fn handle_chunk(&mut self, chunk: Chunk, flow_mode: FlowMode) {
        self.buffer.store_chunk(chunk, flow_mode);
    }

    /// Drain ready chunks
    pub fn drain(&mut self) -> Vec<Chunk> {
        self.buffer.drain_ready()
    }

    /// Update reorder timeout dynamically based on RTT spread
    pub fn set_reorder_timeout_ms(&mut self, timeout_ms: u64) {
        self.buffer.config.reorder_timeout_ms = timeout_ms;
    }

    /// Get stats
    pub fn stats(&self) -> ReassemblyStats {
        self.buffer.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chainlightning_common::ChunkId;

    #[test]
    fn test_realtime_immediate_forward() {
        let config = ReceiverConfig {
            reorder_timeout_ms: 100,
            max_buffer_chunks: 1000,
            immediate_forward: false,
        };

        let mut buffer = ReassemblyBuffer::new(config);
        let rt_mode = FlowMode::Realtime { link_id: 2 };

        // Store Realtime chunks out of order
        let mut chunk3 = Chunk::new(ChunkId(3));
        chunk3.add_packet(&[3, 3, 3]);

        let mut chunk1 = Chunk::new(ChunkId(1));
        chunk1.add_packet(&[1, 1, 1]);

        buffer.store_chunk(chunk3, rt_mode);
        buffer.store_chunk(chunk1, rt_mode);

        // Realtime chunks should drain immediately regardless of order
        let ready = buffer.drain_ready();
        assert_eq!(ready.len(), 2);
    }

    #[test]
    fn test_bulk_ordered_with_timeout() {
        let config = ReceiverConfig {
            reorder_timeout_ms: 10,
            max_buffer_chunks: 1000,
            immediate_forward: false,
        };

        let mut buffer = ReassemblyBuffer::new(config);
        let bulk_mode = FlowMode::Bulk;

        // Store chunk 1 (skipping 0) as Bulk
        let mut chunk1 = Chunk::new(ChunkId(1));
        chunk1.add_packet(&[1, 1, 1]);
        buffer.store_chunk(chunk1, bulk_mode);

        // Should not drain (waiting for 0 — Bulk uses ordered delivery)
        let ready = buffer.drain_ready();
        assert_eq!(ready.len(), 0);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(15));

        // Now should drain (chunk 0 timed out)
        let ready = buffer.drain_ready();
        assert_eq!(ready.len(), 1);
    }

    #[test]
    fn test_singlelink_bypasses_reorder() {
        let config = ReceiverConfig {
            reorder_timeout_ms: 100,
            max_buffer_chunks: 1000,
            immediate_forward: false,
        };

        let mut buffer = ReassemblyBuffer::new(config);
        let sl_mode = FlowMode::SingleLink { link_id: 0 };

        // Store SingleLink chunk out of order
        let mut chunk5 = Chunk::new(ChunkId(5));
        chunk5.add_packet(&[5, 5, 5]);
        buffer.store_chunk(chunk5, sl_mode);

        // SingleLink should forward immediately
        let ready = buffer.drain_ready();
        assert_eq!(ready.len(), 1);
    }
}

//! Wire protocol for ChainLightning v4.
//!
//! Chunk-based protocol with announcements for coordinated multi-link delivery.

use serde::{Deserialize, Serialize};

/// Message types
pub const MSG_DATA: u8 = 0x01;
pub const MSG_ANNOUNCE: u8 = 0x02;
pub const MSG_ACK: u8 = 0x03;
pub const MSG_STATS: u8 = 0x04;
pub const MSG_KEEPALIVE: u8 = 0x05;
pub const MSG_PROBE: u8 = 0x06;

/// Maximum chunk payload size (1MB)
pub const MAX_CHUNK_PAYLOAD: usize = 1_048_576;

/// Chunk header size: type(1) + chunk_id(8) + link_id(1) + total_size(4) + offset(4) + payload_len(2)
pub const CHUNK_HEADER_SIZE: usize = 20;

/// Announcement header size: type(1) + chunk_id(8) + link_id(1) + expected_size(4) + timestamp(8)
pub const ANNOUNCE_HEADER_SIZE: usize = 22;

/// Flow identifier (5-tuple hash)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FlowId(pub u64);

impl FlowId {
    /// Create flow ID from IP packet (5-tuple hash)
    pub fn from_packet(packet: &[u8]) -> Option<Self> {
        if packet.len() < 20 {
            return None;
        }

        // IPv4 header
        let version = (packet[0] >> 4) & 0x0F;
        if version != 4 {
            return None; // Only IPv4 for now
        }

        let ihl = (packet[0] & 0x0F) as usize * 4;
        let protocol = packet[9];
        let src_ip = u32::from_be_bytes([packet[12], packet[13], packet[14], packet[15]]);
        let dst_ip = u32::from_be_bytes([packet[16], packet[17], packet[18], packet[19]]);

        // Extract ports for TCP/UDP
        let (src_port, dst_port) = if packet.len() >= ihl + 4 && (protocol == 6 || protocol == 17) {
            let src = u16::from_be_bytes([packet[ihl], packet[ihl + 1]]);
            let dst = u16::from_be_bytes([packet[ihl + 2], packet[ihl + 3]]);
            (src, dst)
        } else {
            (0, 0)
        };

        // Simple hash combining all 5 tuple elements
        let hash = (src_ip as u64)
            .wrapping_mul(31)
            .wrapping_add(dst_ip as u64)
            .wrapping_mul(31)
            .wrapping_add(src_port as u64)
            .wrapping_mul(31)
            .wrapping_add(dst_port as u64)
            .wrapping_mul(31)
            .wrapping_add(protocol as u64);

        Some(FlowId(hash))
    }

    /// Create flow ID for ICMP (uses src_ip, dst_ip, protocol, icmp_id)
    pub fn from_icmp(packet: &[u8]) -> Option<Self> {
        if packet.len() < 28 {
            return None;
        }

        let version = (packet[0] >> 4) & 0x0F;
        if version != 4 {
            return None;
        }

        let ihl = (packet[0] & 0x0F) as usize * 4;
        let protocol = packet[9];
        if protocol != 1 {
            return None; // Not ICMP
        }

        let src_ip = u32::from_be_bytes([packet[12], packet[13], packet[14], packet[15]]);
        let dst_ip = u32::from_be_bytes([packet[16], packet[17], packet[18], packet[19]]);

        // ICMP identifier (for echo request/reply)
        let icmp_id = if packet.len() >= ihl + 6 {
            u16::from_be_bytes([packet[ihl + 4], packet[ihl + 5]])
        } else {
            0
        };

        let hash = (src_ip as u64)
            .wrapping_mul(31)
            .wrapping_add(dst_ip as u64)
            .wrapping_mul(31)
            .wrapping_add(icmp_id as u64)
            .wrapping_mul(31)
            .wrapping_add(protocol as u64);

        Some(FlowId(hash))
    }
}

/// Chunk identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId(pub u64);

impl ChunkId {
    pub fn next(&self) -> Self {
        ChunkId(self.0.wrapping_add(1))
    }
}

/// Data chunk packet
#[derive(Debug, Clone)]
pub struct ChunkPacket {
    /// Unique chunk identifier
    pub chunk_id: ChunkId,
    /// Link this chunk is being sent on
    pub link_id: u8,
    /// Total size of the chunk (for multi-fragment chunks)
    pub total_size: u32,
    /// Offset within the chunk (for fragmentation)
    pub offset: u32,
    /// Payload data
    pub payload: Vec<u8>,
}

impl ChunkPacket {
    /// Encode chunk packet to bytes
    pub fn encode(&self) -> Vec<u8> {
        let payload_len = self.payload.len() as u16;
        let mut buf = Vec::with_capacity(CHUNK_HEADER_SIZE + self.payload.len());

        buf.push(MSG_DATA);
        buf.extend_from_slice(&self.chunk_id.0.to_be_bytes());
        buf.push(self.link_id);
        buf.extend_from_slice(&self.total_size.to_be_bytes());
        buf.extend_from_slice(&self.offset.to_be_bytes());
        buf.extend_from_slice(&payload_len.to_be_bytes());
        buf.extend_from_slice(&self.payload);

        buf
    }

    /// Decode chunk packet from bytes
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < CHUNK_HEADER_SIZE {
            return None;
        }
        if buf[0] != MSG_DATA {
            return None;
        }

        let chunk_id = ChunkId(u64::from_be_bytes(buf[1..9].try_into().ok()?));
        let link_id = buf[9];
        let total_size = u32::from_be_bytes(buf[10..14].try_into().ok()?);
        let offset = u32::from_be_bytes(buf[14..18].try_into().ok()?);
        let payload_len = u16::from_be_bytes(buf[18..20].try_into().ok()?) as usize;

        if buf.len() < CHUNK_HEADER_SIZE + payload_len {
            return None;
        }

        let payload = buf[CHUNK_HEADER_SIZE..CHUNK_HEADER_SIZE + payload_len].to_vec();

        Some(ChunkPacket {
            chunk_id,
            link_id,
            total_size,
            offset,
            payload,
        })
    }
}

/// Announcement packet - sent ahead of data to pre-allocate buffer space
#[derive(Debug, Clone, Copy)]
pub struct AnnouncePacket {
    /// Chunk being announced
    pub chunk_id: ChunkId,
    /// Link the chunk will arrive on
    pub link_id: u8,
    /// Expected total size
    pub expected_size: u32,
    /// Send timestamp (microseconds since epoch)
    pub timestamp_us: u64,
}

impl AnnouncePacket {
    /// Encode announcement to bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(ANNOUNCE_HEADER_SIZE);

        buf.push(MSG_ANNOUNCE);
        buf.extend_from_slice(&self.chunk_id.0.to_be_bytes());
        buf.push(self.link_id);
        buf.extend_from_slice(&self.expected_size.to_be_bytes());
        buf.extend_from_slice(&self.timestamp_us.to_be_bytes());

        buf
    }

    /// Decode announcement from bytes
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < ANNOUNCE_HEADER_SIZE {
            return None;
        }
        if buf[0] != MSG_ANNOUNCE {
            return None;
        }

        let chunk_id = ChunkId(u64::from_be_bytes(buf[1..9].try_into().ok()?));
        let link_id = buf[9];
        let expected_size = u32::from_be_bytes(buf[10..14].try_into().ok()?);
        let timestamp_us = u64::from_be_bytes(buf[14..22].try_into().ok()?);

        Some(AnnouncePacket {
            chunk_id,
            link_id,
            expected_size,
            timestamp_us,
        })
    }
}

/// ACK packet - acknowledges chunk receipt for RTT measurement
#[derive(Debug, Clone, Copy)]
pub struct AckPacket {
    /// Chunk being acknowledged
    pub chunk_id: ChunkId,
    /// Link it was received on
    pub link_id: u8,
    /// Original send timestamp (echoed back)
    pub echo_timestamp_us: u64,
    /// Receive timestamp
    pub recv_timestamp_us: u64,
}

impl AckPacket {
    pub const SIZE: usize = 26;

    /// Encode ACK to bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::SIZE);

        buf.push(MSG_ACK);
        buf.extend_from_slice(&self.chunk_id.0.to_be_bytes());
        buf.push(self.link_id);
        buf.extend_from_slice(&self.echo_timestamp_us.to_be_bytes());
        buf.extend_from_slice(&self.recv_timestamp_us.to_be_bytes());

        buf
    }

    /// Decode ACK from bytes
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }
        if buf[0] != MSG_ACK {
            return None;
        }

        let chunk_id = ChunkId(u64::from_be_bytes(buf[1..9].try_into().ok()?));
        let link_id = buf[9];
        let echo_timestamp_us = u64::from_be_bytes(buf[10..18].try_into().ok()?);
        let recv_timestamp_us = u64::from_be_bytes(buf[18..26].try_into().ok()?);

        Some(AckPacket {
            chunk_id,
            link_id,
            echo_timestamp_us,
            recv_timestamp_us,
        })
    }
}

/// Stats exchange packet - for sharing link statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsPacket {
    /// Link ID
    pub link_id: u8,
    /// Measured bandwidth (bytes/sec)
    pub bandwidth_bps: u64,
    /// Measured RTT (microseconds)
    pub rtt_us: u64,
    /// Packet loss ratio (0.0 - 1.0)
    pub loss_ratio: f32,
    /// Timestamp
    pub timestamp_us: u64,
}

impl StatsPacket {
    /// Encode stats to bytes (JSON for simplicity, not performance critical)
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![MSG_STATS];
        let json = serde_json::to_vec(self).unwrap_or_default();
        buf.extend_from_slice(&(json.len() as u16).to_be_bytes());
        buf.extend_from_slice(&json);
        buf
    }

    /// Decode stats from bytes
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < 4 || buf[0] != MSG_STATS {
            return None;
        }
        let len = u16::from_be_bytes([buf[1], buf[2]]) as usize;
        if buf.len() < 3 + len {
            return None;
        }
        serde_json::from_slice(&buf[3..3 + len]).ok()
    }
}

/// Keepalive packet - for link health monitoring
#[derive(Debug, Clone, Copy)]
pub struct KeepalivePacket {
    /// Link ID
    pub link_id: u8,
    /// Timestamp
    pub timestamp_us: u64,
}

impl KeepalivePacket {
    pub const SIZE: usize = 10;

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::SIZE);
        buf.push(MSG_KEEPALIVE);
        buf.push(self.link_id);
        buf.extend_from_slice(&self.timestamp_us.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE || buf[0] != MSG_KEEPALIVE {
            return None;
        }
        Some(KeepalivePacket {
            link_id: buf[1],
            timestamp_us: u64::from_be_bytes(buf[2..10].try_into().ok()?),
        })
    }
}

/// Path state for rate control probes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PathState {
    Running = 0,
    Lossy = 1,
    Down = 2,
    Probing = 3,
}

impl PathState {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => PathState::Running,
            1 => PathState::Lossy,
            2 => PathState::Down,
            3 => PathState::Probing,
            _ => PathState::Running,
        }
    }
}

/// Probe packet for Glorytun/MUD-style rate control
/// Sent every 200ms per link by both sides. 51 bytes on wire.
#[derive(Debug, Clone, Copy)]
pub struct ProbePacket {
    /// Link this probe is for
    pub link_id: u8,
    /// Sequence number
    pub seq: u32,
    /// Sender's timestamp (microseconds) - echoed back for RTT
    pub timestamp_us: u64,
    /// Echoed timestamp from last received probe
    pub echo_timestamp_us: u64,
    /// Bytes sent since last probe
    pub tx_bytes: u64,
    /// Packets sent since last probe
    pub tx_packets: u32,
    /// Bytes received since last probe (tells remote what we got)
    pub rx_bytes: u64,
    /// Packets received since last probe
    pub rx_packets: u32,
    /// Loss ratio 0-255
    pub loss_ratio: u8,
    /// Path state
    pub path_state: PathState,
}

impl ProbePacket {
    pub const SIZE: usize = 51;

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::SIZE);
        buf.push(MSG_PROBE);                                        // 1
        buf.push(self.link_id);                                     // 1
        buf.extend_from_slice(&self.seq.to_be_bytes());             // 4
        buf.extend_from_slice(&self.timestamp_us.to_be_bytes());    // 8
        buf.extend_from_slice(&self.echo_timestamp_us.to_be_bytes()); // 8
        buf.extend_from_slice(&self.tx_bytes.to_be_bytes());        // 8
        buf.extend_from_slice(&self.tx_packets.to_be_bytes());      // 4
        buf.extend_from_slice(&self.rx_bytes.to_be_bytes());        // 8
        buf.extend_from_slice(&self.rx_packets.to_be_bytes());      // 4
        buf.push(self.loss_ratio);                                  // 1
        buf.push(self.path_state as u8);                            // 1
        buf.extend_from_slice(&[0u8; 3]);                           // 3 reserved
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE || buf[0] != MSG_PROBE {
            return None;
        }
        Some(ProbePacket {
            link_id: buf[1],
            seq: u32::from_be_bytes(buf[2..6].try_into().ok()?),
            timestamp_us: u64::from_be_bytes(buf[6..14].try_into().ok()?),
            echo_timestamp_us: u64::from_be_bytes(buf[14..22].try_into().ok()?),
            tx_bytes: u64::from_be_bytes(buf[22..30].try_into().ok()?),
            tx_packets: u32::from_be_bytes(buf[30..34].try_into().ok()?),
            rx_bytes: u64::from_be_bytes(buf[34..42].try_into().ok()?),
            rx_packets: u32::from_be_bytes(buf[42..46].try_into().ok()?),
            loss_ratio: buf[46],
            path_state: PathState::from_u8(buf[47]),
        })
    }
}

/// Packet type enum for decoding
#[derive(Debug)]
pub enum Packet {
    Data(ChunkPacket),
    Announce(AnnouncePacket),
    Ack(AckPacket),
    Stats(StatsPacket),
    Keepalive(KeepalivePacket),
    Probe(ProbePacket),
}

impl Packet {
    /// Decode any packet type from bytes
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.is_empty() {
            return None;
        }

        match buf[0] {
            MSG_DATA => ChunkPacket::decode(buf).map(Packet::Data),
            MSG_ANNOUNCE => AnnouncePacket::decode(buf).map(Packet::Announce),
            MSG_ACK => AckPacket::decode(buf).map(Packet::Ack),
            MSG_STATS => StatsPacket::decode(buf).map(Packet::Stats),
            MSG_KEEPALIVE => KeepalivePacket::decode(buf).map(Packet::Keepalive),
            MSG_PROBE => ProbePacket::decode(buf).map(Packet::Probe),
            _ => None,
        }
    }
}

/// Get current timestamp in microseconds
pub fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_roundtrip() {
        let chunk = ChunkPacket {
            chunk_id: ChunkId(12345),
            link_id: 2,
            total_size: 65536,
            offset: 0,
            payload: vec![1, 2, 3, 4, 5],
        };

        let encoded = chunk.encode();
        let decoded = ChunkPacket::decode(&encoded).unwrap();

        assert_eq!(chunk.chunk_id, decoded.chunk_id);
        assert_eq!(chunk.link_id, decoded.link_id);
        assert_eq!(chunk.total_size, decoded.total_size);
        assert_eq!(chunk.offset, decoded.offset);
        assert_eq!(chunk.payload, decoded.payload);
    }

    #[test]
    fn test_announce_roundtrip() {
        let announce = AnnouncePacket {
            chunk_id: ChunkId(99999),
            link_id: 4,
            expected_size: 1048576,
            timestamp_us: 1234567890,
        };

        let encoded = announce.encode();
        let decoded = AnnouncePacket::decode(&encoded).unwrap();

        assert_eq!(announce.chunk_id, decoded.chunk_id);
        assert_eq!(announce.link_id, decoded.link_id);
        assert_eq!(announce.expected_size, decoded.expected_size);
        assert_eq!(announce.timestamp_us, decoded.timestamp_us);
    }

    #[test]
    fn test_ack_roundtrip() {
        let ack = AckPacket {
            chunk_id: ChunkId(555),
            link_id: 1,
            echo_timestamp_us: 1000000,
            recv_timestamp_us: 1000500,
        };

        let encoded = ack.encode();
        let decoded = AckPacket::decode(&encoded).unwrap();

        assert_eq!(ack.chunk_id, decoded.chunk_id);
        assert_eq!(ack.link_id, decoded.link_id);
        assert_eq!(ack.echo_timestamp_us, decoded.echo_timestamp_us);
        assert_eq!(ack.recv_timestamp_us, decoded.recv_timestamp_us);
    }

    #[test]
    fn test_flow_id_tcp() {
        // Minimal valid IPv4 TCP packet header
        let mut packet = vec![0u8; 40];
        packet[0] = 0x45; // IPv4, IHL=5
        packet[9] = 6;    // TCP
        packet[12..16].copy_from_slice(&[192, 168, 1, 1]); // src IP
        packet[16..20].copy_from_slice(&[10, 0, 0, 1]);    // dst IP
        packet[20..22].copy_from_slice(&[0x1F, 0x90]);     // src port 8080
        packet[22..24].copy_from_slice(&[0x00, 0x50]);     // dst port 80

        let flow_id = FlowId::from_packet(&packet);
        assert!(flow_id.is_some());

        // Same packet should produce same flow ID
        let flow_id2 = FlowId::from_packet(&packet);
        assert_eq!(flow_id, flow_id2);
    }
}

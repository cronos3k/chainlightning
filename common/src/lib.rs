//! ChainLightning v4 Common Library
//!
//! Shared types, configuration, metrics, and protocol definitions.
//!
//! Direct UDP mode - no WireGuard overhead.

pub mod config;
pub mod metrics;
pub mod protocol;

pub use protocol::{
    AckPacket, AnnouncePacket, ChunkId, ChunkPacket, FlowId, KeepalivePacket, Packet,
    ProbePacket, PathState, StatsPacket,
    MSG_ACK, MSG_ANNOUNCE, MSG_DATA, MSG_KEEPALIVE, MSG_PROBE, MSG_STATS,
    FLOW_MODE_REALTIME, FLOW_MODE_SINGLELINK, FLOW_MODE_BULK,
};

pub use config::{Config, ConfigManager, RateControlConfig};
pub use metrics::{Metrics, MetricsCollector, LinkMetrics};

/// Number of WAN links
pub const NUM_LINKS: usize = 5;

/// Server (VPS) public IP
pub const SERVER_PUBLIC_IP: &str = "157.90.182.45";

/// Link identifiers with their characteristics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkType {
    Adsl,
    Starlink,
}

/// Link configuration for direct UDP (no WireGuard)
#[derive(Debug, Clone)]
pub struct LinkInfo {
    pub id: usize,
    pub link_type: LinkType,
    /// Client's network interface name
    pub interface: &'static str,
    /// Client's local IP on this interface
    pub client_bind_ip: &'static str,
    /// Server port for this link
    pub port: u16,
    pub expected_bandwidth_down: u64,  // bytes/sec
    pub expected_bandwidth_up: u64,    // bytes/sec
    pub expected_rtt_ms: u32,
}

/// Static link configuration - Direct UDP over each WAN
/// Client binds to local IP, sends to SERVER_PUBLIC_IP:port
/// Server binds to 0.0.0.0:port, learns client's NAT address from incoming packets
pub const LINKS: [LinkInfo; NUM_LINKS] = [
    // Link 0: ADSL1 (192.168.178.x network)
    LinkInfo {
        id: 0,
        link_type: LinkType::Adsl,
        interface: "enp1s0f0",
        client_bind_ip: "192.168.178.79",
        port: 50001,
        expected_bandwidth_down: 7_500_000,   // 60 Mbps
        expected_bandwidth_up: 1_375_000,      // 11 Mbps
        expected_rtt_ms: 20,
    },
    // Link 1: Starlink1 (192.168.2.x network)
    LinkInfo {
        id: 1,
        link_type: LinkType::Starlink,
        interface: "enp1s0f3",
        client_bind_ip: "192.168.2.153",
        port: 50002,
        expected_bandwidth_down: 27_500_000,  // 220 Mbps
        expected_bandwidth_up: 2_500_000,      // 20 Mbps
        expected_rtt_ms: 40,
    },
    // Link 2: ADSL2 (192.168.10.x network) - best ADSL latency
    LinkInfo {
        id: 2,
        link_type: LinkType::Adsl,
        interface: "enp1s0f2",
        client_bind_ip: "192.168.10.134",
        port: 50003,
        expected_bandwidth_down: 7_500_000,
        expected_bandwidth_up: 1_375_000,
        expected_rtt_ms: 20,
    },
    // Link 3: Starlink2 (192.168.1.x network)
    LinkInfo {
        id: 3,
        link_type: LinkType::Starlink,
        interface: "enp1s0f1",
        client_bind_ip: "192.168.1.211",
        port: 50004,
        expected_bandwidth_down: 27_500_000,
        expected_bandwidth_up: 2_500_000,
        expected_rtt_ms: 40,
    },
    // Link 4: ADSL3 (192.168.20.x network)
    LinkInfo {
        id: 4,
        link_type: LinkType::Adsl,
        interface: "enp4s0",
        client_bind_ip: "192.168.20.134",
        port: 50005,
        expected_bandwidth_down: 7_500_000,
        expected_bandwidth_up: 1_375_000,
        expected_rtt_ms: 20,
    },
];

/// TUN device configuration
pub const TUN_NAME: &str = "tun-bond";
pub const TUN_SERVER_IP: &str = "10.99.0.1";
pub const TUN_CLIENT_IP: &str = "10.99.0.2";
pub const TUN_MTU: u32 = 1400;

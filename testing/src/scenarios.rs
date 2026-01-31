//! Test scenarios for different traffic patterns.
//!
//! Defines various load patterns and traffic types for testing.

use std::time::Duration;
use serde::{Deserialize, Serialize};

/// Traffic pattern types
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum TrafficPattern {
    /// Constant bitrate
    ConstantBitrate { bitrate_mbps: f64 },
    /// Burst traffic (high then low)
    Burst { peak_mbps: f64, base_mbps: f64, burst_duration_ms: u64 },
    /// Ramp up over time
    Ramp { start_mbps: f64, end_mbps: f64, duration_secs: u64 },
    /// Real-world simulation (mix of patterns)
    Realistic,
}

/// Traffic direction
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Direction {
    Download,
    Upload,
    Bidirectional,
}

/// Test scenario definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestScenario {
    pub name: String,
    pub description: String,
    pub duration: Duration,
    pub pattern: TrafficPattern,
    pub direction: Direction,
    pub concurrent_flows: usize,
    pub packet_sizes: PacketSizeDistribution,
}

/// Packet size distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PacketSizeDistribution {
    /// Fixed size
    Fixed(usize),
    /// Uniform distribution
    Uniform { min: usize, max: usize },
    /// Bimodal (small ACKs + large data)
    Bimodal { small: usize, large: usize, small_ratio: f64 },
    /// Real-world distribution
    Realistic,
}

/// Builder for test scenarios
pub struct ScenarioBuilder {
    name: String,
    description: String,
    duration: Duration,
    pattern: TrafficPattern,
    direction: Direction,
    concurrent_flows: usize,
    packet_sizes: PacketSizeDistribution,
}

impl ScenarioBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            description: String::new(),
            duration: Duration::from_secs(60),
            pattern: TrafficPattern::ConstantBitrate { bitrate_mbps: 100.0 },
            direction: Direction::Download,
            concurrent_flows: 1,
            packet_sizes: PacketSizeDistribution::Fixed(1400),
        }
    }

    pub fn description(mut self, desc: &str) -> Self {
        self.description = desc.to_string();
        self
    }

    pub fn duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    pub fn pattern(mut self, pattern: TrafficPattern) -> Self {
        self.pattern = pattern;
        self
    }

    pub fn direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }

    pub fn concurrent_flows(mut self, flows: usize) -> Self {
        self.concurrent_flows = flows;
        self
    }

    pub fn packet_sizes(mut self, sizes: PacketSizeDistribution) -> Self {
        self.packet_sizes = sizes;
        self
    }

    pub fn build(self) -> TestScenario {
        TestScenario {
            name: self.name,
            description: self.description,
            duration: self.duration,
            pattern: self.pattern,
            direction: self.direction,
            concurrent_flows: self.concurrent_flows,
            packet_sizes: self.packet_sizes,
        }
    }
}

/// Predefined test scenarios
pub fn standard_scenarios() -> Vec<TestScenario> {
    vec![
        // Single link saturation test
        ScenarioBuilder::new("single_link_saturation")
            .description("Saturate fastest single link (Starlink ~220 Mbps)")
            .duration(Duration::from_secs(30))
            .pattern(TrafficPattern::ConstantBitrate { bitrate_mbps: 220.0 })
            .direction(Direction::Download)
            .concurrent_flows(1)
            .build(),

        // Aggregation test
        ScenarioBuilder::new("full_aggregation")
            .description("Test full link aggregation at max capacity")
            .duration(Duration::from_secs(60))
            .pattern(TrafficPattern::ConstantBitrate { bitrate_mbps: 400.0 })
            .direction(Direction::Download)
            .concurrent_flows(4)
            .build(),

        // Burst handling
        ScenarioBuilder::new("burst_handling")
            .description("Test burst traffic handling")
            .duration(Duration::from_secs(60))
            .pattern(TrafficPattern::Burst {
                peak_mbps: 500.0,
                base_mbps: 50.0,
                burst_duration_ms: 1000,
            })
            .direction(Direction::Bidirectional)
            .concurrent_flows(2)
            .build(),

        // Latency sensitive
        ScenarioBuilder::new("latency_sensitive")
            .description("Low bandwidth latency-sensitive traffic (gaming/VoIP)")
            .duration(Duration::from_secs(30))
            .pattern(TrafficPattern::ConstantBitrate { bitrate_mbps: 1.0 })
            .direction(Direction::Bidirectional)
            .concurrent_flows(1)
            .packet_sizes(PacketSizeDistribution::Fixed(64))
            .build(),

        // Mixed traffic
        ScenarioBuilder::new("mixed_realistic")
            .description("Real-world mixed traffic pattern")
            .duration(Duration::from_secs(120))
            .pattern(TrafficPattern::Realistic)
            .direction(Direction::Bidirectional)
            .concurrent_flows(10)
            .packet_sizes(PacketSizeDistribution::Realistic)
            .build(),

        // Upload stress
        ScenarioBuilder::new("upload_stress")
            .description("Saturate upload bandwidth (max ~62 Mbps combined)")
            .duration(Duration::from_secs(60))
            .pattern(TrafficPattern::ConstantBitrate { bitrate_mbps: 70.0 })
            .direction(Direction::Upload)
            .concurrent_flows(3)
            .build(),

        // Ramp test
        ScenarioBuilder::new("capacity_ramp")
            .description("Gradually increase load to find saturation point")
            .duration(Duration::from_secs(120))
            .pattern(TrafficPattern::Ramp {
                start_mbps: 10.0,
                end_mbps: 500.0,
                duration_secs: 120,
            })
            .direction(Direction::Download)
            .concurrent_flows(4)
            .build(),
    ]
}

impl TestScenario {
    /// Calculate expected throughput for scenario
    pub fn expected_throughput_mbps(&self) -> f64 {
        match self.pattern {
            TrafficPattern::ConstantBitrate { bitrate_mbps } => bitrate_mbps,
            TrafficPattern::Burst { peak_mbps, base_mbps, .. } => (peak_mbps + base_mbps) / 2.0,
            TrafficPattern::Ramp { start_mbps, end_mbps, .. } => (start_mbps + end_mbps) / 2.0,
            TrafficPattern::Realistic => 150.0, // Estimated average
        }
    }

    /// Get average packet size for scenario
    pub fn avg_packet_size(&self) -> usize {
        match self.packet_sizes {
            PacketSizeDistribution::Fixed(size) => size,
            PacketSizeDistribution::Uniform { min, max } => (min + max) / 2,
            PacketSizeDistribution::Bimodal { small, large, small_ratio } => {
                ((small as f64 * small_ratio) + (large as f64 * (1.0 - small_ratio))) as usize
            }
            PacketSizeDistribution::Realistic => 800, // Estimated average
        }
    }
}

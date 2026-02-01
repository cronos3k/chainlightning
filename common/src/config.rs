//! Configuration management with hot-reload support for A/B testing.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::sync::watch;
use tracing::{info, warn, error};

/// Main configuration structure.
/// All A/B testable parameters are here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Flow classifier settings
    pub flow_classifier: FlowClassifierConfig,

    /// Chunk aggregator settings
    pub chunk_aggregator: ChunkAggregatorConfig,

    /// Link scheduler settings
    pub link_scheduler: LinkSchedulerConfig,

    /// Announcer settings
    pub announcer: AnnouncerConfig,

    /// Receiver settings
    pub receiver: ReceiverConfig,

    /// Stats collector settings
    pub stats: StatsConfig,

    /// Testing settings
    pub testing: TestingConfig,

    /// Logging settings (runtime toggleable)
    pub logging: LoggingConfig,

    /// Realtime traffic detection settings
    pub realtime: RealtimeConfig,

    /// Rate control settings (Glorytun/MUD-style)
    pub rate_control: RateControlConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowClassifierConfig {
    /// Below this ratio of fastest link, use single-link (default: 0.66)
    pub single_link_threshold: f64,

    /// Above this ratio, use multi-link (default: 0.90)
    pub multi_link_threshold: f64,

    /// How long flow must sustain before switching to multi-link (ms)
    pub monitor_duration_ms: u64,

    /// Sliding window for bandwidth estimation (ms)
    pub flow_window_ms: u64,

    /// Flow expiry timeout (ms) - remove inactive flows
    pub flow_expiry_ms: u64,

    /// Packets/sec threshold for realtime cadence detection
    pub realtime_pps_threshold: u32,

    /// Sampling window for cadence detection (ms)
    pub realtime_window_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkAggregatorConfig {
    /// Minimum chunk size (bytes)
    pub min_chunk_size: usize,

    /// Maximum chunk size (bytes)
    pub max_chunk_size: usize,

    /// Multiplier on BDP calculation
    pub bdp_multiplier: f64,

    /// Maximum wait before sending partial chunk (ms)
    pub aggregation_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkSchedulerConfig {
    /// Target arrival synchronization window (ms)
    pub sync_interval_ms: u64,

    /// Maximum delay for synchronization (ms)
    pub max_send_delay_ms: u64,

    /// Enable/disable arrival synchronization
    pub enable_sync: bool,

    /// Scheduling strategy: "tiered_fill", "round_robin", "single_best", "weighted"
    pub strategy: String,

    /// Link tier configuration (priority order, capacities)
    pub link_tiers: Vec<LinkTierConfig>,

    /// Enable flow affinity (same flow stays on same link)
    pub flow_affinity: bool,

    /// Flow affinity timeout - reassign flow after this many seconds of inactivity
    pub flow_affinity_timeout_secs: u64,
}

/// Configuration for a single link tier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkTierConfig {
    /// Link ID (0-4)
    pub link_id: usize,

    /// Priority (lower = higher priority, filled first)
    pub priority: usize,

    /// Capacity in bytes/sec (download direction)
    pub capacity_down_bps: u64,

    /// Capacity in bytes/sec (upload direction)
    pub capacity_up_bps: u64,

    /// Utilization threshold before overflow to next tier (0.0-1.0, e.g., 0.90)
    pub utilization_threshold: f64,

    /// Is this link eligible for realtime traffic?
    pub realtime_eligible: bool,

    /// Link type for reference: "adsl" or "starlink"
    pub link_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnouncerConfig {
    /// Enable mode: "auto", "true", "false"
    pub enabled: String,

    /// How many chunks to announce ahead
    pub lookahead_count: usize,

    /// CPU timing safety margin (microseconds)
    pub safety_margin_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiverConfig {
    /// Maximum wait for missing chunk (ms)
    pub reorder_timeout_ms: u64,

    /// Maximum buffered chunks per flow
    pub max_buffer_chunks: usize,

    /// Forward in-order chunks immediately
    pub immediate_forward: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsConfig {
    /// EWMA smoothing factor (0.0 - 1.0)
    pub ewma_alpha: f64,

    /// RTT probe frequency (ms)
    pub probe_interval_ms: u64,

    /// Window for bandwidth calculation (ms)
    pub bandwidth_window_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestingConfig {
    /// Enable A/B testing mode
    pub ab_testing_enabled: bool,

    /// Metrics collection interval (ms)
    pub metrics_interval_ms: u64,

    /// Log detailed metrics to file
    pub log_metrics: bool,

    /// Metrics log file path
    pub metrics_log_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Enable debug logging (runtime toggleable)
    pub debug_enabled: bool,

    /// Log scheduler decisions (which link selected, why)
    pub log_scheduler: bool,

    /// Log flow classification decisions
    pub log_flows: bool,

    /// Log chunk aggregation details
    pub log_chunks: bool,

    /// Log link statistics updates
    pub log_stats: bool,

    /// Log RTT measurements
    pub log_rtt: bool,

    /// Log file path (empty = stdout only)
    pub log_file_path: String,

    /// Log rotation size in MB (0 = no rotation)
    pub log_rotation_mb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealtimeConfig {
    /// UDP ports considered realtime (VoIP, gaming)
    pub realtime_udp_ports: Vec<u16>,

    /// TCP ports considered realtime (SSH, etc.)
    pub realtime_tcp_ports: Vec<u16>,

    /// Maximum packet size for realtime classification (bytes)
    pub max_realtime_packet_size: usize,

    /// Force realtime traffic to ADSL-only links
    pub force_adsl_only: bool,
}

/// Glorytun/MUD-style adaptive rate control configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateControlConfig {
    /// Master toggle - when false, falls back to static weights
    pub enabled: bool,

    /// Probe send interval (ms) - how often to send probe packets per link
    pub probe_interval_ms: u64,

    /// Minimum rate as fraction of configured max (prevents death spiral)
    pub min_rate_fraction: f64,

    /// Maximum rate change per cycle as fraction of current rate
    pub max_rate_change_fraction: f64,

    /// RX timing threshold for congestion detection (fraction slower than TX)
    /// If RX is this much slower than TX timing, declare congestion
    pub congestion_threshold: f64,

    /// Rate reduction on congestion: new_rate = this * rx_rate
    pub congestion_reduction: f64,

    /// Growth factor when healthy: rate += rate * this
    pub growth_factor: f64,

    /// Minimum packets before loss calculation is valid
    pub loss_min_packets: u64,

    /// Loss threshold (0-255 scale) to enter LOSSY state (~2% = 5)
    pub loss_threshold: u8,

    /// Loss threshold (0-255 scale) to enter DOWN state (~78% = 200)
    pub loss_down_threshold: u8,

    /// No probe response timeout (ms) to enter DOWN state
    pub down_timeout_ms: u64,

    /// Number of successful probe responses to recover from DOWN->RUNNING
    pub probe_recovery_count: u32,

    /// RTT EWMA smoothing factor (7:1 ratio = 0.125)
    pub rtt_ewma_alpha: f64,

    /// RTT variance EWMA smoothing factor (3:1 ratio = 0.25)
    pub rtt_var_alpha: f64,

    // === Traffic-Aware Probe Attenuation (TAPA) ===

    /// Enable TAPA - adjusts probe trust based on traffic load
    pub tapa_enabled: bool,

    /// Traffic load below this fraction = probes fully trusted
    pub tapa_idle_threshold: f64,

    /// Traffic load above this fraction = probes minimally trusted
    pub tapa_heavy_threshold: f64,

    /// Minimum probe confidence floor (never fully ignore probes)
    pub tapa_min_confidence: f64,

    /// Loss threshold multiplier when probe confidence < 0.5
    pub tapa_loss_threshold_multiplier: u8,

    /// Enable accelerated loss decay when traffic subsides
    pub tapa_accelerated_recovery: bool,

    /// Accelerated decay fraction (applied when traffic drops, e.g. 0.75 = 3/4 decay)
    pub tapa_recovery_decay: f64,

    /// Enable cross-link correlation to suppress all-link phantom loss
    pub tapa_cross_link_check: bool,
}

impl Default for RateControlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            probe_interval_ms: 200,
            min_rate_fraction: 0.10,
            max_rate_change_fraction: 0.10,
            congestion_threshold: 0.125,
            congestion_reduction: 0.875,
            growth_factor: 0.10,
            loss_min_packets: 3000,
            loss_threshold: 20,
            loss_down_threshold: 230,
            down_timeout_ms: 5000,
            probe_recovery_count: 3,
            rtt_ewma_alpha: 0.125,
            rtt_var_alpha: 0.25,
            tapa_enabled: true,
            tapa_idle_threshold: 0.20,
            tapa_heavy_threshold: 0.70,
            tapa_min_confidence: 0.10,
            tapa_loss_threshold_multiplier: 4,
            tapa_accelerated_recovery: true,
            tapa_recovery_decay: 0.75,
            tapa_cross_link_check: true,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            flow_classifier: FlowClassifierConfig {
                single_link_threshold: 0.66,
                multi_link_threshold: 0.90,
                monitor_duration_ms: 2000,
                flow_window_ms: 1000,
                flow_expiry_ms: 30000,
                realtime_pps_threshold: 10,
                realtime_window_ms: 1000,
            },
            chunk_aggregator: ChunkAggregatorConfig {
                min_chunk_size: 1200,         // Allow small-packet aggregation
                max_chunk_size: 1400,         // MTU-safe: one chunk = one UDP datagram, no IP fragmentation
                bdp_multiplier: 1.0,
                aggregation_timeout_ms: 5,    // 5ms max wait (was 50ms - too slow for TCP)
            },
            link_scheduler: LinkSchedulerConfig {
                sync_interval_ms: 50,
                max_send_delay_ms: 100,
                enable_sync: false,  // Sync delay is applied serially in sender loop, blocking all sends
                strategy: "tiered_fill".to_string(),
                link_tiers: vec![
                    // L2 = ADSL2 - BEST latency, priority 1
                    LinkTierConfig {
                        link_id: 2,
                        priority: 1,
                        capacity_down_bps: 7_812_500,  // 62.5 Mbps
                        capacity_up_bps: 1_587_500,    // 12.7 Mbps
                        utilization_threshold: 0.90,   // 90% to avoid crosstalk
                        realtime_eligible: true,
                        link_type: "adsl".to_string(),
                    },
                    // L0 = ADSL1 - priority 2
                    LinkTierConfig {
                        link_id: 0,
                        priority: 2,
                        capacity_down_bps: 7_812_500,  // 62.5 Mbps
                        capacity_up_bps: 1_587_500,    // 12.7 Mbps
                        utilization_threshold: 0.90,
                        realtime_eligible: true,
                        link_type: "adsl".to_string(),
                    },
                    // L4 = ADSL3 - weakest ADSL, priority 3
                    LinkTierConfig {
                        link_id: 4,
                        priority: 3,
                        capacity_down_bps: 7_812_500,  // 62.5 Mbps (may be less)
                        capacity_up_bps: 1_587_500,    // 12.7 Mbps
                        utilization_threshold: 0.90,
                        realtime_eligible: true,
                        link_type: "adsl".to_string(),
                    },
                    // L1 = Starlink1 - overflow, priority 4
                    LinkTierConfig {
                        link_id: 1,
                        priority: 4,
                        capacity_down_bps: 27_500_000, // 220 Mbps
                        capacity_up_bps: 2_500_000,    // 20 Mbps
                        utilization_threshold: 0.95,
                        realtime_eligible: false,      // High latency, variable
                        link_type: "starlink".to_string(),
                    },
                    // L3 = Starlink2 - overflow, priority 5
                    LinkTierConfig {
                        link_id: 3,
                        priority: 5,
                        capacity_down_bps: 27_500_000, // 220 Mbps
                        capacity_up_bps: 2_500_000,    // 20 Mbps
                        utilization_threshold: 0.95,
                        realtime_eligible: false,
                        link_type: "starlink".to_string(),
                    },
                ],
                flow_affinity: true,
                flow_affinity_timeout_secs: 30,
            },
            announcer: AnnouncerConfig {
                enabled: "auto".to_string(),
                lookahead_count: 10,
                safety_margin_us: 100,
            },
            receiver: ReceiverConfig {
                reorder_timeout_ms: 25,       // 25ms — covers 11ms one-way spread of 4-link group + jitter margin
                max_buffer_chunks: 10000,     // Hold up to 10K chunks during reorder window
                immediate_forward: true,      // Skip reorder buffer; per-chunk mode tags handle routing
            },
            stats: StatsConfig {
                ewma_alpha: 0.2,
                probe_interval_ms: 1000,
                bandwidth_window_ms: 1000,
            },
            testing: TestingConfig {
                ab_testing_enabled: true,
                metrics_interval_ms: 1000,
                log_metrics: true,
                metrics_log_path: "./metrics.jsonl".to_string(),
            },
            logging: LoggingConfig {
                debug_enabled: false,
                log_scheduler: false,
                log_flows: false,
                log_chunks: false,
                log_stats: true,           // Stats always useful
                log_rtt: false,
                log_file_path: "./chainlightning.log".to_string(),
                log_rotation_mb: 100,
            },
            realtime: RealtimeConfig {
                // Common VoIP/gaming ports
                realtime_udp_ports: vec![
                    5060, 5061,       // SIP
                    3478, 3479,       // STUN/TURN
                    16384, 16385, 16386, 16387,  // RTP range start
                    27015, 27016, 27017, 27018,  // Steam/gaming
                ],
                realtime_tcp_ports: vec![
                    22,               // SSH
                    23,               // Telnet
                ],
                max_realtime_packet_size: 600,   // VoIP ~160-320B, gaming ~64-256B, DNS ~60-512B
                force_adsl_only: true,           // Keep realtime on stable links
            },
            rate_control: RateControlConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from YAML file
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|e| ConfigError::IoError(e.to_string()))?;

        let config: Config = serde_yaml::from_str(&content)
            .map_err(|e| ConfigError::ParseError(e.to_string()))?;

        config.validate()?;
        Ok(config)
    }

    /// Save configuration to YAML file
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), ConfigError> {
        let content = serde_yaml::to_string(self)
            .map_err(|e| ConfigError::ParseError(e.to_string()))?;

        std::fs::write(path.as_ref(), content)
            .map_err(|e| ConfigError::IoError(e.to_string()))?;

        Ok(())
    }

    /// Validate configuration values
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.flow_classifier.single_link_threshold <= 0.0
            || self.flow_classifier.single_link_threshold >= 1.0 {
            return Err(ConfigError::ValidationError(
                "single_link_threshold must be between 0 and 1".to_string()
            ));
        }

        if self.flow_classifier.multi_link_threshold <= self.flow_classifier.single_link_threshold {
            return Err(ConfigError::ValidationError(
                "multi_link_threshold must be greater than single_link_threshold".to_string()
            ));
        }

        if self.chunk_aggregator.min_chunk_size > self.chunk_aggregator.max_chunk_size {
            return Err(ConfigError::ValidationError(
                "min_chunk_size must be <= max_chunk_size".to_string()
            ));
        }

        if self.stats.ewma_alpha <= 0.0 || self.stats.ewma_alpha >= 1.0 {
            return Err(ConfigError::ValidationError(
                "ewma_alpha must be between 0 and 1".to_string()
            ));
        }

        Ok(())
    }

    /// Get a specific parameter by path (for A/B testing)
    pub fn get_parameter(&self, path: &str) -> Option<ParameterValue> {
        match path {
            "flow_classifier.single_link_threshold" =>
                Some(ParameterValue::Float(self.flow_classifier.single_link_threshold)),
            "flow_classifier.multi_link_threshold" =>
                Some(ParameterValue::Float(self.flow_classifier.multi_link_threshold)),
            "flow_classifier.monitor_duration_ms" =>
                Some(ParameterValue::Uint(self.flow_classifier.monitor_duration_ms)),
            "chunk_aggregator.min_chunk_size" =>
                Some(ParameterValue::Usize(self.chunk_aggregator.min_chunk_size)),
            "chunk_aggregator.max_chunk_size" =>
                Some(ParameterValue::Usize(self.chunk_aggregator.max_chunk_size)),
            "chunk_aggregator.bdp_multiplier" =>
                Some(ParameterValue::Float(self.chunk_aggregator.bdp_multiplier)),
            "receiver.reorder_timeout_ms" =>
                Some(ParameterValue::Uint(self.receiver.reorder_timeout_ms)),
            "link_scheduler.sync_interval_ms" =>
                Some(ParameterValue::Uint(self.link_scheduler.sync_interval_ms)),
            "announcer.enabled" =>
                Some(ParameterValue::String(self.announcer.enabled.clone())),
            _ => None,
        }
    }

    /// Set a specific parameter by path (for A/B testing)
    pub fn set_parameter(&mut self, path: &str, value: ParameterValue) -> Result<(), ConfigError> {
        match (path, value) {
            ("flow_classifier.single_link_threshold", ParameterValue::Float(v)) => {
                self.flow_classifier.single_link_threshold = v;
            }
            ("flow_classifier.multi_link_threshold", ParameterValue::Float(v)) => {
                self.flow_classifier.multi_link_threshold = v;
            }
            ("flow_classifier.monitor_duration_ms", ParameterValue::Uint(v)) => {
                self.flow_classifier.monitor_duration_ms = v;
            }
            ("chunk_aggregator.min_chunk_size", ParameterValue::Usize(v)) => {
                self.chunk_aggregator.min_chunk_size = v;
            }
            ("chunk_aggregator.max_chunk_size", ParameterValue::Usize(v)) => {
                self.chunk_aggregator.max_chunk_size = v;
            }
            ("chunk_aggregator.bdp_multiplier", ParameterValue::Float(v)) => {
                self.chunk_aggregator.bdp_multiplier = v;
            }
            ("receiver.reorder_timeout_ms", ParameterValue::Uint(v)) => {
                self.receiver.reorder_timeout_ms = v;
            }
            ("link_scheduler.sync_interval_ms", ParameterValue::Uint(v)) => {
                self.link_scheduler.sync_interval_ms = v;
            }
            ("announcer.enabled", ParameterValue::String(v)) => {
                self.announcer.enabled = v;
            }
            _ => {
                return Err(ConfigError::ValidationError(
                    format!("Unknown parameter or type mismatch: {}", path)
                ));
            }
        }
        self.validate()
    }
}

/// Parameter value types for A/B testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterValue {
    Float(f64),
    Uint(u64),
    Usize(usize),
    Bool(bool),
    String(String),
}

/// Configuration errors
#[derive(Debug)]
pub enum ConfigError {
    IoError(String),
    ParseError(String),
    ValidationError(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::IoError(e) => write!(f, "IO error: {}", e),
            ConfigError::ParseError(e) => write!(f, "Parse error: {}", e),
            ConfigError::ValidationError(e) => write!(f, "Validation error: {}", e),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Configuration manager with hot-reload support
pub struct ConfigManager {
    config: Arc<RwLock<Config>>,
    config_path: Option<String>,
    tx: watch::Sender<Config>,
    rx: watch::Receiver<Config>,
}

impl ConfigManager {
    /// Create new ConfigManager with default config
    pub fn new() -> Self {
        let config = Config::default();
        let (tx, rx) = watch::channel(config.clone());
        Self {
            config: Arc::new(RwLock::new(config)),
            config_path: None,
            tx,
            rx,
        }
    }

    /// Create ConfigManager from file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let config = Config::load(&path)?;
        let (tx, rx) = watch::channel(config.clone());
        Ok(Self {
            config: Arc::new(RwLock::new(config)),
            config_path: Some(path.as_ref().to_string_lossy().to_string()),
            tx,
            rx,
        })
    }

    /// Get current configuration (read-only)
    pub fn get(&self) -> Config {
        self.config.read().unwrap().clone()
    }

    /// Get configuration receiver for watching changes
    pub fn subscribe(&self) -> watch::Receiver<Config> {
        self.rx.clone()
    }

    /// Update configuration (triggers notification to all subscribers)
    pub fn update(&self, new_config: Config) -> Result<(), ConfigError> {
        new_config.validate()?;

        {
            let mut config = self.config.write().unwrap();
            *config = new_config.clone();
        }

        // Notify subscribers
        let _ = self.tx.send(new_config);

        info!("Configuration updated");
        Ok(())
    }

    /// Update a single parameter (for A/B testing)
    pub fn set_parameter(&self, path: &str, value: ParameterValue) -> Result<(), ConfigError> {
        let mut config = self.get();
        config.set_parameter(path, value)?;
        self.update(config)
    }

    /// Reload configuration from file
    pub fn reload(&self) -> Result<(), ConfigError> {
        if let Some(ref path) = self.config_path {
            let new_config = Config::load(path)?;
            self.update(new_config)?;
            info!("Configuration reloaded from {}", path);
        }
        Ok(())
    }

    /// Save current configuration to file
    pub fn save(&self) -> Result<(), ConfigError> {
        if let Some(ref path) = self.config_path {
            let config = self.get();
            config.save(path)?;
            info!("Configuration saved to {}", path);
        }
        Ok(())
    }
}

impl Default for ConfigManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_threshold() {
        let mut config = Config::default();
        config.flow_classifier.single_link_threshold = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_parameter_get_set() {
        let mut config = Config::default();

        // Get
        let val = config.get_parameter("flow_classifier.single_link_threshold");
        assert!(matches!(val, Some(ParameterValue::Float(v)) if (v - 0.66).abs() < 0.01));

        // Set
        config.set_parameter(
            "flow_classifier.single_link_threshold",
            ParameterValue::Float(0.5)
        ).unwrap();

        let val = config.get_parameter("flow_classifier.single_link_threshold");
        assert!(matches!(val, Some(ParameterValue::Float(v)) if (v - 0.5).abs() < 0.01));
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let yaml = serde_yaml::to_string(&config).unwrap();
        let parsed: Config = serde_yaml::from_str(&yaml).unwrap();
        assert!(parsed.validate().is_ok());
    }
}

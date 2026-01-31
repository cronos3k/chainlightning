//! A/B Testing Framework
//!
//! Provides infrastructure for running controlled experiments to optimize parameters.

use std::collections::HashMap;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use chainlightning_common::config::{Config, ParameterValue};
use chainlightning_common::metrics::{Metrics, MetricsSummary};

/// Test phase tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TestPhase {
    /// Baseline measurement with current config
    Baseline,
    /// Testing variant A
    VariantA,
    /// Testing variant B
    VariantB,
    /// Cooldown between tests
    Cooldown,
    /// Test complete
    Complete,
}

/// Configuration for a single A/B test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestConfig {
    /// Name of the test
    pub name: String,
    /// Parameter path being tested (e.g., "flow_classifier.single_link_threshold")
    pub parameter_path: String,
    /// Baseline value
    pub baseline_value: ParameterValue,
    /// Variant A value
    pub variant_a_value: ParameterValue,
    /// Variant B value (optional, for A/B/n tests)
    pub variant_b_value: Option<ParameterValue>,
    /// Duration for each test phase
    pub phase_duration: Duration,
    /// Cooldown duration between phases
    pub cooldown_duration: Duration,
    /// Minimum samples required for statistical significance
    pub min_samples: usize,
    /// Success metric (what we're optimizing for)
    pub success_metric: SuccessMetric,
}

/// What metric defines success
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SuccessMetric {
    /// Maximize throughput (bytes/sec)
    MaxThroughput,
    /// Minimize latency (p50)
    MinLatencyP50,
    /// Minimize latency (p99)
    MinLatencyP99,
    /// Minimize packet loss
    MinPacketLoss,
    /// Custom weighted score
    WeightedScore {
        throughput_weight: f64,
        latency_weight: f64,
        loss_weight: f64,
    },
}

/// Result of a single test phase
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseResult {
    pub phase: TestPhase,
    pub parameter_value: ParameterValue,
    pub duration: Duration,
    pub samples: usize,
    pub metrics_summary: MetricsSummary,
    pub score: f64,
}

/// Complete test result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    pub config: TestConfig,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: chrono::DateTime<chrono::Utc>,
    pub phases: Vec<PhaseResult>,
    pub winner: Option<String>,
    pub confidence: f64,
    pub recommendation: String,
}

impl TestResult {
    /// Determine the winning variant
    pub fn determine_winner(&mut self) {
        if self.phases.len() < 2 {
            self.winner = None;
            self.confidence = 0.0;
            self.recommendation = "Insufficient data".to_string();
            return;
        }

        // Find best score
        let mut best_phase: Option<&PhaseResult> = None;
        let mut best_score = f64::NEG_INFINITY;

        for phase in &self.phases {
            if phase.phase == TestPhase::Cooldown {
                continue;
            }
            if phase.score > best_score {
                best_score = phase.score;
                best_phase = Some(phase);
            }
        }

        if let Some(winner) = best_phase {
            let phase_name = match winner.phase {
                TestPhase::Baseline => "Baseline",
                TestPhase::VariantA => "Variant A",
                TestPhase::VariantB => "Variant B",
                _ => "Unknown",
            };

            // Calculate confidence (simplified - would use proper stats in production)
            let scores: Vec<f64> = self.phases.iter()
                .filter(|p| p.phase != TestPhase::Cooldown)
                .map(|p| p.score)
                .collect();

            let mean = scores.iter().sum::<f64>() / scores.len() as f64;
            let variance = scores.iter().map(|s| (s - mean).powi(2)).sum::<f64>() / scores.len() as f64;
            let std_dev = variance.sqrt();

            // Confidence based on how far winner is from others
            let winner_distance = (winner.score - mean).abs();
            self.confidence = if std_dev > 0.0 {
                (winner_distance / std_dev).min(1.0)
            } else if winner.score > mean {
                1.0
            } else {
                0.0
            };

            self.winner = Some(phase_name.to_string());
            self.recommendation = format!(
                "{} wins with score {:.2} (confidence: {:.0}%). Set {} = {:?}",
                phase_name,
                winner.score,
                self.confidence * 100.0,
                self.config.parameter_path,
                winner.parameter_value
            );
        }
    }
}

/// A/B Test runner
pub struct ABTest {
    config: TestConfig,
    current_phase: TestPhase,
    phase_start: Instant,
    phase_metrics: Vec<Metrics>,
    results: Vec<PhaseResult>,
    started_at: chrono::DateTime<chrono::Utc>,
}

impl ABTest {
    /// Create a new A/B test
    pub fn new(config: TestConfig) -> Self {
        Self {
            config,
            current_phase: TestPhase::Baseline,
            phase_start: Instant::now(),
            phase_metrics: Vec::new(),
            results: Vec::new(),
            started_at: chrono::Utc::now(),
        }
    }

    /// Get current phase
    pub fn phase(&self) -> TestPhase {
        self.current_phase
    }

    /// Get the parameter value for current phase
    pub fn current_parameter_value(&self) -> ParameterValue {
        match self.current_phase {
            TestPhase::Baseline => self.config.baseline_value.clone(),
            TestPhase::VariantA => self.config.variant_a_value.clone(),
            TestPhase::VariantB => self.config.variant_b_value.clone()
                .unwrap_or_else(|| self.config.baseline_value.clone()),
            _ => self.config.baseline_value.clone(),
        }
    }

    /// Record metrics sample
    pub fn record_metrics(&mut self, metrics: Metrics) {
        if self.current_phase != TestPhase::Cooldown && self.current_phase != TestPhase::Complete {
            self.phase_metrics.push(metrics);
        }
    }

    /// Check if phase should advance, returns true if test is complete
    pub fn tick(&mut self) -> bool {
        let elapsed = self.phase_start.elapsed();

        let phase_duration = if self.current_phase == TestPhase::Cooldown {
            self.config.cooldown_duration
        } else {
            self.config.phase_duration
        };

        if elapsed >= phase_duration {
            self.advance_phase();
        }

        self.current_phase == TestPhase::Complete
    }

    /// Advance to next phase
    fn advance_phase(&mut self) {
        // Save current phase results (if not cooldown)
        if self.current_phase != TestPhase::Cooldown && !self.phase_metrics.is_empty() {
            let summary = self.summarize_metrics();
            let score = self.calculate_score(&summary);

            self.results.push(PhaseResult {
                phase: self.current_phase,
                parameter_value: self.current_parameter_value(),
                duration: self.phase_start.elapsed(),
                samples: self.phase_metrics.len(),
                metrics_summary: summary,
                score,
            });
        }

        // Clear metrics for next phase
        self.phase_metrics.clear();
        self.phase_start = Instant::now();

        // Determine next phase
        self.current_phase = match self.current_phase {
            TestPhase::Baseline => TestPhase::Cooldown,
            TestPhase::Cooldown if self.results.len() == 1 => TestPhase::VariantA,
            TestPhase::VariantA => {
                if self.config.variant_b_value.is_some() {
                    TestPhase::Cooldown
                } else {
                    TestPhase::Complete
                }
            }
            TestPhase::Cooldown if self.results.len() == 2 => TestPhase::VariantB,
            TestPhase::VariantB | _ => TestPhase::Complete,
        };
    }

    /// Summarize collected metrics
    fn summarize_metrics(&self) -> MetricsSummary {
        if self.phase_metrics.is_empty() {
            return MetricsSummary::default();
        }

        let mut latencies = Vec::new();
        let mut throughputs_down = Vec::new();
        let mut throughputs_up = Vec::new();
        let mut losses = Vec::new();
        let mut reorders = Vec::new();
        let mut cpus = Vec::new();

        for m in &self.phase_metrics {
            if m.latency_avg_ms > 0.0 {
                latencies.push(m.latency_avg_ms);
            }
            if m.throughput_down_mbps > 0.0 {
                throughputs_down.push(m.throughput_down_mbps);
            }
            if m.throughput_up_mbps > 0.0 {
                throughputs_up.push(m.throughput_up_mbps);
            }
            losses.push(m.loss_ratio);
            reorders.push(m.reorder_ratio);
            cpus.push(m.cpu_usage_percent);
        }

        let n = self.phase_metrics.len() as f64;

        let avg_latency_ms = if latencies.is_empty() {
            0.0
        } else {
            latencies.iter().sum::<f64>() / latencies.len() as f64
        };

        let max_latency_ms = latencies.iter().cloned().fold(0.0f64, f64::max);

        let avg_throughput_down_mbps = if throughputs_down.is_empty() {
            0.0
        } else {
            throughputs_down.iter().sum::<f64>() / throughputs_down.len() as f64
        };

        let avg_throughput_up_mbps = if throughputs_up.is_empty() {
            0.0
        } else {
            throughputs_up.iter().sum::<f64>() / throughputs_up.len() as f64
        };

        let max_throughput_down_mbps = throughputs_down.iter().cloned().fold(0.0f64, f64::max);
        let max_throughput_up_mbps = throughputs_up.iter().cloned().fold(0.0f64, f64::max);

        MetricsSummary {
            sample_count: self.phase_metrics.len(),
            avg_throughput_down_mbps,
            avg_throughput_up_mbps,
            max_throughput_down_mbps,
            max_throughput_up_mbps,
            avg_latency_ms,
            max_latency_ms,
            avg_loss_ratio: losses.iter().sum::<f64>() / n,
            avg_reorder_ratio: reorders.iter().sum::<f64>() / n,
            avg_cpu_percent: cpus.iter().sum::<f64>() / n,
        }
    }

    /// Calculate score based on success metric
    fn calculate_score(&self, summary: &MetricsSummary) -> f64 {
        match self.config.success_metric {
            SuccessMetric::MaxThroughput => summary.avg_throughput_down_mbps,
            SuccessMetric::MinLatencyP50 => {
                if summary.avg_latency_ms > 0.0 {
                    1000.0 / summary.avg_latency_ms  // Higher is better
                } else {
                    0.0
                }
            }
            SuccessMetric::MinLatencyP99 => {
                // Using max latency as proxy for p99
                if summary.max_latency_ms > 0.0 {
                    1000.0 / summary.max_latency_ms
                } else {
                    0.0
                }
            }
            SuccessMetric::MinPacketLoss => {
                // Lower loss = higher score
                1.0 - summary.avg_loss_ratio
            }
            SuccessMetric::WeightedScore { throughput_weight, latency_weight, loss_weight } => {
                let throughput_score = summary.avg_throughput_down_mbps / 100.0; // Normalize to ~1
                let latency_score = if summary.avg_latency_ms > 0.0 {
                    10.0 / summary.avg_latency_ms  // Normalize to ~1 for 10ms
                } else {
                    0.0
                };
                let loss_score = 1.0 - summary.avg_loss_ratio;

                throughput_score * throughput_weight
                    + latency_score * latency_weight
                    + loss_score * loss_weight
            }
        }
    }

    /// Get final test result
    pub fn result(self) -> TestResult {
        let mut result = TestResult {
            config: self.config,
            started_at: self.started_at,
            completed_at: chrono::Utc::now(),
            phases: self.results,
            winner: None,
            confidence: 0.0,
            recommendation: String::new(),
        };

        result.determine_winner();
        result
    }
}

/// Test suite - runs multiple A/B tests
pub struct TestSuite {
    tests: Vec<TestConfig>,
    results: Vec<TestResult>,
    current_test_idx: usize,
    current_test: Option<ABTest>,
}

impl TestSuite {
    pub fn new() -> Self {
        Self {
            tests: Vec::new(),
            results: Vec::new(),
            current_test_idx: 0,
            current_test: None,
        }
    }

    /// Add a test to the suite
    pub fn add_test(&mut self, config: TestConfig) {
        self.tests.push(config);
    }

    /// Start running tests
    pub fn start(&mut self) {
        if !self.tests.is_empty() {
            self.current_test = Some(ABTest::new(self.tests[0].clone()));
        }
    }

    /// Get current active test
    pub fn current_test_mut(&mut self) -> Option<&mut ABTest> {
        self.current_test.as_mut()
    }

    /// Record metrics to current test
    pub fn record_metrics(&mut self, metrics: Metrics) {
        if let Some(test) = &mut self.current_test {
            test.record_metrics(metrics);
        }
    }

    /// Tick and potentially advance tests, returns true when all tests complete
    pub fn tick(&mut self) -> bool {
        if let Some(mut test) = self.current_test.take() {
            if test.tick() {
                // Test complete
                self.results.push(test.result());
                self.current_test_idx += 1;

                // Start next test if available
                if self.current_test_idx < self.tests.len() {
                    self.current_test = Some(ABTest::new(self.tests[self.current_test_idx].clone()));
                    return false;
                }
                return true;
            } else {
                self.current_test = Some(test);
            }
        }
        self.tests.is_empty() || self.current_test_idx >= self.tests.len()
    }

    /// Get all results
    pub fn results(&self) -> &[TestResult] {
        &self.results
    }
}

impl Default for TestSuite {
    fn default() -> Self {
        Self::new()
    }
}

/// Create common test configurations
pub fn create_standard_tests() -> Vec<TestConfig> {
    vec![
        // Test single-link threshold
        TestConfig {
            name: "Single-Link Threshold".to_string(),
            parameter_path: "flow_classifier.single_link_threshold".to_string(),
            baseline_value: ParameterValue::Float(0.66),
            variant_a_value: ParameterValue::Float(0.50),
            variant_b_value: Some(ParameterValue::Float(0.75)),
            phase_duration: Duration::from_secs(60),
            cooldown_duration: Duration::from_secs(10),
            min_samples: 10,
            success_metric: SuccessMetric::MaxThroughput,
        },
        // Test chunk size
        TestConfig {
            name: "Chunk Size".to_string(),
            parameter_path: "chunk_aggregator.min_chunk_size".to_string(),
            baseline_value: ParameterValue::Usize(65536),
            variant_a_value: ParameterValue::Usize(32768),
            variant_b_value: Some(ParameterValue::Usize(131072)),
            phase_duration: Duration::from_secs(60),
            cooldown_duration: Duration::from_secs(10),
            min_samples: 10,
            success_metric: SuccessMetric::WeightedScore {
                throughput_weight: 0.6,
                latency_weight: 0.3,
                loss_weight: 0.1,
            },
        },
        // Test BDP multiplier
        TestConfig {
            name: "BDP Multiplier".to_string(),
            parameter_path: "chunk_aggregator.bdp_multiplier".to_string(),
            baseline_value: ParameterValue::Float(1.0),
            variant_a_value: ParameterValue::Float(0.5),
            variant_b_value: Some(ParameterValue::Float(2.0)),
            phase_duration: Duration::from_secs(60),
            cooldown_duration: Duration::from_secs(10),
            min_samples: 10,
            success_metric: SuccessMetric::MaxThroughput,
        },
        // Test reorder timeout
        TestConfig {
            name: "Reorder Timeout".to_string(),
            parameter_path: "receiver.reorder_timeout_ms".to_string(),
            baseline_value: ParameterValue::Uint(100),
            variant_a_value: ParameterValue::Uint(50),
            variant_b_value: Some(ParameterValue::Uint(200)),
            phase_duration: Duration::from_secs(60),
            cooldown_duration: Duration::from_secs(10),
            min_samples: 10,
            success_metric: SuccessMetric::MinLatencyP50,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ab_test_phases() {
        let config = TestConfig {
            name: "Test".to_string(),
            parameter_path: "test.param".to_string(),
            baseline_value: ParameterValue::Float(1.0),
            variant_a_value: ParameterValue::Float(2.0),
            variant_b_value: None,
            phase_duration: Duration::from_millis(10),
            cooldown_duration: Duration::from_millis(5),
            min_samples: 1,
            success_metric: SuccessMetric::MaxThroughput,
        };

        let test = ABTest::new(config);
        assert_eq!(test.phase(), TestPhase::Baseline);
    }
}

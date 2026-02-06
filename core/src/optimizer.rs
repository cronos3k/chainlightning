//! Throughput Optimizer
//!
//! Hill-climbing optimizer that continuously perturbs per-link weight factors,
//! measures aggregate throughput, keeps improvements, and reverts regressions.
//!
//! Runs alongside the rate controller:
//! - RC handles per-link health (200ms probe cycle)
//! - Optimizer handles inter-link distribution (30-second experiment cycle)

use std::collections::VecDeque;
use std::time::Instant;
use chainlightning_common::config::OptimizerConfig;
use tracing::{info, debug};

/// Optimizer state machine phases
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerState {
    /// No significant traffic — optimizer is dormant
    Idle,
    /// Collecting baseline measurements before experimenting
    Baseline,
    /// Actively perturbing one link's weight factor
    Experimenting,
    /// Comparing experiment results against baseline
    Evaluating,
    /// Brief pause between experiments
    Cooldown,
}

impl std::fmt::Display for OptimizerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OptimizerState::Idle => write!(f, "IDLE"),
            OptimizerState::Baseline => write!(f, "BASELINE"),
            OptimizerState::Experimenting => write!(f, "EXPERIMENT"),
            OptimizerState::Evaluating => write!(f, "EVALUATING"),
            OptimizerState::Cooldown => write!(f, "COOLDOWN"),
        }
    }
}

/// A throughput sample recorded each tick
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
struct ThroughputSample {
    /// Aggregate throughput across all links (bytes/sec)
    throughput_bps: u64,
    /// Composite score: throughput * (1 - weighted_loss)
    score: f64,
    /// Timestamp of this sample
    timestamp: Instant,
}

/// Describes what perturbation is being tested
#[derive(Debug, Clone)]
struct Experiment {
    /// Which link is being perturbed
    link_id: usize,
    /// Direction: true = increase, false = decrease
    increase: bool,
    /// The step magnitude applied
    step: f64,
    /// Original factor before perturbation (for revert)
    original_factor: f64,
}

/// Snapshot of current system state, provided each tick by the caller
pub struct OptimizerSnapshot {
    /// Per-link measured bandwidth in bytes/sec
    pub per_link_bandwidth_bps: Vec<u64>,
    /// Per-link loss ratio (0.0 - 1.0)
    pub per_link_loss_ratio: Vec<f64>,
    /// Per-link health status
    pub per_link_healthy: Vec<bool>,
    /// Number of active flows
    pub active_flow_count: usize,
}

/// The throughput optimizer
pub struct ThroughputOptimizer {
    config: OptimizerConfig,
    num_links: usize,

    /// Current weight factors per link [0.5 - 2.0], default 1.0
    weight_factors: Vec<f64>,
    /// Last-known-good factors for rollback
    best_known_factors: Vec<f64>,

    /// Sliding window of throughput samples
    history: VecDeque<ThroughputSample>,

    /// Current state machine phase
    state: OptimizerState,
    /// When the current phase started
    state_entered_at: Instant,
    /// Seconds elapsed in current phase
    phase_ticks: u64,

    /// Active experiment (if any)
    current_experiment: Option<Experiment>,

    /// Adaptive step size
    current_step_size: f64,

    /// Round-robin index: which link to experiment on next
    next_link_index: usize,
    /// Whether we've tried +step for the current link (false = try -step next)
    tried_increase: bool,
    /// Count of consecutive full cycles with no improvement (triggers step shrink)
    no_improvement_streak: usize,

    /// Baseline average score (computed during Baseline phase)
    baseline_score: f64,
    /// Experiment average score (computed during Experimenting phase)
    experiment_score: f64,
    /// Accumulator for computing averages
    phase_score_sum: f64,
    phase_score_count: u64,

    /// Best score ever seen (for logging)
    best_score: f64,
    /// Total experiments run
    total_experiments: u64,
    /// Total accepted experiments
    total_accepts: u64,
}

impl ThroughputOptimizer {
    pub fn new(config: OptimizerConfig, num_links: usize) -> Self {
        let step_size = config.step_size;
        Self {
            config,
            num_links,
            weight_factors: vec![1.0; num_links],
            best_known_factors: vec![1.0; num_links],
            history: VecDeque::new(),
            state: OptimizerState::Idle,
            state_entered_at: Instant::now(),
            phase_ticks: 0,
            current_experiment: None,
            current_step_size: step_size,
            next_link_index: 0,
            tried_increase: false,
            no_improvement_streak: 0,
            baseline_score: 0.0,
            experiment_score: 0.0,
            phase_score_sum: 0.0,
            phase_score_count: 0,
            best_score: 0.0,
            total_experiments: 0,
            total_accepts: 0,
        }
    }

    /// Returns true if the optimizer is enabled
    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    /// Current state
    pub fn state(&self) -> OptimizerState {
        self.state
    }

    /// Current weight factors (read-only)
    pub fn weight_factors(&self) -> &[f64] {
        &self.weight_factors
    }

    /// Main tick function — called every 1 second from stats loop.
    ///
    /// Returns Some(new_factors) when weight factors have changed and should
    /// be applied to the flow classifier. Returns None if no change.
    pub fn tick(&mut self, snapshot: OptimizerSnapshot) -> Option<Vec<f64>> {
        if !self.config.enabled {
            return None;
        }

        // Compute aggregate throughput and composite score
        let total_throughput: u64 = snapshot.per_link_bandwidth_bps.iter().sum();
        let weighted_loss = self.compute_weighted_loss(&snapshot);
        let score = total_throughput as f64 * (1.0 - weighted_loss);

        // Record sample
        let sample = ThroughputSample {
            throughput_bps: total_throughput,
            score,
            timestamp: Instant::now(),
        };
        self.history.push_back(sample);
        while self.history.len() > self.config.history_window_size {
            self.history.pop_front();
        }

        if score > self.best_score {
            self.best_score = score;
        }

        self.phase_ticks += 1;

        // If traffic is too low, go idle and revert any active experiment
        if total_throughput < self.config.min_active_throughput {
            if self.state != OptimizerState::Idle {
                self.revert_experiment();
                self.enter_state(OptimizerState::Idle);
                info!("Optimizer: -> IDLE (throughput {:.1} Mbps < threshold)",
                    total_throughput as f64 * 8.0 / 1_000_000.0);
            }
            return None;
        }

        // If we were idle and now have traffic, start baseline
        if self.state == OptimizerState::Idle {
            self.enter_state(OptimizerState::Baseline);
            info!("Optimizer: -> BASELINE (traffic detected: {:.1} Mbps)",
                total_throughput as f64 * 8.0 / 1_000_000.0);
            self.phase_score_sum = score;
            self.phase_score_count = 1;
            return None;
        }

        // State machine
        match self.state {
            OptimizerState::Idle => unreachable!(), // handled above
            OptimizerState::Baseline => {
                self.phase_score_sum += score;
                self.phase_score_count += 1;

                if self.phase_ticks >= self.config.measurement_window_secs {
                    self.baseline_score = self.phase_score_sum / self.phase_score_count as f64;
                    debug!("Optimizer: baseline score = {:.0}", self.baseline_score);
                    self.start_experiment(&snapshot);
                }
                None
            }
            OptimizerState::Experimenting => {
                self.phase_score_sum += score;
                self.phase_score_count += 1;

                if self.phase_ticks >= self.config.measurement_window_secs {
                    self.experiment_score = self.phase_score_sum / self.phase_score_count as f64;
                    self.enter_state(OptimizerState::Evaluating);
                    self.evaluate_experiment()
                } else {
                    None
                }
            }
            OptimizerState::Evaluating => {
                // Evaluation happens instantly in the transition above; shouldn't stay here
                self.enter_state(OptimizerState::Cooldown);
                None
            }
            OptimizerState::Cooldown => {
                if self.phase_ticks >= self.config.cooldown_secs {
                    self.enter_state(OptimizerState::Baseline);
                    self.phase_score_sum = score;
                    self.phase_score_count = 1;
                }
                None
            }
        }
    }

    /// Compute weighted loss across links, weighted by their bandwidth contribution
    fn compute_weighted_loss(&self, snapshot: &OptimizerSnapshot) -> f64 {
        let total_bw: u64 = snapshot.per_link_bandwidth_bps.iter().sum();
        if total_bw == 0 {
            return 0.0;
        }

        let mut weighted_loss = 0.0;
        for i in 0..self.num_links {
            let bw = snapshot.per_link_bandwidth_bps.get(i).copied().unwrap_or(0);
            let loss = snapshot.per_link_loss_ratio.get(i).copied().unwrap_or(0.0);
            let weight = bw as f64 / total_bw as f64;
            weighted_loss += loss * weight;
        }

        weighted_loss.clamp(0.0, 1.0)
    }

    /// Start a new experiment: perturb one link's weight factor
    fn start_experiment(&mut self, snapshot: &OptimizerSnapshot) {
        // Find a healthy link to experiment on
        let start = self.next_link_index;
        let mut found = false;

        for offset in 0..self.num_links {
            let link_id = (start + offset) % self.num_links;
            let healthy = snapshot.per_link_healthy.get(link_id).copied().unwrap_or(false);
            let has_traffic = snapshot.per_link_bandwidth_bps.get(link_id).copied().unwrap_or(0) > 0;

            if healthy || has_traffic {
                self.next_link_index = link_id;
                found = true;
                break;
            }
        }

        if !found {
            // All links unhealthy, go back to baseline
            self.enter_state(OptimizerState::Baseline);
            self.phase_score_sum = 0.0;
            self.phase_score_count = 0;
            return;
        }

        let link_id = self.next_link_index;
        let increase = !self.tried_increase; // First try increase, then decrease
        let step = self.current_step_size;
        let original_factor = self.weight_factors[link_id];

        let new_factor = if increase {
            (original_factor * (1.0 + step)).min(self.config.max_weight_factor)
        } else {
            (original_factor * (1.0 - step)).max(self.config.min_weight_factor)
        };

        self.weight_factors[link_id] = new_factor;

        let experiment = Experiment {
            link_id,
            increase,
            step,
            original_factor,
        };

        info!("Optimizer[EXPERIMENT]: L{} {}{:.0}% ({:.3} -> {:.3})",
            link_id,
            if increase { "+" } else { "-" },
            step * 100.0,
            original_factor,
            new_factor);

        self.current_experiment = Some(experiment);
        self.total_experiments += 1;
        self.enter_state(OptimizerState::Experimenting);
        // phase_score_sum/count reset happens in enter_state
    }

    /// Evaluate the completed experiment against baseline
    fn evaluate_experiment(&mut self) -> Option<Vec<f64>> {
        let experiment = match self.current_experiment.take() {
            Some(e) => e,
            None => return None,
        };

        let improvement = if self.baseline_score > 0.0 {
            (self.experiment_score - self.baseline_score) / self.baseline_score
        } else {
            0.0
        };

        if improvement >= self.config.min_improvement {
            // ACCEPT — keep new weights
            self.best_known_factors = self.weight_factors.clone();
            self.total_accepts += 1;

            // Grow step size on success
            self.current_step_size = (self.current_step_size * self.config.step_growth_factor)
                .min(self.config.max_step_size);

            // Advance to next link
            self.advance_link();
            self.no_improvement_streak = 0;

            info!("Optimizer: ACCEPT L{} {}{:.0}% (improvement: {:.1}%, score: {:.0} -> {:.0}, step: {:.0}%)",
                experiment.link_id,
                if experiment.increase { "+" } else { "-" },
                experiment.step * 100.0,
                improvement * 100.0,
                self.baseline_score,
                self.experiment_score,
                self.current_step_size * 100.0);

            self.enter_state(OptimizerState::Cooldown);
            Some(self.weight_factors.clone())
        } else {
            // REVERT — restore original factor
            self.weight_factors[experiment.link_id] = experiment.original_factor;

            info!("Optimizer: REVERT L{} {}{:.0}% (change: {:.1}%, score: {:.0} -> {:.0})",
                experiment.link_id,
                if experiment.increase { "+" } else { "-" },
                experiment.step * 100.0,
                improvement * 100.0,
                self.baseline_score,
                self.experiment_score);

            // If we tried increase, now try decrease
            if experiment.increase {
                self.tried_increase = true;
            } else {
                // Both directions tried for this link, move to next
                self.tried_increase = false;
                self.advance_link();
            }

            self.enter_state(OptimizerState::Cooldown);
            Some(self.weight_factors.clone())
        }
    }

    /// Advance to the next link in the round-robin
    fn advance_link(&mut self) {
        self.tried_increase = false;
        self.next_link_index = (self.next_link_index + 1) % self.num_links;

        // If we've completed a full cycle, check if we need to shrink step
        if self.next_link_index == 0 {
            self.no_improvement_streak += 1;
            if self.no_improvement_streak >= 1 {
                let old_step = self.current_step_size;
                self.current_step_size = (self.current_step_size * self.config.step_shrink_factor)
                    .max(self.config.min_step_size);
                if (old_step - self.current_step_size).abs() > 0.001 {
                    info!("Optimizer: step shrink {:.0}% -> {:.0}% (no improvement in full cycle)",
                        old_step * 100.0, self.current_step_size * 100.0);
                }
                self.no_improvement_streak = 0;
            }
        }
    }

    /// Revert any active experiment
    fn revert_experiment(&mut self) {
        if let Some(experiment) = self.current_experiment.take() {
            self.weight_factors[experiment.link_id] = experiment.original_factor;
            debug!("Optimizer: reverted active experiment on L{}", experiment.link_id);
        }
    }

    /// Enter a new state
    fn enter_state(&mut self, new_state: OptimizerState) {
        self.state = new_state;
        self.state_entered_at = Instant::now();
        self.phase_ticks = 0;
        self.phase_score_sum = 0.0;
        self.phase_score_count = 0;
    }

    /// Generate a status summary for logging
    pub fn status_summary(&self) -> String {
        let factors_str: Vec<String> = self.weight_factors.iter()
            .enumerate()
            .map(|(i, f)| format!("L{}={:.2}", i, f))
            .collect();

        let exp_str = if let Some(ref exp) = self.current_experiment {
            format!(" exp=L{}{}{:.0}%",
                exp.link_id,
                if exp.increase { "+" } else { "-" },
                exp.step * 100.0)
        } else {
            String::new()
        };

        format!("Optimizer[{}]: [{}]{} step={:.0}% exps={}/{} best={:.0}",
            self.state,
            factors_str.join(" "),
            exp_str,
            self.current_step_size * 100.0,
            self.total_accepts,
            self.total_experiments,
            self.best_score * 8.0 / 1_000_000.0, // Convert to Mbps for readability
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> OptimizerConfig {
        OptimizerConfig {
            enabled: true,
            measurement_window_secs: 3,  // Short for testing
            step_size: 0.10,
            min_improvement: 0.05,
            max_weight_factor: 2.0,
            min_weight_factor: 0.5,
            cooldown_secs: 1,
            history_window_size: 60,
            min_active_throughput: 100_000,
            step_growth_factor: 1.2,
            step_shrink_factor: 0.7,
            min_step_size: 0.02,
            max_step_size: 0.30,
        }
    }

    fn make_snapshot(bw: &[u64], loss: &[f64]) -> OptimizerSnapshot {
        let n = bw.len();
        OptimizerSnapshot {
            per_link_bandwidth_bps: bw.to_vec(),
            per_link_loss_ratio: loss.to_vec(),
            per_link_healthy: vec![true; n],
            active_flow_count: 10,
        }
    }

    #[test]
    fn test_idle_when_disabled() {
        let mut config = default_config();
        config.enabled = false;
        let mut opt = ThroughputOptimizer::new(config, 5);

        let snap = make_snapshot(
            &[5_000_000; 5],
            &[0.0; 5],
        );
        assert!(opt.tick(snap).is_none());
        assert_eq!(opt.state(), OptimizerState::Idle);
    }

    #[test]
    fn test_idle_when_low_traffic() {
        let mut opt = ThroughputOptimizer::new(default_config(), 5);

        // Traffic below threshold
        let snap = make_snapshot(
            &[10_000; 5], // 50KB total, well below 100KB threshold
            &[0.0; 5],
        );
        assert!(opt.tick(snap).is_none());
        assert_eq!(opt.state(), OptimizerState::Idle);
    }

    #[test]
    fn test_transitions_to_baseline_on_traffic() {
        let mut opt = ThroughputOptimizer::new(default_config(), 5);

        // Sufficient traffic
        let snap = make_snapshot(
            &[500_000; 5], // 2.5MB total
            &[0.0; 5],
        );
        opt.tick(snap);
        assert_eq!(opt.state(), OptimizerState::Baseline);
    }

    #[test]
    fn test_baseline_to_experiment_transition() {
        let mut opt = ThroughputOptimizer::new(default_config(), 3);

        // Tick through baseline phase: 1 tick enters Baseline from Idle,
        // then measurement_window_secs (3) ticks to complete baseline = 4 total
        for _ in 0..4 {
            let snap = make_snapshot(
                &[1_000_000, 2_000_000, 1_000_000],
                &[0.0, 0.0, 0.0],
            );
            opt.tick(snap);
        }

        assert_eq!(opt.state(), OptimizerState::Experimenting);
        assert!(opt.current_experiment.is_some());
    }

    #[test]
    fn test_experiment_accept_on_improvement() {
        let config = OptimizerConfig {
            enabled: true,
            measurement_window_secs: 2,
            step_size: 0.10,
            min_improvement: 0.05,
            max_weight_factor: 2.0,
            min_weight_factor: 0.5,
            cooldown_secs: 1,
            history_window_size: 60,
            min_active_throughput: 100_000,
            step_growth_factor: 1.2,
            step_shrink_factor: 0.7,
            min_step_size: 0.02,
            max_step_size: 0.30,
        };
        let mut opt = ThroughputOptimizer::new(config, 3);

        // Baseline: 1 tick to enter + 2 ticks to measure (window=2) = 3 ticks
        for _ in 0..3 {
            let snap = make_snapshot(
                &[1_000_000, 2_000_000, 1_000_000],
                &[0.0, 0.0, 0.0],
            );
            opt.tick(snap);
        }
        assert_eq!(opt.state(), OptimizerState::Experimenting);

        // Experiment: 10% improvement -> 4.4M total (2 ticks matching window)
        let mut got_factors = false;
        for _ in 0..2 {
            let snap = make_snapshot(
                &[1_200_000, 2_200_000, 1_000_000],
                &[0.0, 0.0, 0.0],
            );
            if let Some(_) = opt.tick(snap) {
                got_factors = true;
            }
        }

        assert!(got_factors, "Expected new factors after evaluation");
        // Should have accepted and be in cooldown
        assert_eq!(opt.state(), OptimizerState::Cooldown);
        assert_eq!(opt.total_accepts, 1);
    }

    #[test]
    fn test_experiment_revert_on_no_improvement() {
        let config = OptimizerConfig {
            enabled: true,
            measurement_window_secs: 2,
            step_size: 0.10,
            min_improvement: 0.05,
            max_weight_factor: 2.0,
            min_weight_factor: 0.5,
            cooldown_secs: 1,
            history_window_size: 60,
            min_active_throughput: 100_000,
            step_growth_factor: 1.2,
            step_shrink_factor: 0.7,
            min_step_size: 0.02,
            max_step_size: 0.30,
        };
        let mut opt = ThroughputOptimizer::new(config, 3);

        // Baseline: 1 tick to enter + 2 ticks to measure = 3 ticks
        for _ in 0..3 {
            opt.tick(make_snapshot(&[1_000_000, 2_000_000, 1_000_000], &[0.0; 3]));
        }
        assert_eq!(opt.state(), OptimizerState::Experimenting);

        let experiment_link = opt.current_experiment.as_ref().unwrap().link_id;

        // Experiment: same throughput (no improvement), 2 ticks
        for _ in 0..2 {
            opt.tick(make_snapshot(&[1_000_000, 2_000_000, 1_000_000], &[0.0; 3]));
        }

        // Factor should be reverted to 1.0
        assert!((opt.weight_factors[experiment_link] - 1.0).abs() < 0.001,
            "Expected factor reverted to 1.0, got {}", opt.weight_factors[experiment_link]);
        assert_eq!(opt.total_accepts, 0);
    }

    #[test]
    fn test_weight_factor_clamping() {
        let config = OptimizerConfig {
            enabled: true,
            measurement_window_secs: 1,
            step_size: 0.50, // Large step
            min_improvement: 0.01,
            max_weight_factor: 2.0,
            min_weight_factor: 0.5,
            cooldown_secs: 0,
            history_window_size: 60,
            min_active_throughput: 100_000,
            step_growth_factor: 1.0,
            step_shrink_factor: 1.0,
            min_step_size: 0.01,
            max_step_size: 0.90,
        };
        let mut opt = ThroughputOptimizer::new(config, 2);

        // Start with max factor
        opt.weight_factors[0] = 1.9;

        // Baseline tick
        opt.tick(make_snapshot(&[1_000_000, 1_000_000], &[0.0; 2]));

        // The experiment should clamp to max
        if let Some(ref exp) = opt.current_experiment {
            let new_factor = opt.weight_factors[exp.link_id];
            assert!(new_factor <= 2.0,
                "Factor {} should be clamped to max 2.0", new_factor);
        }
    }

    #[test]
    fn test_goes_idle_on_traffic_loss() {
        let mut opt = ThroughputOptimizer::new(default_config(), 3);

        // Get into baseline
        opt.tick(make_snapshot(&[1_000_000; 3], &[0.0; 3]));
        assert_eq!(opt.state(), OptimizerState::Baseline);

        // Traffic drops
        opt.tick(make_snapshot(&[1_000; 3], &[0.0; 3]));
        assert_eq!(opt.state(), OptimizerState::Idle);
    }

    #[test]
    fn test_status_summary() {
        let opt = ThroughputOptimizer::new(default_config(), 3);
        let summary = opt.status_summary();
        assert!(summary.contains("Optimizer[IDLE]"));
        assert!(summary.contains("L0=1.00"));
        assert!(summary.contains("L1=1.00"));
        assert!(summary.contains("L2=1.00"));
    }

    #[test]
    fn test_weighted_loss_computation() {
        let opt = ThroughputOptimizer::new(default_config(), 3);

        // Link 1 has 2x bandwidth of others, 10% loss
        let snap = OptimizerSnapshot {
            per_link_bandwidth_bps: vec![1_000_000, 2_000_000, 1_000_000],
            per_link_loss_ratio: vec![0.0, 0.10, 0.0],
            per_link_healthy: vec![true; 3],
            active_flow_count: 5,
        };

        let loss = opt.compute_weighted_loss(&snap);
        // Link 1 contributes 2/4 * 0.10 = 0.05
        assert!((loss - 0.05).abs() < 0.001, "Expected ~0.05, got {}", loss);
    }
}

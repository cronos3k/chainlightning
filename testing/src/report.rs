//! Test reporting and result formatting.

use std::io::Write;
use serde::{Deserialize, Serialize};
use crate::framework::{TestResult, TestPhase};

/// Complete test report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestReport {
    pub title: String,
    pub generated_at: chrono::DateTime<chrono::Utc>,
    pub results: Vec<TestResult>,
    pub summary: ReportSummary,
}

/// Summary statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportSummary {
    pub total_tests: usize,
    pub tests_with_winner: usize,
    pub avg_confidence: f64,
    pub recommendations: Vec<String>,
}

impl TestReport {
    /// Create a new report from test results
    pub fn new(title: &str, results: Vec<TestResult>) -> Self {
        let summary = Self::compute_summary(&results);
        Self {
            title: title.to_string(),
            generated_at: chrono::Utc::now(),
            results,
            summary,
        }
    }

    fn compute_summary(results: &[TestResult]) -> ReportSummary {
        let tests_with_winner = results.iter()
            .filter(|r| r.winner.is_some())
            .count();

        let avg_confidence = if tests_with_winner > 0 {
            results.iter()
                .filter(|r| r.winner.is_some())
                .map(|r| r.confidence)
                .sum::<f64>() / tests_with_winner as f64
        } else {
            0.0
        };

        let recommendations: Vec<String> = results.iter()
            .filter(|r| !r.recommendation.is_empty())
            .map(|r| r.recommendation.clone())
            .collect();

        ReportSummary {
            total_tests: results.len(),
            tests_with_winner,
            avg_confidence,
            recommendations,
        }
    }

    /// Generate markdown report
    pub fn to_markdown(&self) -> String {
        let mut md = String::new();

        md.push_str(&format!("# {}\n\n", self.title));
        md.push_str(&format!("Generated: {}\n\n", self.generated_at.format("%Y-%m-%d %H:%M:%S UTC")));

        // Summary
        md.push_str("## Summary\n\n");
        md.push_str(&format!("- **Total Tests**: {}\n", self.summary.total_tests));
        md.push_str(&format!("- **Tests with Clear Winner**: {}\n", self.summary.tests_with_winner));
        md.push_str(&format!("- **Average Confidence**: {:.1}%\n\n", self.summary.avg_confidence * 100.0));

        // Recommendations
        if !self.summary.recommendations.is_empty() {
            md.push_str("### Recommendations\n\n");
            for (i, rec) in self.summary.recommendations.iter().enumerate() {
                md.push_str(&format!("{}. {}\n", i + 1, rec));
            }
            md.push_str("\n");
        }

        // Detailed Results
        md.push_str("## Detailed Results\n\n");

        for result in &self.results {
            md.push_str(&format!("### {}\n\n", result.config.name));
            md.push_str(&format!("**Parameter**: `{}`\n\n", result.config.parameter_path));
            md.push_str(&format!("**Duration**: {} - {}\n\n",
                result.started_at.format("%H:%M:%S"),
                result.completed_at.format("%H:%M:%S")
            ));

            // Phase results table
            md.push_str("| Phase | Value | Samples | Throughput (Mbps) | Latency (ms) | Score |\n");
            md.push_str("|-------|-------|---------|-------------------|--------------|-------|\n");

            for phase in &result.phases {
                let phase_name = match phase.phase {
                    TestPhase::Baseline => "Baseline",
                    TestPhase::VariantA => "Variant A",
                    TestPhase::VariantB => "Variant B",
                    _ => "Other",
                };

                let value = format!("{:?}", phase.parameter_value);
                let latency_ms = phase.metrics_summary.avg_latency_ms;

                md.push_str(&format!(
                    "| {} | {} | {} | {:.2} | {:.2} | {:.2} |\n",
                    phase_name,
                    value,
                    phase.samples,
                    phase.metrics_summary.avg_throughput_down_mbps,
                    latency_ms,
                    phase.score
                ));
            }

            md.push_str("\n");

            if let Some(winner) = &result.winner {
                md.push_str(&format!("**Winner**: {} (confidence: {:.0}%)\n\n",
                    winner, result.confidence * 100.0));
            }

            md.push_str("---\n\n");
        }

        md
    }

    /// Generate JSON report
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Save report to file
    pub fn save(&self, path: &str) -> std::io::Result<()> {
        let content = if path.ends_with(".json") {
            self.to_json().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
        } else {
            self.to_markdown()
        };

        let mut file = std::fs::File::create(path)?;
        file.write_all(content.as_bytes())?;
        Ok(())
    }

    /// Print report to console
    pub fn print(&self) {
        println!("{}", self.to_markdown());
    }
}

/// Metrics logger - writes metrics to JSONL file
pub struct MetricsLogger {
    path: String,
    file: Option<std::fs::File>,
}

impl MetricsLogger {
    pub fn new(path: &str) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            path: path.to_string(),
            file: Some(file),
        })
    }

    /// Log a metrics record
    pub fn log<T: Serialize>(&mut self, record: &T) -> std::io::Result<()> {
        if let Some(file) = &mut self.file {
            let json = serde_json::to_string(record)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            writeln!(file, "{}", json)?;
            file.flush()?;
        }
        Ok(())
    }

    /// Close the logger
    pub fn close(&mut self) {
        self.file = None;
    }
}

impl Drop for MetricsLogger {
    fn drop(&mut self) {
        self.close();
    }
}

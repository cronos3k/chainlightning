//! ChainLightning v4 Testing Framework
//!
//! A/B testing infrastructure for parameter optimization.

pub mod framework;
pub mod scenarios;
pub mod report;

pub use framework::{ABTest, TestConfig, TestResult, TestPhase, TestSuite, create_standard_tests};
pub use scenarios::{TestScenario, ScenarioBuilder};
pub use report::TestReport;

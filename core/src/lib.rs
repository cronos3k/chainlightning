//! ChainLightning v4 Core Library
//!
//! Core bonding components: flow classifier, chunk aggregator, link scheduler, receiver.

pub mod flow_classifier;
pub mod chunk_aggregator;
pub mod link_scheduler;
pub mod rate_controller;
pub mod receiver;
pub mod tun;
pub mod stats;

pub use flow_classifier::{FlowClassifier, FlowState, FlowMode, FlowClassifierStats};
pub use chunk_aggregator::{ChunkAggregator, Chunk};
pub use link_scheduler::{LinkScheduler, ScheduleDecision, LinkHealth};
pub use rate_controller::RateController;
pub use receiver::{ChunkReceiver, ReassemblyBuffer};
pub use stats::{LinkStats, StatsCollector};

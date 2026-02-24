//! Runtime dispatch system with hybrid indexes

pub mod agg;
pub mod dispatch;
pub mod engine;
pub mod ids;
pub mod indexes;
pub mod partition;
pub mod predicate;

pub use agg::{AggKernel, CountKernel};
pub use dispatch::MatchedUsers;
pub use engine::SubscriptionEngine;

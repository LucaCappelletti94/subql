//! Runtime dispatch system with hybrid indexes

pub mod agg;
pub mod dispatch;
pub mod engine;
pub mod ids;
pub mod indexes;
pub mod partition;
pub mod predicate;

pub use agg::{agg_delta_for_row, AggKernel, AvgKernel, CountColumnKernel, CountKernel, SumKernel};
pub use dispatch::MatchedUsers;
pub use engine::SubscriptionEngine;

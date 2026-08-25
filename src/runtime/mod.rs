//! Runtime dispatch system with hybrid indexes

pub(crate) mod agg;
pub(crate) mod aggregate;
pub mod dispatch;
pub mod engine;
pub mod ids;
pub mod indexes;
pub mod partition;
pub mod predicate;

pub use dispatch::MatchedConsumers;
pub use engine::SubscriptionEngine;

// ConsumerNotifications is re-exported from types.rs via `pub use types::*` in lib.rs.

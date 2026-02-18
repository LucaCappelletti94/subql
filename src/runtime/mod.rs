//! Runtime dispatch system with hybrid indexes

pub mod dispatch;
pub mod engine;
pub mod ids;
pub mod indexes;
pub mod partition;
pub mod predicate;

pub use dispatch::MatchedUsers;
pub use engine::SubscriptionEngine;

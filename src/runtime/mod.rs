//! Runtime dispatch system with hybrid indexes

pub mod ids;
pub mod predicate;
pub mod indexes;
pub mod partition;
pub mod dispatch;
pub mod engine;

pub use engine::SubscriptionEngine;
pub use dispatch::MatchedUsers;

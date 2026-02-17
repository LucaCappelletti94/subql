//! # subql - SQL Subscription Dispatch Engine
//!
//! A high-performance engine for dispatching `PostgreSQL` CDC events to users
//! based on SQL WHERE clause subscriptions.
//!
//! ## Features
//!
//! - **SQL-correct semantics**: Full tri-state logic (TRUE/FALSE/UNKNOWN)
//! - **Hybrid indexing**: Equality, range, NULL, and fallback indexes for candidate pruning
//! - **Session lifecycle**: Durable and session-bound subscriptions
//! - **Online merge**: Zero-downtime background merge with atomic swap
//! - **Generic dialects**: Supports `PostgreSQL`, `MySQL`, `SQLite`, and more
//!
//! ## Example
//!
//! ```rust,ignore
//! use subql::{SubscriptionEngine, SubscriptionSpec, WalEvent};
//!
//! let mut engine = SubscriptionEngine::new(catalog);
//!
//! // Register subscription
//! engine.register(SubscriptionSpec {
//!     subscription_id: 1,
//!     user_id: 42,
//!     session_id: Some(100),
//!     sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
//!     updated_at_unix_ms: 1704067200000,
//! })?;
//!
//! // Dispatch event
//! let event = WalEvent { /* ... */ };
//! let interested_users = engine.users(&event)?;
//! ```

#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::missing_errors_doc)]  // Will be added in later phases
#![allow(clippy::missing_panics_doc)]  // Will be added in later phases
#![allow(clippy::doc_markdown)]        // Phase 1: Allow missing backticks in docs
#![forbid(unsafe_code)]

// Re-export public API
pub use types::*;
pub use errors::*;

// Internal modules
mod types;
mod errors;

pub mod compiler;
pub mod runtime;
pub mod persistence;

// Version and metadata
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn it_compiles() {
        assert_eq!(2 + 2, 4);
    }
}

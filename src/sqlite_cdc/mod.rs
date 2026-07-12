//! SQLite CDC surface.
//!
//! Two pieces:
//!
//! * [`SqliteChangesetParser`]: unconditional parser that turns SQLite
//!   session-extension changeset binary bytes (via `sqlite-diff-rs`) into
//!   typed [`SqliteChangesetEvent`] instances. Analogue of
//!   [`crate::PgOutputParser`] on the Postgres side.
//! * [`SqliteCdcSource`](https://docs.rs/subql): a live-connection
//!   wrapper that owns a diesel `SqliteConnection`, attaches tables to a
//!   SQLite session, calls `.changeset()` on poll, and feeds the bytes to
//!   the parser. Gated behind the `sqlite-cdc` cargo feature because it
//!   pulls `diesel-sqlite-session` (which needs the diesel `future`
//!   branch already pinned in the top-level `[patch.crates-io]`). Not
//!   part of the `no_std + alloc` surface.
//!
//! Mirror of how [`crate::PgOutputParser`] is unconditional while
//! [`crate::PgStreamingCdcSource`] hides behind the `pg-streaming`
//! feature.

mod event;
mod parser;
#[cfg(feature = "sqlite-cdc")]
mod source;

pub use event::SqliteChangesetEvent;
pub use parser::SqliteChangesetParser;
#[cfg(feature = "sqlite-cdc")]
pub use source::{SqliteCdcError, SqliteCdcSource};

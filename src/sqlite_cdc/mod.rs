//! SQLite CDC surface.
//!
//! Two pieces:
//!
//! * [`SqlitePatchsetParser`]: unconditional parser that turns SQLite
//!   session-extension patchset binary bytes (via `sqlite-diff-rs`) into
//!   typed [`SqlitePatchsetEvent`] instances. Analogue of
//!   [`crate::PgOutputParser`] on the Postgres side.
//! * [`SqliteCdcSource`](https://docs.rs/subql): a live-connection
//!   wrapper that owns a diesel `SqliteConnection`, attaches tables to a
//!   SQLite session, calls `.patchset()` on poll, and feeds the bytes to
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

pub use event::SqlitePatchsetEvent;
pub use parser::SqlitePatchsetParser;

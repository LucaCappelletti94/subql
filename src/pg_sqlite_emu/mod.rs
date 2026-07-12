//! Fake Postgres CDC source layered on SQLite (`pg-sqlite-emu` feature).
//!
//! [`PgSqliteEmuSource`] owns a diesel `SqliteConnection` plus a
//! session-extension [`diesel_sqlite_session::Session`], translates the
//! caller's Postgres DDL to SQLite DDL via [`pg2sqlite`], applies it to
//! the local database, and each poll drains a fresh session changeset,
//! re-encodes each row change as `pgoutput` wire bytes with
//! [`pg_walstream::encode_message`], and feeds those bytes back through
//! the production [`crate::PgOutputParser`]. The emitted events look
//! identical to what a real Postgres logical replication stream would
//! produce (`Backend = Postgres`, `Event = PgOutputEvent`).
//!
//! # When to use
//!
//! Doctests and fuzz harnesses that need to exercise the pgoutput
//! decode + engine dispatch path end-to-end without spinning up a
//! Docker Postgres. The source is deliberately named `PgSqliteEmu`
//! (not `SqliteCdc`) to keep it distinct from
//! [`crate::SqliteCdcSource`], which is the honest `Backend = SQLite`
//! source and consumes changesets directly without a wire round trip.
//!
//! # What it does NOT do
//!
//! It is not a compatibility shim for talking to real Postgres from
//! SQLite. It is a testing surface. Types outside the small set the
//! encoder understands (INT, FLOAT, BOOL, TEXT, BYTEA) fall back to
//! text-mode pass-through, which is fine for the fuzz corpus but may
//! not round-trip exotic types byte-perfectly.
//!
//! # Quickstart
//!
//! Build a source, register a subscription on the shared catalog,
//! run one INSERT, and observe the notification end to end. This is
//! the pipeline every doctest and integration test in this module
//! walks through.
//!
//! ```
//! use sqlparser::dialect::PostgreSqlDialect;
//! use subql::{
//!     DefaultIds, PgOutputEvent, PgSqliteEmuSource, SubscriptionEngine,
//!     SubscriptionRequest,
//! };
//!
//! const PG_DDL: &str =
//!     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";
//!
//! let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL)?;
//!
//! let mut engine: SubscriptionEngine<PgOutputEvent, DefaultIds, _> =
//!     SubscriptionEngine::new(source.pg_catalog().clone(), PostgreSqlDialect {});
//! engine.register(SubscriptionRequest::new(
//!     1,
//!     "SELECT * FROM orders WHERE amount > 100",
//! ))?;
//!
//! source.execute("INSERT INTO orders (id, amount, status) VALUES (1, 250, 'paid')")?;
//!
//! let event = source.poll_next_event()?.expect("insert reaches the queue");
//! let notifs = engine.consumers(&event)?;
//! assert_eq!(notifs.inserted(), &[1]);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Test coverage
//!
//! This module is the replacement for the pre-Phase-8.1
//! `sqlite_cdc` surface that shipped a `SqliteCdcSource` +
//! `PgOutputBridge` pair. Phase 8.1 deleted all six integration
//! tests plus four doctests that exercised that pair. The rebuilt
//! integration coverage lives under `tests/pg_sqlite_emu_*.rs` and
//! `tests/proptest_pg_sqlite_emu_*.rs`; the doctest coverage lives
//! on the public items in this module. When you touch any of the
//! items below, keep the tests in lockstep or extend the plan in
//! `docs/emulator-coverage-restoration.md`.
//!
//! Anything with a `TODO(coverage-restoration)` marker in the tree
//! is on the plan doc's checklist and should not ship without a
//! matching test.

mod error;
mod source;

pub use error::PgSqliteEmuError;
pub use source::PgSqliteEmuSource;

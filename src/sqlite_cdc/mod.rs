//! In-process SQLite-backed [`CdcSource`](crate::CdcSource) for Tier-2
//! testing.
//!
//! Activated by the `sqlite-cdc` Cargo feature. Wraps a Diesel
//! [`SqliteConnection`](diesel::SqliteConnection) plus a
//! [`ParserDB`](sql_traits::structs::ParserDB) and installs
//! `AFTER INSERT` / `AFTER UPDATE` / `BEFORE DELETE` triggers that
//! record each row mutation into a unified shadow log; the consumer
//! drains that log via [`CdcSource::next_event`](crate::CdcSource::next_event)
//! and receives a typed [`WalEvent`](crate::WalEvent). The intended
//! consumer is a `cargo test` body that wants real DML, real change
//! capture, and the real subql engine without spinning a Docker
//! container.
//!
//! # Column-type limitations
//!
//! Row images are serialized in-trigger via SQLite `json_array(...)`.
//! That function accepts integer, real, text, and null values, plus
//! booleans (which SQLite stores as integers). Columns whose catalog
//! type maps to `BYTES`/BLOB are out of scope for Step 2: mutations on
//! tables with such columns would fail inside the trigger. Either gate
//! such tables out of the catalog passed to
//! [`SqliteCdcSource::new`] or wait for a future step to add BLOB-aware
//! encoding.
//!
//! See `docs/handoff-sqlite-cdc-source.md` for design rationale,
//! non-goals, and the multi-step implementation plan. Steps 1 and 2 are
//! live: INSERT, UPDATE, and DELETE on single-column and composite
//! primary keys are surfaced through the [`CdcSource`](crate::CdcSource)
//! trait via a trigger-installed shadow log. TRUNCATE has no SQLite
//! analogue and is not synthesized: bulk `DELETE FROM T` produces one
//! DELETE event per row.
//!
//! # Per-connection trigger semantics
//!
//! Triggers and the shadow log live inside the owned
//! [`diesel::SqliteConnection`]. Every mutation issued through
//! [`SqliteCdcSource::execute`] is observed. Mutations performed
//! through a sibling connection (a second handle to the same database
//! file, for example) bypass the source. Tests that fan out writes
//! across connections must drive them through the source.
//!
//! # End-to-end Tier-2 example
//!
//! Drive the same `SubscriptionEngine` you would use in production
//! against an in-memory SQLite, with the real ingest path:
//!
//! ```
//! use std::sync::Arc;
//!
//! use diesel::{Connection, SqliteConnection};
//! use sql_traits::structs::ParserDB;
//! use sqlparser::dialect::PostgreSqlDialect;
//! use subql::{
//!     DefaultIds, SqliteCdcConfig, SqliteCdcSource, SubscriptionEngine,
//!     SubscriptionRequest,
//! };
//!
//! const PG_DDL: &str =
//!     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";
//!
//! let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(PG_DDL)?);
//! let mut engine = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
//!     Arc::clone(&catalog),
//!     PostgreSqlDialect {},
//! );
//! engine.register(SubscriptionRequest::new(42u64, "SELECT * FROM orders WHERE amount > 100"))?;
//!
//! let conn = SqliteConnection::establish(":memory:")?;
//! let mut source = SqliteCdcSource::with_pg_ddl(conn, PG_DDL, SqliteCdcConfig::default())?;
//!
//! source.execute("INSERT INTO orders (id, amount, status) VALUES (1, 250, 'paid')")?;
//!
//! let event = source.poll_next_event()?.unwrap();
//! let notifs = engine.consumers(&event)?;
//! assert_eq!(notifs.inserted(), &[42]);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![cfg(feature = "sqlite-cdc")]

pub mod catalog;
mod error;
mod pgoutput_bridge;
mod source;

pub use catalog::{apply_pg_ddl, translate_pg_ddl};
pub use error::SqliteCdcError;
pub use pgoutput_bridge::PgOutputBridge;
pub use source::{SqliteCdcConfig, SqliteCdcSource};

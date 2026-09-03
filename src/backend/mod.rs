#![allow(clippy::match_same_arms)]
//! Backend abstractions and the generic CDC event surface the subscription
//! engine consumes.
//!
//! # Backend
//!
//! [`Backend`] names one SQL database subql observes (Postgres, MySQL, SQLite).
//! Each impl declares:
//!
//! * The [`sqlparser::dialect::Dialect`] used to parse subscription text and
//!   catalog DDL for that database.
//! * A Rust type per SQL scalar (`Bool`, `Int`, `Float`, `String`, `Bytes`,
//!   `Uuid`, `Timestamp`, `TimestampTz`, `Date`, `Time`, `Decimal`, `Json`,
//!   `Jsonb`), spelling out how CDC payloads that observe this backend carry
//!   each scalar value.
//!
//! Concrete markers: [`Postgres`], [`MySql`], [`SQLite`].
//!
//! # CdcEvent
//!
//! [`CdcEvent`] describes one CDC row event as read by the engine. It carries
//! the observing [`Backend`] as an associated type, so [`CdcEvent::value_at`]
//! returns a typed [`Value`] (`Value<Postgres>` on a Postgres-backed payload,
//! `Value<SQLite>` on a SQLite-backed payload).
//!
//! One CDC event is always about exactly one row identity. [`RowKind`] selects
//! the view: the old-row image (Delete + Update), the new-row image (Insert +
//! Update), or the PK projection.
//!
//! Cell state is three-valued through [`Value`]: [`Value::Missing`] for cells
//! the source did not carry, [`Value::Null`] for SQL NULL, and a typed variant
//! for a present value.

use crate::checkpoint::Checkpoint;
use crate::types::{ColumnId, EventKind, TableId};
use alloc::borrow::Cow;
use sql_traits::prelude::DatabaseLike;

/// The PostgreSQL majors [`Postgres`] can target, re-exported so callers naming one need
/// not depend on the canonical crate directly.
pub use postgres_jsonb_canonical::{Pg14, Pg15, Pg16, Pg17, Pg18, PgVersion};

pub(crate) mod cdc_event;
pub(crate) mod row_kind;
pub(crate) mod scalar_value;
pub(crate) mod scalars_and_backend;
pub(crate) mod shipped;

pub(crate) use cdc_event::ResolvedEvent;
pub use cdc_event::{decode_cell, encode_value_key, CdcEvent};
pub use row_kind::RowKind;
pub(crate) use scalar_value::jsonb_payloads_equal;
pub use scalar_value::{
    BuiltinKind, Carried, CustomScalars, GroupKeyCollation, GroupKeyCollationName, GroupKeyColumn,
    GroupKeyColumnOf, GroupKeyEncoder, NoCustom, NoCustomScalars, ScalarKind, ScalarKindOf, Value,
};
pub use scalars_and_backend::{
    Backend, JsonDocument, ScalarCore, ScalarKey, ScalarText, ScalarTruth, SqliteJson,
    SqliteJsonStorage,
};
pub use shipped::{MySql, Postgres, SQLite};

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
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;

// ---------------------------------------------------------------------------
// RowKind
// ---------------------------------------------------------------------------

/// Selector for which row view of a CDC event to read.
///
/// Every CDC event concerns one row identity. `Old` and `New` name the
/// before/after images (which may be absent depending on `EventKind`).
/// `Pk` names the PK projection of that row. `value_at` called with
/// `RowKind::Pk` and a `col` that is not in
/// [`CdcEvent::pk_columns`] returns [`Value::Missing`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RowKind {
    /// Old row image — populated for Delete and (source-permitting) Update.
    Old,
    /// New row image — populated for Insert and Update.
    New,
    /// Primary-key projection — always populated for row-level events.
    Pk,
}

// ---------------------------------------------------------------------------
// ScalarKind, Value
// ---------------------------------------------------------------------------

/// Compile-time tag naming one scalar type on a [`Backend`].
///
/// Returned by
/// [`column_scalar_kind`](crate::catalog_helpers::column_scalar_kind) and
/// used in two places: the compiler coerces a comparison's literal to the
/// paired column's kind, and the WAL decoders route a wire cell to its
/// typed [`Value`] variant against the catalog at decode time. It is never
/// carried in the bytecode nor consumed by the runtime VM, which reads
/// cells through [`CdcEvent::value_at`] directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ScalarKind {
    /// [`Backend::Bool`].
    Bool,
    /// [`Backend::Int`].
    Int,
    /// [`Backend::Float`].
    Float,
    /// [`Backend::String`].
    String,
    /// [`Backend::Bytes`].
    Bytes,
    /// [`Backend::Uuid`].
    Uuid,
    /// [`Backend::Timestamp`].
    Timestamp,
    /// [`Backend::TimestampTz`].
    TimestampTz,
    /// [`Backend::Date`].
    Date,
    /// [`Backend::Time`].
    Time,
    /// [`Backend::Decimal`].
    Decimal,
    /// [`Backend::Json`].
    Json,
    /// [`Backend::Jsonb`].
    Jsonb,
}

/// A scalar value carried in the VM's evaluation stack, or as a literal
/// operand in the compiled bytecode.
///
/// One variant per scalar type on the backend, plus [`Value::Missing`] and
/// [`Value::Null`], which name cells the source did not carry and SQL NULL
/// respectively. [`CdcEvent::value_at`] returns this type directly.
///
/// The variants own their payload so a `Value` moved onto the VM stack does
/// not need to keep the event alive. LoadColumn instructions clone the
/// scalar out of the event when they push. String, Bytes, Decimal, Json,
/// and Jsonb pay for that clone; primitive scalars are cheap.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound = "")]
pub enum Value<B: Backend> {
    /// Cell not carried by the source's row image.
    Missing,
    /// SQL NULL.
    Null,
    /// [`Backend::Bool`] payload.
    Bool(B::Bool),
    /// [`Backend::Int`] payload.
    Int(B::Int),
    /// [`Backend::Float`] payload.
    Float(B::Float),
    /// [`Backend::String`] payload.
    String(B::String),
    /// [`Backend::Bytes`] payload.
    Bytes(B::Bytes),
    /// [`Backend::Uuid`] payload.
    Uuid(B::Uuid),
    /// [`Backend::Timestamp`] payload.
    Timestamp(B::Timestamp),
    /// [`Backend::TimestampTz`] payload.
    TimestampTz(B::TimestampTz),
    /// [`Backend::Date`] payload.
    Date(B::Date),
    /// [`Backend::Time`] payload.
    Time(B::Time),
    /// [`Backend::Decimal`] payload.
    Decimal(B::Decimal),
    /// [`Backend::Json`] payload.
    Json(B::Json),
    /// [`Backend::Jsonb`] payload.
    Jsonb(B::Jsonb),
}

impl<B: Backend> Value<B> {
    /// Discriminant tag for this value, or `None` for [`Value::Missing`] and
    /// [`Value::Null`] (which do not correspond to a specific scalar type).
    #[inline]
    pub const fn scalar_kind(&self) -> Option<ScalarKind> {
        match self {
            Self::Missing | Self::Null => None,
            Self::Bool(_) => Some(ScalarKind::Bool),
            Self::Int(_) => Some(ScalarKind::Int),
            Self::Float(_) => Some(ScalarKind::Float),
            Self::String(_) => Some(ScalarKind::String),
            Self::Bytes(_) => Some(ScalarKind::Bytes),
            Self::Uuid(_) => Some(ScalarKind::Uuid),
            Self::Timestamp(_) => Some(ScalarKind::Timestamp),
            Self::TimestampTz(_) => Some(ScalarKind::TimestampTz),
            Self::Date(_) => Some(ScalarKind::Date),
            Self::Time(_) => Some(ScalarKind::Time),
            Self::Decimal(_) => Some(ScalarKind::Decimal),
            Self::Json(_) => Some(ScalarKind::Json),
            Self::Jsonb(_) => Some(ScalarKind::Jsonb),
        }
    }

    /// True when this value carries no scalar (`Missing` or `Null`).
    #[inline]
    pub const fn is_absent(&self) -> bool {
        matches!(self, Self::Missing | Self::Null)
    }

    /// True when this value is `Missing`.
    #[inline]
    pub const fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    /// True when this value is `Null`.
    #[inline]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

// `Clone`, `Debug`, and `PartialEq` on `Value<B>` are hand-implemented so
// their bounds fall on the scalar payloads (all of which are `ScalarCore`
// and therefore `Clone + Debug + PartialEq`) rather than on `B` itself.
// `#[derive(...)]` would defensively add `B: Clone` / `B: Debug` / etc.
// which is not implied by `Backend` and would prevent `Vm<B: Backend>`,
// `Instruction<B: Backend>` etc. from being usable in generic contexts.

impl<B: Backend> Clone for Value<B> {
    fn clone(&self) -> Self {
        match self {
            Self::Missing => Self::Missing,
            Self::Null => Self::Null,
            Self::Bool(x) => Self::Bool(x.clone()),
            Self::Int(x) => Self::Int(x.clone()),
            Self::Float(x) => Self::Float(x.clone()),
            Self::String(x) => Self::String(x.clone()),
            Self::Bytes(x) => Self::Bytes(x.clone()),
            Self::Uuid(x) => Self::Uuid(x.clone()),
            Self::Timestamp(x) => Self::Timestamp(x.clone()),
            Self::TimestampTz(x) => Self::TimestampTz(x.clone()),
            Self::Date(x) => Self::Date(x.clone()),
            Self::Time(x) => Self::Time(x.clone()),
            Self::Decimal(x) => Self::Decimal(x.clone()),
            Self::Json(x) => Self::Json(x.clone()),
            Self::Jsonb(x) => Self::Jsonb(x.clone()),
        }
    }
}

impl<B: Backend> core::fmt::Debug for Value<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Missing => f.write_str("Missing"),
            Self::Null => f.write_str("Null"),
            Self::Bool(x) => f.debug_tuple("Bool").field(x).finish(),
            Self::Int(x) => f.debug_tuple("Int").field(x).finish(),
            Self::Float(x) => f.debug_tuple("Float").field(x).finish(),
            Self::String(x) => f.debug_tuple("String").field(x).finish(),
            Self::Bytes(x) => f.debug_tuple("Bytes").field(x).finish(),
            Self::Uuid(x) => f.debug_tuple("Uuid").field(x).finish(),
            Self::Timestamp(x) => f.debug_tuple("Timestamp").field(x).finish(),
            Self::TimestampTz(x) => f.debug_tuple("TimestampTz").field(x).finish(),
            Self::Date(x) => f.debug_tuple("Date").field(x).finish(),
            Self::Time(x) => f.debug_tuple("Time").field(x).finish(),
            Self::Decimal(x) => f.debug_tuple("Decimal").field(x).finish(),
            Self::Json(x) => f.debug_tuple("Json").field(x).finish(),
            Self::Jsonb(x) => f.debug_tuple("Jsonb").field(x).finish(),
        }
    }
}

impl<B: Backend> PartialEq for Value<B> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Missing, Self::Missing) => true,
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => a == b,
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Bytes(a), Self::Bytes(b)) => a == b,
            (Self::Uuid(a), Self::Uuid(b)) => a == b,
            (Self::Timestamp(a), Self::Timestamp(b)) => a == b,
            (Self::TimestampTz(a), Self::TimestampTz(b)) => a == b,
            (Self::Date(a), Self::Date(b)) => a == b,
            (Self::Time(a), Self::Time(b)) => a == b,
            (Self::Decimal(a), Self::Decimal(b)) => a == b,
            (Self::Json(a), Self::Json(b)) => a == b,
            (Self::Jsonb(a), Self::Jsonb(b)) => a == b,
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

/// Trait bounds every [`Backend`] associated scalar type must satisfy.
///
/// Baseline for the interpreter and dispatch layers: values are cloned into
/// the VM stack (`Clone`), compared for equality (`PartialEq`), formatted in
/// error messages (`Debug`), and serialised alongside their bytecode
/// programs (`Serialize` + `Deserialize`). `Send + Sync + 'static` keep the
/// dispatch surface usable from multi-threaded runtimes.
///
/// Individual scalars add further bounds (arithmetic on `Int` / `Float` /
/// `Decimal`, ordering on comparable scalars, `AsRef<str>` / `AsRef<[u8]>`
/// on string and byte scalars). See [`Backend`]'s associated type list for
/// the exact bounds per scalar.
pub trait ScalarCore:
    Clone
    + core::fmt::Debug
    + PartialEq
    + serde::Serialize
    + for<'de> serde::Deserialize<'de>
    + Send
    + Sync
    + 'static
{
}

impl<T> ScalarCore for T where
    T: Clone
        + core::fmt::Debug
        + PartialEq
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + Send
        + Sync
        + 'static
{
}

/// One SQL database subql observes.
///
/// An implementation names a database (via its sqlparser dialect) and
/// declares the Rust type its CDC payloads carry for each SQL scalar.
///
/// See [`Postgres`], [`MySql`], and [`SQLite`] for the shipped markers.
///
/// # Notes on the scalar choices
///
/// * `Int` rolls up every integer-shaped SQL type at `i64` breadth. Backends
///   that carry sub-i64 integers (Postgres SMALLINT, MySQL TINYINT/MEDIUMINT)
///   sign-extend into `i64`.
/// * `Float` similarly rolls up `f32` sources into `f64`.
/// * `Decimal` uses [`bigdecimal::BigDecimal`] because arbitrary-precision
///   `NUMERIC`/`DECIMAL` cannot round-trip through `f64`.
/// * `Uuid` diverges per backend: Postgres carries a native [`uuid::Uuid`]
///   from its wire protocol; SQLite carries a `String` because SQLite stores
///   UUIDs as text.
/// * `Bool` diverges per backend: Postgres carries `bool`; SQLite carries
///   `i64` because SQLite has no native BOOL.
/// * `Json` and `Jsonb` are distinct (Diesel treats them as distinct
///   `sql_types`; Postgres assigns them different OIDs). Both use
///   [`serde_json::Value`] for the Rust representation.
pub trait Backend: 'static {
    /// sqlparser dialect for parsing subscription text and DDL under this
    /// backend.
    type Dialect: sqlparser::dialect::Dialect;

    /// SQL `BOOL` representation. Only equality-shaped operations are
    /// applied to booleans; no ordering or arithmetic bound is required.
    type Bool: ScalarCore;
    /// SQL integer representation (all integer widths roll up to this type).
    /// Supports arithmetic and ordering.
    type Int: ScalarCore
        + PartialOrd
        + core::ops::Add<Output = Self::Int>
        + core::ops::Sub<Output = Self::Int>
        + core::ops::Mul<Output = Self::Int>
        + core::ops::Div<Output = Self::Int>
        + core::ops::Rem<Output = Self::Int>
        + core::ops::Neg<Output = Self::Int>;
    /// SQL floating-point representation. Supports arithmetic and
    /// ordering. No `Rem` bound because SQL modulo is defined only on
    /// integers; the VM's `Modulo` instruction coerces to `Int` first.
    type Float: ScalarCore
        + PartialOrd
        + core::ops::Add<Output = Self::Float>
        + core::ops::Sub<Output = Self::Float>
        + core::ops::Mul<Output = Self::Float>
        + core::ops::Div<Output = Self::Float>
        + core::ops::Neg<Output = Self::Float>;
    /// SQL text representation (`TEXT`, `VARCHAR`, `CHAR`, ...).
    /// `AsRef<str>` supports `LIKE` / `ILIKE` pattern matching.
    type String: ScalarCore + PartialOrd + AsRef<str>;
    /// SQL binary representation (`BYTEA`, `BLOB`, `VARBINARY`, ...).
    /// `AsRef<[u8]>` supports byte-level comparisons.
    type Bytes: ScalarCore + PartialOrd + AsRef<[u8]>;
    /// SQL UUID representation. Ordered by underlying bytes for `<` / `>`.
    type Uuid: ScalarCore + PartialOrd;
    /// SQL `TIMESTAMP` (no time zone). Ordered chronologically.
    type Timestamp: ScalarCore + PartialOrd;
    /// SQL `TIMESTAMP WITH TIME ZONE`. Ordered chronologically.
    type TimestampTz: ScalarCore + PartialOrd;
    /// SQL `DATE`. Ordered chronologically.
    type Date: ScalarCore + PartialOrd;
    /// SQL `TIME` (no time zone). Ordered chronologically.
    type Time: ScalarCore + PartialOrd;
    /// SQL arbitrary-precision `NUMERIC` / `DECIMAL`. Supports arithmetic
    /// and ordering; no `Rem` bound (see `Float`).
    type Decimal: ScalarCore
        + PartialOrd
        + core::ops::Add<Output = Self::Decimal>
        + core::ops::Sub<Output = Self::Decimal>
        + core::ops::Mul<Output = Self::Decimal>
        + core::ops::Div<Output = Self::Decimal>
        + core::ops::Neg<Output = Self::Decimal>;
    /// SQL `JSON` (text-shaped). Comparison beyond equality is undefined,
    /// so no ordering bound.
    type Json: ScalarCore;
    /// SQL `JSONB` (binary-shaped). No ordering bound (see `Json`).
    type Jsonb: ScalarCore;
}

// ---------------------------------------------------------------------------
// Shipped backends
// ---------------------------------------------------------------------------

/// Postgres backend marker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Postgres;

impl Backend for Postgres {
    type Dialect = sqlparser::dialect::PostgreSqlDialect;
    type Bool = bool;
    type Int = i64;
    type Float = f64;
    type String = alloc::string::String;
    type Bytes = alloc::vec::Vec<u8>;
    type Uuid = uuid::Uuid;
    type Timestamp = chrono::NaiveDateTime;
    type TimestampTz = chrono::DateTime<chrono::Utc>;
    type Date = chrono::NaiveDate;
    type Time = chrono::NaiveTime;
    type Decimal = bigdecimal::BigDecimal;
    type Json = serde_json::Value;
    type Jsonb = serde_json::Value;
}

/// MySQL backend marker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct MySql;

impl Backend for MySql {
    type Dialect = sqlparser::dialect::MySqlDialect;
    type Bool = bool;
    type Int = i64;
    type Float = f64;
    type String = alloc::string::String;
    type Bytes = alloc::vec::Vec<u8>;
    // MySQL stores UUIDs as CHAR(36) or BINARY(16) with no native type;
    // downstream code treats them as strings on the wire.
    type Uuid = alloc::string::String;
    type Timestamp = chrono::NaiveDateTime;
    type TimestampTz = chrono::DateTime<chrono::Utc>;
    type Date = chrono::NaiveDate;
    type Time = chrono::NaiveTime;
    type Decimal = bigdecimal::BigDecimal;
    type Json = serde_json::Value;
    // MySQL does not distinguish JSON from JSONB; keep the type alias for
    // symmetry with Postgres so the engine surface stays uniform.
    type Jsonb = serde_json::Value;
}

/// SQLite backend marker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct SQLite;

impl Backend for SQLite {
    type Dialect = sqlparser::dialect::SQLiteDialect;
    // SQLite has no native BOOL; the column-type contract stores 0 / 1
    // as INTEGER. The backend surfaces the wire type honestly rather than
    // fabricating a `bool`.
    type Bool = i64;
    type Int = i64;
    type Float = f64;
    type String = alloc::string::String;
    type Bytes = alloc::vec::Vec<u8>;
    // SQLite stores UUIDs as TEXT (36-byte hyphenated) by convention.
    type Uuid = alloc::string::String;
    // SQLite has no native temporal types; downstream code stores dates and
    // times as ISO-8601 TEXT. `Timestamp` and friends carry the parsed
    // `chrono` type after decoding.
    type Timestamp = chrono::NaiveDateTime;
    type TimestampTz = chrono::DateTime<chrono::Utc>;
    type Date = chrono::NaiveDate;
    type Time = chrono::NaiveTime;
    type Decimal = bigdecimal::BigDecimal;
    type Json = serde_json::Value;
    type Jsonb = serde_json::Value;
}

// ---------------------------------------------------------------------------
// CdcEvent
// ---------------------------------------------------------------------------

/// One CDC row event as seen by the engine.
///
/// An impl exposes the event's structure (kind, table, PK layout, changed
/// columns, checkpoint) plus [`value_at`](CdcEvent::value_at), which decodes
/// one cell to a typed [`Value`]. It takes a [`RowKind`] to select the row
/// view and a [`ColumnId`] to address the cell.
///
/// # Access patterns
///
/// * Insert: `new_row` populated for every column, `old_row` empty, `pk_row`
///   populated for PK columns only.
/// * Update: both `new_row` and `old_row` may be populated; `changed_columns`
///   names the columns whose cell changed. Sources that follow
///   `REPLICA IDENTITY DEFAULT` (Postgres) carry only PK columns in `old_row`.
/// * Delete: `old_row` populated (extent depends on the source's replica
///   identity), `new_row` empty, `pk_row` populated.
/// * Truncate: no row images. `pk_columns` is empty; `changed_columns` is
///   empty. Structural event only.
///
/// # PK access
///
/// [`value_at`](CdcEvent::value_at) called with `RowKind::Pk` and a `col`
/// that is not in [`pk_columns`](Self::pk_columns) returns
/// [`Value::Missing`]. Composite PKs are read by iterating `pk_columns()`
/// and calling `value_at` per column.
pub trait CdcEvent {
    /// The database this event observes.
    type Backend: Backend;
    /// The checkpoint type this event carries (LSN, binlog position, ...).
    type Checkpoint: Checkpoint;

    // ---------- Structural surface ----------

    /// Which flavour of event this is.
    fn kind(&self) -> EventKind;

    /// Table the event belongs to.
    ///
    /// `db` is the catalog for the observed database. A raw ecosystem
    /// event knows only the table name, so resolving it to a subql
    /// [`TableId`] needs the catalog.
    fn table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId;

    /// Checkpoint (position in the source stream) when the source carries one.
    ///
    /// Returned owned so an event can bridge a source-native position
    /// type (a `pg_walstream` LSN, say) to a subql
    /// [`Checkpoint`](Self::Checkpoint) on demand.
    fn checkpoint(&self) -> Option<Self::Checkpoint>;

    /// Column ids that make up the primary key, in PK declaration order.
    ///
    /// For a composite PK the returned length is greater than one, and
    /// ordering matches the schema. For a Truncate event the result is
    /// empty. The identity reflects the event's replica identity plus
    /// the catalog, not the catalog alone, so `db` resolves the wire
    /// key layout to subql column ordinals.
    ///
    /// Returned owned as a [`Vec`] because a raw ecosystem event stores
    /// no subql [`ColumnId`] ordinals to borrow. PK arity is small, so
    /// the per-call allocation is cheap. A fill-into-buffer variant can
    /// replace this later if profiling shows it matters.
    fn pk_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId>;

    /// Column ids whose cells changed on an Update event.
    ///
    /// For non-Update events the result is empty. For Update events
    /// sources vary in whether they populate this: sources that only
    /// carry the changed columns (wal2json v2 with `add-tables`) list
    /// only those. Sources that carry the full row image list every
    /// column whose value differs. Consumers should treat the result as
    /// a hint for optimisation, not as an authoritative diff. `db`
    /// resolves wire names to subql column ordinals.
    fn changed_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId>;

    // ---------- Cell accessor ----------

    /// Decode one cell to an owned [`Value<Self::Backend>`].
    ///
    /// `db` is the catalog for the observed database. Formats whose wire
    /// carries no column type metadata (Maxwell, and positional pgoutput
    /// tuples) resolve the column's type from `db` at decode time. Formats
    /// that already carry their own type state may ignore it.
    ///
    /// Returns `Ok(Value::Missing)` for cells the source did not carry,
    /// `Ok(Value::Null)` for SQL NULL, `Ok(_)` with the matching typed
    /// variant for a present value, and `Err` when the source carried a
    /// cell of a known type that cannot be decoded (for example an
    /// integer above `i64::MAX`).
    fn value_at<DB: DatabaseLike>(
        &self,
        db: &DB,
        row: RowKind,
        col: ColumnId,
    ) -> Result<Value<Self::Backend>, crate::ValueError>;
}

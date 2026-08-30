//! Split out of the former single `backend.rs`; the module boundary is now real
//! rather than a banner comment.

use super::scalar_value::default_group_key_encoder;
use super::{Cow, CustomScalars, GroupKeyColumnOf, GroupKeyEncoder, ScalarKindOf, Value};

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

/// Extra bounds a scalar must satisfy to key a membership term's lookup table.
///
/// A term groups subscribers by the value a row carries, so the value has to be
/// usable as a map key, which [`ScalarCore`] alone does not give: it bounds
/// equality at `PartialEq`, because `Float` may be `f64` and `NaN` is not equal
/// to itself.
///
/// `Float`, `Json` and `Jsonb` deliberately do not carry this. A membership term
/// comparing such a column is refused at registration, which is what makes the
/// bound honest here rather than asserted.
pub trait ScalarKey: ScalarCore + Eq + core::hash::Hash {}

impl<T> ScalarKey for T where T: ScalarCore + Eq + core::hash::Hash {}

/// Text payload a row can hand to typed record rendering.
pub trait ScalarText: ScalarCore {
    /// Return the canonical payload text.
    fn scalar_text(&self) -> Cow<'_, str>;
}

impl ScalarText for alloc::string::String {
    fn scalar_text(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.as_str())
    }
}

impl ScalarText for i64 {
    fn scalar_text(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }
}

impl ScalarText for uuid::Uuid {
    fn scalar_text(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }
}

impl ScalarText for chrono::NaiveDateTime {
    fn scalar_text(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }
}

impl ScalarText for chrono::DateTime<chrono::Utc> {
    fn scalar_text(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
    }
}

impl ScalarText for chrono::NaiveDate {
    fn scalar_text(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }
}

impl ScalarText for chrono::NaiveTime {
    fn scalar_text(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }
}

impl ScalarText for bigdecimal::BigDecimal {
    fn scalar_text(&self) -> Cow<'_, str> {
        Cow::Owned(self.to_string())
    }
}

/// Payload that can answer a SQL boolean guard.
pub trait ScalarTruth: ScalarCore {
    /// Return the SQL truth value.
    fn scalar_truth(&self) -> bool;
}

impl ScalarTruth for bool {
    fn scalar_truth(&self) -> bool {
        *self
    }
}

impl ScalarTruth for i64 {
    fn scalar_truth(&self) -> bool {
        *self != 0
    }
}

/// SQLite storage classes carried by a column declared as JSON-like.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SqliteJsonStorage {
    /// Stored text, preserved byte-for-byte.
    Text(alloc::string::String),
    /// Stored integer.
    Integer(i64),
    /// Stored real.
    Real(f64),
    /// Stored blob, including SQLite JSONB.
    Blob(alloc::vec::Vec<u8>),
}

/// Lossless SQLite JSON-like value with an optional parsed document.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SqliteJson {
    storage: SqliteJsonStorage,
    document: Option<serde_json::Value>,
}

impl SqliteJson {
    /// Preserves a text value and parses it when valid JSON.
    #[must_use]
    pub fn text(raw: alloc::string::String) -> Self {
        let document = serde_json::from_str(&raw).ok();
        Self {
            storage: SqliteJsonStorage::Text(raw),
            document,
        }
    }

    /// Preserves an integer storage value.
    #[must_use]
    pub fn integer(value: i64) -> Self {
        Self {
            storage: SqliteJsonStorage::Integer(value),
            document: Some(serde_json::Value::Number(value.into())),
        }
    }

    /// Preserves a real storage value.
    #[must_use]
    pub fn real(value: f64) -> Self {
        Self {
            storage: SqliteJsonStorage::Real(value),
            document: serde_json::Number::from_f64(value).map(serde_json::Value::Number),
        }
    }

    /// Preserves a blob storage value.
    #[must_use]
    pub const fn blob(value: alloc::vec::Vec<u8>) -> Self {
        Self {
            storage: SqliteJsonStorage::Blob(value),
            document: None,
        }
    }

    /// Returns the original SQLite storage value.
    #[must_use]
    pub const fn storage(&self) -> &SqliteJsonStorage {
        &self.storage
    }
}

impl From<serde_json::Value> for SqliteJson {
    fn from(document: serde_json::Value) -> Self {
        let raw = document.to_string();
        Self {
            storage: SqliteJsonStorage::Text(raw),
            document: Some(document),
        }
    }
}

/// Payload that may expose a parsed JSON document.
pub trait JsonDocument: ScalarCore {
    /// Borrows the parsed document when the stored value has one.
    fn json_document(&self) -> Option<&serde_json::Value>;
}

impl JsonDocument for serde_json::Value {
    fn json_document(&self) -> Option<&serde_json::Value> {
        Some(self)
    }
}

impl JsonDocument for SqliteJson {
    fn json_document(&self) -> Option<&serde_json::Value> {
        self.document.as_ref()
    }
}

/// One SQL database subql observes.
///
/// An implementation names a database (via its sqlparser dialect) and
/// declares the Rust type its CDC payloads carry for each SQL scalar.
///
/// See [`crate::backend::Postgres`], [`crate::backend::MySql`], and [`crate::backend::SQLite`] for the shipped markers.
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
/// * Postgres and MySQL JSON use [`serde_json::Value`]. SQLite uses
///   [`SqliteJson`] to preserve the storage class and original text.
pub trait Backend: 'static {
    /// Selects a canonical encoder for resolved group columns.
    #[must_use]
    fn group_key_encoder(
        columns: alloc::vec::Vec<GroupKeyColumnOf<Self>>,
    ) -> Option<GroupKeyEncoder<Self>>
    where
        Self: Sized,
    {
        default_group_key_encoder(columns)
    }

    /// Reinterprets a database row field using the planned group-column kind.
    #[must_use]
    fn decode_group_value(_kind: ScalarKindOf<Self>, value: Value<Self>) -> Option<Value<Self>>
    where
        Self: Sized,
    {
        (!value.is_missing()).then_some(value)
    }
    /// SQL parser dialect for this backend.
    type Dialect: sqlparser::dialect::Dialect;

    /// SQL `BOOL` representation. Only equality-shaped operations are
    /// applied to booleans, so truth is the extra row-side capability.
    type Bool: ScalarKey + ScalarTruth;
    /// The embedder's own scalar types, or [`crate::backend::NoCustomScalars`] for a backend
    /// serving none. One rule classifies a declared type name into this set
    /// and both the read and the write side consult it, so a column cannot
    /// bind as one type and read back as another.
    type Custom: CustomScalars<Carrier = Self>;
    /// SQL integer representation (all integer widths roll up to this type).
    /// Supports arithmetic and ordering.
    type Int: ScalarKey
        + ScalarText
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
    ///
    /// Not a [`ScalarKey`]: `NaN` is not equal to itself, so a float cannot key
    /// a membership term's lookup table.
    type Float: ScalarCore
        + PartialOrd
        + core::ops::Add<Output = Self::Float>
        + core::ops::Sub<Output = Self::Float>
        + core::ops::Mul<Output = Self::Float>
        + core::ops::Div<Output = Self::Float>
        + core::ops::Neg<Output = Self::Float>;
    /// SQL text representation (`TEXT`, `VARCHAR`, `CHAR`, ...).
    /// `AsRef<str>` supports `LIKE` / `ILIKE` pattern matching.
    type String: ScalarKey + ScalarText + PartialOrd + AsRef<str>;
    /// SQL binary representation (`BYTEA`, `BLOB`, `VARBINARY`, ...).
    /// `AsRef<[u8]>` supports byte-level comparisons.
    type Bytes: ScalarKey + PartialOrd + AsRef<[u8]>;
    /// SQL UUID representation. Ordered by underlying bytes for `<` / `>`.
    type Uuid: ScalarKey + ScalarText + PartialOrd;
    /// SQL `TIMESTAMP` (no time zone). Ordered chronologically.
    type Timestamp: ScalarKey + ScalarText + PartialOrd;
    /// SQL `TIMESTAMP WITH TIME ZONE`. Ordered chronologically.
    type TimestampTz: ScalarKey + ScalarText + PartialOrd;
    /// SQL `DATE`. Ordered chronologically.
    type Date: ScalarKey + ScalarText + PartialOrd;
    /// SQL `TIME` (no time zone). Ordered chronologically.
    type Time: ScalarKey + ScalarText + PartialOrd;
    /// SQL arbitrary-precision `NUMERIC` / `DECIMAL`. Supports arithmetic
    /// and ordering. No `Rem` bound (see `Float`).
    type Decimal: ScalarKey
        + ScalarText
        + PartialOrd
        + core::ops::Add<Output = Self::Decimal>
        + core::ops::Sub<Output = Self::Decimal>
        + core::ops::Mul<Output = Self::Decimal>
        + core::ops::Div<Output = Self::Decimal>
        + core::ops::Neg<Output = Self::Decimal>;
    /// SQL `JSON` (text-shaped). Comparison beyond equality is undefined,
    /// so no ordering bound.
    type Json: ScalarCore + JsonDocument;
    /// SQL `JSONB` (binary-shaped). No ordering bound (see `Json`).
    type Jsonb: ScalarCore + JsonDocument;
    /// PostgreSQL major whose `jsonb` acceptance rules apply.
    ///
    /// The majors differ only in which numbers they accept, never in the canonical bytes,
    /// and `Postgres<V>` forwards its own parameter here. Backends with their own JSON
    /// semantics name one and never consult it.
    type JsonbVersion: postgres_jsonb_canonical::PgVersion;
}

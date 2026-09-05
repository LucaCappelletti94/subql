//! Split out of the former single `backend.rs`; the module boundary is now real
//! rather than a banner comment.

use super::scalar_value::default_group_key_encoder;
use super::{ColumnComparisonOf, Cow, CustomScalars, GroupKeyEncoder, Value};
use alloc::string::ToString;

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
        columns: alloc::vec::Vec<ColumnComparisonOf<Self>>,
    ) -> Option<GroupKeyEncoder<Self>>
    where
        Self: Sized,
    {
        default_group_key_encoder(columns)
    }

    /// Reinterprets a database row field using the planned group-column kind.
    #[must_use]
    fn decode_group_value(
        _kind: super::scalar_value::ValueKindOf<Self>,
        value: Value<Self>,
    ) -> Option<Value<Self>>
    where
        Self: Sized,
    {
        (!value.is_missing()).then_some(value)
    }

    /// The default `LIKE` escape for this engine, or `None` when it gives
    /// `LIKE` no default escape.
    ///
    /// Required rather than defaulted, because there is no rule that is
    /// right for an unknown engine: PostgreSQL and MySQL escape with a
    /// backslash, SQLite escapes with nothing, and guessing either way
    /// answers some pattern wrongly.
    ///
    /// One answer rather than two constants: an engine with no default
    /// escape cannot have a dangling one, so the two facts belong together.
    const LIKE_ESCAPE: Option<crate::compiler::vm::refusal::LikeEscape>;

    /// The width an arithmetic result is held at, given each operand's
    /// width, or `None` when the operation is not on floats.
    ///
    /// Required rather than defaulted, because the engines disagree.
    /// Measured on PostgreSQL 16.11: `real + real + real` is computed in
    /// float4 and answers `0.3`, while `real * 3` has no operator and
    /// promotes, answering the double product. Measured on MySQL 8.4.11:
    /// `FLOAT + FLOAT + FLOAT` answers the double sum, so MySQL narrows
    /// nothing, and SQLite has no float4 column to narrow.
    #[must_use]
    fn float_arithmetic_width(
        left: Option<super::scalar_value::FloatWidth>,
        right: Option<super::scalar_value::FloatWidth>,
    ) -> Option<super::scalar_value::FloatWidth>
    where
        Self: Sized;

    /// One float value put on the float4 grid, for a result this backend
    /// holds at single width.
    ///
    /// Required rather than defaulted, and deliberately so: a default of
    /// "unchanged" lets this and
    /// [`Backend::float_arithmetic_width`](Backend::float_arithmetic_width)
    /// disagree in silence, so a backend could resolve a float4 result and
    /// then not hold it at float4. A backend on the standard carrier
    /// delegates to [`crate::backend::at_float4`]; one whose float is not
    /// `f64` has no float4 grid this crate can compute and says so by
    /// returning the value.
    #[must_use]
    fn hold_float_at_single(value: Self::Float) -> Self::Float
    where
        Self: Sized;

    /// The exact type a declaration of `family` names on this engine.
    ///
    /// Required rather than defaulted, because the spellings differ: `real`
    /// and `float4` are single width on PostgreSQL, `FLOAT` is on MySQL, and
    /// SQLite's one `REAL` is double. Guessing a width is the defect this
    /// replaces, so each backend states its own.
    #[must_use]
    fn refine_builtin(
        family: super::scalar_value::BuiltinKind,
        declared_type: &str,
    ) -> super::scalar_value::BuiltinType
    where
        Self: Sized;

    /// How this backend answers one text comparison in process, or `None`
    /// when no in-process comparison reproduces it and the statement must
    /// take a database read.
    ///
    /// Asked per operation because reproducibility does not factor per
    /// column: PostgreSQL's default collation has byte equality and locale
    /// ordering at once. Resolved once per comparison at registration and
    /// carried in the compiled program, so no row consults a collation.
    ///
    /// Required rather than defaulted: byte comparison is right for some
    /// engines and silently wrong for others, and guessing is the defect
    /// this answers.
    #[must_use]
    fn text_rule(
        comparison: &super::scalar_value::ComparisonContext<'_, Self>,
        operation: crate::backend::TextOperation,
    ) -> Option<crate::backend::TextRule>
    where
        Self: Sized;

    /// How this backend reads a numeric pair whose operands are two
    /// different scalars, or `None` when it does not compare that pair at
    /// all, which classifies the comparison as a database read.
    ///
    /// Required, and per backend, because the engines disagree: measured,
    /// PostgreSQL and MySQL cast the other operand to `double precision`
    /// against a float and compare exactly against a decimal, while SQLite
    /// compares an integer against a real exactly. Both kinds are builtin,
    /// since only the numeric builtins have a widening.
    ///
    /// The compiler asks this at registration to decide whether to serve
    /// the comparison, and the comparator asks it to answer one, so the
    /// two cannot disagree.
    #[must_use]
    fn numeric_widening(
        left: super::scalar_value::BuiltinKind,
        right: super::scalar_value::BuiltinKind,
    ) -> Option<super::scalar_value::NumericWidening>
    where
        Self: Sized;

    /// How a numeric pair of two different scalars orders under this
    /// backend's widening, or `None` when there is none.
    ///
    /// Defaults to `None`, which classifies every cross-kind comparison as
    /// a database read: a backend carrying its numbers in types other than
    /// `i64`, `f64` and `BigDecimal` has no widening this crate can
    /// perform. A backend on the standard carriers delegates to
    /// [`crate::backend::cross_kind_numeric_ordering`], which
    /// reads the policy from [`Backend::numeric_widening`].
    #[must_use]
    fn compare_cross_kind_numeric(
        _left: &Value<Self>,
        _right: &Value<Self>,
    ) -> Option<core::cmp::Ordering>
    where
        Self: Sized,
    {
        None
    }

    /// What this backend answers when a divisor is zero.
    ///
    /// Required, and per backend, because the engines disagree: measured,
    /// PostgreSQL raises `division by zero` for `/` and `%` alike and for
    /// every numeric type, while MySQL and SQLite answer `NULL`.
    const DIVISION_BY_ZERO: crate::compiler::vm::refusal::DivisionByZero;

    /// What `/` answers on this backend: whether two integers divide to an
    /// integer, and what scale a decimal quotient carries.
    ///
    /// Required, and per backend, because the engines disagree at every
    /// input rather than at a boundary: measured, `7 / 2 > 3` is false on
    /// PostgreSQL and SQLite and true on MySQL, whose `/` is decimal
    /// division whatever its operands are. See
    /// [`DivisionRule`](super::scalar_value::DivisionRule).
    const DIVISION: super::scalar_value::DivisionRule;

    /// Two decimals divided at the scale this backend's
    /// [`Backend::DIVISION`] computes it to.
    ///
    /// Required rather than defaulted, for the reason
    /// [`Backend::hold_float_at_single`] is: a default would let the rule
    /// and the arithmetic disagree in silence. A backend on the standard
    /// `bigdecimal` carrier delegates to
    /// [`crate::compiler::vm::arithmetic::quotient_in_words`] or
    /// [`crate::compiler::vm::arithmetic::quotient_at_significant_digits`]
    /// according to the rule it states.
    #[must_use]
    fn decimal_quotient(
        dividend: Self::Decimal,
        divisor: Self::Decimal,
        quotient: crate::compiler::bytecode::Quotient,
    ) -> Self::Decimal
    where
        Self: Sized;

    /// Two integers divided to a decimal, for a backend whose `/` answers
    /// one.
    ///
    /// Called only under
    /// [`DivisionRule::QuotientsAreDecimalInWords`](super::scalar_value::DivisionRule::QuotientsAreDecimalInWords),
    /// where `7 / 2` is `3.5000` rather than `3`. A backend on the
    /// standard carriers widens both sides and delegates to
    /// [`crate::compiler::vm::arithmetic::quotient_in_words`].
    #[must_use]
    fn integer_quotient(
        dividend: Self::Int,
        divisor: Self::Int,
        increment: super::scalar_value::DivisionPrecisionIncrement,
    ) -> Self::Decimal
    where
        Self: Sized;

    /// Integer `+`, `-` or `*` as this backend answers it, including what
    /// it answers when the result does not fit.
    ///
    /// Required, and per backend, because the engines disagree: measured,
    /// PostgreSQL and MySQL raise `out of range` while SQLite promotes the
    /// result to a real. A backend on the standard `i64` carrier delegates
    /// to [`crate::compiler::vm::arithmetic::checked_integer_binary`].
    ///
    /// # Errors
    ///
    /// The failure this backend's engine raises, which the caller reports
    /// per subscription rather than folding into `Value::Null`.
    fn integer_binary(
        operation: crate::compiler::vm::refusal::ArithmeticOp,
        left: Self::Int,
        right: Self::Int,
    ) -> Result<Value<Self>, crate::compiler::vm::refusal::EvaluationRefusal>
    where
        Self: Sized;

    /// Integer unary `-`, same contract as [`Backend::integer_binary`].
    ///
    /// # Errors
    ///
    /// The failure this backend's engine raises.
    fn integer_negate(
        value: Self::Int,
    ) -> Result<Value<Self>, crate::compiler::vm::refusal::EvaluationRefusal>
    where
        Self: Sized;

    /// Whether two scalars are equal for this backend, given both operands'
    /// catalog facts.
    ///
    /// The default is the structural same-scalar rule and reads no facts. A
    /// backend whose engine disagrees (PostgreSQL NaN, a collation the
    /// comparator can reproduce, `char(n)` padding, a cross-width numeric
    /// pair) overrides this and reads the context, which is why the context
    /// carries both sides rather than one.
    #[must_use]
    fn scalars_equal(
        comparison: super::scalar_value::ComparisonContext<'_, Self>,
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> bool
    where
        Self: Sized,
    {
        crate::compiler::value_cmp::structural_equality(comparison, left, right)
    }

    /// How two scalars order for this backend, or `None` when the pair has
    /// no defined order, which the caller lifts to `Tri::Unknown`.
    ///
    /// Same contract as [`Backend::scalars_equal`].
    #[must_use]
    fn compare_scalars(
        comparison: super::scalar_value::ComparisonContext<'_, Self>,
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> Option<core::cmp::Ordering>
    where
        Self: Sized,
    {
        crate::compiler::value_cmp::structural_ordering(comparison, left, right)
    }

    /// SQL parser dialect for this backend.
    type Dialect: sqlparser::dialect::Dialect;

    /// SQL `BOOL` representation.
    ///
    /// `PartialOrd` is required because SQL orders booleans, and the order
    /// has to be the carrier's own: a backend whose boolean really is an
    /// integer, as SQLite's is, reports `2` above `1`, which deriving the
    /// order from [`ScalarTruth`] would flatten to equal.
    type Bool: ScalarKey + ScalarTruth + PartialOrd;
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

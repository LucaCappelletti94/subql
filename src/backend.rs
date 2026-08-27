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
use alloc::{borrow::Cow, string::ToString, vec::Vec};
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
///
/// `C` names the embedder's own scalar types, one variant per type they
/// taught the backend (see [`CustomScalars`]). It carries no default on
/// purpose: a site that wrote the bare name would silently get the
/// no-customs universe and skip deciding what a custom column means there,
/// which is the whole point of threading it. A backend serving no custom
/// types instantiates it at [`NoCustom`], which is uninhabited, so
/// [`Self::Custom`] is unreachable and its arms discharge by matching the
/// payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ScalarKind<C> {
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
    /// One of the embedder's own types, named by its compile-time variant.
    Custom(C),
}

/// The kind of a column a custom type travels as on the wire, and of a
/// column no embedder extended: [`ScalarKind`] with the custom position
/// made unreachable.
///
/// This is the only place a kind may be spelled without a custom universe,
/// and it is a distinct type from `ScalarKind<C>` for any real `C`, so it
/// cannot stand in for a column type by accident.
pub type BuiltinKind = ScalarKind<NoCustom>;

/// The kind of a column under backend `B`, custom position included.
///
/// Spelling this alias is how a site says "a column type of this backend",
/// as against [`BuiltinKind`], which says "a builtin shape".
pub type ScalarKindOf<B> = ScalarKind<<<B as Backend>::Custom as CustomScalars>::Kind>;
/// Owned SQL name for a group-key column's declared collation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GroupKeyCollationName {
    /// Identifier without surrounding quotes.
    pub name: alloc::string::String,
    /// Whether the identifier was quoted.
    pub name_is_quoted: bool,
    /// Optional schema identifier without surrounding quotes.
    pub schema: Option<alloc::string::String>,
    /// Whether the schema identifier was quoted.
    pub schema_is_quoted: bool,
}

/// Comparison metadata for a group-key column.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum GroupKeyCollation {
    /// The database default applies.
    DatabaseDefault,
    /// The column declares a named collation.
    Named {
        /// Declared collation name.
        name: GroupKeyCollationName,
        /// PostgreSQL determinism when known.
        postgres_deterministic: Option<bool>,
        /// MySQL padding behavior when known.
        mysql_padding: Option<sql_traits::traits::MySqlCollationPadding>,
    },
    /// Comparison rules changed without a resolved collation name.
    Unknown,
}

/// Catalog facts needed to decide whether a column can form a group key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GroupKeyColumn<C> {
    /// Scalar kind used by subql.
    pub kind: ScalarKind<C>,
    /// Canonical declared SQL type.
    pub declared_type: alloc::string::String,
    /// Column comparison metadata.
    pub collation: GroupKeyCollation,
}

/// Group-key catalog facts under backend `B`.
pub type GroupKeyColumnOf<B> = GroupKeyColumn<<<B as Backend>::Custom as CustomScalars>::Kind>;

/// The custom scalar set of a backend that has none. Uninhabited, so
/// [`ScalarKind::Custom`] cannot be constructed for such a backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum NoCustom {}

impl<C> ScalarKind<C> {
    /// Re-tag a kind that names no custom type into any custom universe.
    ///
    /// The carrier a custom type declares is a [`BuiltinKind`], and the
    /// decoders it feeds speak `ScalarKind<C>`, so one total mapping sits
    /// between them rather than an ad-hoc match at each call.
    #[must_use]
    pub const fn from_builtin(kind: BuiltinKind) -> Self {
        match kind {
            ScalarKind::Bool => Self::Bool,
            ScalarKind::Int => Self::Int,
            ScalarKind::Float => Self::Float,
            ScalarKind::String => Self::String,
            ScalarKind::Bytes => Self::Bytes,
            ScalarKind::Uuid => Self::Uuid,
            ScalarKind::Timestamp => Self::Timestamp,
            ScalarKind::TimestampTz => Self::TimestampTz,
            ScalarKind::Date => Self::Date,
            ScalarKind::Time => Self::Time,
            ScalarKind::Decimal => Self::Decimal,
            ScalarKind::Json => Self::Json,
            ScalarKind::Jsonb => Self::Jsonb,
            ScalarKind::Custom(none) => match none {},
        }
    }

    /// This kind as a builtin, or `None` when it names a custom type.
    ///
    /// The inverse of [`Self::from_builtin`], used where a builtin-only
    /// decoder has to be handed a kind it can actually accept.
    #[must_use]
    pub const fn as_builtin(&self) -> Option<BuiltinKind> {
        Some(match self {
            Self::Bool => ScalarKind::Bool,
            Self::Int => ScalarKind::Int,
            Self::Float => ScalarKind::Float,
            Self::String => ScalarKind::String,
            Self::Bytes => ScalarKind::Bytes,
            Self::Uuid => ScalarKind::Uuid,
            Self::Timestamp => ScalarKind::Timestamp,
            Self::TimestampTz => ScalarKind::TimestampTz,
            Self::Date => ScalarKind::Date,
            Self::Time => ScalarKind::Time,
            Self::Decimal => ScalarKind::Decimal,
            Self::Json => ScalarKind::Json,
            Self::Jsonb => ScalarKind::Jsonb,
            Self::Custom(_) => return None,
        })
    }

    /// The custom type this kind names, or `None` for a builtin.
    pub const fn custom(&self) -> Option<&C> {
        match self {
            Self::Custom(c) => Some(c),
            _ => None,
        }
    }
}

/// What an embedder teaches a [`Backend`] about its own scalar types.
///
/// One implementation describes the whole custom set: [`Self::Kind`] names
/// the types (one variant each, so identity is compile-time and two
/// same-named types in different schemas stay distinct), and
/// [`Self::Value`] carries a decoded one.
///
/// The four methods are the whole surface, and each answers exactly once
/// for the read side, the write side, and SQL literals alike:
///
/// * [`Self::classify`] turns a declared catalog type name into a kind, and
///   is the single rule both directions consult, so a column cannot bind as
///   one type and read back as another.
/// * [`Self::carrier`] names the builtin shape the type travels as on the
///   wire, which lets the existing decoders keep producing what they always
///   did before conversion.
/// * [`Self::convert`] turns that builtin value into a custom one. A change
///   event, a diesel bind and a SQL literal all arrive here, so a filter's
///   value and a row's value cannot disagree about what a spelling means.
/// * [`Self::can_key`] answers whether a membership term may compare the
///   type, the same question [`crate::term::kind_can_key`] answers for
///   builtins.
pub trait CustomScalars: 'static {
    /// The embedder's types, one variant per type.
    type Kind: Copy + Eq + core::hash::Hash + core::fmt::Debug + Send + Sync + 'static;

    /// A decoded custom value. `Eq + Hash` because a membership term keys
    /// its lookup on this value (see [`Self::can_key`]), and the serde
    /// bounds because a literal of this type reaches a persisted predicate.
    type Value: Clone
        + core::fmt::Debug
        + Eq
        + core::hash::Hash
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static;
    /// Which custom type, if any, a declared catalog type name means.
    ///
    /// Receives the name as the catalog spells it. An embedder that wants
    /// two same-named types in different schemas told apart resolves that
    /// here, which a bare name comparison could not.
    fn classify(declared_type: &str) -> Option<Self::Kind>;

    /// The builtin shape `kind` travels as on the wire.
    fn carrier(kind: Self::Kind) -> BuiltinKind;

    /// Turn a wire or literal value of `kind`'s carrier shape into a custom
    /// value, or refuse it.
    ///
    /// Refusing is reported against the column that carried it rather than
    /// silently dropping the cell. The argument is a [`Carried`] rather than
    /// a [`Value`] because a [`Value`] can itself be a custom, and a custom
    /// never carries a custom.
    fn convert(kind: Self::Kind, carried: Carried<'_, Self::Carrier>) -> Option<Self::Value>;

    /// The backend these types extend, named so [`Self::convert`] can read
    /// its payload types.
    type Carrier: Backend;

    /// May a membership term compare a column of this type?
    ///
    /// The builtin rule is that equality must be reflexive, since a lookup
    /// keyed on a value has to find what it stored. `true` requires the same
    /// of [`Self::Value`], which its `Eq` bound already promises, so this
    /// exists for a type that wants to refuse keying for its own reasons.
    fn can_key(kind: Self::Kind) -> bool;

    /// Which custom type a decoded value is.
    ///
    /// Keeps [`Value::scalar_kind`] total: a custom cell can name its own
    /// kind instead of reporting no kind at all, which would read as
    /// "carries no scalar".
    fn kind_of(value: &Self::Value) -> Self::Kind;
}

/// The custom set of a backend serving none.
///
/// Uninhabited [`Kind`](CustomScalars::Kind) and
/// [`Value`](CustomScalars::Value), so [`ScalarKind::Custom`] and
/// [`Value::Custom`] cannot be constructed for such a backend and every
/// method below discharges by matching an impossible value.
pub struct NoCustomScalars<B>(core::marker::PhantomData<fn() -> B>);

impl<B: Backend> CustomScalars for NoCustomScalars<B> {
    type Kind = NoCustom;
    type Value = NoCustom;
    type Carrier = B;

    fn classify(_declared_type: &str) -> Option<Self::Kind> {
        None
    }

    fn carrier(kind: Self::Kind) -> BuiltinKind {
        match kind {}
    }

    fn convert(kind: Self::Kind, _carried: Carried<'_, B>) -> Option<Self::Value> {
        match kind {}
    }

    fn can_key(kind: Self::Kind) -> bool {
        match kind {}
    }

    fn kind_of(value: &Self::Value) -> Self::Kind {
        // No `&NoCustom` can exist, since the type is uninhabited, so this is
        // unreachable. Discharging it by matching through the reference would
        // be a dereference of an uninhabited place.
        let _ = value;
        unreachable!("NoCustom is uninhabited, so no reference to one exists")
    }
}

/// A borrowed builtin payload of `B`, handed to
/// [`CustomScalars::convert`].
///
/// One variant per builtin kind and no custom position, so the type states
/// what the contract needs: whatever carries a custom value is itself never
/// custom. Borrowed rather than owned, since the decoders already hold the
/// value they just built.
#[derive(Debug)]
pub enum Carried<'a, B: Backend> {
    /// [`Backend::Bool`] payload.
    Bool(&'a B::Bool),
    /// [`Backend::Int`] payload.
    Int(&'a B::Int),
    /// [`Backend::Float`] payload.
    Float(&'a B::Float),
    /// [`Backend::String`] payload.
    String(&'a B::String),
    /// [`Backend::Bytes`] payload.
    Bytes(&'a B::Bytes),
    /// [`Backend::Uuid`] payload.
    Uuid(&'a B::Uuid),
    /// [`Backend::Timestamp`] payload.
    Timestamp(&'a B::Timestamp),
    /// [`Backend::TimestampTz`] payload.
    TimestampTz(&'a B::TimestampTz),
    /// [`Backend::Date`] payload.
    Date(&'a B::Date),
    /// [`Backend::Time`] payload.
    Time(&'a B::Time),
    /// [`Backend::Decimal`] payload.
    Decimal(&'a B::Decimal),
    /// [`Backend::Json`] payload.
    Json(&'a B::Json),
    /// [`Backend::Jsonb`] payload.
    Jsonb(&'a B::Jsonb),
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
    /// One of the embedder's own types, decoded through
    /// [`CustomScalars::convert`].
    Custom(<B::Custom as CustomScalars>::Value),
}

impl<B: Backend> Value<B> {
    /// Discriminant tag for this value, or `None` for [`Value::Missing`] and
    /// [`Value::Null`] (which do not correspond to a specific scalar type).
    #[inline]
    pub fn scalar_kind(&self) -> Option<ScalarKindOf<B>> {
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
            Self::Custom(x) => Some(ScalarKind::Custom(<B::Custom as CustomScalars>::kind_of(x))),
        }
    }

    /// This value as a borrowed builtin payload, or `None` when it is
    /// absent or already custom.
    ///
    /// Feeds [`CustomScalars::convert`], which reads what a carrier
    /// delivered and never reads a custom.
    #[must_use]
    pub const fn as_carried(&self) -> Option<Carried<'_, B>> {
        Some(match self {
            Self::Missing | Self::Null | Self::Custom(_) => return None,
            Self::Bool(x) => Carried::Bool(x),
            Self::Int(x) => Carried::Int(x),
            Self::Float(x) => Carried::Float(x),
            Self::String(x) => Carried::String(x),
            Self::Bytes(x) => Carried::Bytes(x),
            Self::Uuid(x) => Carried::Uuid(x),
            Self::Timestamp(x) => Carried::Timestamp(x),
            Self::TimestampTz(x) => Carried::TimestampTz(x),
            Self::Date(x) => Carried::Date(x),
            Self::Time(x) => Carried::Time(x),
            Self::Decimal(x) => Carried::Decimal(x),
            Self::Json(x) => Carried::Json(x),
            Self::Jsonb(x) => Carried::Jsonb(x),
        })
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
            Self::Custom(x) => Self::Custom(x.clone()),
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
            Self::Custom(x) => f.debug_tuple("Custom").field(x).finish(),
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
            // Without this arm the wildcard below answers `false` for two
            // equal custom values, which is the silent wrong answer the
            // wildcard hides from the compiler.
            (Self::Custom(a), Self::Custom(b)) => a == b,
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

/// Payload that stores a JSON document.
pub trait JsonDocument: ScalarCore {
    /// Borrow the stored document.
    fn json_document(&self) -> &serde_json::Value;
}

impl JsonDocument for serde_json::Value {
    fn json_document(&self) -> &serde_json::Value {
        self
    }
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
    /// Whether two text values that differ in bytes are two different values
    /// to this database.
    ///
    /// Grouping needs it. A grouped fold decides a row's group by encoding the
    /// group columns, while the database decides it by its own equality, and
    /// the two must agree or a seed row and a later change land in different
    /// groups and both totals go wrong with nothing failing.
    ///
    /// Measured rather than assumed. Postgres text collations are deterministic
    /// unless a column declares otherwise, and SQLite's default is `BINARY`, so
    /// `'a'` and `'A'` are two groups on both. MySQL 8.0 ships
    /// `utf8mb4_0900_ai_ci` as the server default, so they are one group there
    /// out of the box, and because that collation comes from server and table
    /// defaults rather than the column's DDL it is not even visible in the
    /// schema text, so it cannot be detected per column.
    ///
    /// [`ScalarKind::Uuid`] rides this too, since MySQL and SQLite carry a uuid
    /// as text while Postgres carries a parsed one.
    const TEXT_GROUPS_BY_BYTES: bool;

    /// sqlparser dialect for parsing subscription text and DDL under this
    /// backend.
    type Dialect: sqlparser::dialect::Dialect;

    /// SQL `BOOL` representation. Only equality-shaped operations are
    /// applied to booleans, so truth is the extra row-side capability.
    type Bool: ScalarKey + ScalarTruth;
    /// The embedder's own scalar types, or [`NoCustomScalars`] for a backend
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
}

// ---------------------------------------------------------------------------
// Shipped backends
// ---------------------------------------------------------------------------

/// Postgres backend marker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Postgres;

impl Backend for Postgres {
    const TEXT_GROUPS_BY_BYTES: bool = true;
    type Custom = NoCustomScalars<Self>;
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
    const TEXT_GROUPS_BY_BYTES: bool = false;
    type Custom = NoCustomScalars<Self>;
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
    const TEXT_GROUPS_BY_BYTES: bool = true;
    type Custom = NoCustomScalars<Self>;
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

/// Decode one cell whose column kind may name a custom type, by handing a
/// builtin kind to `decode` and converting afterwards when it does.
///
/// The four wire decoders stay builtin-only and unchanged: this is the one
/// place that knows a custom column is read by decoding its carrier and then
/// converting, so the four paths cannot drift apart on it.
///
/// # Errors
///
/// [`crate::ValueError::Builtin`] when `decode` could not read the bytes as the
/// kind (or as the carrier, for a custom), and [`crate::ValueError::Custom`] when
/// the carrier read fine and the type's own conversion declined it. Keeping
/// those apart is why this returns a `Result` rather than
/// [`Value::Missing`].
pub fn decode_cell<B, F>(
    column: crate::ColumnId,
    kind: ScalarKindOf<B>,
    decode: F,
) -> Result<Value<B>, crate::ValueError>
where
    B: Backend,
    F: FnOnce(BuiltinKind) -> Value<B>,
{
    let Some(custom) = kind.custom().copied() else {
        // Total: `custom()` answered `None`, so `as_builtin` answers `Some`.
        let builtin = kind.as_builtin().unwrap_or(ScalarKind::String);
        let decoded = decode(builtin);
        return if decoded.is_missing() {
            Err(crate::ValueError::Builtin {
                column,
                kind: builtin,
            })
        } else {
            Ok(decoded)
        };
    };

    let carrier = <B::Custom as CustomScalars>::carrier(custom);
    let raw = decode(carrier);
    let Some(view) = raw.as_carried() else {
        return Err(crate::ValueError::Builtin {
            column,
            kind: carrier,
        });
    };
    <B::Custom as CustomScalars>::convert(custom, view)
        .map(Value::Custom)
        .ok_or_else(|| crate::ValueError::Custom {
            column,
            custom: alloc::format!("{custom:?}"),
        })
}

/// Encode a tuple of values into stable bytes usable as a map key.
///
/// [`Value`] carries floats, so it has neither [`Hash`](core::hash::Hash) nor
/// [`Ord`] and cannot key a map directly. Postcard's encoding is
/// length-prefixed per element, so `["a", "b"]` and `["ab", ""]` differ, which
/// a naive concatenation would not.
///
/// Public because it is the encoding behind the opaque group key on
/// [`AggregateValueUpdate`](crate::AggregateValueUpdate), pinned byte for
/// byte, so a consumer may compute a group's key from its values.
///
/// Returns `None` when the tuple cannot be encoded. Callers must not treat
/// that as "no match": a keyed read falls back to comparing values, and a
/// grouped fold refuses the column kinds that could produce it before any row
/// is read.
pub fn encode_value_key<B: Backend>(values: &[Value<B>]) -> Option<alloc::vec::Vec<u8>> {
    postcard::to_allocvec(values).ok()
}

/// Can a column of this kind identify a group?
///
/// A grouped fold decides a row's group by encoding the group columns with
/// [`encode_value_key`], while the database decides it with its own equality.
/// A kind qualifies only when the two agree, so that two rows the database
/// puts in one group always encode alike and two rows it separates never do.
/// Where they disagree the fold would seed one group and then open a second
/// from zero on the next change, leaving both totals wrong and nothing failing,
/// so such a query is refused here and served by re-reading it instead.
///
/// Exhaustive rather than a wildcard, so a new kind has to be classified
/// instead of silently joining whichever side is the default.
///
/// Measured, not reasoned. Postgres and SQLite both put `0.0` and `-0.0` in one
/// group and two `NaN`s in one group while their bit patterns differ, and
/// Postgres puts `1.0::numeric` and `1.00::numeric` in one group while subql
/// carries decimals as text to keep precision. Text and uuid vary by backend
/// and ride [`Backend::TEXT_GROUPS_BY_BYTES`].
pub(crate) const fn kind_groups_one_to_one<B: Backend>(kind: Option<BuiltinKind>) -> bool {
    match kind {
        // Exact values with a canonical representation. `Timestamp`, `Date` and
        // `Time` are parsed `chrono` types and `TimestampTz` normalises to UTC
        // before subql sees it, so two spellings of one instant are one value.
        Some(
            ScalarKind::Int
            | ScalarKind::Bool
            | ScalarKind::Bytes
            | ScalarKind::Timestamp
            | ScalarKind::TimestampTz
            | ScalarKind::Date
            | ScalarKind::Time,
        ) => true,
        Some(ScalarKind::String | ScalarKind::Uuid) => B::TEXT_GROUPS_BY_BYTES,
        // Float: the database groups `-0.0` with `0.0` and `NaN` with `NaN`.
        // Decimal: it groups `1.0` with `1.00`, which differ as text.
        // Json: whitespace and key order vary without changing the document.
        // Custom and unknown: subql cannot know how the database compares them.
        Some(
            ScalarKind::Float
            | ScalarKind::Decimal
            | ScalarKind::Json
            | ScalarKind::Jsonb
            | ScalarKind::Custom(_),
        )
        | None => false,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod value_key_tests {
    use super::{encode_value_key, Postgres, Value};
    use alloc::vec;

    /// The encoding is frozen, byte for byte.
    ///
    /// These bytes leave the process. A consumer stores them as the key of a
    /// row it keeps across restarts and across subql versions, so changing how
    /// a value encodes does not produce a migration, it produces a second row
    /// for a group that already had one, with the old row never updated again.
    /// Injectivity is deliberately not pinned here: postcard gives it
    /// structurally, and both wrong implementations tried against an
    /// injectivity test kept it, so such a test asserts nothing. Stability is
    /// what nothing else defends.
    #[test]
    fn the_encoding_is_frozen() {
        let cases: vec::Vec<(vec::Vec<Value<Postgres>>, &[u8])> = vec![
            (vec![], &[0]),
            (vec![Value::Int(1)], &[1, 3, 2]),
            (vec![Value::String("eu".into())], &[1, 5, 2, b'e', b'u']),
            (
                vec![Value::String("a".into()), Value::String("b".into())],
                &[2, 5, 1, b'a', 5, 1, b'b'],
            ),
            (
                vec![
                    Value::String("ab".into()),
                    Value::String(alloc::string::String::new()),
                ],
                &[2, 5, 2, b'a', b'b', 5, 0],
            ),
            (vec![Value::Null], &[1, 1]),
            (vec![Value::Bool(true)], &[1, 2, 1]),
            (vec![Value::Bytes(vec![1, 2])], &[1, 6, 2, 1, 2]),
            (
                vec![Value::Uuid(
                    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
                        .expect("valid UUID"),
                )],
                &[
                    1, 7, 16, 85, 14, 132, 0, 226, 155, 65, 212, 167, 22, 68, 102, 85, 68, 0, 0,
                ],
            ),
            (
                vec![Value::Timestamp(
                    chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
                        .expect("valid date")
                        .and_hms_opt(3, 4, 5)
                        .expect("valid time"),
                )],
                &[
                    1, 8, 19, 50, 48, 50, 54, 45, 48, 49, 45, 48, 50, 84, 48, 51, 58, 48, 52, 58,
                    48, 53,
                ],
            ),
            (
                vec![Value::TimestampTz(
                    chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
                        .expect("valid date")
                        .and_hms_opt(3, 4, 5)
                        .expect("valid time")
                        .and_utc(),
                )],
                &[
                    1, 9, 20, 50, 48, 50, 54, 45, 48, 49, 45, 48, 50, 84, 48, 51, 58, 48, 52, 58,
                    48, 53, 90,
                ],
            ),
            (
                vec![Value::Date(
                    chrono::NaiveDate::from_ymd_opt(2026, 1, 2).expect("valid date"),
                )],
                &[1, 10, 10, 50, 48, 50, 54, 45, 48, 49, 45, 48, 50],
            ),
            (
                vec![Value::Time(
                    chrono::NaiveTime::from_hms_opt(3, 4, 5).expect("valid time"),
                )],
                &[1, 11, 8, 48, 51, 58, 48, 52, 58, 48, 53],
            ),
        ];
        for (tuple, want) in cases {
            let got = encode_value_key(&tuple).expect("every kind here is encodable");
            assert_eq!(
                got, want,
                "the encoding of {tuple:?} changed, which orphans every stored key that used it"
            );
        }
    }
}

#[cfg(test)]
mod grouping_kind_tests {
    use super::{kind_groups_one_to_one, MySql, Postgres, SQLite, ScalarKind};

    /// Every kind, classified, on every backend.
    ///
    /// Exhaustive by construction rather than by sampling: a new [`ScalarKind`]
    /// makes this fail to compile until it is classified, which is the point.
    /// Getting one wrong is silent, since the fold and the database would
    /// simply disagree about how many groups there are.
    fn expect<B: super::Backend>(text_safe: bool) {
        for kind in [
            ScalarKind::Int,
            ScalarKind::Bool,
            ScalarKind::Bytes,
            ScalarKind::Timestamp,
            ScalarKind::TimestampTz,
            ScalarKind::Date,
            ScalarKind::Time,
        ] {
            assert!(
                kind_groups_one_to_one::<B>(Some(kind)),
                "{kind:?} encodes one-to-one on every backend"
            );
        }
        for kind in [
            ScalarKind::Float,
            ScalarKind::Decimal,
            ScalarKind::Json,
            ScalarKind::Jsonb,
        ] {
            assert!(
                !kind_groups_one_to_one::<B>(Some(kind)),
                "{kind:?} has values the database groups together and the encoding separates"
            );
        }
        // Text, and uuid where the backend carries it as text.
        assert_eq!(
            kind_groups_one_to_one::<B>(Some(ScalarKind::String)),
            text_safe
        );
        assert_eq!(
            kind_groups_one_to_one::<B>(Some(ScalarKind::Uuid)),
            text_safe
        );
        // An unknown column type is not a licence to guess.
        assert!(!kind_groups_one_to_one::<B>(None));
    }

    /// Postgres text collations are deterministic unless one is declared
    /// otherwise per column, measured as two groups for 'a' and 'A'.
    #[test]
    fn postgres_groups_text_by_bytes() {
        expect::<Postgres>(true);
    }

    /// SQLite's default collation is BINARY, measured as two groups.
    #[test]
    fn sqlite_groups_text_by_bytes() {
        expect::<SQLite>(true);
    }

    /// MySQL's server default is `utf8mb4_0900_ai_ci`, so 'a' and 'A' are one
    /// group, measured on the image this repo's tests use. The collation comes
    /// from server and table defaults rather than the column's DDL, so it is
    /// absent from the schema text and cannot be detected per column either.
    #[test]
    fn mysql_does_not_group_text_by_bytes() {
        expect::<MySql>(false);
    }
}

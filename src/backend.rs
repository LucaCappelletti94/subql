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

type GroupKeyComponentEncoder<B> =
    fn(&GroupKeyColumnOf<B>, &Value<B>, &mut alloc::vec::Vec<u8>) -> bool;

/// Canonical identity encoder selected for one grouped projection.
pub struct GroupKeyEncoder<B: Backend> {
    columns: alloc::sync::Arc<[GroupKeyColumnOf<B>]>,
    encode_component: GroupKeyComponentEncoder<B>,
}

impl<B: Backend> GroupKeyEncoder<B> {
    /// Creates an encoder from resolved columns and one backend component writer.
    #[must_use]
    pub fn new(
        columns: alloc::vec::Vec<GroupKeyColumnOf<B>>,
        encode_component: GroupKeyComponentEncoder<B>,
    ) -> Self {
        Self {
            columns: columns.into(),
            encode_component,
        }
    }

    /// Encodes one tuple, or refuses values outside the selected column domain.
    #[must_use]
    pub fn encode(&self, values: &[Value<B>]) -> Option<alloc::vec::Vec<u8>> {
        let count = u16::try_from(self.columns.len()).ok()?;
        if values.len() != self.columns.len() {
            return None;
        }
        let mut key = alloc::vec::Vec::new();
        key.extend_from_slice(b"SQGK");
        key.push(1);
        key.extend_from_slice(&count.to_be_bytes());
        for (column, value) in self.columns.iter().zip(values) {
            let length_at = key.len();
            key.extend_from_slice(&[0; 4]);
            let component_at = key.len();
            if !(self.encode_component)(column, value, &mut key) {
                return None;
            }
            let length = u32::try_from(key.len() - component_at).ok()?;
            key[length_at..component_at].copy_from_slice(&length.to_be_bytes());
        }
        Some(key)
    }

    /// Columns whose comparison contract selected this encoder.
    #[must_use]
    pub fn columns(&self) -> &[GroupKeyColumnOf<B>] {
        &self.columns
    }
}

impl<B: Backend> Clone for GroupKeyEncoder<B> {
    fn clone(&self) -> Self {
        Self {
            columns: alloc::sync::Arc::clone(&self.columns),
            encode_component: self.encode_component,
        }
    }
}

impl<B: Backend> core::fmt::Debug for GroupKeyEncoder<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("GroupKeyEncoder")
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

struct AppendPostcard<'a>(&'a mut alloc::vec::Vec<u8>);

impl postcard::ser_flavors::Flavor for AppendPostcard<'_> {
    type Output = ();

    fn try_extend(&mut self, data: &[u8]) -> postcard::Result<()> {
        self.0.extend_from_slice(data);
        Ok(())
    }

    fn try_push(&mut self, data: u8) -> postcard::Result<()> {
        self.0.push(data);
        Ok(())
    }

    fn finalize(self) -> postcard::Result<Self::Output> {
        Ok(())
    }
}

fn append_postcard<T: serde::Serialize + ?Sized>(
    output: &mut alloc::vec::Vec<u8>,
    value: &T,
) -> bool {
    postcard::serialize_with_flavor::<T, AppendPostcard<'_>, ()>(value, AppendPostcard(output))
        .is_ok()
}

fn encode_exact_component<B: Backend>(
    column: &GroupKeyColumnOf<B>,
    value: &Value<B>,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    macro_rules! tagged {
        ($tag:literal, $value:expr) => {{
            output.push($tag);
            append_postcard(output, $value)
        }};
    }
    match (column.kind, value) {
        (_, Value::Null) => {
            output.push(0);
            true
        }
        (ScalarKind::Bool, Value::Bool(value)) => tagged!(1, value),
        (ScalarKind::Int, Value::Int(value)) => tagged!(2, value),
        (ScalarKind::Float, Value::Float(value)) => tagged!(3, value),
        (ScalarKind::String, Value::String(value)) => tagged!(4, value),
        (ScalarKind::Bytes, Value::Bytes(value)) => tagged!(5, value),
        (ScalarKind::Uuid, Value::Uuid(value)) => tagged!(6, value),
        (ScalarKind::Timestamp, Value::Timestamp(value)) => tagged!(7, value),
        (ScalarKind::TimestampTz, Value::TimestampTz(value)) => tagged!(8, value),
        (ScalarKind::Date, Value::Date(value)) => tagged!(9, value),
        (ScalarKind::Time, Value::Time(value)) => tagged!(10, value),
        (ScalarKind::Decimal, Value::Decimal(value)) => tagged!(11, value),
        (ScalarKind::Json, Value::Json(value)) => tagged!(12, value),
        (ScalarKind::Jsonb, Value::Jsonb(value)) => tagged!(13, value),
        (ScalarKind::Custom(kind), Value::Custom(value))
            if kind == <B::Custom as CustomScalars>::kind_of(value) =>
        {
            tagged!(14, value)
        }
        _ => false,
    }
}

fn default_group_key_encoder<B: Backend>(
    columns: alloc::vec::Vec<GroupKeyColumnOf<B>>,
) -> Option<GroupKeyEncoder<B>> {
    let supported = columns.iter().all(|column| {
        matches!(
            column.kind,
            ScalarKind::Int
                | ScalarKind::Bool
                | ScalarKind::Bytes
                | ScalarKind::Timestamp
                | ScalarKind::TimestampTz
                | ScalarKind::Date
                | ScalarKind::Time
        )
    });
    supported.then(|| GroupKeyEncoder::new(columns, encode_exact_component::<B>))
}

fn decode_exact_group_value<B: Backend>(
    kind: ScalarKindOf<B>,
    value: Value<B>,
) -> Option<Value<B>> {
    if value.is_null() || value.scalar_kind() == Some(kind) {
        Some(value)
    } else {
        None
    }
}

#[derive(Clone, Copy)]
enum TextKey {
    Exact,
    AsciiNoCase,
    TrimTrailingSpace,
}

fn canonical_f64(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else if value.is_nan() {
        f64::from_bits(0x7ff8_0000_0000_0000)
    } else {
        value
    }
}

#[allow(clippy::cast_precision_loss)]
const fn widen_i64_to_f64(value: i64) -> f64 {
    value as f64 // Deliberate SQL double-precision rounding.
}

fn append_tagged<T: serde::Serialize + ?Sized>(
    output: &mut alloc::vec::Vec<u8>,
    tag: u8,
    value: &T,
) -> bool {
    output.push(tag);
    append_postcard(output, value)
}

fn append_text(output: &mut alloc::vec::Vec<u8>, tag: u8, value: &str, mode: TextKey) -> bool {
    match mode {
        TextKey::Exact => append_tagged(output, tag, value),
        TextKey::TrimTrailingSpace => append_tagged(output, tag, value.trim_end_matches(' ')),
        TextKey::AsciiNoCase => {
            let mut canonical = value.as_bytes().to_vec();
            canonical.make_ascii_lowercase();
            output.push(tag);
            append_postcard(output, canonical.as_slice())
        }
    }
}

const fn postgres_text_key(column: &GroupKeyColumnOf<Postgres>) -> Option<TextKey> {
    match &column.collation {
        // PostgreSQL CREATE DATABASE cannot select nondeterministic comparisons.
        GroupKeyCollation::DatabaseDefault => Some(TextKey::Exact),
        GroupKeyCollation::Named {
            postgres_deterministic: Some(true),
            ..
        } => Some(TextKey::Exact),
        GroupKeyCollation::Named { .. } | GroupKeyCollation::Unknown => None,
    }
}

fn sqlite_text_key(column: &GroupKeyColumnOf<SQLite>) -> Option<TextKey> {
    match &column.collation {
        GroupKeyCollation::DatabaseDefault => Some(TextKey::Exact),
        GroupKeyCollation::Named { name, .. } if name.name.eq_ignore_ascii_case("binary") => {
            Some(TextKey::Exact)
        }
        GroupKeyCollation::Named { name, .. } if name.name.eq_ignore_ascii_case("nocase") => {
            Some(TextKey::AsciiNoCase)
        }
        GroupKeyCollation::Named { name, .. } if name.name.eq_ignore_ascii_case("rtrim") => {
            Some(TextKey::TrimTrailingSpace)
        }
        GroupKeyCollation::Named { .. } | GroupKeyCollation::Unknown => None,
    }
}

fn mysql_text_key(column: &GroupKeyColumnOf<MySql>) -> Option<TextKey> {
    let GroupKeyCollation::Named {
        name,
        mysql_padding,
        ..
    } = &column.collation
    else {
        return None;
    };
    if !name.name.to_ascii_lowercase().ends_with("_bin") {
        return None;
    }
    match mysql_padding {
        Some(sql_traits::traits::MySqlCollationPadding::PadSpace) => {
            Some(TextKey::TrimTrailingSpace)
        }
        Some(sql_traits::traits::MySqlCollationPadding::NoPad) => Some(TextKey::Exact),
        None if name.name.eq_ignore_ascii_case("utf8mb4_bin") => Some(TextKey::TrimTrailingSpace),
        None if name.name.eq_ignore_ascii_case("utf8mb4_0900_bin") => Some(TextKey::Exact),
        None => None,
    }
}

fn append_json(value: &serde_json::Value, output: &mut alloc::vec::Vec<u8>) -> bool {
    match value {
        serde_json::Value::Null => {
            output.push(0);
            true
        }
        serde_json::Value::Bool(value) => append_tagged(output, 1, value),
        serde_json::Value::Number(value) => {
            let Ok(number) = value.to_string().parse::<bigdecimal::BigDecimal>() else {
                return false;
            };
            append_tagged(output, 2, &number.normalized().to_string())
        }
        serde_json::Value::String(value) => append_tagged(output, 3, value),
        serde_json::Value::Array(values) => {
            let Ok(length) = u32::try_from(values.len()) else {
                return false;
            };
            output.push(4);
            output.extend_from_slice(&length.to_be_bytes());
            values.iter().all(|value| append_json(value, output))
        }
        serde_json::Value::Object(values) => {
            let Ok(length) = u32::try_from(values.len()) else {
                return false;
            };
            output.push(5);
            output.extend_from_slice(&length.to_be_bytes());
            let mut fields: alloc::vec::Vec<_> = values.iter().collect();
            fields.sort_unstable_by(|left, right| {
                left.0
                    .len()
                    .cmp(&right.0.len())
                    .then_with(|| left.0.as_bytes().cmp(right.0.as_bytes()))
            });
            fields
                .into_iter()
                .all(|(name, value)| append_postcard(output, name) && append_json(value, output))
        }
    }
}

pub(crate) fn jsonb_values_equal(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    let mut left_key = alloc::vec::Vec::new();
    let mut right_key = alloc::vec::Vec::new();
    append_json(left, &mut left_key) && append_json(right, &mut right_key) && left_key == right_key
}

pub(crate) fn jsonb_payloads_equal<B: Backend>(left: &B::Jsonb, right: &B::Jsonb) -> bool {
    let left_json = (left as &dyn core::any::Any).downcast_ref::<serde_json::Value>();
    let right_json = (right as &dyn core::any::Any).downcast_ref::<serde_json::Value>();
    match (left_json, right_json) {
        (Some(left), Some(right)) => jsonb_values_equal(left, right),
        _ => left == right,
    }
}

fn encode_postgres_component(
    column: &GroupKeyColumnOf<Postgres>,
    value: &Value<Postgres>,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    match (column.kind, value) {
        (ScalarKind::Float, Value::Float(value)) => {
            append_tagged(output, 3, &canonical_f64(*value))
        }
        (ScalarKind::String, Value::String(value)) => {
            postgres_text_key(column).is_some_and(|mode| append_text(output, 4, value, mode))
        }
        (ScalarKind::Jsonb, Value::Jsonb(value)) => {
            output.push(13);
            append_json(value, output)
        }
        _ => encode_exact_component(column, value, output),
    }
}

fn encode_mysql_component(
    column: &GroupKeyColumnOf<MySql>,
    value: &Value<MySql>,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    match (column.kind, value) {
        (ScalarKind::Float, Value::Float(value)) => {
            append_tagged(output, 3, &canonical_f64(*value))
        }
        (ScalarKind::String, Value::String(value)) => {
            mysql_text_key(column).is_some_and(|mode| append_text(output, 4, value, mode))
        }
        (ScalarKind::Uuid, Value::Uuid(value)) => {
            mysql_text_key(column).is_some_and(|mode| append_text(output, 6, value, mode))
        }
        (ScalarKind::Decimal, Value::Decimal(value)) => {
            append_tagged(output, 11, &value.normalized())
        }
        _ => encode_exact_component(column, value, output),
    }
}

fn append_sqlite_json(
    column: &GroupKeyColumnOf<SQLite>,
    value: &SqliteJson,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    match value.storage() {
        SqliteJsonStorage::Text(value) => {
            sqlite_text_key(column).is_some_and(|mode| append_text(output, 0, value, mode))
        }
        SqliteJsonStorage::Integer(value) => append_tagged(output, 1, value),
        SqliteJsonStorage::Real(value) => {
            let canonical = canonical_f64(*value);
            if canonical.fract() == 0.0 {
                if let Ok(integer) = canonical.to_string().parse::<i64>() {
                    return append_tagged(output, 1, &integer);
                }
            }
            append_tagged(output, 2, &canonical)
        }
        SqliteJsonStorage::Blob(value) => append_tagged(output, 3, value),
    }
}

fn encode_sqlite_component(
    column: &GroupKeyColumnOf<SQLite>,
    value: &Value<SQLite>,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    match (column.kind, value) {
        // SQLite stores NaN as SQL NULL, so only synthetic values reach this arm.
        (ScalarKind::Float, Value::Float(value)) => {
            append_tagged(output, 3, &canonical_f64(*value))
        }
        (ScalarKind::String, Value::String(value)) => {
            sqlite_text_key(column).is_some_and(|mode| append_text(output, 4, value, mode))
        }
        (ScalarKind::Uuid, Value::Uuid(value)) => {
            sqlite_text_key(column).is_some_and(|mode| append_text(output, 6, value, mode))
        }
        (ScalarKind::Json, Value::Json(value)) => {
            output.push(12);
            append_sqlite_json(column, value, output)
        }
        (ScalarKind::Jsonb, Value::Jsonb(value)) => {
            output.push(13);
            append_sqlite_json(column, value, output)
        }
        _ => encode_exact_component(column, value, output),
    }
}

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
            (Self::Jsonb(a), Self::Jsonb(b)) => jsonb_payloads_equal::<B>(a, b),
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
    type Custom = NoCustomScalars<Self>;

    fn group_key_encoder(
        columns: alloc::vec::Vec<GroupKeyColumnOf<Self>>,
    ) -> Option<GroupKeyEncoder<Self>> {
        let supported = columns.iter().all(|column| match column.kind {
            ScalarKind::Int
            | ScalarKind::Bool
            | ScalarKind::Bytes
            | ScalarKind::Uuid
            | ScalarKind::Timestamp
            | ScalarKind::TimestampTz
            | ScalarKind::Date
            | ScalarKind::Time
            | ScalarKind::Float
            | ScalarKind::Jsonb => true,
            ScalarKind::String => postgres_text_key(column).is_some(),
            // PostgreSQL numeric waits on Diesel #5168 for infinity support.
            ScalarKind::Decimal | ScalarKind::Json | ScalarKind::Custom(_) => false,
        });
        supported.then(|| GroupKeyEncoder::new(columns, encode_postgres_component))
    }

    fn decode_group_value(kind: ScalarKindOf<Self>, value: Value<Self>) -> Option<Value<Self>> {
        match (kind, value) {
            (ScalarKind::Float, Value::Int(value)) => Some(Value::Float(widen_i64_to_f64(value))),
            (ScalarKind::Float, Value::Decimal(value)) => {
                value.to_string().parse().ok().map(Value::Float)
            }
            (_, value) => (!value.is_missing()).then_some(value),
        }
    }
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
    type Custom = NoCustomScalars<Self>;

    fn group_key_encoder(
        columns: alloc::vec::Vec<GroupKeyColumnOf<Self>>,
    ) -> Option<GroupKeyEncoder<Self>> {
        let supported = columns.iter().all(|column| match column.kind {
            ScalarKind::Int
            | ScalarKind::Bool
            | ScalarKind::Bytes
            | ScalarKind::Timestamp
            | ScalarKind::TimestampTz
            | ScalarKind::Date
            | ScalarKind::Time
            | ScalarKind::Decimal => true,
            ScalarKind::String | ScalarKind::Uuid => mysql_text_key(column).is_some(),
            // MySQL 8.0 groups persisted signed zero into two groups.
            ScalarKind::Float | ScalarKind::Json | ScalarKind::Jsonb | ScalarKind::Custom(_) => {
                false
            }
        });
        supported.then(|| GroupKeyEncoder::new(columns, encode_mysql_component))
    }

    fn decode_group_value(kind: ScalarKindOf<Self>, value: Value<Self>) -> Option<Value<Self>> {
        match (kind, value) {
            (ScalarKind::Float, Value::Int(value)) => Some(Value::Float(widen_i64_to_f64(value))),
            (ScalarKind::Float, Value::Decimal(value)) => {
                value.to_string().parse().ok().map(Value::Float)
            }
            (_, value) => (!value.is_missing()).then_some(value),
        }
    }
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
    type Custom = NoCustomScalars<Self>;

    fn group_key_encoder(
        columns: alloc::vec::Vec<GroupKeyColumnOf<Self>>,
    ) -> Option<GroupKeyEncoder<Self>> {
        let supported = columns.iter().all(|column| match column.kind {
            ScalarKind::Int
            | ScalarKind::Bool
            | ScalarKind::Bytes
            | ScalarKind::Timestamp
            | ScalarKind::TimestampTz
            | ScalarKind::Date
            | ScalarKind::Time
            | ScalarKind::Float
            | ScalarKind::Json
            | ScalarKind::Jsonb => true,
            ScalarKind::String | ScalarKind::Uuid => sqlite_text_key(column).is_some(),
            ScalarKind::Decimal | ScalarKind::Custom(_) => false,
        });
        supported.then(|| GroupKeyEncoder::new(columns, encode_sqlite_component))
    }
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
    type Json = SqliteJson;

    fn decode_group_value(kind: ScalarKindOf<Self>, value: Value<Self>) -> Option<Value<Self>> {
        match (kind, value) {
            (ScalarKind::Bool, Value::Int(value)) => Some(Value::Bool(value)),
            (ScalarKind::Uuid, Value::String(value)) => Some(Value::Uuid(value)),
            (ScalarKind::Timestamp, Value::String(value)) => {
                crate::temporal::parse_timestamp(&value).map(Value::Timestamp)
            }
            (ScalarKind::TimestampTz, Value::String(value)) => {
                crate::temporal::parse_timestamp_tz(&value).map(Value::TimestampTz)
            }
            (ScalarKind::Date, Value::String(value)) => {
                crate::temporal::parse_date(&value).map(Value::Date)
            }
            (ScalarKind::Time, Value::String(value)) => {
                crate::temporal::parse_time(&value).map(Value::Time)
            }
            (ScalarKind::Decimal, Value::String(value)) => value.parse().ok().map(Value::Decimal),
            (ScalarKind::Float, Value::Int(value)) => Some(Value::Float(widen_i64_to_f64(value))),
            (ScalarKind::Decimal, Value::Int(value)) => {
                Some(Value::Decimal(bigdecimal::BigDecimal::from(value)))
            }
            (ScalarKind::Decimal, Value::Float(value)) => {
                value.to_string().parse().ok().map(Value::Decimal)
            }
            (ScalarKind::Json, Value::String(value)) => Some(Value::Json(SqliteJson::text(value))),
            (ScalarKind::Json, Value::Int(value)) => Some(Value::Json(SqliteJson::integer(value))),
            (ScalarKind::Json, Value::Float(value)) => Some(Value::Json(SqliteJson::real(value))),
            (ScalarKind::Json, Value::Bytes(value)) => Some(Value::Json(SqliteJson::blob(value))),
            (ScalarKind::Jsonb, Value::String(value)) => {
                Some(Value::Jsonb(SqliteJson::text(value)))
            }
            (ScalarKind::Jsonb, Value::Int(value)) => {
                Some(Value::Jsonb(SqliteJson::integer(value)))
            }
            (ScalarKind::Jsonb, Value::Float(value)) => Some(Value::Jsonb(SqliteJson::real(value))),
            (ScalarKind::Jsonb, Value::Bytes(value)) => Some(Value::Jsonb(SqliteJson::blob(value))),
            (kind, value) => decode_exact_group_value(kind, value),
        }
    }
    type Jsonb = SqliteJson;
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
/// This is the transport identity used by keyed row matching. Grouped results
/// use [`GroupKeyEncoder`], whose backend policy follows database equality.
///
/// Returns `None` when postcard cannot encode the tuple.
pub fn encode_value_key<B: Backend>(values: &[Value<B>]) -> Option<alloc::vec::Vec<u8>> {
    postcard::to_allocvec(values).ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod value_key_tests {
    use super::{encode_value_key, Postgres, Value};
    use alloc::vec;

    /// The transport encoding remains byte-for-byte stable.
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
#[allow(clippy::unwrap_used)]
mod canonical_group_key_tests {
    use super::{
        Backend, GroupKeyCollation, GroupKeyCollationName, GroupKeyColumn, MySql, Postgres, SQLite,
        ScalarKind, SqliteJson, Value,
    };
    use alloc::{string::String, vec};
    use sql_traits::traits::MySqlCollationPadding;

    fn column(kind: super::BuiltinKind) -> GroupKeyColumn<super::NoCustom> {
        column_with_collation(kind, GroupKeyCollation::DatabaseDefault)
    }

    fn column_with_collation(
        kind: super::BuiltinKind,
        collation: GroupKeyCollation,
    ) -> GroupKeyColumn<super::NoCustom> {
        GroupKeyColumn {
            kind,
            declared_type: String::from("test"),
            collation,
        }
    }

    fn named_collation(
        name: &str,
        postgres_deterministic: Option<bool>,
        mysql_padding: Option<MySqlCollationPadding>,
    ) -> GroupKeyCollation {
        GroupKeyCollation::Named {
            name: GroupKeyCollationName {
                name: String::from(name),
                name_is_quoted: false,
                schema: None,
                schema_is_quoted: false,
            },
            postgres_deterministic,
            mysql_padding,
        }
    }

    #[test]
    fn canonical_key_has_one_versioned_tuple_format() {
        let encoder = Postgres::group_key_encoder(vec![column(ScalarKind::Int)])
            .expect("integer groups have a canonical encoder");
        let key = encoder
            .encode(&[Value::Int(42)])
            .expect("integer value matches the plan");

        assert_eq!(
            key,
            vec![b'S', b'Q', b'G', b'K', 1, 0, 1, 0, 0, 0, 2, 2, 84]
        );
    }

    #[test]
    fn canonical_key_rejects_values_outside_the_selected_domain() {
        let encoder = Postgres::group_key_encoder(vec![column(ScalarKind::Int)])
            .expect("integer groups have a canonical encoder");

        assert!(encoder.encode(&[]).is_none());
        assert!(encoder.encode(&[Value::Missing]).is_none());
        assert!(encoder.encode(&[Value::Null]).is_some());
        assert_ne!(
            encoder.encode(&[Value::Null]),
            encoder.encode(&[Value::Int(0)])
        );
        assert!(encoder
            .encode(&[Value::String(String::from("42"))])
            .is_none());
    }

    #[test]
    fn postgres_float_keys_follow_grouping_equality() {
        let encoder = Postgres::group_key_encoder(vec![column(ScalarKind::Float)])
            .expect("Postgres float grouping is canonical");

        let zero = encoder.encode(&[Value::Float(0.0)]).unwrap();
        let negative_zero = encoder.encode(&[Value::Float(-0.0)]).unwrap();
        assert_eq!(zero, negative_zero);

        let nan = encoder.encode(&[Value::Float(f64::NAN)]).unwrap();
        let other_nan = encoder
            .encode(&[Value::Float(f64::from_bits(0x7ff0_0000_0000_0001))])
            .unwrap();
        assert_eq!(nan, other_nan);
        assert_ne!(zero, nan);
    }

    #[test]
    fn postgres_text_requires_deterministic_comparison() {
        assert!(
            Postgres::group_key_encoder(vec![column(ScalarKind::String)]).is_some(),
            "the database default is deterministic"
        );
        assert!(Postgres::group_key_encoder(vec![column_with_collation(
            ScalarKind::String,
            named_collation("unicode", Some(true), None),
        )])
        .is_some());
        assert!(Postgres::group_key_encoder(vec![column_with_collation(
            ScalarKind::String,
            named_collation("ci", Some(false), None),
        )])
        .is_none());
        assert!(Postgres::group_key_encoder(vec![column_with_collation(
            ScalarKind::String,
            GroupKeyCollation::Unknown,
        )])
        .is_none());
    }

    #[test]
    fn sqlite_builtin_collations_have_exact_canonical_forms() {
        let nocase = SQLite::group_key_encoder(vec![column_with_collation(
            ScalarKind::String,
            named_collation("NOCASE", None, None),
        )])
        .unwrap();
        assert_eq!(
            nocase.encode(&[Value::String(String::from("A\0IGNORED"))]),
            nocase.encode(&[Value::String(String::from("a\0ignored"))])
        );
        assert_ne!(
            nocase.encode(&[Value::String(String::from("A\0ignored"))]),
            nocase.encode(&[Value::String(String::from("a\0different"))])
        );
        assert_ne!(
            nocase.encode(&[Value::String(String::from("Æ"))]),
            nocase.encode(&[Value::String(String::from("æ"))])
        );

        let rtrim = SQLite::group_key_encoder(vec![column_with_collation(
            ScalarKind::String,
            named_collation("RTRIM", None, None),
        )])
        .unwrap();
        assert_eq!(
            rtrim.encode(&[Value::String(String::from("value"))]),
            rtrim.encode(&[Value::String(String::from("value  "))])
        );
    }

    #[test]
    fn mysql_binary_collations_apply_their_padding_rule() {
        assert!(MySql::group_key_encoder(vec![column(ScalarKind::String)]).is_none());

        let pad = MySql::group_key_encoder(vec![column_with_collation(
            ScalarKind::String,
            named_collation("utf8mb4_bin", None, Some(MySqlCollationPadding::PadSpace)),
        )])
        .unwrap();
        assert_eq!(
            pad.encode(&[Value::String(String::from("value"))]),
            pad.encode(&[Value::String(String::from("value  "))])
        );

        let no_pad = MySql::group_key_encoder(vec![column_with_collation(
            ScalarKind::String,
            named_collation("utf8mb4_0900_bin", None, Some(MySqlCollationPadding::NoPad)),
        )])
        .unwrap();
        assert_ne!(
            no_pad.encode(&[Value::String(String::from("value"))]),
            no_pad.encode(&[Value::String(String::from("value  "))])
        );
    }

    #[test]
    fn mysql_decimal_keys_ignore_scale_spelling() {
        let encoder = MySql::group_key_encoder(vec![column(ScalarKind::Decimal)]).unwrap();
        assert_eq!(
            encoder.encode(&[Value::Decimal("1.0".parse().unwrap())]),
            encoder.encode(&[Value::Decimal("1.00".parse().unwrap())])
        );
    }

    #[test]
    fn postgres_jsonb_keys_follow_structural_equality() {
        let encoder = Postgres::group_key_encoder(vec![column(ScalarKind::Jsonb)]).unwrap();
        let left: serde_json::Value =
            serde_json::from_str(r#"{"a": 1.0, "b": [true, null]}"#).unwrap();
        let right: serde_json::Value =
            serde_json::from_str(r#"{"b": [true, null], "a": 1.00}"#).unwrap();
        assert_eq!(
            encoder.encode(&[Value::Jsonb(left.clone())]),
            encoder.encode(&[Value::Jsonb(right.clone())])
        );
        assert_eq!(Value::<Postgres>::Jsonb(left), Value::Jsonb(right));
    }

    #[test]
    fn sqlite_json_keys_preserve_storage_equality() {
        let encoder = SQLite::group_key_encoder(vec![column(ScalarKind::Json)]).unwrap();
        assert_eq!(
            encoder.encode(&[Value::Json(SqliteJson::integer(1))]),
            encoder.encode(&[Value::Json(SqliteJson::real(1.0))])
        );
        assert_ne!(
            encoder.encode(&[Value::Json(SqliteJson::text(String::from("{\"a\":1}")))]),
            encoder.encode(&[Value::Json(SqliteJson::text(String::from("{ \"a\": 1 }")))])
        );
        assert_ne!(
            encoder.encode(&[Value::Json(SqliteJson::blob(vec![1]))]),
            encoder.encode(&[Value::Json(SqliteJson::text(String::from("1")))])
        );
    }

    proptest::proptest! {
        #[test]
        fn sqlite_nocase_folds_every_ascii_case_pair(value in "[A-Za-z0-9]{0,64}") {
            let encoder = SQLite::group_key_encoder(vec![column_with_collation(
                ScalarKind::String,
                named_collation("NOCASE", None, None),
            )])
            .unwrap();
            proptest::prop_assert_eq!(
                encoder.encode(&[Value::String(value.to_ascii_lowercase())]),
                encoder.encode(&[Value::String(value.to_ascii_uppercase())])
            );
        }

        #[test]
        fn postgres_float_collapses_every_nan_payload(bits in proptest::prelude::any::<u64>()) {
            let value = f64::from_bits(bits);
            if value.is_nan() {
                let encoder = Postgres::group_key_encoder(vec![column(ScalarKind::Float)]).unwrap();
                proptest::prop_assert_eq!(
                    encoder.encode(&[Value::Float(value)]),
                    encoder.encode(&[Value::Float(f64::NAN)])
                );
            }
        }
    }
}

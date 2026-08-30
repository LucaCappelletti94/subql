//! Split out of the former single `backend.rs`; the module boundary is now real
//! rather than a banner comment.

use super::{Backend, MySql, Postgres, SQLite, SqliteJson, SqliteJsonStorage};
use alloc::string::ToString;

/// Runtime tag naming either an upstream builtin family or one custom type.
///
/// `C` names the embedder's own scalar types. Builtin classification is owned
/// by sql-traits and enters through [`From<BuiltinKind>`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ScalarKind<C> {
    /// One builtin SQL scalar family.
    Builtin(#[serde(with = "scalar_family_serde")] BuiltinKind),
    /// One of the embedder's own types.
    Custom(C),
}

/// Builtin scalar families are classified and owned by sql-traits.
pub type BuiltinKind = sql_traits::utils::scalar_family::ScalarFamily;

/// The kind of a column under backend `B`, custom position included.
pub type ScalarKindOf<B> = ScalarKind<<<B as Backend>::Custom as CustomScalars>::Kind>;

mod scalar_family_serde {
    use super::BuiltinKind;

    // serde's `serialize_with` fixes the signature, so the one-byte family
    // arrives by reference whatever clippy would prefer.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(family: &BuiltinKind, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u8(match family {
            BuiltinKind::Bool => 0,
            BuiltinKind::Int => 1,
            BuiltinKind::Float => 2,
            BuiltinKind::Decimal => 3,
            BuiltinKind::String => 4,
            BuiltinKind::Bytes => 5,
            BuiltinKind::Uuid => 6,
            BuiltinKind::Date => 7,
            BuiltinKind::Time => 8,
            BuiltinKind::Timestamp => 9,
            BuiltinKind::TimestampTz => 10,
            BuiltinKind::Json => 11,
            BuiltinKind::Jsonb => 12,
        })
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BuiltinKind, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match <u8 as serde::Deserialize>::deserialize(deserializer)? {
            0 => Ok(BuiltinKind::Bool),
            1 => Ok(BuiltinKind::Int),
            2 => Ok(BuiltinKind::Float),
            3 => Ok(BuiltinKind::Decimal),
            4 => Ok(BuiltinKind::String),
            5 => Ok(BuiltinKind::Bytes),
            6 => Ok(BuiltinKind::Uuid),
            7 => Ok(BuiltinKind::Date),
            8 => Ok(BuiltinKind::Time),
            9 => Ok(BuiltinKind::Timestamp),
            10 => Ok(BuiltinKind::TimestampTz),
            11 => Ok(BuiltinKind::Json),
            12 => Ok(BuiltinKind::Jsonb),
            value => Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Unsigned(u64::from(value)),
                &"a scalar family tag from 0 through 12",
            )),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod scalar_kind_serde_tests {
    use super::{BuiltinKind, ScalarKind};

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    enum TestCustom {
        Named,
    }

    #[test]
    fn builtin_and_custom_kinds_round_trip_with_stable_tags() {
        let families = [
            BuiltinKind::Bool,
            BuiltinKind::Int,
            BuiltinKind::Float,
            BuiltinKind::Decimal,
            BuiltinKind::String,
            BuiltinKind::Bytes,
            BuiltinKind::Uuid,
            BuiltinKind::Date,
            BuiltinKind::Time,
            BuiltinKind::Timestamp,
            BuiltinKind::TimestampTz,
            BuiltinKind::Json,
            BuiltinKind::Jsonb,
        ];
        for (tag, family) in (0_u8..).zip(families) {
            let kind = ScalarKind::<TestCustom>::from(family);
            let encoded = postcard::to_allocvec(&kind).unwrap();
            assert_eq!(encoded, [0, tag]);
            assert_eq!(
                postcard::from_bytes::<ScalarKind<TestCustom>>(&encoded),
                Ok(kind)
            );
        }

        let custom = ScalarKind::Custom(TestCustom::Named);
        let encoded = postcard::to_allocvec(&custom).unwrap();
        assert_eq!(encoded, [1, 0]);
        assert_eq!(
            postcard::from_bytes::<ScalarKind<TestCustom>>(&encoded),
            Ok(custom)
        );
    }

    #[test]
    fn scalar_kind_rejects_an_unknown_builtin_tag() {
        assert!(postcard::from_bytes::<ScalarKind<TestCustom>>(&[0, 13]).is_err());
    }
}
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
        (ScalarKind::Builtin(BuiltinKind::Bool), Value::Bool(value)) => tagged!(1, value),
        (ScalarKind::Builtin(BuiltinKind::Int), Value::Int(value)) => tagged!(2, value),
        (ScalarKind::Builtin(BuiltinKind::Float), Value::Float(value)) => tagged!(3, value),
        (ScalarKind::Builtin(BuiltinKind::String), Value::String(value)) => tagged!(4, value),
        (ScalarKind::Builtin(BuiltinKind::Bytes), Value::Bytes(value)) => tagged!(5, value),
        (ScalarKind::Builtin(BuiltinKind::Uuid), Value::Uuid(value)) => tagged!(6, value),
        (ScalarKind::Builtin(BuiltinKind::Timestamp), Value::Timestamp(value)) => {
            tagged!(7, value)
        }
        (ScalarKind::Builtin(BuiltinKind::TimestampTz), Value::TimestampTz(value)) => {
            tagged!(8, value)
        }
        (ScalarKind::Builtin(BuiltinKind::Date), Value::Date(value)) => tagged!(9, value),
        (ScalarKind::Builtin(BuiltinKind::Time), Value::Time(value)) => tagged!(10, value),
        (ScalarKind::Builtin(BuiltinKind::Decimal), Value::Decimal(value)) => tagged!(11, value),
        (ScalarKind::Builtin(BuiltinKind::Json), Value::Json(value)) => tagged!(12, value),
        (ScalarKind::Builtin(BuiltinKind::Jsonb), Value::Jsonb(value)) => tagged!(13, value),
        (ScalarKind::Custom(kind), Value::Custom(value))
            if kind == <B::Custom as CustomScalars>::kind_of(value) =>
        {
            tagged!(14, value)
        }
        _ => false,
    }
}

pub(super) fn default_group_key_encoder<B: Backend>(
    columns: alloc::vec::Vec<GroupKeyColumnOf<B>>,
) -> Option<GroupKeyEncoder<B>> {
    let supported = columns.iter().all(|column| {
        matches!(
            column.kind,
            ScalarKind::Builtin(
                BuiltinKind::Int
                    | BuiltinKind::Bool
                    | BuiltinKind::Bytes
                    | BuiltinKind::Timestamp
                    | BuiltinKind::TimestampTz
                    | BuiltinKind::Date
                    | BuiltinKind::Time
            )
        )
    });
    supported.then(|| GroupKeyEncoder::new(columns, encode_exact_component::<B>))
}

pub(super) fn decode_exact_group_value<B: Backend>(
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
pub(super) enum TextKey {
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
pub(super) const fn widen_i64_to_f64(value: i64) -> f64 {
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

pub(super) const fn postgres_text_key(column: &GroupKeyColumnOf<Postgres>) -> Option<TextKey> {
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

pub(super) fn sqlite_text_key(column: &GroupKeyColumnOf<SQLite>) -> Option<TextKey> {
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

pub(super) fn mysql_text_key(column: &GroupKeyColumnOf<MySql>) -> Option<TextKey> {
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

/// PostgreSQL `jsonb` equality, for values already known to be storable.
///
/// A value refused by the canonical crate cannot have come from a server, and the one path
/// that could introduce one, `SqlLiteralParse::parse_literal`, rejects it before it reaches
/// here. Should one arrive anyway, comparing the payloads directly keeps the relation
/// reflexive, which answering `false` would not.
pub fn jsonb_payloads_equal<B: Backend>(left: &B::Jsonb, right: &B::Jsonb) -> bool {
    let left_json = (left as &dyn core::any::Any).downcast_ref::<serde_json::Value>();
    let right_json = (right as &dyn core::any::Any).downcast_ref::<serde_json::Value>();
    match (left_json, right_json) {
        (Some(left), Some(right)) => {
            postgres_jsonb_canonical::equivalent::<B::JsonbVersion>(left, right)
                .unwrap_or_else(|_| left == right)
        }
        _ => left == right,
    }
}

pub(super) fn encode_postgres_component<V: postgres_jsonb_canonical::PgVersion + 'static>(
    column: &GroupKeyColumnOf<Postgres<V>>,
    value: &Value<Postgres<V>>,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    match (column.kind, value) {
        (ScalarKind::Builtin(BuiltinKind::Float), Value::Float(value)) => {
            append_tagged(output, 3, &canonical_f64(*value))
        }
        (ScalarKind::Builtin(BuiltinKind::String), Value::String(value)) => {
            postgres_text_key(column).is_some_and(|mode| append_text(output, 4, value, mode))
        }
        (ScalarKind::Builtin(BuiltinKind::Jsonb), Value::Jsonb(value)) => {
            output.push(13);
            // Restores `output` itself on refusal, so a rejected value leaves no partial
            // component behind in the group key.
            postgres_jsonb_canonical::encode_into::<V>(value, output).is_ok()
        }
        _ => encode_exact_component(column, value, output),
    }
}

pub(super) fn encode_mysql_component(
    column: &GroupKeyColumnOf<MySql>,
    value: &Value<MySql>,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    match (column.kind, value) {
        (ScalarKind::Builtin(BuiltinKind::Float), Value::Float(value)) => {
            append_tagged(output, 3, &canonical_f64(*value))
        }
        (ScalarKind::Builtin(BuiltinKind::String), Value::String(value)) => {
            mysql_text_key(column).is_some_and(|mode| append_text(output, 4, value, mode))
        }
        (ScalarKind::Builtin(BuiltinKind::Uuid), Value::Uuid(value)) => {
            mysql_text_key(column).is_some_and(|mode| append_text(output, 6, value, mode))
        }
        (ScalarKind::Builtin(BuiltinKind::Decimal), Value::Decimal(value)) => {
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
                if let Some(integer) = sql_scalar_text::parse_i64(&canonical.to_string()) {
                    return append_tagged(output, 1, &integer);
                }
            }
            append_tagged(output, 2, &canonical)
        }
        SqliteJsonStorage::Blob(value) => append_tagged(output, 3, value),
    }
}

pub(super) fn encode_sqlite_component(
    column: &GroupKeyColumnOf<SQLite>,
    value: &Value<SQLite>,
    output: &mut alloc::vec::Vec<u8>,
) -> bool {
    match (column.kind, value) {
        // SQLite stores NaN as SQL NULL, so only synthetic values reach this arm.
        (ScalarKind::Builtin(BuiltinKind::Float), Value::Float(value)) => {
            append_tagged(output, 3, &canonical_f64(*value))
        }
        (ScalarKind::Builtin(BuiltinKind::String), Value::String(value)) => {
            sqlite_text_key(column).is_some_and(|mode| append_text(output, 4, value, mode))
        }
        (ScalarKind::Builtin(BuiltinKind::Uuid), Value::Uuid(value)) => {
            sqlite_text_key(column).is_some_and(|mode| append_text(output, 6, value, mode))
        }
        (ScalarKind::Builtin(BuiltinKind::Json), Value::Json(value)) => {
            output.push(12);
            append_sqlite_json(column, value, output)
        }
        (ScalarKind::Builtin(BuiltinKind::Jsonb), Value::Jsonb(value)) => {
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

impl<C> From<BuiltinKind> for ScalarKind<C> {
    fn from(family: BuiltinKind) -> Self {
        Self::Builtin(family)
    }
}

impl<C> ScalarKind<C> {
    /// This kind as a builtin family, or `None` for a custom type.
    #[must_use]
    pub const fn as_builtin(&self) -> Option<BuiltinKind> {
        match self {
            Self::Builtin(family) => Some(*family),
            Self::Custom(_) => None,
        }
    }

    /// The custom type this kind names, or `None` for a builtin.
    pub const fn custom(&self) -> Option<&C> {
        match self {
            Self::Custom(custom) => Some(custom),
            Self::Builtin(_) => None,
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
/// respectively. [`crate::backend::CdcEvent::value_at`] returns this type directly.
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
        Some(match self {
            Self::Missing | Self::Null => return None,
            Self::Bool(_) => BuiltinKind::Bool.into(),
            Self::Int(_) => BuiltinKind::Int.into(),
            Self::Float(_) => BuiltinKind::Float.into(),
            Self::String(_) => BuiltinKind::String.into(),
            Self::Bytes(_) => BuiltinKind::Bytes.into(),
            Self::Uuid(_) => BuiltinKind::Uuid.into(),
            Self::Timestamp(_) => BuiltinKind::Timestamp.into(),
            Self::TimestampTz(_) => BuiltinKind::TimestampTz.into(),
            Self::Date(_) => BuiltinKind::Date.into(),
            Self::Time(_) => BuiltinKind::Time.into(),
            Self::Decimal(_) => BuiltinKind::Decimal.into(),
            Self::Json(_) => BuiltinKind::Json.into(),
            Self::Jsonb(_) => BuiltinKind::Jsonb.into(),
            Self::Custom(value) => ScalarKind::Custom(<B::Custom as CustomScalars>::kind_of(value)),
        })
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

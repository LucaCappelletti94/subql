//! Split out of the former single `backend.rs`; the module boundary is now real
//! rather than a banner comment.

use super::scalar_value::{
    decode_exact_group_value, encode_mysql_component, encode_postgres_component,
    encode_sqlite_component, mysql_text_key, postgres_text_key, sqlite_text_key, widen_i64_to_f64,
};
use super::{
    Backend, BuiltinKind, ColumnComparisonOf, GroupKeyEncoder, NoCustomScalars, ScalarKindOf,
    SqliteJson, TextKey, Value,
};
use alloc::string::ToString;

/// Postgres backend marker, parameterised by the server major it targets.
///
/// The default covers the newest supported server. Name another, as `Postgres<Pg14>`, to
/// hold `jsonb` to what an older major accepts. Only acceptance changes, never the bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Postgres<V = postgres_jsonb_canonical::Pg18>(core::marker::PhantomData<V>);

impl<V: postgres_jsonb_canonical::PgVersion + 'static> Backend for Postgres<V> {
    /// PostgreSQL decides on the declared type: a `char(n)` side is padded
    /// out to its width on write, and the padding is ignored when comparing
    /// against another `char`, a `varchar` or a literal.
    ///
    /// Measured asymmetry: a `char` against a `text` column strips only the
    /// `char` side, because converting `char` to `text` drops the padding
    /// and the comparison is then exact. A side whose facts are unknown is
    /// treated as a literal, which is what an unknown side usually is.
    fn trailing_spaces(
        comparison: &super::scalar_value::ComparisonContext<'_, Self>,
    ) -> super::scalar_value::TrailingSpaces {
        use super::scalar_value::TrailingSpaces;
        let padded = |side: Option<&ColumnComparisonOf<Self>>| {
            side.is_some_and(ColumnComparisonOf::<Self>::declares_char_type)
        };
        let text_typed = |side: Option<&ColumnComparisonOf<Self>>| {
            side.is_some_and(|facts| facts.declared_type.trim().eq_ignore_ascii_case("text"))
        };
        match (padded(comparison.left), padded(comparison.right)) {
            (true, true) => TrailingSpaces::BothIgnored,
            (true, false) if text_typed(comparison.right) => TrailingSpaces::LeftStripped,
            (false, true) if text_typed(comparison.left) => TrailingSpaces::RightStripped,
            (true, false) | (false, true) => TrailingSpaces::BothIgnored,
            (false, false) => TrailingSpaces::BothSignificant,
        }
    }

    /// Measured: a backslash escapes the next character.
    const LIKE_DEFAULT_ESCAPE: Option<char> = Some('\\');

    type Custom = NoCustomScalars<Self>;

    fn text_key(column: &ColumnComparisonOf<Self>) -> Option<TextKey> {
        postgres_text_key(column)
    }

    /// PostgreSQL's own float rule: NaN equals NaN. IEEE, which is what
    /// `PartialOrd` on `f64` implements, says a NaN equals nothing, so
    /// `WHERE value = value` skipped the row the server returns.
    ///
    /// Only the float variants differ. `numeric` also has a NaN in the
    /// server, but [`Backend::Decimal`] is a `BigDecimal`, which cannot
    /// represent one, so no such value reaches here.
    fn scalars_equal(
        comparison: super::scalar_value::ComparisonContext<'_, Self>,
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> bool {
        match (left, right) {
            (Value::Float(x), Value::Float(y)) if x.is_nan() || y.is_nan() => {
                x.is_nan() && y.is_nan()
            }
            (Value::String(x), Value::String(y)) => {
                let (x, y) = Self::trailing_spaces(&comparison).apply(x, y);
                x == y
            }
            _ => crate::compiler::value_cmp::structural_equality(left, right),
        }
    }

    /// NaN is PostgreSQL's largest float: above every non-NaN value, and
    /// equal to another NaN. IEEE leaves every such pair unordered, which
    /// answered `Tri::Unknown` and dropped the row.
    fn compare_scalars(
        comparison: super::scalar_value::ComparisonContext<'_, Self>,
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> Option<core::cmp::Ordering> {
        match (left, right) {
            (Value::Float(x), Value::Float(y)) if x.is_nan() || y.is_nan() => {
                Some(if x.is_nan() && y.is_nan() {
                    core::cmp::Ordering::Equal
                } else if x.is_nan() {
                    core::cmp::Ordering::Greater
                } else {
                    core::cmp::Ordering::Less
                })
            }
            (Value::String(x), Value::String(y)) => {
                let (x, y) = Self::trailing_spaces(&comparison).apply(x, y);
                Some(x.cmp(y))
            }
            _ => crate::compiler::value_cmp::structural_ordering(left, right),
        }
    }

    fn group_key_encoder(
        columns: alloc::vec::Vec<ColumnComparisonOf<Self>>,
    ) -> Option<GroupKeyEncoder<Self>> {
        let supported = columns.iter().all(|column| match column.kind.as_builtin() {
            Some(
                BuiltinKind::Int
                | BuiltinKind::Bool
                | BuiltinKind::Bytes
                | BuiltinKind::Uuid
                | BuiltinKind::Timestamp
                | BuiltinKind::TimestampTz
                | BuiltinKind::Date
                | BuiltinKind::Time
                | BuiltinKind::Float
                | BuiltinKind::Jsonb,
            ) => true,
            Some(BuiltinKind::String) => Self::text_key(column).is_some(),
            // PostgreSQL numeric waits on Diesel #5168 for infinity support.
            Some(BuiltinKind::Decimal | BuiltinKind::Json) | None => false,
        });
        supported.then(|| GroupKeyEncoder::new(columns, encode_postgres_component))
    }

    fn decode_group_value(kind: ScalarKindOf<Self>, value: Value<Self>) -> Option<Value<Self>> {
        match (kind.as_builtin(), value) {
            (Some(BuiltinKind::Float), Value::Int(value)) => {
                Some(Value::Float(widen_i64_to_f64(value)))
            }
            (Some(BuiltinKind::Float), Value::Decimal(value)) => {
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
    type JsonbVersion = V;
}

/// MySQL backend marker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct MySql;

impl Backend for MySql {
    /// A `PAD SPACE` collation ignores trailing spaces on both sides, which
    /// no structural comparison reproduces.
    fn scalars_equal(
        comparison: super::scalar_value::ComparisonContext<'_, Self>,
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> bool {
        match (left, right) {
            (Value::String(x), Value::String(y)) => {
                let (x, y) = Self::trailing_spaces(&comparison).apply(x, y);
                x == y
            }
            _ => crate::compiler::value_cmp::structural_equality(left, right),
        }
    }

    /// Ordering reads trailing spaces the same way equality does.
    fn compare_scalars(
        comparison: super::scalar_value::ComparisonContext<'_, Self>,
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> Option<core::cmp::Ordering> {
        match (left, right) {
            (Value::String(x), Value::String(y)) => {
                let (x, y) = Self::trailing_spaces(&comparison).apply(x, y);
                Some(x.cmp(y))
            }
            _ => crate::compiler::value_cmp::structural_ordering(left, right),
        }
    }

    /// MySQL decides on the collation, not the type: a `PAD SPACE`
    /// collation ignores trailing spaces and `NO PAD`, the 8.0 default,
    /// does not. A `CHAR` column never delivers a padded cell at all,
    /// because the server strips trailing spaces on write.
    ///
    /// A collation whose padding the catalog does not carry compares
    /// exactly, which is right for the 8.0 default and the reason the
    /// unknown case is asserted rather than assumed.
    fn trailing_spaces(
        comparison: &super::scalar_value::ComparisonContext<'_, Self>,
    ) -> super::scalar_value::TrailingSpaces {
        use super::scalar_value::TrailingSpaces;
        let pads = |side: Option<&ColumnComparisonOf<Self>>| {
            side.is_some_and(ColumnComparisonOf::<Self>::collation_pads_trailing_spaces)
        };
        if pads(comparison.left) || pads(comparison.right) {
            TrailingSpaces::BothIgnored
        } else {
            TrailingSpaces::BothSignificant
        }
    }

    /// Measured: a backslash escapes the next character.
    const LIKE_DEFAULT_ESCAPE: Option<char> = Some('\\');

    type Custom = NoCustomScalars<Self>;

    fn text_key(column: &ColumnComparisonOf<Self>) -> Option<TextKey> {
        mysql_text_key(column)
    }

    fn group_key_encoder(
        columns: alloc::vec::Vec<ColumnComparisonOf<Self>>,
    ) -> Option<GroupKeyEncoder<Self>> {
        let supported = columns.iter().all(|column| match column.kind.as_builtin() {
            Some(
                BuiltinKind::Int
                | BuiltinKind::Bool
                | BuiltinKind::Bytes
                | BuiltinKind::Timestamp
                | BuiltinKind::TimestampTz
                | BuiltinKind::Date
                | BuiltinKind::Time
                | BuiltinKind::Decimal,
            ) => true,
            Some(BuiltinKind::String | BuiltinKind::Uuid) => Self::text_key(column).is_some(),
            // MySQL 8.0 groups persisted signed zero into two groups.
            Some(BuiltinKind::Float | BuiltinKind::Json | BuiltinKind::Jsonb) | None => false,
        });
        supported.then(|| GroupKeyEncoder::new(columns, encode_mysql_component))
    }

    fn decode_group_value(kind: ScalarKindOf<Self>, value: Value<Self>) -> Option<Value<Self>> {
        match (kind.as_builtin(), value) {
            (Some(BuiltinKind::Float), Value::Int(value)) => {
                Some(Value::Float(widen_i64_to_f64(value)))
            }
            (Some(BuiltinKind::Float), Value::Decimal(value)) => {
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
    // MySQL stores UUIDs as CHAR(36) or BINARY(16) with no native type.
    // Downstream code treats them as strings on the wire.
    type Uuid = alloc::string::String;
    type Timestamp = chrono::NaiveDateTime;
    type TimestampTz = chrono::DateTime<chrono::Utc>;
    type Date = chrono::NaiveDate;
    type Time = chrono::NaiveTime;
    type Decimal = bigdecimal::BigDecimal;
    type Json = serde_json::Value;
    // MySQL does not distinguish JSON from JSONB. Keep the type alias for
    // symmetry with Postgres so the engine surface stays uniform.
    type Jsonb = serde_json::Value;
    // Named because associated type defaults are unstable; MySQL and SQLite JSON
    // semantics never route through the PostgreSQL crate.
    type JsonbVersion = postgres_jsonb_canonical::Pg18;
}

/// SQLite backend marker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct SQLite;

impl Backend for SQLite {
    /// SQLite pads nothing: `CHAR(n)` is not a fixed-width type there, and
    /// trailing spaces are always significant under `BINARY`.
    fn trailing_spaces(
        _comparison: &super::scalar_value::ComparisonContext<'_, Self>,
    ) -> super::scalar_value::TrailingSpaces {
        super::scalar_value::TrailingSpaces::BothSignificant
    }

    /// Measured: SQLite has no default escape, so a backslash in a
    /// pattern matches a backslash.
    const LIKE_DEFAULT_ESCAPE: Option<char> = None;

    type Custom = NoCustomScalars<Self>;

    fn text_key(column: &ColumnComparisonOf<Self>) -> Option<TextKey> {
        sqlite_text_key(column)
    }

    fn group_key_encoder(
        columns: alloc::vec::Vec<ColumnComparisonOf<Self>>,
    ) -> Option<GroupKeyEncoder<Self>> {
        let supported = columns.iter().all(|column| match column.kind.as_builtin() {
            Some(
                BuiltinKind::Int
                | BuiltinKind::Bool
                | BuiltinKind::Bytes
                | BuiltinKind::Timestamp
                | BuiltinKind::TimestampTz
                | BuiltinKind::Date
                | BuiltinKind::Time
                | BuiltinKind::Float
                | BuiltinKind::Json
                | BuiltinKind::Jsonb,
            ) => true,
            Some(BuiltinKind::String | BuiltinKind::Uuid) => Self::text_key(column).is_some(),
            Some(BuiltinKind::Decimal) | None => false,
        });
        supported.then(|| GroupKeyEncoder::new(columns, encode_sqlite_component))
    }
    type Dialect = sqlparser::dialect::SQLiteDialect;
    // SQLite has no native BOOL. The column-type contract stores 0 or 1
    // as INTEGER. The backend surfaces the wire type rather than inventing
    // a `bool`.
    type Bool = i64;
    type Int = i64;
    type Float = f64;
    type String = alloc::string::String;
    type Bytes = alloc::vec::Vec<u8>;
    // SQLite stores UUIDs as TEXT (36-byte hyphenated) by convention.
    type Uuid = alloc::string::String;
    // SQLite has no native temporal types. Downstream code stores dates and
    // times as ISO-8601 TEXT. `Timestamp` and related types carry parsed
    // `chrono` values after decoding.
    type Timestamp = chrono::NaiveDateTime;
    type TimestampTz = chrono::DateTime<chrono::Utc>;
    type Date = chrono::NaiveDate;
    type Time = chrono::NaiveTime;
    type Decimal = bigdecimal::BigDecimal;
    type Json = SqliteJson;

    fn decode_group_value(kind: ScalarKindOf<Self>, value: Value<Self>) -> Option<Value<Self>> {
        match (kind.as_builtin(), value) {
            (Some(BuiltinKind::Bool), Value::Int(value)) => Some(Value::Bool(value)),
            (Some(BuiltinKind::Uuid), Value::String(value)) => Some(Value::Uuid(value)),
            (Some(BuiltinKind::Timestamp), Value::String(value)) => {
                sql_scalar_text::parse_timestamp(&value).map(Value::Timestamp)
            }
            (Some(BuiltinKind::TimestampTz), Value::String(value)) => {
                sql_scalar_text::parse_timestamp_tz(&value).map(Value::TimestampTz)
            }
            (Some(BuiltinKind::Date), Value::String(value)) => {
                sql_scalar_text::parse_date(&value).map(Value::Date)
            }
            (Some(BuiltinKind::Time), Value::String(value)) => {
                sql_scalar_text::parse_time(&value).map(Value::Time)
            }
            (Some(BuiltinKind::Decimal), Value::String(value)) => {
                sql_scalar_text::parse_decimal(&value).map(Value::Decimal)
            }
            (Some(BuiltinKind::Float), Value::Int(value)) => {
                Some(Value::Float(widen_i64_to_f64(value)))
            }
            (Some(BuiltinKind::Decimal), Value::Int(value)) => {
                Some(Value::Decimal(bigdecimal::BigDecimal::from(value)))
            }
            (Some(BuiltinKind::Decimal), Value::Float(value)) => {
                value.to_string().parse().ok().map(Value::Decimal)
            }
            (Some(BuiltinKind::Json), Value::String(value)) => {
                Some(Value::Json(SqliteJson::text(value)))
            }
            (Some(BuiltinKind::Json), Value::Int(value)) => {
                Some(Value::Json(SqliteJson::integer(value)))
            }
            (Some(BuiltinKind::Json), Value::Float(value)) => {
                Some(Value::Json(SqliteJson::real(value)))
            }
            (Some(BuiltinKind::Json), Value::Bytes(value)) => {
                Some(Value::Json(SqliteJson::blob(value)))
            }
            (Some(BuiltinKind::Jsonb), Value::String(value)) => {
                Some(Value::Jsonb(SqliteJson::text(value)))
            }
            (Some(BuiltinKind::Jsonb), Value::Int(value)) => {
                Some(Value::Jsonb(SqliteJson::integer(value)))
            }
            (Some(BuiltinKind::Jsonb), Value::Float(value)) => {
                Some(Value::Jsonb(SqliteJson::real(value)))
            }
            (Some(BuiltinKind::Jsonb), Value::Bytes(value)) => {
                Some(Value::Jsonb(SqliteJson::blob(value)))
            }
            (_, value) => decode_exact_group_value(kind, value),
        }
    }
    type Jsonb = SqliteJson;
    // Named because associated type defaults are unstable; MySQL and SQLite JSON
    // semantics never route through the PostgreSQL crate.
    type JsonbVersion = postgres_jsonb_canonical::Pg18;
}

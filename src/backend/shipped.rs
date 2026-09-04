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
    type Custom = NoCustomScalars<Self>;

    fn text_key(column: &ColumnComparisonOf<Self>) -> Option<TextKey> {
        postgres_text_key(column)
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

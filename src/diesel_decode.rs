//! Value decoding shared by the diesel-typed query API and the diesel-backed
//! re-execution connectors.
//!
//! One decode convention, reachable from both `diesel-typed` (which renders a
//! typed query and reads back its binds) and `executor-diesel` (which runs a
//! captured query and reads back its rows). Both need the same question
//! answered: given a raw value the database handed over and whatever runtime
//! type tag came with it, which [`Value`] variant is it?
//!
//! Every arm delegates the byte reading to diesel's own `FromSql` for the Rust
//! type the tag means. Diesel wrote those readers for the wire format it also
//! serializes, so a second reader here would only be a second place for the two
//! to disagree. What stays subql's own is the dispatch: which `Value` variant a
//! tag becomes, and which tags have no variant and are refused.

#[cfg(any(feature = "diesel-typed", feature = "executor-diesel-postgres"))]
use crate::backend::Postgres;
use crate::backend::Value;
use crate::RegisterError;
use alloc::format;
use alloc::string::{String, ToString};
#[cfg(any(feature = "diesel-typed", feature = "executor-diesel-postgres"))]
use core::num::NonZeroU32;
use diesel::backend::Backend;
use diesel::deserialize::FromSql;
#[cfg(any(feature = "diesel-typed", feature = "executor-diesel-postgres"))]
use diesel::pg::{Pg, PgValue};
#[cfg(any(feature = "diesel-typed", feature = "executor-diesel-postgres"))]
use diesel::sql_types;

/// Postgres built-in scalar type OIDs, as `diesel` itself declares them in
/// `sql_types` (`#[diesel(postgres_type(oid = ...))]`).
#[cfg(any(feature = "diesel-typed", feature = "executor-diesel-postgres"))]
pub mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

/// Map an owned SQLite bind value to a `Value<SQLite>`.
///
/// SQLite has no boolean or uuid storage class, so those arrive as integer or
/// text, and a BLOB maps to [`Value::Bytes`]. The mapping is total over
/// `OwnedSqliteBindValue`.
#[cfg(feature = "diesel-typed-sqlite")]
#[must_use]
pub fn owned_sqlite_to_value(
    value: &diesel::sqlite::OwnedSqliteBindValue,
) -> Value<crate::backend::SQLite> {
    use diesel::sqlite::OwnedSqliteBindValue as V;
    match value {
        V::I32(i) => Value::Int(i64::from(*i)),
        V::I64(i) => Value::Int(*i),
        V::F64(f) => Value::Float(*f),
        V::String(s) => Value::String(s.as_ref().to_string()),
        V::Binary(b) => Value::Bytes(b.to_vec()),
        V::Null => Value::Null,
    }
}

/// Decode one MySQL bind into a `Value<MySql>` by its `MysqlType`.
///
/// The buffer is a native-endian client buffer, and `None` bytes are a SQL
/// NULL. Integers read by their actual byte length, which also handles `bool`
/// (serialized as a 4-byte `i32`).
#[cfg(any(feature = "diesel-typed-mysql", feature = "executor-diesel-mysql"))]
pub fn decode_mysql_bind(
    bytes: Option<&[u8]>,
    ty: diesel::mysql::MysqlType,
) -> Result<Value<crate::backend::MySql>, RegisterError> {
    use diesel::mysql::MysqlType as T;
    let Some(b) = bytes else {
        return Ok(Value::Null);
    };
    let value = match ty {
        T::Tiny | T::Short | T::Long | T::LongLong => Value::Int(mysql_int_ne(b, true)?),
        T::UnsignedTiny | T::UnsignedShort | T::UnsignedLong | T::UnsignedLongLong => {
            Value::Int(mysql_int_ne(b, false)?)
        }
        T::Float => Value::Float(f64::from(f32::from_ne_bytes(fixed(b, "mysql float")?))),
        T::Double => Value::Float(f64::from_ne_bytes(fixed(b, "mysql double")?)),
        T::String | T::Enum | T::Set => {
            let s = core::str::from_utf8(b).map_err(|_| {
                RegisterError::UnsupportedSql(
                    "diesel MySQL text bind is not valid UTF-8".to_string(),
                )
            })?;
            Value::String(s.to_string())
        }
        T::Blob => Value::Bytes(mysql_from_sql::<
            diesel::sql_types::Binary,
            alloc::vec::Vec<u8>,
        >(b, ty, "blob")?),
        T::Numeric => Value::Decimal(mysql_from_sql::<
            diesel::sql_types::Numeric,
            bigdecimal::BigDecimal,
        >(b, ty, "numeric")?),
        T::Date => Value::Date(
            mysql_from_sql::<diesel::sql_types::Date, chrono::NaiveDate>(b, ty, "date")?,
        ),
        T::Time => Value::Time(
            mysql_from_sql::<diesel::sql_types::Time, chrono::NaiveTime>(b, ty, "time")?,
        ),
        T::DateTime => Value::Timestamp(mysql_from_sql::<
            diesel::sql_types::Datetime,
            chrono::NaiveDateTime,
        >(b, ty, "datetime")?),
        T::Timestamp => Value::Timestamp(mysql_from_sql::<
            diesel::sql_types::Timestamp,
            chrono::NaiveDateTime,
        >(b, ty, "timestamp")?),
        // MySQL has no JSON bind tag: a JSON column binds as `String` and the
        // arm above reads it. `Bit` is the one tag today with no `Value`
        // payload, and `MysqlType` is `#[non_exhaustive]`, so a tag diesel adds
        // later lands here too and is refused by name rather than mis-decoded.
        other => {
            return Err(RegisterError::UnsupportedSql(format!(
                "unsupported diesel MySQL bind type ({other:?}); SubQL's `Value` has no payload \
                 for it to become"
            )))
        }
    };
    Ok(value)
}

/// Read one MySQL client bind through diesel's `FromSql` for `T`.
///
/// The peer of [`pg_from_sql`], and for the same reason: diesel wrote the
/// readers for the buffer shapes it also serializes, so a second reader here
/// would only be a second place for the two to disagree. Notably the temporal
/// tags carry diesel's `MysqlTime` struct rather than text or an integer.
#[cfg(any(feature = "diesel-typed-mysql", feature = "executor-diesel-mysql"))]
fn mysql_from_sql<ST, T>(
    bytes: &[u8],
    ty: diesel::mysql::MysqlType,
    what: &str,
) -> Result<T, RegisterError>
where
    T: FromSql<ST, diesel::mysql::Mysql>,
{
    T::from_sql(diesel::mysql::MysqlValue::new(bytes, ty))
        .map_err(|e| RegisterError::UnsupportedSql(format!("diesel MySQL {what} bind: {e}")))
}

/// Read a native-endian MySQL integer bind of 1/2/4/8 bytes into an `i64`,
/// interpreting the bytes as signed or unsigned per the `MysqlType`.
#[cfg(any(feature = "diesel-typed-mysql", feature = "executor-diesel-mysql"))]
fn mysql_int_ne(b: &[u8], signed: bool) -> Result<i64, RegisterError> {
    Ok(match (b.len(), signed) {
        (1, true) => i64::from(i8::from_ne_bytes(fixed(b, "mysql tiny")?)),
        (1, false) => i64::from(u8::from_ne_bytes(fixed(b, "mysql utiny")?)),
        (2, true) => i64::from(i16::from_ne_bytes(fixed(b, "mysql short")?)),
        (2, false) => i64::from(u16::from_ne_bytes(fixed(b, "mysql ushort")?)),
        (4, true) => i64::from(i32::from_ne_bytes(fixed(b, "mysql long")?)),
        (4, false) => i64::from(u32::from_ne_bytes(fixed(b, "mysql ulong")?)),
        (8, true) => i64::from_ne_bytes(fixed(b, "mysql longlong")?),
        (8, false) => {
            i64::try_from(u64::from_ne_bytes(fixed(b, "mysql ulonglong")?)).map_err(|_| {
                RegisterError::UnsupportedSql(
                    "diesel MySQL unsigned bind exceeds i64 range".to_string(),
                )
            })?
        }
        (n, _) => return Err(bad_len("mysql integer", n)),
    })
}

/// Decode one Postgres binary-format bind value into a `Value<Postgres>`.
/// `None` bytes are a SQL NULL.
///
/// The OID dispatch is subql's, since it decides which `Value` variant a bind
/// becomes, but each arm's bytes are read by diesel's own `FromSql` for the
/// Rust type that OID means. Diesel wrote those binary readers for the wire
/// format it also serializes, so re-implementing them here only added a second
/// place for the two to disagree.
#[cfg(any(feature = "diesel-typed", feature = "executor-diesel-postgres"))]
pub fn decode_pg_bind(
    bytes: Option<&[u8]>,
    type_oid: u32,
) -> Result<Value<Postgres>, RegisterError> {
    let Some(b) = bytes else {
        return Ok(Value::Null);
    };
    let value = match type_oid {
        oid::BOOL => Value::Bool(pg_from_sql::<sql_types::Bool, bool>(b, type_oid, "bool")?),
        oid::INT2 => Value::Int(i64::from(pg_from_sql::<sql_types::SmallInt, i16>(
            b, type_oid, "int2",
        )?)),
        oid::INT4 => Value::Int(i64::from(pg_from_sql::<sql_types::Integer, i32>(
            b, type_oid, "int4",
        )?)),
        oid::INT8 => Value::Int(pg_from_sql::<sql_types::BigInt, i64>(b, type_oid, "int8")?),
        oid::FLOAT4 => Value::Float(f64::from(pg_from_sql::<sql_types::Float, f32>(
            b, type_oid, "float4",
        )?)),
        oid::FLOAT8 => Value::Float(pg_from_sql::<sql_types::Double, f64>(
            b, type_oid, "float8",
        )?),
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME => {
            Value::String(pg_from_sql::<sql_types::Text, String>(b, type_oid, "text")?)
        }
        oid::UUID => Value::Uuid(pg_from_sql::<sql_types::Uuid, uuid::Uuid>(
            b, type_oid, "uuid",
        )?),
        oid::BYTEA => Value::Bytes(pg_from_sql::<sql_types::Binary, alloc::vec::Vec<u8>>(
            b, type_oid, "bytea",
        )?),
        oid::TIMESTAMP => Value::Timestamp(pg_from_sql::<
            sql_types::Timestamp,
            chrono::NaiveDateTime,
        >(b, type_oid, "timestamp")?),
        oid::TIMESTAMPTZ => Value::TimestampTz(pg_from_sql::<
            sql_types::Timestamptz,
            chrono::DateTime<chrono::Utc>,
        >(b, type_oid, "timestamptz")?),
        oid::DATE => Value::Date(pg_from_sql::<sql_types::Date, chrono::NaiveDate>(
            b, type_oid, "date",
        )?),
        oid::TIME => Value::Time(pg_from_sql::<sql_types::Time, chrono::NaiveTime>(
            b, type_oid, "time",
        )?),
        oid::NUMERIC => Value::Decimal(pg_from_sql::<sql_types::Numeric, bigdecimal::BigDecimal>(
            b, type_oid, "numeric",
        )?),
        oid::JSON => Value::Json(pg_from_sql::<sql_types::Json, serde_json::Value>(
            b, type_oid, "json",
        )?),
        oid::JSONB => Value::Jsonb(pg_from_sql::<sql_types::Jsonb, serde_json::Value>(
            b, type_oid, "jsonb",
        )?),
        other => {
            return Err(RegisterError::UnsupportedSql(format!(
                "unsupported diesel bind type (Postgres OID {other}); SubQL reads the scalar \
                 types its own `Value` carries, so an array, a range, an interval, or a \
                 user-declared enum, domain or composite has no `Value` variant to become"
            )))
        }
    };
    Ok(value)
}

/// Read one Postgres binary bind through diesel's `FromSql` for `T`.
///
/// `ty` names the arm in the error, since diesel's own message describes the
/// Rust type it was decoding rather than the bind that carried it.
#[cfg(any(feature = "diesel-typed", feature = "executor-diesel-postgres"))]
fn pg_from_sql<ST, T: FromSql<ST, Pg>>(
    bytes: &[u8],
    type_oid: u32,
    ty: &str,
) -> Result<T, RegisterError> {
    // Zero is not a Postgres OID, and every arm above matches a real one.
    let lookup = NonZeroU32::new(type_oid)
        .ok_or_else(|| RegisterError::UnsupportedSql("diesel bind carries OID 0".to_string()))?;
    T::from_sql(PgValue::new(bytes, &lookup))
        .map_err(|e| RegisterError::UnsupportedSql(format!("diesel {ty} bind: {e}")))
}

#[must_use]
pub fn bad_len(ty: &str, got: usize) -> RegisterError {
    RegisterError::UnsupportedSql(format!("diesel {ty} bind has unexpected byte length {got}"))
}

pub fn fixed<const N: usize>(b: &[u8], ty: &str) -> Result<[u8; N], RegisterError> {
    b.try_into().map_err(|_| bad_len(ty, b.len()))
}

/// Decode a column value returned by an executed query into a
/// `Value<B>`, per backend.
///
/// Lets an `INSERT ... RETURNING` row be read without a compile-time row
/// struct.
pub trait FollowRowDecode: Backend {
    /// The subql [`crate::backend::Backend`] whose [`Value`] shape this
    /// diesel backend's returned fields decode to.
    type SubqlBackend: crate::backend::Backend;

    /// Decode one field's raw value (or SQL NULL) into a
    /// `Value<Self::SubqlBackend>`.
    ///
    /// # Errors
    /// [`RegisterError::UnsupportedSql`] for a column type outside the
    /// supported scalar set.
    fn field_to_value(
        value: Option<Self::RawValue<'_>>,
    ) -> Result<Value<Self::SubqlBackend>, RegisterError>;
}

#[cfg(any(feature = "diesel-typed", feature = "executor-diesel-postgres"))]
impl FollowRowDecode for Pg {
    type SubqlBackend = Postgres;

    fn field_to_value(value: Option<PgValue<'_>>) -> Result<Value<Postgres>, RegisterError> {
        value.map_or(Ok(Value::Null), |v| {
            decode_pg_bind(Some(v.as_bytes()), v.get_oid().get())
        })
    }
}

#[cfg(feature = "diesel-typed-sqlite")]
impl FollowRowDecode for diesel::sqlite::Sqlite {
    type SubqlBackend = crate::backend::SQLite;

    fn field_to_value(
        value: Option<Self::RawValue<'_>>,
    ) -> Result<Value<crate::backend::SQLite>, RegisterError> {
        use diesel::sqlite::SqliteType;
        let Some(mut v) = value else {
            return Ok(Value::Null);
        };
        Ok(match v.value_type() {
            None => Value::Null,
            Some(SqliteType::SmallInt | SqliteType::Integer | SqliteType::Long) => {
                Value::Int(v.read_long())
            }
            Some(SqliteType::Float | SqliteType::Double) => Value::Float(v.read_double()),
            Some(SqliteType::Text) => Value::String(v.read_text().to_string()),
            Some(SqliteType::Binary) => Value::Bytes(v.read_blob().to_vec()),
        })
    }
}

/// One row of a result whose shape is not known at compile time, decoded
/// column by column.
///
/// `diesel::sql_query(...).load::<T>()` needs `T: QueryableByName`, and that
/// trait's only method receives the row itself, so a `T` that iterates the row
/// positionally needs no schema, no `table!`, and no column list. That is what
/// lets a captured query of arbitrary shape be read at all.
#[derive(Clone, Debug, PartialEq)]
pub struct DynamicRow<B: crate::backend::Backend> {
    /// Column names as the database reported them, in projection order.
    pub columns: alloc::vec::Vec<String>,
    /// Column values, in the same order as [`Self::columns`].
    pub values: alloc::vec::Vec<Value<B>>,
}

impl<DB> diesel::deserialize::QueryableByName<DB> for DynamicRow<DB::SubqlBackend>
where
    DB: FollowRowDecode,
{
    fn build<'a>(row: &impl diesel::row::NamedRow<'a, DB>) -> diesel::deserialize::Result<Self> {
        use diesel::row::{Field, Row};

        let count = row.field_count();
        let mut columns = alloc::vec::Vec::with_capacity(count);
        let mut values = alloc::vec::Vec::with_capacity(count);
        for index in 0..count {
            let field = Row::get(row, index).ok_or_else(|| {
                alloc::boxed::Box::<dyn core::error::Error + Send + Sync>::from(alloc::format!(
                    "row reported {count} fields but field {index} is absent"
                ))
            })?;
            columns.push(field.field_name().unwrap_or_default().to_string());
            values.push(DB::field_to_value(field.value()).map_err(|e| {
                alloc::boxed::Box::<dyn core::error::Error + Send + Sync>::from(alloc::format!(
                    "{e}"
                ))
            })?);
        }
        Ok(Self { columns, values })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn decode_scalars() {
        assert_eq!(decode_pg_bind(None, oid::INT4).unwrap(), Value::Null);
        assert_eq!(
            decode_pg_bind(Some(&5i32.to_be_bytes()), oid::INT4).unwrap(),
            Value::Int(5)
        );
        assert_eq!(
            decode_pg_bind(Some(&(-7i64).to_be_bytes()), oid::INT8).unwrap(),
            Value::Int(-7)
        );
        assert_eq!(
            decode_pg_bind(Some(&3.5f64.to_be_bytes()), oid::FLOAT8).unwrap(),
            Value::Float(3.5)
        );
        assert_eq!(
            decode_pg_bind(Some(&[1]), oid::BOOL).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            decode_pg_bind(Some(b"hello"), oid::TEXT).unwrap(),
            Value::String("hello".into())
        );
    }

    #[test]
    fn decode_uuid_is_canonical() {
        let bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        assert_eq!(
            decode_pg_bind(Some(&bytes), oid::UUID).unwrap(),
            Value::Uuid(uuid::Uuid::from_bytes(bytes))
        );
    }

    /// A type with no `Value` variant to become is refused by OID, and the
    /// refusal names the OID so the caller can look it up. Both examples are
    /// types diesel itself can read: `interval` has a `FromSql` impl and
    /// `bytea[]` is the array of a type subql does read, so what is missing is
    /// subql's payload, not diesel's reader.
    #[test]
    fn a_type_with_no_value_variant_is_refused_by_oid() {
        for (type_oid, what) in [(1186_u32, "interval"), (1001, "bytea[]")] {
            let refusal = decode_pg_bind(Some(&[0, 0, 0, 0]), type_oid);
            let Err(RegisterError::UnsupportedSql(message)) = refusal else {
                panic!("{what} (OID {type_oid}) should be refused, got {refusal:?}");
            };
            assert!(
                message.contains(&alloc::format!("OID {type_oid}")),
                "the refusal of {what} should name its OID, got {message:?}"
            );
        }
    }

    /// Every arm's decode, pinned across the value range and the boundaries
    /// the wire can carry. These hold both before and after the arms delegate
    /// to diesel's own `FromSql`, which is what makes them the evidence that
    /// delegation changed no decoded value.
    #[test]
    fn decode_bool_reads_the_first_byte() {
        assert_eq!(
            decode_pg_bind(Some(&[0]), oid::BOOL).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            decode_pg_bind(Some(&[1]), oid::BOOL).unwrap(),
            Value::Bool(true)
        );
        // Any nonzero byte is true, and an empty buffer has no answer.
        assert_eq!(
            decode_pg_bind(Some(&[2]), oid::BOOL).unwrap(),
            Value::Bool(true)
        );
        assert!(decode_pg_bind(Some(&[]), oid::BOOL).is_err());
    }

    #[test]
    fn decode_integers_span_their_range() {
        for v in [0i16, -1, i16::MIN, i16::MAX] {
            assert_eq!(
                decode_pg_bind(Some(&v.to_be_bytes()), oid::INT2).unwrap(),
                Value::Int(i64::from(v)),
                "int2 {v}"
            );
        }
        for v in [0i32, -1, i32::MIN, i32::MAX] {
            assert_eq!(
                decode_pg_bind(Some(&v.to_be_bytes()), oid::INT4).unwrap(),
                Value::Int(i64::from(v)),
                "int4 {v}"
            );
        }
        for v in [0i64, -1, i64::MIN, i64::MAX] {
            assert_eq!(
                decode_pg_bind(Some(&v.to_be_bytes()), oid::INT8).unwrap(),
                Value::Int(v),
                "int8 {v}"
            );
        }
        // An integer arm reads its own width and refuses any other.
        assert!(decode_pg_bind(Some(&[0]), oid::INT2).is_err());
        assert!(decode_pg_bind(Some(&0i64.to_be_bytes()), oid::INT2).is_err());
        assert!(decode_pg_bind(Some(&0i16.to_be_bytes()), oid::INT4).is_err());
        assert!(decode_pg_bind(Some(&0i32.to_be_bytes()), oid::INT8).is_err());
    }

    #[test]
    fn decode_floats_keep_their_bits() {
        for v in [0.0f32, -0.0, 1.5, -1.5, f32::MIN, f32::MAX, f32::EPSILON] {
            let Value::Float(got) = decode_pg_bind(Some(&v.to_be_bytes()), oid::FLOAT4).unwrap()
            else {
                panic!("float4 {v} decodes as a float")
            };
            assert_eq!(got.to_bits(), f64::from(v).to_bits(), "float4 {v}");
        }
        for v in [0.0f64, -0.0, 1.5, -1.5, f64::MIN, f64::MAX, f64::EPSILON] {
            let Value::Float(got) = decode_pg_bind(Some(&v.to_be_bytes()), oid::FLOAT8).unwrap()
            else {
                panic!("float8 {v} decodes as a float")
            };
            assert_eq!(got.to_bits(), v.to_bits(), "float8 {v}");
        }
        // NaN and the infinities survive as themselves.
        for (bytes, oid) in [
            (f32::NAN.to_be_bytes().to_vec(), oid::FLOAT4),
            (f64::NAN.to_be_bytes().to_vec(), oid::FLOAT8),
        ] {
            let Value::Float(got) = decode_pg_bind(Some(&bytes), oid).unwrap() else {
                panic!("NaN decodes as a float")
            };
            assert!(got.is_nan());
        }
        for (bytes, oid, sign) in [
            (f32::INFINITY.to_be_bytes().to_vec(), oid::FLOAT4, 1.0f64),
            (
                f32::NEG_INFINITY.to_be_bytes().to_vec(),
                oid::FLOAT4,
                -1.0f64,
            ),
            (f64::INFINITY.to_be_bytes().to_vec(), oid::FLOAT8, 1.0f64),
            (
                f64::NEG_INFINITY.to_be_bytes().to_vec(),
                oid::FLOAT8,
                -1.0f64,
            ),
        ] {
            assert_eq!(
                decode_pg_bind(Some(&bytes), oid).unwrap(),
                Value::Float(f64::INFINITY * sign)
            );
        }
        assert!(decode_pg_bind(Some(&[0, 0]), oid::FLOAT4).is_err());
        assert!(decode_pg_bind(Some(&[0, 0, 0, 0]), oid::FLOAT8).is_err());
    }

    #[test]
    fn decode_text_covers_every_text_oid() {
        for oid in [oid::TEXT, oid::VARCHAR, oid::BPCHAR, oid::NAME] {
            assert_eq!(
                decode_pg_bind(Some(b""), oid).unwrap(),
                Value::String(String::new()),
                "empty text under oid {oid}"
            );
            assert_eq!(
                decode_pg_bind(Some("héllo".as_bytes()), oid).unwrap(),
                Value::String("héllo".to_string()),
                "text under oid {oid}"
            );
            // A lone continuation byte is not UTF-8.
            assert!(
                decode_pg_bind(Some(&[0x80]), oid).is_err(),
                "invalid utf-8 under oid {oid}"
            );
        }
    }

    #[test]
    fn decode_uuid_spans_the_boundaries() {
        for bytes in [[0x00u8; 16], [0xff; 16]] {
            assert_eq!(
                decode_pg_bind(Some(&bytes), oid::UUID).unwrap(),
                Value::Uuid(uuid::Uuid::from_bytes(bytes))
            );
        }
        assert!(decode_pg_bind(Some(&[0u8; 15]), oid::UUID).is_err());
        assert!(decode_pg_bind(Some(&[0u8; 17]), oid::UUID).is_err());
    }

    /// A NULL bind carries no bytes whatever type it was declared as.
    #[test]
    fn decode_null_ignores_the_oid() {
        for oid in [
            oid::BOOL,
            oid::INT2,
            oid::INT4,
            oid::INT8,
            oid::FLOAT4,
            oid::FLOAT8,
            oid::TEXT,
            oid::UUID,
        ] {
            assert_eq!(decode_pg_bind(None, oid).unwrap(), Value::Null, "oid {oid}");
        }
    }
}

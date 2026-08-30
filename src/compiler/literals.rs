//! SQL literal parsing: sqlparser AST -> typed [`Value`].
//!
//! The compiler emits `PushLiteral(Value<B>)` and `LoadColumn(col)`
//! against a specific [`crate::backend::Backend`]. To keep those two in
//! step, SQL literals need backend-aware coercion: `WHERE bool_col = true`
//! on Postgres produces `Value::<Postgres>::Bool(true)` (Rust `bool`), on
//! SQLite it produces `Value::<SQLite>::Bool(1_i64)`. The [`SqlLiteralParse`]
//! trait captures that per-backend routing.
//!
//! [`SqlLiteralParse`] is a companion trait to `Backend` rather than an
//! extension of it: CDC-runtime paths that only read events don't touch
//! SQL and don't need the sqlparser dependency in their bounds.

use crate::backend::{
    Backend, BuiltinKind, CustomScalars, MySql, NoCustom, Postgres, SQLite, ScalarKind,
    ScalarKindOf, SqliteJson, Value,
};
use crate::{catalog_helpers, ColumnId, RegisterError, TableId};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::{Expr, Value as SqlValue};

/// Backend-aware parsing of sqlparser AST literals into typed [`Value`]s.
///
/// Implemented per shipped [`Backend`]. The compiler bounds its entry
/// points on `B: Backend + SqlLiteralParse` so the parser can turn any
/// sqlparser literal into a `Value<B>` targeting a known
/// [`ScalarKind`].
///
/// # Contract
///
/// * `sqlparser::ast::Value::Null` maps to [`Value::Null`] regardless of
///   `target`.
/// * When the sqlparser value's shape does not fit `target` (e.g.
///   `Boolean(true)` targeting [`BuiltinKind::Timestamp`]) the result is
///   [`RegisterError::TypeError`] naming the mismatch.
/// * When the shape fits but the payload cannot be parsed
///   (e.g. `SingleQuotedString("not-a-uuid")` targeting
///   [`BuiltinKind::Uuid`]) the result is [`RegisterError::TypeError`]
///   with a message naming the underlying parse failure.
pub trait SqlLiteralParse: Backend + Sized {
    /// Coerce a sqlparser AST literal into a [`Value`] typed to `Self`,
    /// targeting scalar kind `target`.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::TypeError`] for shape / payload
    /// mismatches. Returns [`RegisterError::UnsupportedSql`] for
    /// sqlparser variants outside the supported subset (currently
    /// `Placeholder`, `Interval`, `Time`, `Date`, `Boolean` in a
    /// non-boolean context, etc.).
    fn parse_literal(
        sql: &SqlValue,
        target: ScalarKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError>;
}

// ============================================================================
// Shared parsing helpers
// ============================================================================

fn err_shape<C: core::fmt::Debug + Copy>(sql: &SqlValue, target: ScalarKind<C>) -> RegisterError {
    RegisterError::TypeError(format!("cannot use SQL literal {sql:?} as {target:?}"))
}

fn err_parse(sql: &SqlValue, family: BuiltinKind, msg: impl core::fmt::Display) -> RegisterError {
    err_parse_kind(sql, ScalarKind::<NoCustom>::from(family), msg)
}

fn err_parse_kind<C: core::fmt::Debug + Copy>(
    sql: &SqlValue,
    target: ScalarKind<C>,
    msg: impl core::fmt::Display,
) -> RegisterError {
    RegisterError::TypeError(format!(
        "cannot parse SQL literal {sql:?} as {target:?}: {msg}"
    ))
}

fn parse_i64_literal(n: &str, sql: &SqlValue) -> Result<i64, RegisterError> {
    sql_scalar_text::parse_i64(n).ok_or_else(|| match n.parse::<i64>() {
        Err(e) => err_parse(sql, BuiltinKind::Int, e),
        Ok(_) => err_parse(sql, BuiltinKind::Int, "not an integer"),
    })
}

fn parse_f64_literal(n: &str, sql: &SqlValue) -> Result<f64, RegisterError> {
    sql_scalar_text::parse_f64(n).ok_or_else(|| match n.parse::<f64>() {
        Err(e) => err_parse(sql, BuiltinKind::Float, e),
        Ok(_) => err_parse(sql, BuiltinKind::Float, "not a float"),
    })
}

fn parse_decimal_literal(n: &str, sql: &SqlValue) -> Result<bigdecimal::BigDecimal, RegisterError> {
    sql_scalar_text::parse_decimal(n).ok_or_else(|| match n.parse::<bigdecimal::BigDecimal>() {
        Err(e) => err_parse(sql, BuiltinKind::Decimal, e),
        Ok(_) => err_parse(sql, BuiltinKind::Decimal, "not a decimal"),
    })
}

fn parse_hex_bytes(s: &str, sql: &SqlValue) -> Result<Vec<u8>, RegisterError> {
    if !s.len().is_multiple_of(2) {
        return Err(err_parse(sql, BuiltinKind::Bytes, "odd-length hex literal"));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i]).ok_or_else(|| {
            err_parse(sql, BuiltinKind::Bytes, "non-hex character in hex literal")
        })?;
        let lo = hex_nibble(bytes[i + 1]).ok_or_else(|| {
            err_parse(sql, BuiltinKind::Bytes, "non-hex character in hex literal")
        })?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

const fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Encode bytes as uppercase hex, the spelling `SqlValue::HexStringLiteral`
/// renders as `X'...'`. Inverse of [`parse_hex_bytes`]: the pair round-trips
/// any byte vector (empty included) losslessly, since `parse_hex_bytes`
/// accepts either case.
pub(super) fn hex_upper(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        // u8 -> usize widens losslessly. Nibbles are already 0..=15.
        out.push(char::from(HEX[usize::from(b >> 4)]));
        out.push(char::from(HEX[usize::from(b & 0x0f)]));
    }
    out
}

fn parse_uuid(s: &str, sql: &SqlValue) -> Result<uuid::Uuid, RegisterError> {
    uuid::Uuid::parse_str(s).map_err(|e| err_parse(sql, BuiltinKind::Uuid, e))
}

fn parse_uuid_as_string(s: &str, sql: &SqlValue) -> Result<String, RegisterError> {
    // Validate the shape before storing as a string so downstream matches
    // do not silently paper over a malformed UUID literal.
    uuid::Uuid::parse_str(s).map_err(|e| err_parse(sql, BuiltinKind::Uuid, e))?;
    Ok(s.to_string())
}

fn parse_timestamp_literal(
    s: &str,
    sql: &SqlValue,
) -> Result<chrono::NaiveDateTime, RegisterError> {
    sql_scalar_text::parse_timestamp(s)
        .ok_or_else(|| err_parse(sql, BuiltinKind::Timestamp, "not a timestamp"))
}

fn parse_timestamp_tz_literal(
    s: &str,
    sql: &SqlValue,
) -> Result<chrono::DateTime<chrono::Utc>, RegisterError> {
    sql_scalar_text::parse_timestamp_tz(s).ok_or_else(|| {
        err_parse(
            sql,
            BuiltinKind::TimestampTz,
            "not a timestamp with time zone",
        )
    })
}

fn parse_date_literal(s: &str, sql: &SqlValue) -> Result<chrono::NaiveDate, RegisterError> {
    sql_scalar_text::parse_date(s).ok_or_else(|| err_parse(sql, BuiltinKind::Date, "not a date"))
}

fn parse_time_literal(s: &str, sql: &SqlValue) -> Result<chrono::NaiveTime, RegisterError> {
    sql_scalar_text::parse_time(s).ok_or_else(|| err_parse(sql, BuiltinKind::Time, "not a time"))
}

fn parse_json(s: &str, sql: &SqlValue) -> Result<serde_json::Value, RegisterError> {
    serde_json::from_str(s).map_err(|e| err_parse(sql, BuiltinKind::Json, e))
}

/// Extract a string payload from a sqlparser quoted-string variant.
///
/// `SingleQuotedString`, `DoubleQuotedString`, `NationalStringLiteral`,
/// and `EscapedStringLiteral` all carry a plain `String`; hex literals
/// are handled by the bytes path separately.
fn quoted_string(sql: &SqlValue) -> Option<&str> {
    match sql {
        SqlValue::SingleQuotedString(s)
        | SqlValue::DoubleQuotedString(s)
        | SqlValue::NationalStringLiteral(s)
        | SqlValue::EscapedStringLiteral(s) => Some(s),
        _ => None,
    }
}

/// Parse a literal targeting a custom type: read it at the type's carrier
/// shape, then hand that to the type's own conversion.
///
/// The same conversion a change event's cell goes through, so a filter's
/// value and a row's value cannot disagree about what a spelling means. A
/// spelling the conversion declines is a registration error naming the
/// literal, never a silently ignored filter.
pub fn parse_custom_literal<B: SqlLiteralParse>(
    sql: &SqlValue,
    custom: <B::Custom as CustomScalars>::Kind,
) -> Result<Value<B>, RegisterError> {
    let carrier = <B::Custom as CustomScalars>::carrier(custom);
    let raw = B::parse_literal(sql, carrier.into())?;
    let view = raw
        .as_carried()
        .ok_or_else(|| err_shape(sql, ScalarKind::<()>::from(carrier)))?;
    <B::Custom as CustomScalars>::convert(custom, view)
        .map(Value::Custom)
        .ok_or_else(|| {
            RegisterError::TypeError(alloc::format!(
                "the custom type {custom:?} refused SQL literal {sql:?}"
            ))
        })
}

// ============================================================================
// Backend impls
// ============================================================================
//
// Postgres carries the reference impl. MySql and SQLite differ only in
// two associated types: SQLite::Bool = i64 (from bool via 0/1) and
// MySql::Uuid = SQLite::Uuid = String (validated UUID stored as text).
// All other scalars are structurally identical across the three
// backends.

impl SqlLiteralParse for Postgres {
    fn parse_literal(
        sql: &SqlValue,
        target: ScalarKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        if matches!(sql, SqlValue::Null) {
            return Ok(Value::Null);
        }
        let family = match target {
            ScalarKind::Builtin(family) => family,
            ScalarKind::Custom(custom) => return parse_custom_literal::<Self>(sql, custom),
        };
        match (family, sql) {
            (BuiltinKind::Bool, SqlValue::Boolean(b)) => Ok(Value::Bool(*b)),
            (BuiltinKind::Int, SqlValue::Number(n, _)) => {
                Ok(Value::Int(parse_i64_literal(n, sql)?))
            }
            (BuiltinKind::Float, SqlValue::Number(n, _)) => {
                Ok(Value::Float(parse_f64_literal(n, sql)?))
            }
            (BuiltinKind::String, _) => quoted_string(sql)
                .map(|s| Value::String(s.to_string()))
                .ok_or_else(|| err_shape(sql, target)),
            (BuiltinKind::Bytes, SqlValue::HexStringLiteral(s)) => {
                Ok(Value::Bytes(parse_hex_bytes(s, sql)?))
            }
            (BuiltinKind::Uuid, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_uuid(s, sql).map(Value::Uuid)),
            (BuiltinKind::Timestamp, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_timestamp_literal(s, sql).map(Value::Timestamp)),
            (BuiltinKind::TimestampTz, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_timestamp_tz_literal(s, sql).map(Value::TimestampTz)),
            (BuiltinKind::Date, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_date_literal(s, sql).map(Value::Date)),
            (BuiltinKind::Time, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_time_literal(s, sql).map(Value::Time)),
            (BuiltinKind::Decimal, SqlValue::Number(n, _)) => {
                Ok(Value::Decimal(parse_decimal_literal(n, sql)?))
            }
            (BuiltinKind::Json, _) => Err(RegisterError::TypeError(
                "PostgreSQL json has no equality operator".to_string(),
            )),
            (BuiltinKind::Jsonb, _) => {
                let s = quoted_string(sql).ok_or_else(|| err_shape(sql, target))?;
                parse_json(s, sql).map(Value::Jsonb)
            }
            _ => Err(err_shape(sql, target)),
        }
    }
}

impl SqlLiteralParse for MySql {
    fn parse_literal(
        sql: &SqlValue,
        target: ScalarKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        if matches!(sql, SqlValue::Null) {
            return Ok(Value::Null);
        }
        let family = match target {
            ScalarKind::Builtin(family) => family,
            ScalarKind::Custom(custom) => return parse_custom_literal::<Self>(sql, custom),
        };
        match (family, sql) {
            (BuiltinKind::Bool, SqlValue::Boolean(b)) => Ok(Value::Bool(*b)),
            (BuiltinKind::Int, SqlValue::Number(n, _)) => {
                Ok(Value::Int(parse_i64_literal(n, sql)?))
            }
            (BuiltinKind::Float, SqlValue::Number(n, _)) => {
                Ok(Value::Float(parse_f64_literal(n, sql)?))
            }
            (BuiltinKind::String, _) => quoted_string(sql)
                .map(|s| Value::String(s.to_string()))
                .ok_or_else(|| err_shape(sql, target)),
            (BuiltinKind::Bytes, SqlValue::HexStringLiteral(s)) => {
                Ok(Value::Bytes(parse_hex_bytes(s, sql)?))
            }
            // MySQL stores UUIDs as CHAR(36) or BINARY(16). The validated
            // string form passes through as `Backend::Uuid = String`.
            (BuiltinKind::Uuid, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_uuid_as_string(s, sql).map(Value::Uuid)),
            (BuiltinKind::Timestamp, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_timestamp_literal(s, sql).map(Value::Timestamp)),
            (BuiltinKind::TimestampTz, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_timestamp_tz_literal(s, sql).map(Value::TimestampTz)),
            (BuiltinKind::Date, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_date_literal(s, sql).map(Value::Date)),
            (BuiltinKind::Time, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_time_literal(s, sql).map(Value::Time)),
            (BuiltinKind::Decimal, SqlValue::Number(n, _)) => {
                Ok(Value::Decimal(parse_decimal_literal(n, sql)?))
            }
            (BuiltinKind::Json | BuiltinKind::Jsonb, _) => {
                let s = quoted_string(sql).ok_or_else(|| err_shape(sql, target))?;
                let value = parse_json(s, sql)?;
                Ok(if matches!(family, BuiltinKind::Json) {
                    Value::Json(value)
                } else {
                    Value::Jsonb(value)
                })
            }
            _ => Err(err_shape(sql, target)),
        }
    }
}

impl SqlLiteralParse for SQLite {
    fn parse_literal(
        sql: &SqlValue,
        target: ScalarKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        if matches!(sql, SqlValue::Null) {
            return Ok(Value::Null);
        }
        let family = match target {
            ScalarKind::Builtin(family) => family,
            ScalarKind::Custom(custom) => return parse_custom_literal::<Self>(sql, custom),
        };
        match (family, sql) {
            // SQLite has no native BOOL. The column contract stores 0 or 1
            // as INTEGER. Coerce the sqlparser Boolean to that.
            (BuiltinKind::Bool, SqlValue::Boolean(b)) => Ok(Value::Bool(i64::from(*b))),
            (BuiltinKind::Int, SqlValue::Number(n, _)) => {
                Ok(Value::Int(parse_i64_literal(n, sql)?))
            }
            (BuiltinKind::Float, SqlValue::Number(n, _)) => {
                Ok(Value::Float(parse_f64_literal(n, sql)?))
            }
            (BuiltinKind::String, _) => quoted_string(sql)
                .map(|s| Value::String(s.to_string()))
                .ok_or_else(|| err_shape(sql, target)),
            (BuiltinKind::Bytes, SqlValue::HexStringLiteral(s)) => {
                Ok(Value::Bytes(parse_hex_bytes(s, sql)?))
            }
            // SQLite stores UUIDs as TEXT (36-byte hyphenated) by convention.
            // The validated string form passes through as `Backend::Uuid = String`.
            (BuiltinKind::Uuid, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_uuid_as_string(s, sql).map(Value::Uuid)),
            (BuiltinKind::Timestamp, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_timestamp_literal(s, sql).map(Value::Timestamp)),
            (BuiltinKind::TimestampTz, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_timestamp_tz_literal(s, sql).map(Value::TimestampTz)),
            (BuiltinKind::Date, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_date_literal(s, sql).map(Value::Date)),
            (BuiltinKind::Time, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_time_literal(s, sql).map(Value::Time)),
            (BuiltinKind::Decimal, SqlValue::Number(n, _)) => {
                Ok(Value::Decimal(parse_decimal_literal(n, sql)?))
            }
            (BuiltinKind::Json | BuiltinKind::Jsonb, _) => {
                let s = quoted_string(sql).ok_or_else(|| err_shape(sql, target))?;
                let _ = parse_json(s, sql)?;
                let value = SqliteJson::text(s.to_string());
                Ok(if matches!(family, BuiltinKind::Json) {
                    Value::Json(value)
                } else {
                    Value::Jsonb(value)
                })
            }
            _ => Err(err_shape(sql, target)),
        }
    }
}

// ============================================================================
// Column-reference helpers (backend-agnostic)
// ============================================================================

/// Resolve simple column references used by parser / prefilter.
///
/// Supports `col` and `table.col` (table qualifier ignored after
/// SQL-shape validation).
#[must_use]
pub(super) fn resolve_column_ref<DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
) -> Option<ColumnId> {
    match expr {
        Expr::Identifier(ident) => catalog_helpers::column_id(database, table_id, &ident.value),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            catalog_helpers::column_id(database, table_id, &parts[1].value)
        }
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn postgres_null_is_type_agnostic() {
        assert_eq!(
            Postgres::parse_literal(&SqlValue::Null, BuiltinKind::Int.into()).unwrap(),
            Value::Null
        );
        assert_eq!(
            Postgres::parse_literal(&SqlValue::Null, BuiltinKind::String.into()).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn postgres_bool_from_boolean() {
        assert_eq!(
            Postgres::parse_literal(&SqlValue::Boolean(true), BuiltinKind::Bool.into()).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn postgres_int_from_number() {
        let sql = SqlValue::Number("42".to_string(), false);
        assert_eq!(
            Postgres::parse_literal(&sql, BuiltinKind::Int.into()).unwrap(),
            Value::Int(42)
        );
    }

    #[test]
    fn postgres_float_from_number() {
        let sql = SqlValue::Number("3.5".to_string(), false);
        assert_eq!(
            Postgres::parse_literal(&sql, BuiltinKind::Float.into()).unwrap(),
            Value::Float(3.5)
        );
    }

    #[test]
    fn postgres_string_from_quoted() {
        let sql = SqlValue::SingleQuotedString("hi".to_string());
        assert_eq!(
            Postgres::parse_literal(&sql, BuiltinKind::String.into()).unwrap(),
            Value::String("hi".to_string())
        );
    }

    #[test]
    fn postgres_uuid_from_quoted() {
        let sql = SqlValue::SingleQuotedString("550e8400-e29b-41d4-a716-446655440000".to_string());
        let v = Postgres::parse_literal(&sql, BuiltinKind::Uuid.into()).unwrap();
        assert!(matches!(v, Value::Uuid(_)));
    }

    #[test]
    fn postgres_timestamp_from_iso8601() {
        let sql = SqlValue::SingleQuotedString("2024-01-02T03:04:05".to_string());
        let v = Postgres::parse_literal(&sql, BuiltinKind::Timestamp.into()).unwrap();
        assert!(matches!(v, Value::Timestamp(_)));
    }

    #[test]
    fn postgres_date_from_iso8601() {
        let sql = SqlValue::SingleQuotedString("2024-01-02".to_string());
        let v = Postgres::parse_literal(&sql, BuiltinKind::Date.into()).unwrap();
        assert!(matches!(v, Value::Date(_)));
    }

    #[test]
    fn postgres_bytes_from_hex_literal() {
        let sql = SqlValue::HexStringLiteral("deadbeef".to_string());
        let v = Postgres::parse_literal(&sql, BuiltinKind::Bytes.into()).unwrap();
        assert_eq!(v, Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn postgres_json_equality_is_refused() {
        let sql = SqlValue::SingleQuotedString("{\"k\":1}".to_string());
        assert!(matches!(
            Postgres::parse_literal(&sql, BuiltinKind::Json.into()),
            Err(RegisterError::TypeError(_))
        ));
    }

    #[test]
    fn postgres_decimal_from_number() {
        let sql = SqlValue::Number("123.456".to_string(), false);
        let v = Postgres::parse_literal(&sql, BuiltinKind::Decimal.into()).unwrap();
        assert!(matches!(v, Value::Decimal(_)));
    }

    #[test]
    fn postgres_type_mismatch_returns_type_error() {
        let sql = SqlValue::Boolean(true);
        let err = Postgres::parse_literal(&sql, BuiltinKind::Int.into()).unwrap_err();
        assert!(matches!(err, RegisterError::TypeError(_)));
    }

    #[test]
    fn postgres_uuid_invalid_string_errors() {
        let sql = SqlValue::SingleQuotedString("not-a-uuid".to_string());
        let err = Postgres::parse_literal(&sql, BuiltinKind::Uuid.into()).unwrap_err();
        assert!(matches!(err, RegisterError::TypeError(_)));
    }

    #[test]
    fn temporal_literal_maps_representative_values() {
        use chrono::{DateTime, Utc};
        let ts = SqlValue::SingleQuotedString("2026-01-01 00:00:00".to_string());
        assert!(matches!(
            Postgres::parse_literal(&ts, BuiltinKind::Timestamp.into()),
            Ok(Value::Timestamp(_))
        ));
        assert!(matches!(
            SQLite::parse_literal(&ts, BuiltinKind::Timestamp.into()),
            Ok(Value::Timestamp(_))
        ));
        let tstz = SqlValue::SingleQuotedString("2025-12-31 22:00:00-02".to_string());
        let expected_utc: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(
            Postgres::parse_literal(&tstz, BuiltinKind::TimestampTz.into()).unwrap(),
            Value::TimestampTz(expected_utc)
        );
        let date = SqlValue::SingleQuotedString("2026-01-01".to_string());
        assert!(matches!(
            Postgres::parse_literal(&date, BuiltinKind::Date.into()),
            Ok(Value::Date(_))
        ));
        let time = SqlValue::SingleQuotedString("12:34:56.789".to_string());
        assert!(matches!(
            MySql::parse_literal(&time, BuiltinKind::Time.into()),
            Ok(Value::Time(_))
        ));
    }

    #[test]
    fn temporal_literal_rejects_key_boundaries() {
        let no_offset = SqlValue::SingleQuotedString("2026-01-01 00:00:00".to_string());
        assert!(matches!(
            Postgres::parse_literal(&no_offset, BuiltinKind::TimestampTz.into()),
            Err(RegisterError::TypeError(_))
        ));
        let with_offset = SqlValue::SingleQuotedString("2026-01-01 00:00:00+00".to_string());
        assert!(matches!(
            Postgres::parse_literal(&with_offset, BuiltinKind::Timestamp.into()),
            Err(RegisterError::TypeError(_))
        ));
    }
}

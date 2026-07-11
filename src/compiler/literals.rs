//! SQL literal parsing: sqlparser AST -> typed [`Value`].
//!
//! The compiler emits `PushLiteral(Value<B>)` and `LoadColumn(col, kind)`
//! against a specific [`crate::backend::Backend`]. To keep those two in
//! step, SQL literals need backend-aware coercion: `WHERE bool_col = true`
//! on Postgres produces `Value::<Postgres>::Bool(true)` (Rust `bool`), on
//! SQLite it produces `Value::<SQLite>::Bool(1_i64)`. The [`SqlLiteralParse`]
//! trait captures that per-backend routing.
//!
//! [`SqlLiteralParse`] is a companion trait to `Backend` rather than an
//! extension of it: CDC-runtime paths that only read events don't touch
//! SQL and don't need the sqlparser dependency in their bounds.

use crate::backend::{Backend, MySql, Postgres, SQLite, ScalarKind, Value};
use crate::{catalog_helpers, ColumnId, RegisterError, TableId};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::str::FromStr;
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
///   `Boolean(true)` targeting [`ScalarKind::Timestamp`]) the result is
///   [`RegisterError::TypeError`] naming the mismatch.
/// * When the shape fits but the payload cannot be parsed
///   (e.g. `SingleQuotedString("not-a-uuid")` targeting
///   [`ScalarKind::Uuid`]) the result is [`RegisterError::TypeError`]
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
    fn parse_literal(sql: &SqlValue, target: ScalarKind) -> Result<Value<Self>, RegisterError>;
}

// ============================================================================
// Shared parsing helpers
// ============================================================================

fn err_shape(sql: &SqlValue, target: ScalarKind) -> RegisterError {
    RegisterError::TypeError(format!("cannot use SQL literal {sql:?} as {target:?}"))
}

fn err_parse(sql: &SqlValue, target: ScalarKind, msg: impl core::fmt::Display) -> RegisterError {
    RegisterError::TypeError(format!(
        "cannot parse SQL literal {sql:?} as {target:?}: {msg}"
    ))
}

fn parse_i64(n: &str, sql: &SqlValue) -> Result<i64, RegisterError> {
    n.parse::<i64>()
        .map_err(|e| err_parse(sql, ScalarKind::Int, e))
}

fn parse_f64(n: &str, sql: &SqlValue) -> Result<f64, RegisterError> {
    n.parse::<f64>()
        .map_err(|e| err_parse(sql, ScalarKind::Float, e))
}

fn parse_decimal(n: &str, sql: &SqlValue) -> Result<bigdecimal::BigDecimal, RegisterError> {
    bigdecimal::BigDecimal::from_str(n).map_err(|e| err_parse(sql, ScalarKind::Decimal, e))
}

fn parse_hex_bytes(s: &str, sql: &SqlValue) -> Result<Vec<u8>, RegisterError> {
    if s.len() % 2 != 0 {
        return Err(err_parse(sql, ScalarKind::Bytes, "odd-length hex literal"));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i])
            .ok_or_else(|| err_parse(sql, ScalarKind::Bytes, "non-hex character in hex literal"))?;
        let lo = hex_nibble(bytes[i + 1])
            .ok_or_else(|| err_parse(sql, ScalarKind::Bytes, "non-hex character in hex literal"))?;
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

fn parse_uuid(s: &str, sql: &SqlValue) -> Result<uuid::Uuid, RegisterError> {
    uuid::Uuid::parse_str(s).map_err(|e| err_parse(sql, ScalarKind::Uuid, e))
}

fn parse_uuid_as_string(s: &str, sql: &SqlValue) -> Result<String, RegisterError> {
    // Validate the shape before storing as a string so downstream matches
    // do not silently paper over a malformed UUID literal.
    uuid::Uuid::parse_str(s).map_err(|e| err_parse(sql, ScalarKind::Uuid, e))?;
    Ok(s.to_string())
}

fn parse_timestamp(s: &str, sql: &SqlValue) -> Result<chrono::NaiveDateTime, RegisterError> {
    // Accept the ISO-8601 subset the temporal accessors carry.
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| err_parse(sql, ScalarKind::Timestamp, e))
}

fn parse_timestamp_tz(
    s: &str,
    sql: &SqlValue,
) -> Result<chrono::DateTime<chrono::Utc>, RegisterError> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| err_parse(sql, ScalarKind::TimestampTz, e))
}

fn parse_date(s: &str, sql: &SqlValue) -> Result<chrono::NaiveDate, RegisterError> {
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| err_parse(sql, ScalarKind::Date, e))
}

fn parse_time(s: &str, sql: &SqlValue) -> Result<chrono::NaiveTime, RegisterError> {
    chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .map_err(|e| err_parse(sql, ScalarKind::Time, e))
}

fn parse_json(s: &str, sql: &SqlValue) -> Result<serde_json::Value, RegisterError> {
    serde_json::from_str(s).map_err(|e| err_parse(sql, ScalarKind::Json, e))
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
    fn parse_literal(sql: &SqlValue, target: ScalarKind) -> Result<Value<Self>, RegisterError> {
        if matches!(sql, SqlValue::Null) {
            return Ok(Value::Null);
        }
        match (target, sql) {
            (ScalarKind::Bool, SqlValue::Boolean(b)) => Ok(Value::Bool(*b)),
            (ScalarKind::Int, SqlValue::Number(n, _)) => Ok(Value::Int(parse_i64(n, sql)?)),
            (ScalarKind::Float, SqlValue::Number(n, _)) => Ok(Value::Float(parse_f64(n, sql)?)),
            (ScalarKind::String, _) => quoted_string(sql)
                .map(|s| Value::String(s.to_string()))
                .ok_or_else(|| err_shape(sql, target)),
            (ScalarKind::Bytes, SqlValue::HexStringLiteral(s)) => {
                Ok(Value::Bytes(parse_hex_bytes(s, sql)?))
            }
            (ScalarKind::Uuid, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_uuid(s, sql).map(Value::Uuid)),
            (ScalarKind::Timestamp, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_timestamp(s, sql).map(Value::Timestamp)),
            (ScalarKind::TimestampTz, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_timestamp_tz(s, sql).map(Value::TimestampTz)),
            (ScalarKind::Date, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_date(s, sql).map(Value::Date)),
            (ScalarKind::Time, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_time(s, sql).map(Value::Time)),
            (ScalarKind::Decimal, SqlValue::Number(n, _)) => {
                Ok(Value::Decimal(parse_decimal(n, sql)?))
            }
            (ScalarKind::Json | ScalarKind::Jsonb, _) => {
                let s = quoted_string(sql).ok_or_else(|| err_shape(sql, target))?;
                let v = parse_json(s, sql)?;
                Ok(if matches!(target, ScalarKind::Json) {
                    Value::Json(v)
                } else {
                    Value::Jsonb(v)
                })
            }
            _ => Err(err_shape(sql, target)),
        }
    }
}

impl SqlLiteralParse for MySql {
    fn parse_literal(sql: &SqlValue, target: ScalarKind) -> Result<Value<Self>, RegisterError> {
        if matches!(sql, SqlValue::Null) {
            return Ok(Value::Null);
        }
        match (target, sql) {
            (ScalarKind::Bool, SqlValue::Boolean(b)) => Ok(Value::Bool(*b)),
            (ScalarKind::Int, SqlValue::Number(n, _)) => Ok(Value::Int(parse_i64(n, sql)?)),
            (ScalarKind::Float, SqlValue::Number(n, _)) => Ok(Value::Float(parse_f64(n, sql)?)),
            (ScalarKind::String, _) => quoted_string(sql)
                .map(|s| Value::String(s.to_string()))
                .ok_or_else(|| err_shape(sql, target)),
            (ScalarKind::Bytes, SqlValue::HexStringLiteral(s)) => {
                Ok(Value::Bytes(parse_hex_bytes(s, sql)?))
            }
            // MySQL stores UUIDs as CHAR(36) / BINARY(16); wire the
            // validated string form through as `Backend::Uuid = String`.
            (ScalarKind::Uuid, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_uuid_as_string(s, sql).map(Value::Uuid)),
            (ScalarKind::Timestamp, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_timestamp(s, sql).map(Value::Timestamp)),
            (ScalarKind::TimestampTz, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_timestamp_tz(s, sql).map(Value::TimestampTz)),
            (ScalarKind::Date, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_date(s, sql).map(Value::Date)),
            (ScalarKind::Time, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_time(s, sql).map(Value::Time)),
            (ScalarKind::Decimal, SqlValue::Number(n, _)) => {
                Ok(Value::Decimal(parse_decimal(n, sql)?))
            }
            (ScalarKind::Json | ScalarKind::Jsonb, _) => {
                let s = quoted_string(sql).ok_or_else(|| err_shape(sql, target))?;
                let v = parse_json(s, sql)?;
                Ok(if matches!(target, ScalarKind::Json) {
                    Value::Json(v)
                } else {
                    Value::Jsonb(v)
                })
            }
            _ => Err(err_shape(sql, target)),
        }
    }
}

impl SqlLiteralParse for SQLite {
    fn parse_literal(sql: &SqlValue, target: ScalarKind) -> Result<Value<Self>, RegisterError> {
        if matches!(sql, SqlValue::Null) {
            return Ok(Value::Null);
        }
        match (target, sql) {
            // SQLite has no native BOOL; the column contract stores 0 / 1
            // as INTEGER. Coerce the sqlparser Boolean to that.
            (ScalarKind::Bool, SqlValue::Boolean(b)) => {
                Ok(Value::Bool(i64::from(*b)))
            }
            (ScalarKind::Int, SqlValue::Number(n, _)) => Ok(Value::Int(parse_i64(n, sql)?)),
            (ScalarKind::Float, SqlValue::Number(n, _)) => Ok(Value::Float(parse_f64(n, sql)?)),
            (ScalarKind::String, _) => quoted_string(sql)
                .map(|s| Value::String(s.to_string()))
                .ok_or_else(|| err_shape(sql, target)),
            (ScalarKind::Bytes, SqlValue::HexStringLiteral(s)) => {
                Ok(Value::Bytes(parse_hex_bytes(s, sql)?))
            }
            // SQLite stores UUIDs as TEXT (36-byte hyphenated) by convention;
            // wire the validated string form through as `Backend::Uuid = String`.
            (ScalarKind::Uuid, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_uuid_as_string(s, sql).map(Value::Uuid)),
            (ScalarKind::Timestamp, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_timestamp(s, sql).map(Value::Timestamp)),
            (ScalarKind::TimestampTz, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_timestamp_tz(s, sql).map(Value::TimestampTz)),
            (ScalarKind::Date, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_date(s, sql).map(Value::Date)),
            (ScalarKind::Time, _) => quoted_string(sql)
                .ok_or_else(|| err_shape(sql, target))
                .and_then(|s| parse_time(s, sql).map(Value::Time)),
            (ScalarKind::Decimal, SqlValue::Number(n, _)) => {
                Ok(Value::Decimal(parse_decimal(n, sql)?))
            }
            (ScalarKind::Json | ScalarKind::Jsonb, _) => {
                let s = quoted_string(sql).ok_or_else(|| err_shape(sql, target))?;
                let v = parse_json(s, sql)?;
                Ok(if matches!(target, ScalarKind::Json) {
                    Value::Json(v)
                } else {
                    Value::Jsonb(v)
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
            Postgres::parse_literal(&SqlValue::Null, ScalarKind::Int).unwrap(),
            Value::Null
        );
        assert_eq!(
            Postgres::parse_literal(&SqlValue::Null, ScalarKind::String).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn postgres_bool_from_boolean() {
        assert_eq!(
            Postgres::parse_literal(&SqlValue::Boolean(true), ScalarKind::Bool).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn postgres_int_from_number() {
        let sql = SqlValue::Number("42".to_string(), false);
        assert_eq!(
            Postgres::parse_literal(&sql, ScalarKind::Int).unwrap(),
            Value::Int(42)
        );
    }

    #[test]
    fn postgres_float_from_number() {
        let sql = SqlValue::Number("3.5".to_string(), false);
        assert_eq!(
            Postgres::parse_literal(&sql, ScalarKind::Float).unwrap(),
            Value::Float(3.5)
        );
    }

    #[test]
    fn postgres_string_from_quoted() {
        let sql = SqlValue::SingleQuotedString("hi".to_string());
        assert_eq!(
            Postgres::parse_literal(&sql, ScalarKind::String).unwrap(),
            Value::String("hi".to_string())
        );
    }

    #[test]
    fn postgres_uuid_from_quoted() {
        let sql = SqlValue::SingleQuotedString("550e8400-e29b-41d4-a716-446655440000".to_string());
        let v = Postgres::parse_literal(&sql, ScalarKind::Uuid).unwrap();
        assert!(matches!(v, Value::Uuid(_)));
    }

    #[test]
    fn postgres_timestamp_from_iso8601() {
        let sql = SqlValue::SingleQuotedString("2024-01-02T03:04:05".to_string());
        let v = Postgres::parse_literal(&sql, ScalarKind::Timestamp).unwrap();
        assert!(matches!(v, Value::Timestamp(_)));
    }

    #[test]
    fn postgres_date_from_iso8601() {
        let sql = SqlValue::SingleQuotedString("2024-01-02".to_string());
        let v = Postgres::parse_literal(&sql, ScalarKind::Date).unwrap();
        assert!(matches!(v, Value::Date(_)));
    }

    #[test]
    fn postgres_bytes_from_hex_literal() {
        let sql = SqlValue::HexStringLiteral("deadbeef".to_string());
        let v = Postgres::parse_literal(&sql, ScalarKind::Bytes).unwrap();
        assert_eq!(v, Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn postgres_json_from_quoted() {
        let sql = SqlValue::SingleQuotedString("{\"k\":1}".to_string());
        let v = Postgres::parse_literal(&sql, ScalarKind::Json).unwrap();
        assert!(matches!(v, Value::Json(_)));
    }

    #[test]
    fn postgres_decimal_from_number() {
        let sql = SqlValue::Number("123.456".to_string(), false);
        let v = Postgres::parse_literal(&sql, ScalarKind::Decimal).unwrap();
        assert!(matches!(v, Value::Decimal(_)));
    }

    #[test]
    fn postgres_type_mismatch_returns_type_error() {
        let sql = SqlValue::Boolean(true);
        let err = Postgres::parse_literal(&sql, ScalarKind::Int).unwrap_err();
        assert!(matches!(err, RegisterError::TypeError(_)));
    }

    #[test]
    fn postgres_uuid_invalid_string_errors() {
        let sql = SqlValue::SingleQuotedString("not-a-uuid".to_string());
        let err = Postgres::parse_literal(&sql, ScalarKind::Uuid).unwrap_err();
        assert!(matches!(err, RegisterError::TypeError(_)));
    }
}

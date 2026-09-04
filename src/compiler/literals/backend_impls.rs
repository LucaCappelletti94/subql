//! `SqlLiteralParse` implementations for the three shipped backends.
//!
//! Postgres carries the reference impl. MySql and SQLite differ only in
//! two associated types: `SQLite::Bool = i64` (from bool via 0/1) and
//! `MySql::Uuid = SQLite::Uuid = String` (validated UUID stored as text).
//! All other scalars are structurally identical across the three backends.

use super::parse_helpers::{
    err_shape, parse_date_literal, parse_decimal_literal, parse_f64_literal, parse_hex_bytes,
    parse_i64_literal, parse_json, parse_time_literal, parse_timestamp_literal,
    parse_timestamp_tz_literal, parse_uuid, parse_uuid_as_string, quoted_string,
};
use super::{parse_custom_literal, SqlLiteralParse};
use crate::backend::{
    BuiltinKind, MySql, Postgres, SQLite, SqliteJson, Value, ValueKind, ValueKindOf,
};
use crate::RegisterError;
use alloc::string::ToString;
use sqlparser::ast::Value as SqlValue;

impl<V: postgres_jsonb_canonical::PgVersion + 'static> SqlLiteralParse for Postgres<V> {
    fn parse_literal(
        sql: &SqlValue,
        target: ValueKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        if matches!(sql, SqlValue::Null) {
            return Ok(Value::Null);
        }
        let family = match target {
            ValueKind::Builtin(family) => family,
            ValueKind::Custom(custom) => return parse_custom_literal::<Self>(sql, custom),
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
                let value = parse_json(s, sql)?;
                // The one path that can introduce a `jsonb` value the server could not
                // store, so refusal belongs here, where it means the literal is invalid.
                // Downstream every value came either from here or from a row, so equality
                // never has to flatten a refusal into a boolean. One throwaway encoding
                // per registration, not per row.
                postgres_jsonb_canonical::encode::<V>(&value)
                    .map_err(|error| super::parse_helpers::err_parse(sql, family, error))?;
                Ok(Value::Jsonb(value))
            }
            _ => Err(err_shape(sql, target)),
        }
    }
}

impl SqlLiteralParse for MySql {
    fn parse_literal(
        sql: &SqlValue,
        target: ValueKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        if matches!(sql, SqlValue::Null) {
            return Ok(Value::Null);
        }
        let family = match target {
            ValueKind::Builtin(family) => family,
            ValueKind::Custom(custom) => return parse_custom_literal::<Self>(sql, custom),
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
        target: ValueKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        if matches!(sql, SqlValue::Null) {
            return Ok(Value::Null);
        }
        let family = match target {
            ValueKind::Builtin(family) => family,
            ValueKind::Custom(custom) => return parse_custom_literal::<Self>(sql, custom),
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

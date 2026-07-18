#![allow(clippy::match_same_arms)]
//! PostgreSQL wire encoding to typed [`Value<Postgres>`] and
//! [`Value<MySql>`] conversion.
//!
//! Shared by all JSON-based WAL parsers (wal2json, Maxwell)
//! and the pgoutput binary wire path. Every consumer is Phase-7 typed.
//! The legacy untyped-scalar decoders were retired in Phase 7F.

use alloc::string::ToString;
use core::str::FromStr;

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use uuid::Uuid;

use crate::backend::{MySql, Postgres, ScalarKind, Value};

// ============================================================================
// Typed decoders producing `Value<Postgres>` (Phase 7)
// ============================================================================

/// Decode a pgoutput text-format value into a typed [`Value<Postgres>`],
/// routed by the column's catalog [`ScalarKind`] (the schema-driven path).
///
/// pgoutput carries only positional text, so the type comes from the
/// catalog at decode time rather than the wire OID. Parse failures and
/// shapes the kind cannot accept collapse to [`Value::Missing`], matching
/// the `value_at` contract (a corrupt cell escalates to re-execution).
pub(super) fn text_to_pg_value_by_kind(text: &str, kind: ScalarKind) -> Value<Postgres> {
    match kind {
        ScalarKind::Bool => match text {
            "t" => Value::Bool(true),
            "f" => Value::Bool(false),
            _ => Value::Missing,
        },
        ScalarKind::Int => text.parse::<i64>().map_or(Value::Missing, Value::Int),
        ScalarKind::Float => text.parse::<f64>().map_or(Value::Missing, Value::Float),
        ScalarKind::Decimal => BigDecimal::from_str(text).map_or(Value::Missing, Value::Decimal),
        ScalarKind::String => Value::String(text.to_string()),
        ScalarKind::Bytes => decode_pg_bytea_hex(text).map_or(Value::Missing, Value::Bytes),
        ScalarKind::Uuid => Uuid::parse_str(text).map_or(Value::Missing, Value::Uuid),
        ScalarKind::Timestamp => parse_pg_timestamp(text).map_or(Value::Missing, Value::Timestamp),
        ScalarKind::TimestampTz => {
            parse_pg_timestamptz(text).map_or(Value::Missing, Value::TimestampTz)
        }
        ScalarKind::Date => {
            NaiveDate::parse_from_str(text, "%Y-%m-%d").map_or(Value::Missing, Value::Date)
        }
        ScalarKind::Time => parse_pg_time(text).map_or(Value::Missing, Value::Time),
        ScalarKind::Json => serde_json::from_str(text).map_or(Value::Missing, Value::Json),
        ScalarKind::Jsonb => serde_json::from_str(text).map_or(Value::Missing, Value::Jsonb),
    }
}

/// Decode a PostgreSQL `bytea` value in the wire text `hex` format
/// (`\x` prefix followed by an even number of hex digits). Returns `None`
/// on a missing prefix, non-hex digits, or an odd nibble count.
fn decode_pg_bytea_hex(text: &str) -> Option<alloc::vec::Vec<u8>> {
    let hex = text.strip_prefix(r"\x")?;
    if !hex.is_ascii() || hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

/// Decode a JSON wire value into a typed [`Value<Postgres>`] routed by the
/// column's catalog [`ScalarKind`] (the schema-driven path for wal2json).
///
/// A wire-carried JSON null becomes [`Value::Null`]. Any shape the kind
/// cannot accept collapses to [`Value::Missing`], matching the `value_at`
/// contract (a corrupt cell escalates to re-execution).
pub(super) fn json_value_to_pg_value_by_kind(
    value: &serde_json::Value,
    kind: ScalarKind,
) -> Value<Postgres> {
    if value.is_null() {
        return Value::Null;
    }
    match kind {
        ScalarKind::Bool => json_bool(value).map_or(Value::Missing, Value::Bool),
        ScalarKind::Int => json_i64(value).map_or(Value::Missing, Value::Int),
        ScalarKind::Float => json_f64(value).map_or(Value::Missing, Value::Float),
        ScalarKind::Decimal => json_bigdecimal(value).map_or(Value::Missing, Value::Decimal),
        ScalarKind::String => Value::String(json_string(value)),
        ScalarKind::Bytes => json_bytea(value).map_or(Value::Missing, Value::Bytes),
        ScalarKind::Uuid => value
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .map_or(Value::Missing, Value::Uuid),
        ScalarKind::Timestamp => json_timestamp(value).map_or(Value::Missing, Value::Timestamp),
        ScalarKind::TimestampTz => {
            json_timestamptz(value).map_or(Value::Missing, Value::TimestampTz)
        }
        ScalarKind::Date => json_date(value).map_or(Value::Missing, Value::Date),
        ScalarKind::Time => json_time(value).map_or(Value::Missing, Value::Time),
        ScalarKind::Json => json_document(value).map_or(Value::Missing, Value::Json),
        ScalarKind::Jsonb => json_document(value).map_or(Value::Missing, Value::Jsonb),
    }
}

/// Decode a JSON wire value into a typed [`Value<MySql>`] routed by the
/// column's catalog [`ScalarKind`] (the schema-driven path for Maxwell).
///
/// Mirrors [`json_value_to_pg_value_by_kind`]. The two differ only on
/// [`ScalarKind::Uuid`]: MySQL has no native UUID type and stores it as
/// text, so the wire string is taken verbatim rather than parsed into a
/// [`uuid::Uuid`].
pub(super) fn json_value_to_mysql_value_by_kind(
    value: &serde_json::Value,
    kind: ScalarKind,
) -> Value<MySql> {
    if value.is_null() {
        return Value::Null;
    }
    match kind {
        ScalarKind::Bool => json_bool(value).map_or(Value::Missing, Value::Bool),
        ScalarKind::Int => json_i64(value).map_or(Value::Missing, Value::Int),
        ScalarKind::Float => json_f64(value).map_or(Value::Missing, Value::Float),
        ScalarKind::Decimal => json_bigdecimal(value).map_or(Value::Missing, Value::Decimal),
        ScalarKind::String => Value::String(json_string(value)),
        ScalarKind::Bytes => json_bytea(value).map_or(Value::Missing, Value::Bytes),
        ScalarKind::Uuid => value
            .as_str()
            .map_or(Value::Missing, |s| Value::Uuid(s.to_string())),
        ScalarKind::Timestamp => json_timestamp(value).map_or(Value::Missing, Value::Timestamp),
        ScalarKind::TimestampTz => {
            json_timestamptz(value).map_or(Value::Missing, Value::TimestampTz)
        }
        ScalarKind::Date => json_date(value).map_or(Value::Missing, Value::Date),
        ScalarKind::Time => json_time(value).map_or(Value::Missing, Value::Time),
        ScalarKind::Json => json_document(value).map_or(Value::Missing, Value::Json),
        ScalarKind::Jsonb => json_document(value).map_or(Value::Missing, Value::Jsonb),
    }
}

// Backend-agnostic pure JSON scalar parsers shared by the two by-kind
// decoders above. Only the `Value<B>` wrapping and the Uuid arm differ
// between backends, so the parsing itself lives here once.

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::option_if_let_else
)]
fn json_i64(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(i)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u).ok()
            } else {
                n.as_f64().and_then(|f| {
                    (f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64)
                        .then_some(f as i64)
                })
            }
        }
        serde_json::Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn json_f64(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn json_bigdecimal(value: &serde_json::Value) -> Option<BigDecimal> {
    match value {
        serde_json::Value::String(s) => BigDecimal::from_str(s).ok(),
        serde_json::Value::Number(n) => BigDecimal::from_str(&n.to_string()).ok(),
        _ => None,
    }
}

fn json_bool(value: &serde_json::Value) -> Option<bool> {
    match value {
        serde_json::Value::Bool(b) => Some(*b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(0) => Some(false),
            Some(1) => Some(true),
            _ => None,
        },
        serde_json::Value::String(s) => match s.as_str() {
            "t" | "true" | "TRUE" | "True" | "1" => Some(true),
            "f" | "false" | "FALSE" | "False" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn json_string(value: &serde_json::Value) -> alloc::string::String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn json_bytea(value: &serde_json::Value) -> Option<alloc::vec::Vec<u8>> {
    value.as_str().and_then(decode_pg_bytea_hex)
}

fn json_timestamp(value: &serde_json::Value) -> Option<NaiveDateTime> {
    value.as_str().and_then(parse_pg_timestamp)
}

fn json_timestamptz(value: &serde_json::Value) -> Option<DateTime<Utc>> {
    value.as_str().and_then(parse_pg_timestamptz)
}

fn json_date(value: &serde_json::Value) -> Option<NaiveDate> {
    value
        .as_str()
        .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
}

fn json_time(value: &serde_json::Value) -> Option<NaiveTime> {
    value.as_str().and_then(parse_pg_time)
}

fn json_document(value: &serde_json::Value) -> Option<serde_json::Value> {
    match value {
        serde_json::Value::String(s) => serde_json::from_str(s).ok(),
        other => Some(other.clone()),
    }
}

fn parse_pg_timestamp(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .ok()
}

fn parse_pg_timestamptz(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%.f%z",
        "%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%d %H:%M:%S%z",
    ] {
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Some(dt.with_timezone(&Utc));
        }
    }
    None
}

fn parse_pg_time(s: &str) -> Option<NaiveTime> {
    NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .ok()
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::approx_constant,
    clippy::unreadable_literal
)]
mod tests {
    use super::*;

    // Sub-block E: text_to_pg_value_by_kind (pgoutput catalog-driven decode)

    #[test]
    fn kind_bool_uses_pg_text_forms_only() {
        assert_eq!(
            text_to_pg_value_by_kind("t", ScalarKind::Bool),
            Value::Bool(true)
        );
        assert_eq!(
            text_to_pg_value_by_kind("f", ScalarKind::Bool),
            Value::Bool(false)
        );
        // pgoutput never emits these spellings; they must not decode.
        assert_eq!(
            text_to_pg_value_by_kind("true", ScalarKind::Bool),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind("1", ScalarKind::Bool),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind("", ScalarKind::Bool),
            Value::Missing
        );
    }

    #[test]
    fn kind_int_extremes_and_garbage() {
        assert_eq!(
            text_to_pg_value_by_kind("0", ScalarKind::Int),
            Value::Int(0)
        );
        assert_eq!(
            text_to_pg_value_by_kind("-9223372036854775808", ScalarKind::Int),
            Value::Int(i64::MIN)
        );
        assert_eq!(
            text_to_pg_value_by_kind("9223372036854775807", ScalarKind::Int),
            Value::Int(i64::MAX)
        );
        // i64::MAX + 1 overflows: Missing, never a wrapped or clamped value.
        assert_eq!(
            text_to_pg_value_by_kind("9223372036854775808", ScalarKind::Int),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind("3.5", ScalarKind::Int),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind("0x10", ScalarKind::Int),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind("", ScalarKind::Int),
            Value::Missing
        );
    }

    #[test]
    fn kind_float_specials() {
        assert_eq!(
            text_to_pg_value_by_kind("1.5", ScalarKind::Float),
            Value::Float(1.5)
        );
        assert_eq!(
            text_to_pg_value_by_kind("1e10", ScalarKind::Float),
            Value::Float(1e10)
        );
        // pg emits Infinity / -Infinity / NaN for float specials.
        assert_eq!(
            text_to_pg_value_by_kind("Infinity", ScalarKind::Float),
            Value::Float(f64::INFINITY)
        );
        assert_eq!(
            text_to_pg_value_by_kind("-Infinity", ScalarKind::Float),
            Value::Float(f64::NEG_INFINITY)
        );
        match text_to_pg_value_by_kind("NaN", ScalarKind::Float) {
            Value::Float(f) => assert!(f.is_nan()),
            other => panic!("expected NaN float, got {other:?}"),
        }
        assert_eq!(
            text_to_pg_value_by_kind("abc", ScalarKind::Float),
            Value::Missing
        );
    }

    #[test]
    fn kind_decimal_high_precision_preserved() {
        let s = "12345678901234567890.12345678901234567890";
        assert_eq!(
            text_to_pg_value_by_kind(s, ScalarKind::Decimal),
            Value::Decimal(BigDecimal::from_str(s).unwrap())
        );
        // f64 would lose these digits; BigDecimal must not.
        assert_ne!(
            text_to_pg_value_by_kind(s, ScalarKind::Decimal),
            Value::Decimal(BigDecimal::from_str("12345678901234567890").unwrap())
        );
        assert_eq!(
            text_to_pg_value_by_kind("nope", ScalarKind::Decimal),
            Value::Missing
        );
    }

    #[test]
    fn kind_bytes_decodes_pg_hex_format() {
        // pgoutput text format for bytea is `\x` + hex. The OID decoder
        // never handled this (it fell back to String); the catalog-driven
        // path decodes it to real bytes.
        assert_eq!(
            text_to_pg_value_by_kind(r"\xdeadbeef", ScalarKind::Bytes),
            Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef])
        );
        assert_eq!(
            text_to_pg_value_by_kind(r"\x", ScalarKind::Bytes),
            Value::Bytes(vec![])
        );
        // Missing prefix / bad hex / odd nibble count all fail closed.
        assert_eq!(
            text_to_pg_value_by_kind("deadbeef", ScalarKind::Bytes),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind(r"\xzz", ScalarKind::Bytes),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind(r"\xabc", ScalarKind::Bytes),
            Value::Missing
        );
    }

    #[test]
    fn kind_uuid() {
        let u = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            text_to_pg_value_by_kind(u, ScalarKind::Uuid),
            Value::Uuid(Uuid::parse_str(u).unwrap())
        );
        assert_eq!(
            text_to_pg_value_by_kind("not-a-uuid", ScalarKind::Uuid),
            Value::Missing
        );
    }

    #[test]
    fn kind_string_is_verbatim_including_cursed_text() {
        // A string column keeps the exact bytes, even text that looks like
        // another type, is empty, unicode, or holds backslashes / braces.
        for s in [
            "",
            "t",
            "42",
            r"\xdeadbeef",
            "hello world",
            "a\nb",
            "{\"k\":1}",
        ] {
            assert_eq!(
                text_to_pg_value_by_kind(s, ScalarKind::String),
                Value::String(s.to_string())
            );
        }
    }

    #[test]
    fn kind_json_vs_jsonb_same_text_distinct_variants() {
        let j = text_to_pg_value_by_kind(r#"{"k":1}"#, ScalarKind::Json);
        let jb = text_to_pg_value_by_kind(r#"{"k":1}"#, ScalarKind::Jsonb);
        assert_eq!(j, Value::Json(serde_json::json!({"k": 1})));
        assert_eq!(jb, Value::Jsonb(serde_json::json!({"k": 1})));
        // Identical payload, different SQL type: different Value variant.
        assert_ne!(j, jb);
        assert_eq!(
            text_to_pg_value_by_kind("5", ScalarKind::Json),
            Value::Json(serde_json::json!(5))
        );
        assert_eq!(
            text_to_pg_value_by_kind("[1,2]", ScalarKind::Jsonb),
            Value::Jsonb(serde_json::json!([1, 2]))
        );
        assert_eq!(
            text_to_pg_value_by_kind("{bad", ScalarKind::Json),
            Value::Missing
        );
    }

    #[test]
    fn kind_temporal_including_offset_normalization() {
        assert_eq!(
            text_to_pg_value_by_kind("2021-02-03", ScalarKind::Date),
            Value::Date(NaiveDate::from_ymd_opt(2021, 2, 3).unwrap())
        );
        assert_eq!(
            text_to_pg_value_by_kind("2021-02-03 04:05:06", ScalarKind::Timestamp),
            Value::Timestamp(
                NaiveDate::from_ymd_opt(2021, 2, 3)
                    .unwrap()
                    .and_hms_opt(4, 5, 6)
                    .unwrap()
            )
        );
        match text_to_pg_value_by_kind("04:05:06", ScalarKind::Time) {
            Value::Time(_) => {}
            other => panic!("expected time, got {other:?}"),
        }
        // pg timestamptz text carries an offset; result normalizes to UTC.
        let expected: DateTime<Utc> = "2021-02-03T02:05:06Z".parse().unwrap();
        assert_eq!(
            text_to_pg_value_by_kind("2021-02-03 04:05:06+02", ScalarKind::TimestampTz),
            Value::TimestampTz(expected)
        );
        assert_eq!(
            text_to_pg_value_by_kind("nope", ScalarKind::Date),
            Value::Missing
        );
    }

    // ------------------------------------------------------------------
    // Sub-block F: json_value_to_{pg,mysql}_value_by_kind. Catalog-driven
    // decode for the JSON wire parsers (wal2json = Postgres, Maxwell =
    // MySQL). Decode is infallible: a shape the kind cannot accept
    // collapses to Value::Missing, matching the value_at re-exec contract.
    // ------------------------------------------------------------------

    #[test]
    fn json_kind_null_is_sql_null_not_missing() {
        // A wire-carried JSON null is SQL NULL under any catalog kind.
        for kind in [
            ScalarKind::Int,
            ScalarKind::String,
            ScalarKind::Uuid,
            ScalarKind::Json,
        ] {
            assert_eq!(
                json_value_to_pg_value_by_kind(&serde_json::json!(null), kind),
                Value::<Postgres>::Null
            );
            assert_eq!(
                json_value_to_mysql_value_by_kind(&serde_json::json!(null), kind),
                Value::<MySql>::Null
            );
        }
    }

    #[test]
    fn json_kind_int_accepts_number_and_string() {
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(250), ScalarKind::Int),
            Value::Int(250)
        );
        // Postgres numeric-as-string still lands as Int.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!("250"), ScalarKind::Int),
            Value::Int(250)
        );
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(-7), ScalarKind::Int),
            Value::Int(-7)
        );
    }

    #[test]
    fn json_kind_int_rejects_fractional_and_overflow() {
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(1.5), ScalarKind::Int),
            Value::Missing
        );
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(u64::MAX), ScalarKind::Int),
            Value::Missing
        );
        // A whole-valued JSON float is accepted under an Int kind.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(42.0), ScalarKind::Int),
            Value::Int(42)
        );
    }

    #[test]
    fn json_kind_float_accepts_number_and_string() {
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(1.25), ScalarKind::Float),
            Value::Float(1.25)
        );
        // Integer-valued JSON number under a Float column widens to f64.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(250), ScalarKind::Float),
            Value::Float(250.0)
        );
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!("3.5"), ScalarKind::Float),
            Value::Float(3.5)
        );
    }

    #[test]
    fn json_kind_decimal_preserves_precision() {
        let s = "12345678901234567890.0987654321";
        let Value::Decimal(d) =
            json_value_to_pg_value_by_kind(&serde_json::json!(s), ScalarKind::Decimal)
        else {
            panic!("decimal kind must decode a numeric string");
        };
        assert_eq!(d, BigDecimal::from_str(s).unwrap());
        // A bare JSON number also decodes through its lexical form.
        assert!(matches!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(1.5), ScalarKind::Decimal),
            Value::Decimal(_)
        ));
    }

    #[test]
    fn json_kind_bool_accepts_bool_number_and_string() {
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(true), ScalarKind::Bool),
            Value::Bool(true)
        );
        // MySQL tinyint(1) arrives as a bare 0 / 1 number.
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(1), ScalarKind::Bool),
            Value::Bool(true)
        );
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(0), ScalarKind::Bool),
            Value::Bool(false)
        );
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!("t"), ScalarKind::Bool),
            Value::Bool(true)
        );
        // 2 is neither true nor false.
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(2), ScalarKind::Bool),
            Value::Missing
        );
    }

    #[test]
    fn json_kind_string_stringifies_nonstring() {
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!("paid"), ScalarKind::String),
            Value::String("paid".to_string())
        );
        // A number under a String kind stringifies rather than dropping.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(42), ScalarKind::String),
            Value::String("42".to_string())
        );
    }

    #[test]
    fn json_kind_bytea_decodes_pg_hex_only() {
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(r"\x0102ff"), ScalarKind::Bytes),
            Value::Bytes(alloc::vec![0x01, 0x02, 0xff])
        );
        // Missing prefix or non-hex -> Missing (no MySQL base64 support yet).
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!("0102ff"), ScalarKind::Bytes),
            Value::Missing
        );
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(r"\x00"), ScalarKind::Bytes),
            Value::Bytes(alloc::vec![0])
        );
    }

    #[test]
    fn json_kind_uuid_diverges_by_backend() {
        let u = "550e8400-e29b-41d4-a716-446655440000";
        let Value::Uuid(parsed) =
            json_value_to_pg_value_by_kind(&serde_json::json!(u), ScalarKind::Uuid)
        else {
            panic!("pg uuid kind must parse a native uuid");
        };
        assert_eq!(parsed.to_string(), u);
        // MySQL keeps the textual form verbatim.
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(u), ScalarKind::Uuid),
            Value::Uuid(u.to_string())
        );
        // Malformed uuid: Postgres -> Missing; MySQL takes the raw text.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!("not-a-uuid"), ScalarKind::Uuid),
            Value::Missing
        );
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!("not-a-uuid"), ScalarKind::Uuid),
            Value::Uuid("not-a-uuid".to_string())
        );
    }

    #[test]
    fn json_kind_temporal_parses_pg_and_offset_forms() {
        assert!(matches!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("2024-01-15 10:30:00"),
                ScalarKind::Timestamp
            ),
            Value::Timestamp(_)
        ));
        // timestamptz with a bare +02 pg offset.
        assert!(matches!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("2024-01-15 10:30:00+02"),
                ScalarKind::TimestampTz
            ),
            Value::TimestampTz(_)
        ));
        assert!(matches!(
            json_value_to_pg_value_by_kind(&serde_json::json!("2024-01-15"), ScalarKind::Date),
            Value::Date(_)
        ));
        assert!(matches!(
            json_value_to_pg_value_by_kind(&serde_json::json!("10:30:00"), ScalarKind::Time),
            Value::Time(_)
        ));
        // Garbage temporal -> Missing.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!("nope"), ScalarKind::Date),
            Value::Missing
        );
    }

    #[test]
    fn json_kind_json_vs_jsonb_and_nested_vs_string() {
        let nested = serde_json::json!({"a": 1});
        assert_eq!(
            json_value_to_pg_value_by_kind(&nested, ScalarKind::Json),
            Value::Json(serde_json::json!({"a": 1}))
        );
        assert_eq!(
            json_value_to_pg_value_by_kind(&nested, ScalarKind::Jsonb),
            Value::Jsonb(serde_json::json!({"a": 1}))
        );
        // A stringified JSON payload is parsed.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!("{\"b\":2}"), ScalarKind::Json),
            Value::Json(serde_json::json!({"b": 2}))
        );
        // Invalid JSON string -> Missing.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!("{not json"), ScalarKind::Jsonb),
            Value::Missing
        );
    }

    #[test]
    fn json_kind_shape_mismatch_collapses_to_missing() {
        // Int kind but a JSON bool -> Missing (escalates to re-exec).
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(true), ScalarKind::Int),
            Value::Missing
        );
        // Float kind but a JSON object -> Missing.
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!({"x": 1}), ScalarKind::Float),
            Value::Missing
        );
        // Timestamp kind but a JSON number (not a string) -> Missing.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(123), ScalarKind::Timestamp),
            Value::Missing
        );
    }
}

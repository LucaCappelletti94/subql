#![allow(clippy::match_same_arms)]
//! PostgreSQL wire encoding to typed [`Value<Postgres>`] and
//! [`Value<MySql>`] conversion.
//!
//! Shared by all JSON-based WAL parsers (wal2json, Maxwell)
//! and the pgoutput binary wire path. Every consumer is Phase-7 typed.
//! The legacy untyped-scalar decoders were retired in Phase 7F.

use alloc::string::ToString;

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use uuid::Uuid;

use crate::backend::{at_float4, BuiltinKind, BuiltinType, FloatWidth, MySql, Postgres, Value};

/// Parse float text at the width the column declares.
///
/// The wire text for a float4 column is the shortest round-trip text of the
/// float4 value, so reading it as `f64` yields a different number: `0.1` on
/// a `real` column means `0.10000000149011612`, which is what the server
/// compares. Parsing at float4 and widening reproduces the server's value
/// exactly, because every float4 is an f64.
fn parse_float_at_width(text: &str, width: FloatWidth) -> Value<Postgres> {
    match width {
        FloatWidth::Single => text
            .parse::<f32>()
            .map_or(Value::Missing, |single| Value::Float(f64::from(single))),
        FloatWidth::Double => sql_scalar_text::parse_f64(text).map_or(Value::Missing, Value::Float),
    }
}

/// pgoutput carries only positional text, so the type comes from the
/// catalog at decode time rather than the wire OID. Parse failures and
/// shapes the kind cannot accept collapse to [`Value::Missing`], matching
/// the `value_at` contract (a corrupt cell escalates to re-execution).
pub(super) fn text_to_pg_value_by_kind(text: &str, kind: BuiltinType) -> Value<Postgres> {
    if let BuiltinType::Float(width) = kind {
        return parse_float_at_width(text, width);
    }
    match kind.family() {
        BuiltinKind::Bool => {
            if matches!(text, "t" | "f") {
                sql_scalar_text::parse_bool(text).map_or(Value::Missing, Value::Bool)
            } else {
                Value::Missing
            }
        }
        BuiltinKind::Int => sql_scalar_text::parse_i64(text).map_or(Value::Missing, Value::Int),
        BuiltinKind::Decimal => {
            sql_scalar_text::parse_decimal(text).map_or(Value::Missing, Value::Decimal)
        }
        BuiltinKind::String => Value::String(text.to_string()),
        BuiltinKind::Bytes => {
            sql_scalar_text::parse_pg_bytea_hex(text).map_or(Value::Missing, Value::Bytes)
        }
        BuiltinKind::Uuid => Uuid::parse_str(text).map_or(Value::Missing, Value::Uuid),
        BuiltinKind::Timestamp => {
            sql_scalar_text::parse_timestamp(text).map_or(Value::Missing, Value::Timestamp)
        }
        BuiltinKind::TimestampTz => {
            sql_scalar_text::parse_timestamp_tz(text).map_or(Value::Missing, Value::TimestampTz)
        }
        BuiltinKind::Date => sql_scalar_text::parse_date(text).map_or(Value::Missing, Value::Date),
        BuiltinKind::Time => sql_scalar_text::parse_time(text).map_or(Value::Missing, Value::Time),
        BuiltinKind::Json => serde_json::from_str(text).map_or(Value::Missing, Value::Json),
        BuiltinKind::Jsonb => serde_json::from_str(text).map_or(Value::Missing, Value::Jsonb),
        // Answered above, where the width decides.
        BuiltinKind::Float => Value::Missing,
    }
}

fn decode_hex_bytes(hex: &str) -> Option<alloc::vec::Vec<u8>> {
    if !hex.is_ascii() || !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).ok())
        .collect()
}

/// Parse a JSON float at the width the column declares, for the same reason
/// [`parse_float_at_width`] does: wal2json prints the float4 value's own
/// shortest text.
fn json_float_at_width(value: &serde_json::Value, width: FloatWidth) -> Value<Postgres> {
    match width {
        FloatWidth::Single => {
            json_f64(value).map_or(Value::Missing, |double| Value::Float(at_float4(double)))
        }
        FloatWidth::Double => json_f64(value).map_or(Value::Missing, Value::Float),
    }
}

/// Decode a JSON wire value into a typed [`Value<Postgres>`] routed by the
/// column's catalog [`BuiltinKind`] (the schema-driven path for wal2json).
///
/// A wire-carried JSON null becomes [`Value::Null`]. Any shape the kind
/// cannot accept collapses to [`Value::Missing`], matching the `value_at`
/// contract (a corrupt cell escalates to re-execution).
pub(super) fn json_value_to_pg_value_by_kind(
    value: &serde_json::Value,
    kind: BuiltinType,
) -> Value<Postgres> {
    if value.is_null() {
        return Value::Null;
    }
    if let BuiltinType::Float(width) = kind {
        return json_float_at_width(value, width);
    }
    match kind.family() {
        BuiltinKind::Bool => json_bool(value).map_or(Value::Missing, Value::Bool),
        BuiltinKind::Int => json_i64(value).map_or(Value::Missing, Value::Int),
        BuiltinKind::Decimal => json_bigdecimal(value).map_or(Value::Missing, Value::Decimal),
        BuiltinKind::String => Value::String(json_string(value)),
        BuiltinKind::Bytes => json_pg_bytea(value).map_or(Value::Missing, Value::Bytes),
        BuiltinKind::Uuid => value
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .map_or(Value::Missing, Value::Uuid),
        BuiltinKind::Timestamp => json_timestamp(value).map_or(Value::Missing, Value::Timestamp),
        BuiltinKind::TimestampTz => {
            json_timestamptz(value).map_or(Value::Missing, Value::TimestampTz)
        }
        BuiltinKind::Date => json_date(value).map_or(Value::Missing, Value::Date),
        BuiltinKind::Time => json_time(value).map_or(Value::Missing, Value::Time),
        BuiltinKind::Json => json_document(value).map_or(Value::Missing, Value::Json),
        BuiltinKind::Jsonb => json_document(value).map_or(Value::Missing, Value::Jsonb),
        // Answered above, where the width decides.
        BuiltinKind::Float => Value::Missing,
    }
}

/// Decode a JSON wire value into a typed [`Value<MySql>`] routed by the
/// column's catalog [`BuiltinKind`] (the schema-driven path for Maxwell).
///
/// Mirrors [`json_value_to_pg_value_by_kind`] except on two kinds:
/// [`BuiltinKind::Uuid`], which MySQL stores as text so the wire string is
/// taken verbatim rather than parsed into a [`uuid::Uuid`], and
/// [`BuiltinKind::Bytes`], which accepts only the `\x`-prefixed hex form while
/// the Postgres path also accepts bare hex.
pub(super) fn json_value_to_mysql_value_by_kind(
    value: &serde_json::Value,
    kind: BuiltinType,
) -> Value<MySql> {
    if value.is_null() {
        return Value::Null;
    }
    if kind == BuiltinType::Float(FloatWidth::Single) {
        // MySQL's `FLOAT` is float4, and Maxwell prints the float4 value's
        // own shortest text, exactly as PostgreSQL's wire does.
        return json_f64(value).map_or(Value::Missing, |double| Value::Float(at_float4(double)));
    }
    match kind.family() {
        BuiltinKind::Bool => json_bool(value).map_or(Value::Missing, Value::Bool),
        BuiltinKind::Int => json_i64(value).map_or(Value::Missing, Value::Int),
        BuiltinKind::Float => json_f64(value).map_or(Value::Missing, Value::Float),
        BuiltinKind::Decimal => json_bigdecimal(value).map_or(Value::Missing, Value::Decimal),
        BuiltinKind::String => Value::String(json_string(value)),
        BuiltinKind::Bytes => json_bytea(value).map_or(Value::Missing, Value::Bytes),
        BuiltinKind::Uuid => value
            .as_str()
            .map_or(Value::Missing, |s| Value::Uuid(s.to_string())),
        BuiltinKind::Timestamp => json_timestamp(value).map_or(Value::Missing, Value::Timestamp),
        BuiltinKind::TimestampTz => {
            json_timestamptz(value).map_or(Value::Missing, Value::TimestampTz)
        }
        BuiltinKind::Date => json_date(value).map_or(Value::Missing, Value::Date),
        BuiltinKind::Time => json_time(value).map_or(Value::Missing, Value::Time),
        BuiltinKind::Json => json_document(value).map_or(Value::Missing, Value::Json),
        BuiltinKind::Jsonb => json_document(value).map_or(Value::Missing, Value::Jsonb),
    }
}

// Backend-agnostic pure JSON scalar parsers shared by the two by-kind
// decoders above. The `Value<B>` wrapping, the Uuid arm and the Bytes arm are
// what differ between backends, so the parsing itself lives here once.

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
        serde_json::Value::String(s) => sql_scalar_text::parse_i64(s),
        _ => None,
    }
}

fn json_f64(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => sql_scalar_text::parse_f64(s),
        _ => None,
    }
}

fn json_bigdecimal(value: &serde_json::Value) -> Option<BigDecimal> {
    match value {
        serde_json::Value::String(s) => sql_scalar_text::parse_decimal(s),
        serde_json::Value::Number(n) => sql_scalar_text::parse_decimal(&n.to_string()),
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
        serde_json::Value::String(s) => {
            sql_scalar_text::parse_bool(s.as_str()).or(match s.as_str() {
                "true" | "TRUE" | "True" => Some(true),
                "false" | "FALSE" | "False" => Some(false),
                _ => None,
            })
        }
        _ => None,
    }
}

fn json_string(value: &serde_json::Value) -> alloc::string::String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn json_pg_bytea(value: &serde_json::Value) -> Option<alloc::vec::Vec<u8>> {
    value.as_str().and_then(|text| {
        sql_scalar_text::parse_pg_bytea_hex(text).or_else(|| decode_hex_bytes(text))
    })
}

fn json_bytea(value: &serde_json::Value) -> Option<alloc::vec::Vec<u8>> {
    value.as_str().and_then(sql_scalar_text::parse_pg_bytea_hex)
}

fn json_timestamp(value: &serde_json::Value) -> Option<NaiveDateTime> {
    value.as_str().and_then(sql_scalar_text::parse_timestamp)
}

fn json_timestamptz(value: &serde_json::Value) -> Option<DateTime<Utc>> {
    value.as_str().and_then(sql_scalar_text::parse_timestamp_tz)
}

fn json_date(value: &serde_json::Value) -> Option<NaiveDate> {
    value.as_str().and_then(sql_scalar_text::parse_date)
}

fn json_time(value: &serde_json::Value) -> Option<NaiveTime> {
    value.as_str().and_then(sql_scalar_text::parse_time)
}

fn json_document(value: &serde_json::Value) -> Option<serde_json::Value> {
    match value {
        serde_json::Value::String(s) => serde_json::from_str(s).ok(),
        other => Some(other.clone()),
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::approx_constant,
    clippy::unreadable_literal
)]
mod tests {
    /// A family widened to a declared type for these fixtures, which assert
    /// the kind-routed decode rather than the width. The width has its own
    /// tests.
    fn declared(family: BuiltinKind) -> BuiltinType {
        crate::backend::refined_builtin(
            family,
            crate::backend::FloatWidth::Double,
            crate::backend::TextWidth::Varying,
        )
    }

    /// The width decides what float text means, on both wire paths.
    ///
    /// pgoutput carries positional text and wal2json carries a JSON number,
    /// and in each case a `real` column's text is the shortest round-trip
    /// text of the float4 value: `0.1` there is `0.10000000149011612`, which
    /// is the number the server compares.
    #[test]
    fn float_text_decodes_at_the_declared_width() {
        use crate::backend::{BuiltinType, FloatWidth};

        let single = BuiltinType::Float(FloatWidth::Single);
        let double = BuiltinType::Float(FloatWidth::Double);

        assert_eq!(
            text_to_pg_value_by_kind("0.1", single),
            Value::Float(f64::from(0.1_f32)),
            "a real column's text names a float4 value"
        );
        assert_eq!(
            text_to_pg_value_by_kind("0.1", double),
            Value::Float(0.1),
            "and a double precision column's text names an f64"
        );
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(0.1), single),
            Value::Float(f64::from(0.1_f32)),
            "the same on the wal2json path"
        );
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(0.1), double),
            Value::Float(0.1)
        );
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(0.1), single),
            Value::Float(f64::from(0.1_f32)),
            "MySQL's FLOAT is float4 too, and Maxwell prints it the same way"
        );
        assert_eq!(
            text_to_pg_value_by_kind("not a number", single),
            Value::Missing,
            "an unparseable cell still collapses to missing"
        );
    }
    use super::*;
    use core::str::FromStr;

    // Sub-block E: text_to_pg_value_by_kind (pgoutput catalog-driven decode)

    #[test]
    fn kind_bool_uses_pg_text_forms_only() {
        assert_eq!(
            text_to_pg_value_by_kind("t", declared(BuiltinKind::Bool)),
            Value::Bool(true)
        );
        assert_eq!(
            text_to_pg_value_by_kind("f", declared(BuiltinKind::Bool)),
            Value::Bool(false)
        );
        // pgoutput never emits these spellings; they must not decode.
        assert_eq!(
            text_to_pg_value_by_kind("true", declared(BuiltinKind::Bool)),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind("1", declared(BuiltinKind::Bool)),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind("", declared(BuiltinKind::Bool)),
            Value::Missing
        );
    }

    #[test]
    fn kind_int_extremes_and_garbage() {
        assert_eq!(
            text_to_pg_value_by_kind("0", declared(BuiltinKind::Int)),
            Value::Int(0)
        );
        assert_eq!(
            text_to_pg_value_by_kind("-9223372036854775808", declared(BuiltinKind::Int)),
            Value::Int(i64::MIN)
        );
        assert_eq!(
            text_to_pg_value_by_kind("9223372036854775807", declared(BuiltinKind::Int)),
            Value::Int(i64::MAX)
        );
        // i64::MAX + 1 overflows: Missing, never a wrapped or clamped value.
        assert_eq!(
            text_to_pg_value_by_kind("9223372036854775808", declared(BuiltinKind::Int)),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind("3.5", declared(BuiltinKind::Int)),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind("0x10", declared(BuiltinKind::Int)),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind("", declared(BuiltinKind::Int)),
            Value::Missing
        );
    }

    #[test]
    fn kind_float_specials() {
        assert_eq!(
            text_to_pg_value_by_kind("1.5", declared(BuiltinKind::Float)),
            Value::Float(1.5)
        );
        assert_eq!(
            text_to_pg_value_by_kind("1e10", declared(BuiltinKind::Float)),
            Value::Float(1e10)
        );
        // pg emits Infinity / -Infinity / NaN for float specials.
        assert_eq!(
            text_to_pg_value_by_kind("Infinity", declared(BuiltinKind::Float)),
            Value::Float(f64::INFINITY)
        );
        assert_eq!(
            text_to_pg_value_by_kind("-Infinity", declared(BuiltinKind::Float)),
            Value::Float(f64::NEG_INFINITY)
        );
        match text_to_pg_value_by_kind("NaN", declared(BuiltinKind::Float)) {
            Value::Float(f) => assert!(f.is_nan()),
            other => panic!("expected NaN float, got {other:?}"),
        }
        assert_eq!(
            text_to_pg_value_by_kind("abc", declared(BuiltinKind::Float)),
            Value::Missing
        );
    }

    #[test]
    fn kind_decimal_high_precision_preserved() {
        let s = "12345678901234567890.12345678901234567890";
        assert_eq!(
            text_to_pg_value_by_kind(s, declared(BuiltinKind::Decimal)),
            Value::Decimal(BigDecimal::from_str(s).unwrap())
        );
        // f64 would lose these digits; BigDecimal must not.
        assert_ne!(
            text_to_pg_value_by_kind(s, declared(BuiltinKind::Decimal)),
            Value::Decimal(BigDecimal::from_str("12345678901234567890").unwrap())
        );
        assert_eq!(
            text_to_pg_value_by_kind("nope", declared(BuiltinKind::Decimal)),
            Value::Missing
        );
    }

    #[test]
    fn kind_bytes_decodes_pg_hex_format() {
        // pgoutput text format for bytea is `\x` + hex. The OID decoder
        // never handled this (it fell back to String); the catalog-driven
        // path decodes it to real bytes.
        assert_eq!(
            text_to_pg_value_by_kind(r"\xdeadbeef", declared(BuiltinKind::Bytes)),
            Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef])
        );
        assert_eq!(
            text_to_pg_value_by_kind(r"\x", declared(BuiltinKind::Bytes)),
            Value::Bytes(vec![])
        );
        // Missing prefix / bad hex / odd nibble count all fail closed.
        assert_eq!(
            text_to_pg_value_by_kind("deadbeef", declared(BuiltinKind::Bytes)),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind(r"\xzz", declared(BuiltinKind::Bytes)),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind(r"\xabc", declared(BuiltinKind::Bytes)),
            Value::Missing
        );
    }

    #[test]
    fn kind_uuid() {
        let u = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            text_to_pg_value_by_kind(u, declared(BuiltinKind::Uuid)),
            Value::Uuid(Uuid::parse_str(u).unwrap())
        );
        assert_eq!(
            text_to_pg_value_by_kind("not-a-uuid", declared(BuiltinKind::Uuid)),
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
                text_to_pg_value_by_kind(s, declared(BuiltinKind::String)),
                Value::String(s.to_string())
            );
        }
    }

    #[test]
    fn kind_json_vs_jsonb_same_text_distinct_variants() {
        let j = text_to_pg_value_by_kind(r#"{"k":1}"#, declared(BuiltinKind::Json));
        let jb = text_to_pg_value_by_kind(r#"{"k":1}"#, declared(BuiltinKind::Jsonb));
        assert_eq!(j, Value::Json(serde_json::json!({"k": 1})));
        assert_eq!(jb, Value::Jsonb(serde_json::json!({"k": 1})));
        // Identical payload, different SQL type: different Value variant.
        assert_ne!(j, jb);
        assert_eq!(
            text_to_pg_value_by_kind("5", declared(BuiltinKind::Json)),
            Value::Json(serde_json::json!(5))
        );
        assert_eq!(
            text_to_pg_value_by_kind("[1,2]", declared(BuiltinKind::Jsonb)),
            Value::Jsonb(serde_json::json!([1, 2]))
        );
        assert_eq!(
            text_to_pg_value_by_kind("{bad", declared(BuiltinKind::Json)),
            Value::Missing
        );
    }

    #[test]
    fn kind_temporal_including_offset_normalization() {
        assert_eq!(
            text_to_pg_value_by_kind("2021-02-03", declared(BuiltinKind::Date)),
            Value::Date(NaiveDate::from_ymd_opt(2021, 2, 3).unwrap())
        );
        assert_eq!(
            text_to_pg_value_by_kind("2021-02-03 04:05:06", declared(BuiltinKind::Timestamp)),
            Value::Timestamp(
                NaiveDate::from_ymd_opt(2021, 2, 3)
                    .unwrap()
                    .and_hms_opt(4, 5, 6)
                    .unwrap()
            )
        );
        match text_to_pg_value_by_kind("04:05:06", declared(BuiltinKind::Time)) {
            Value::Time(_) => {}
            other => panic!("expected time, got {other:?}"),
        }
        // pg timestamptz text carries an offset; result normalizes to UTC.
        let expected: DateTime<Utc> = "2021-02-03T02:05:06Z".parse().unwrap();
        assert_eq!(
            text_to_pg_value_by_kind("2021-02-03 04:05:06+02", declared(BuiltinKind::TimestampTz)),
            Value::TimestampTz(expected)
        );
        assert_eq!(
            text_to_pg_value_by_kind("nope", declared(BuiltinKind::Date)),
            Value::Missing
        );
    }

    // Sub-block F: json_value_to_{pg,mysql}_value_by_kind. Catalog-driven
    // decode for the JSON wire parsers (wal2json = Postgres, Maxwell =
    // MySQL). Decode is infallible: a shape the kind cannot accept
    // collapses to Value::Missing, matching the value_at re-exec contract.

    #[test]
    fn json_kind_null_is_sql_null_not_missing() {
        // A wire-carried JSON null is SQL NULL under any catalog kind.
        for kind in [
            BuiltinKind::Int,
            BuiltinKind::String,
            BuiltinKind::Uuid,
            BuiltinKind::Json,
        ] {
            assert_eq!(
                json_value_to_pg_value_by_kind(&serde_json::json!(null), declared(kind)),
                Value::<Postgres>::Null
            );
            assert_eq!(
                json_value_to_mysql_value_by_kind(&serde_json::json!(null), declared(kind)),
                Value::<MySql>::Null
            );
        }
    }

    #[test]
    fn json_kind_int_accepts_number_and_string() {
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(250), declared(BuiltinKind::Int)),
            Value::Int(250)
        );
        // Postgres numeric-as-string still lands as Int.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!("250"), declared(BuiltinKind::Int)),
            Value::Int(250)
        );
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(-7), declared(BuiltinKind::Int)),
            Value::Int(-7)
        );
    }

    #[test]
    fn json_kind_int_rejects_fractional_and_overflow() {
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(1.5), declared(BuiltinKind::Int)),
            Value::Missing
        );
        assert_eq!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!(u64::MAX),
                declared(BuiltinKind::Int)
            ),
            Value::Missing
        );
        // A whole-valued JSON float is accepted under an Int kind.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(42.0), declared(BuiltinKind::Int)),
            Value::Int(42)
        );
    }

    #[test]
    fn json_kind_float_accepts_number_and_string() {
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(1.25), declared(BuiltinKind::Float)),
            Value::Float(1.25)
        );
        // Integer-valued JSON number under a Float column widens to f64.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(250), declared(BuiltinKind::Float)),
            Value::Float(250.0)
        );
        assert_eq!(
            json_value_to_mysql_value_by_kind(
                &serde_json::json!("3.5"),
                declared(BuiltinKind::Float)
            ),
            Value::Float(3.5)
        );
    }

    #[test]
    fn json_kind_decimal_preserves_precision() {
        let s = "12345678901234567890.0987654321";
        let Value::Decimal(d) =
            json_value_to_pg_value_by_kind(&serde_json::json!(s), declared(BuiltinKind::Decimal))
        else {
            panic!("decimal kind must decode a numeric string");
        };
        assert_eq!(d, BigDecimal::from_str(s).unwrap());
        // A bare JSON number also decodes through its lexical form.
        assert!(matches!(
            json_value_to_mysql_value_by_kind(
                &serde_json::json!(1.5),
                declared(BuiltinKind::Decimal)
            ),
            Value::Decimal(_)
        ));
    }

    #[test]
    fn json_kind_bool_accepts_bool_number_and_string() {
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(true), declared(BuiltinKind::Bool)),
            Value::Bool(true)
        );
        // MySQL tinyint(1) arrives as a bare 0 / 1 number.
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(1), declared(BuiltinKind::Bool)),
            Value::Bool(true)
        );
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(0), declared(BuiltinKind::Bool)),
            Value::Bool(false)
        );
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!("t"), declared(BuiltinKind::Bool)),
            Value::Bool(true)
        );
        // 2 is neither true nor false.
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(2), declared(BuiltinKind::Bool)),
            Value::Missing
        );
    }

    #[test]
    fn json_kind_string_stringifies_nonstring() {
        assert_eq!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("paid"),
                declared(BuiltinKind::String)
            ),
            Value::String("paid".to_string())
        );
        // A number under a String kind stringifies rather than dropping.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(42), declared(BuiltinKind::String)),
            Value::String("42".to_string())
        );
    }

    #[test]
    fn json_kind_bytea_decodes_pg_hex_forms() {
        assert_eq!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!(r"\x0102ff"),
                declared(BuiltinKind::Bytes)
            ),
            Value::Bytes(alloc::vec![0x01, 0x02, 0xff])
        );
        assert_eq!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("0102ff"),
                declared(BuiltinKind::Bytes)
            ),
            Value::Bytes(alloc::vec![0x01, 0x02, 0xff])
        );
        assert_eq!(
            json_value_to_mysql_value_by_kind(
                &serde_json::json!("0102ff"),
                declared(BuiltinKind::Bytes)
            ),
            Value::Missing
        );
        assert_eq!(
            json_value_to_mysql_value_by_kind(
                &serde_json::json!(r"\x00"),
                declared(BuiltinKind::Bytes)
            ),
            Value::Bytes(alloc::vec![0])
        );
    }

    #[test]
    fn json_kind_uuid_diverges_by_backend() {
        let u = "550e8400-e29b-41d4-a716-446655440000";
        let Value::Uuid(parsed) =
            json_value_to_pg_value_by_kind(&serde_json::json!(u), declared(BuiltinKind::Uuid))
        else {
            panic!("pg uuid kind must parse a native uuid");
        };
        assert_eq!(parsed.to_string(), u);
        // MySQL keeps the textual form verbatim.
        assert_eq!(
            json_value_to_mysql_value_by_kind(&serde_json::json!(u), declared(BuiltinKind::Uuid)),
            Value::Uuid(u.to_string())
        );
        // Malformed uuid: Postgres -> Missing; MySQL takes the raw text.
        assert_eq!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("not-a-uuid"),
                declared(BuiltinKind::Uuid)
            ),
            Value::Missing
        );
        assert_eq!(
            json_value_to_mysql_value_by_kind(
                &serde_json::json!("not-a-uuid"),
                declared(BuiltinKind::Uuid)
            ),
            Value::Uuid("not-a-uuid".to_string())
        );
    }

    #[test]
    fn json_kind_temporal_parses_pg_and_offset_forms() {
        assert!(matches!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("2024-01-15 10:30:00"),
                declared(BuiltinKind::Timestamp)
            ),
            Value::Timestamp(_)
        ));
        // timestamptz with a bare +02 pg offset.
        assert!(matches!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("2024-01-15 10:30:00+02"),
                declared(BuiltinKind::TimestampTz)
            ),
            Value::TimestampTz(_)
        ));
        assert!(matches!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("2024-01-15"),
                declared(BuiltinKind::Date)
            ),
            Value::Date(_)
        ));
        assert!(matches!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("10:30:00"),
                declared(BuiltinKind::Time)
            ),
            Value::Time(_)
        ));
        // Garbage temporal -> Missing.
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!("nope"), declared(BuiltinKind::Date)),
            Value::Missing
        );
    }

    #[test]
    fn json_kind_json_vs_jsonb_and_nested_vs_string() {
        let nested = serde_json::json!({"a": 1});
        assert_eq!(
            json_value_to_pg_value_by_kind(&nested, declared(BuiltinKind::Json)),
            Value::Json(serde_json::json!({"a": 1}))
        );
        assert_eq!(
            json_value_to_pg_value_by_kind(&nested, declared(BuiltinKind::Jsonb)),
            Value::Jsonb(serde_json::json!({"a": 1}))
        );
        // A stringified JSON payload is parsed.
        assert_eq!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("{\"b\":2}"),
                declared(BuiltinKind::Json)
            ),
            Value::Json(serde_json::json!({"b": 2}))
        );
        // Invalid JSON string -> Missing.
        assert_eq!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("{not json"),
                declared(BuiltinKind::Jsonb)
            ),
            Value::Missing
        );
    }

    #[test]
    fn json_kind_shape_mismatch_collapses_to_missing() {
        // Int kind but a JSON bool -> Missing (escalates to re-exec).
        assert_eq!(
            json_value_to_pg_value_by_kind(&serde_json::json!(true), declared(BuiltinKind::Int)),
            Value::Missing
        );
        // Float kind but a JSON object -> Missing.
        assert_eq!(
            json_value_to_mysql_value_by_kind(
                &serde_json::json!({"x": 1}),
                declared(BuiltinKind::Float)
            ),
            Value::Missing
        );
        // Timestamp kind but a JSON number (not a string) -> Missing.
        assert_eq!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!(123),
                declared(BuiltinKind::Timestamp)
            ),
            Value::Missing
        );
    }

    #[test]
    fn temporal_wire_cell_maps_representative_values() {
        assert!(matches!(
            text_to_pg_value_by_kind("2026-01-01 00:00:00", declared(BuiltinKind::Timestamp)),
            Value::Timestamp(_)
        ));
        assert!(matches!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("2026-01-01 00:00:00"),
                declared(BuiltinKind::Timestamp)
            ),
            Value::Timestamp(_)
        ));
        let expected: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(
            text_to_pg_value_by_kind("2025-12-31 22:00:00-02", declared(BuiltinKind::TimestampTz)),
            Value::TimestampTz(expected)
        );
        assert_eq!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("2025-12-31 22:00:00-02"),
                declared(BuiltinKind::TimestampTz)
            ),
            Value::TimestampTz(expected)
        );
        assert!(matches!(
            text_to_pg_value_by_kind("2026-01-01", declared(BuiltinKind::Date)),
            Value::Date(_)
        ));
        assert!(matches!(
            text_to_pg_value_by_kind("12:34:56.789", declared(BuiltinKind::Time)),
            Value::Time(_)
        ));
    }

    #[test]
    fn temporal_wire_cell_rejects_key_boundaries() {
        assert_eq!(
            text_to_pg_value_by_kind("2026-01-01 00:00:00", declared(BuiltinKind::TimestampTz)),
            Value::Missing
        );
        assert_eq!(
            json_value_to_pg_value_by_kind(
                &serde_json::json!("2026-01-01 00:00:00"),
                declared(BuiltinKind::TimestampTz)
            ),
            Value::Missing
        );
        assert_eq!(
            text_to_pg_value_by_kind("2026-01-01 00:00:00+00", declared(BuiltinKind::Timestamp)),
            Value::Missing
        );
    }
}

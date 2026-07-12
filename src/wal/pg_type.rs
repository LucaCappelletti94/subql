#![allow(clippy::match_same_arms)]
//! PostgreSQL wire encoding to typed [`Value<Postgres>`] and
//! [`Value<MySql>`] conversion.
//!
//! Shared by all JSON-based WAL parsers (wal2json, Maxwell, Debezium)
//! and the pgoutput binary wire path. Every consumer is Phase-7 typed.
//! The legacy untyped-scalar decoders were retired in Phase 7F.

use alloc::borrow::Cow;
use alloc::string::ToString;
use core::str::FromStr;

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use uuid::Uuid;

use super::WalParseError;
use crate::backend::{MySql, Postgres, Value};

// ============================================================================
// Typed decoders producing `Value<Postgres>` (Phase 7)
// ============================================================================

/// Decode a JSON value shaped by the declared PG type into a typed
/// [`Value<Postgres>`].
///
/// Routes on the lowercased PG type string. Numeric, uuid, temporal,
/// and json/jsonb payloads parse into their typed [`Value`] variants.
/// Genuinely text-shaped types (`text`, `varchar`, `char`, unknown
/// user-defined types, ...) fall back to [`Value::String`].
///
/// Errors on lossy numeric conversions, malformed textual encodings, and
/// PostgreSQL array types (which subql does not yet model).
pub(super) fn json_value_to_pg_value(
    value: &serde_json::Value,
    pg_type: &str,
    field: &str,
) -> Result<Value<Postgres>, WalParseError> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    let ty = pg_type.to_ascii_lowercase();
    if ty.starts_with('_') {
        return Err(WalParseError::MalformedPayload(format!(
            "array columns not yet supported: '{pg_type}'"
        )));
    }
    match ty.as_str() {
        "integer" | "int" | "int2" | "int4" | "int8" | "smallint" | "bigint" | "serial"
        | "bigserial" | "smallserial" | "oid" => pg_int_value(value, field),
        "real" | "float4" | "double precision" | "float8" | "money" => pg_float_value(value, field),
        "numeric" | "decimal" => pg_decimal_value(value, field),
        "boolean" | "bool" => pg_bool_value(value, field),
        "uuid" => pg_uuid_value(value, field),
        "timestamp" | "timestamp without time zone" => pg_timestamp_value(value, field),
        "timestamptz" | "timestamp with time zone" => pg_timestamptz_value(value, field),
        "date" => pg_date_value(value, field),
        "time" | "time without time zone" | "timetz" | "time with time zone" => {
            pg_time_value(value, field)
        }
        "json" => pg_json_value(value, field),
        "jsonb" => pg_jsonb_value(value, field),
        _ => Ok(pg_string_value(value)),
    }
}

/// Infer a typed [`Value<Postgres>`] from a bare JSON value with no
/// column type metadata.
///
/// Used by wire formats that do not carry column type metadata (Maxwell,
/// Debezium). Bare JSON booleans / numbers / strings map to the natural
/// [`Value<Postgres>`] variants. JSON objects and arrays are stringified
/// into [`Value::String`].
// Consumed by MaxwellEvent (Phase 7C) and DebeziumEvent (Phase 7D).
#[allow(dead_code)]
pub(super) fn infer_pg_value_from_json_strict(
    value: &serde_json::Value,
    field: &str,
) -> Result<Value<Postgres>, WalParseError> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        #[allow(clippy::option_if_let_else)]
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(u) = n.as_u64() {
                let i = i64::try_from(u).map_err(|_| WalParseError::NumericOverflow {
                    field: field.to_string(),
                    value: u.to_string(),
                    target: "i64",
                })?;
                Ok(Value::Int(i))
            } else {
                Ok(n.as_f64().map_or(Value::Null, Value::Float))
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(s.clone())),
        other => Ok(Value::String(other.to_string())),
    }
}

/// MySQL variant of [`infer_pg_value_from_json_strict`].
///
/// Same shape-inference logic but produces [`Value<MySql>`]. Used by wire
/// formats without column type metadata (Maxwell).
pub(super) fn infer_mysql_value_from_json_strict(
    value: &serde_json::Value,
    field: &str,
) -> Result<Value<MySql>, WalParseError> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        #[allow(clippy::option_if_let_else)]
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(u) = n.as_u64() {
                let i = i64::try_from(u).map_err(|_| WalParseError::NumericOverflow {
                    field: field.to_string(),
                    value: u.to_string(),
                    target: "i64",
                })?;
                Ok(Value::Int(i))
            } else {
                Ok(n.as_f64().map_or(Value::Null, Value::Float))
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(s.clone())),
        other => Ok(Value::String(other.to_string())),
    }
}

/// Typed variant of [`text_to_cell_strict`] used by the pgoutput wire path.
///
/// Errors on malformed textual encodings for the declared OID.
// Consumed by PgOutputEvent (Phase 7E).
#[allow(dead_code)]
pub(super) fn text_to_pg_value(
    text: &str,
    type_oid: u32,
) -> Result<Value<Postgres>, WalParseError> {
    match type_oid {
        // bool
        16 => match text {
            "t" => Ok(Value::Bool(true)),
            "f" => Ok(Value::Bool(false)),
            _ => Err(WalParseError::MalformedPayload(format!(
                "invalid boolean text value for type oid {type_oid}: {text}"
            ))),
        },
        // int8, int2, int4, oid
        20 | 21 | 23 | 26 => text.parse::<i64>().map(Value::Int).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid integer text value for type oid {type_oid}: {text}"
            ))
        }),
        // float4, float8
        700 | 701 => text.parse::<f64>().map(Value::Float).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid floating text value for type oid {type_oid}: {text}"
            ))
        }),
        // numeric
        1700 => BigDecimal::from_str(text).map(Value::Decimal).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid numeric text value for type oid {type_oid}: {text}"
            ))
        }),
        // uuid
        2950 => Uuid::parse_str(text).map(Value::Uuid).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid uuid text value for type oid {type_oid}: {text}"
            ))
        }),
        // timestamp
        1114 => parse_pg_timestamp(text)
            .map(Value::Timestamp)
            .ok_or_else(|| {
                WalParseError::MalformedPayload(format!(
                    "invalid timestamp text value for type oid {type_oid}: {text}"
                ))
            }),
        // timestamptz
        1184 => parse_pg_timestamptz(text)
            .map(Value::TimestampTz)
            .ok_or_else(|| {
                WalParseError::MalformedPayload(format!(
                    "invalid timestamptz text value for type oid {type_oid}: {text}"
                ))
            }),
        // date
        1082 => NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .map(Value::Date)
            .map_err(|_| {
                WalParseError::MalformedPayload(format!(
                    "invalid date text value for type oid {type_oid}: {text}"
                ))
            }),
        // time, timetz
        1083 | 1266 => parse_pg_time(text).map(Value::Time).ok_or_else(|| {
            WalParseError::MalformedPayload(format!(
                "invalid time text value for type oid {type_oid}: {text}"
            ))
        }),
        // json
        114 => serde_json::from_str(text).map(Value::Json).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid json text value for type oid {type_oid}: {text}"
            ))
        }),
        // jsonb
        3802 => serde_json::from_str(text).map(Value::Jsonb).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid jsonb text value for type oid {type_oid}: {text}"
            ))
        }),
        // text, bpchar, varchar, name
        25 | 1042 | 1043 | 19 => Ok(Value::String(text.to_string())),
        // interval and all other types: text fallback
        _ => Ok(Value::String(text.to_string())),
    }
}

fn pg_int_value(value: &serde_json::Value, field: &str) -> Result<Value<Postgres>, WalParseError> {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return Ok(Value::Int(i));
            }
            if let Some(u) = n.as_u64() {
                let i = i64::try_from(u).map_err(|_| WalParseError::NumericOverflow {
                    field: field.to_string(),
                    value: u.to_string(),
                    target: "i64",
                })?;
                return Ok(Value::Int(i));
            }
            #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
            if let Some(f) = n.as_f64() {
                if f.fract() != 0.0 {
                    return Err(WalParseError::MalformedPayload(format!(
                        "fractional numeric value in integer field '{field}': {n}"
                    )));
                }
                if f > i64::MAX as f64 || f < i64::MIN as f64 {
                    return Err(WalParseError::NumericOverflow {
                        field: field.to_string(),
                        value: n.to_string(),
                        target: "i64",
                    });
                }
                return Ok(Value::Int(f as i64));
            }
            Ok(Value::Null)
        }
        serde_json::Value::String(s) => s.parse::<i64>().map(Value::Int).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid integer value in field '{field}': {s}"
            ))
        }),
        _ => Err(WalParseError::MalformedPayload(format!(
            "invalid integer value in field '{field}': {value}"
        ))),
    }
}

fn pg_float_value(
    value: &serde_json::Value,
    field: &str,
) -> Result<Value<Postgres>, WalParseError> {
    match value {
        serde_json::Value::Number(n) => n.as_f64().map(Value::Float).ok_or_else(|| {
            WalParseError::MalformedPayload(format!(
                "invalid floating value in field '{field}': {n}"
            ))
        }),
        serde_json::Value::String(s) => s.parse::<f64>().map(Value::Float).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid floating value in field '{field}': {s}"
            ))
        }),
        _ => Err(WalParseError::MalformedPayload(format!(
            "invalid floating value in field '{field}': {value}"
        ))),
    }
}

fn pg_decimal_value(
    value: &serde_json::Value,
    field: &str,
) -> Result<Value<Postgres>, WalParseError> {
    let text: Cow<'_, str> = match value {
        serde_json::Value::String(s) => Cow::Borrowed(s.as_str()),
        serde_json::Value::Number(n) => Cow::Owned(n.to_string()),
        _ => {
            return Err(WalParseError::MalformedPayload(format!(
                "invalid decimal value in field '{field}': {value}"
            )));
        }
    };
    BigDecimal::from_str(&text)
        .map(Value::Decimal)
        .map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid decimal value in field '{field}': {text}"
            ))
        })
}

fn pg_bool_value(value: &serde_json::Value, field: &str) -> Result<Value<Postgres>, WalParseError> {
    match value {
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::String(s) => match s.as_str() {
            "t" | "true" | "TRUE" | "True" | "1" => Ok(Value::Bool(true)),
            "f" | "false" | "FALSE" | "False" | "0" => Ok(Value::Bool(false)),
            _ => Err(WalParseError::MalformedPayload(format!(
                "invalid boolean value in field '{field}': {s}"
            ))),
        },
        _ => Err(WalParseError::MalformedPayload(format!(
            "invalid boolean value in field '{field}': {value}"
        ))),
    }
}

fn pg_uuid_value(value: &serde_json::Value, field: &str) -> Result<Value<Postgres>, WalParseError> {
    let serde_json::Value::String(s) = value else {
        return Err(WalParseError::MalformedPayload(format!(
            "invalid uuid value in field '{field}': {value}"
        )));
    };
    Uuid::parse_str(s).map(Value::Uuid).map_err(|_| {
        WalParseError::MalformedPayload(format!("invalid uuid value in field '{field}': {s}"))
    })
}

fn pg_timestamp_value(
    value: &serde_json::Value,
    field: &str,
) -> Result<Value<Postgres>, WalParseError> {
    let serde_json::Value::String(s) = value else {
        return Err(WalParseError::MalformedPayload(format!(
            "invalid timestamp value in field '{field}': {value}"
        )));
    };
    parse_pg_timestamp(s).map(Value::Timestamp).ok_or_else(|| {
        WalParseError::MalformedPayload(format!("invalid timestamp value in field '{field}': {s}"))
    })
}

fn pg_timestamptz_value(
    value: &serde_json::Value,
    field: &str,
) -> Result<Value<Postgres>, WalParseError> {
    let serde_json::Value::String(s) = value else {
        return Err(WalParseError::MalformedPayload(format!(
            "invalid timestamptz value in field '{field}': {value}"
        )));
    };
    parse_pg_timestamptz(s)
        .map(Value::TimestampTz)
        .ok_or_else(|| {
            WalParseError::MalformedPayload(format!(
                "invalid timestamptz value in field '{field}': {s}"
            ))
        })
}

fn pg_date_value(value: &serde_json::Value, field: &str) -> Result<Value<Postgres>, WalParseError> {
    let serde_json::Value::String(s) = value else {
        return Err(WalParseError::MalformedPayload(format!(
            "invalid date value in field '{field}': {value}"
        )));
    };
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map(Value::Date)
        .map_err(|_| {
            WalParseError::MalformedPayload(format!("invalid date value in field '{field}': {s}"))
        })
}

fn pg_time_value(value: &serde_json::Value, field: &str) -> Result<Value<Postgres>, WalParseError> {
    let serde_json::Value::String(s) = value else {
        return Err(WalParseError::MalformedPayload(format!(
            "invalid time value in field '{field}': {value}"
        )));
    };
    parse_pg_time(s).map(Value::Time).ok_or_else(|| {
        WalParseError::MalformedPayload(format!("invalid time value in field '{field}': {s}"))
    })
}

fn pg_json_value(value: &serde_json::Value, field: &str) -> Result<Value<Postgres>, WalParseError> {
    if let serde_json::Value::String(s) = value {
        return serde_json::from_str(s).map(Value::Json).map_err(|_| {
            WalParseError::MalformedPayload(format!("invalid json value in field '{field}': {s}"))
        });
    }
    Ok(Value::Json(value.clone()))
}

fn pg_jsonb_value(
    value: &serde_json::Value,
    field: &str,
) -> Result<Value<Postgres>, WalParseError> {
    if let serde_json::Value::String(s) = value {
        return serde_json::from_str(s).map(Value::Jsonb).map_err(|_| {
            WalParseError::MalformedPayload(format!("invalid jsonb value in field '{field}': {s}"))
        });
    }
    Ok(Value::Jsonb(value.clone()))
}

fn pg_string_value(value: &serde_json::Value) -> Value<Postgres> {
    match value {
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Null => Value::Null,
        other => Value::String(other.to_string()),
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
mod tests {
    use super::*;

    // Sub-block A: json_value_to_pg_value

    #[test]
    fn json_value_int_integer() {
        let result = json_value_to_pg_value(&serde_json::json!(42), "integer", "f");
        assert_eq!(result.unwrap(), Value::<Postgres>::Int(42));
    }

    #[test]
    fn json_value_int_int8_uppercase() {
        let result =
            json_value_to_pg_value(&serde_json::json!(9223372036854775807_i64), "INT8", "f");
        assert_eq!(result.unwrap(), Value::<Postgres>::Int(9223372036854775807));
    }

    #[test]
    fn json_value_float_real() {
        let result = json_value_to_pg_value(&serde_json::json!(3.14), "real", "f");
        assert_eq!(result.unwrap(), Value::<Postgres>::Float(3.14));
    }

    #[test]
    fn json_value_float_money() {
        let result = json_value_to_pg_value(&serde_json::json!(19.99), "money", "f");
        assert_eq!(result.unwrap(), Value::<Postgres>::Float(19.99));
    }

    #[test]
    fn json_value_decimal_numeric() {
        let result = json_value_to_pg_value(&serde_json::json!("123.456"), "numeric", "f");
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Decimal(BigDecimal::from_str("123.456").unwrap())
        );
    }

    #[test]
    fn json_value_bool_true() {
        let result = json_value_to_pg_value(&serde_json::json!(true), "boolean", "f");
        assert_eq!(result.unwrap(), Value::<Postgres>::Bool(true));
    }

    #[test]
    fn json_value_uuid() {
        let result = json_value_to_pg_value(
            &serde_json::json!("550e8400-e29b-41d4-a716-446655440000"),
            "uuid",
            "f",
        );
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Uuid(
                Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
            )
        );
    }

    #[test]
    fn json_value_timestamp() {
        let result =
            json_value_to_pg_value(&serde_json::json!("2024-01-15 12:34:56"), "timestamp", "f");
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Timestamp(
                NaiveDateTime::parse_from_str("2024-01-15 12:34:56", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        );
    }

    #[test]
    fn json_value_timestamptz() {
        let result = json_value_to_pg_value(
            &serde_json::json!("2024-01-15 12:34:56+00:00"),
            "timestamptz",
            "f",
        );
        let expected = DateTime::parse_from_rfc3339("2024-01-15 12:34:56+00:00")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(result.unwrap(), Value::<Postgres>::TimestampTz(expected));
    }

    #[test]
    fn json_value_date() {
        let result = json_value_to_pg_value(&serde_json::json!("2024-01-15"), "date", "f");
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap())
        );
    }

    #[test]
    fn json_value_time() {
        let result = json_value_to_pg_value(&serde_json::json!("12:34:56"), "time", "f");
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Time(NaiveTime::from_hms_opt(12, 34, 56).unwrap())
        );
    }

    #[test]
    fn json_value_json() {
        let result = json_value_to_pg_value(&serde_json::json!({"k": "v"}), "json", "f");
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Json(serde_json::json!({"k": "v"}))
        );
    }

    #[test]
    fn json_value_jsonb() {
        let result = json_value_to_pg_value(&serde_json::json!({"k": "v"}), "jsonb", "f");
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Jsonb(serde_json::json!({"k": "v"}))
        );
    }

    #[test]
    fn json_value_unknown_type_falls_back_to_string() {
        let result = json_value_to_pg_value(&serde_json::json!("hello"), "user_defined_thing", "f");
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::String("hello".to_string())
        );
    }

    #[test]
    fn json_value_array_type_rejected() {
        let err = json_value_to_pg_value(&serde_json::json!(1), "_int4", "f").unwrap_err();
        let msg = format!("{err}");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
        assert!(msg.contains("array"));
    }

    #[test]
    fn json_value_null_input_produces_null() {
        let result = json_value_to_pg_value(&serde_json::json!(null), "integer", "f");
        assert_eq!(result.unwrap(), Value::<Postgres>::Null);
    }

    // Sub-block B: text_to_pg_value

    #[test]
    fn text_bool_true() {
        let result = text_to_pg_value("t", 16);
        assert_eq!(result.unwrap(), Value::<Postgres>::Bool(true));
    }

    #[test]
    fn text_bool_false() {
        let result = text_to_pg_value("f", 16);
        assert_eq!(result.unwrap(), Value::<Postgres>::Bool(false));
    }

    #[test]
    fn text_bool_malformed() {
        let err = text_to_pg_value("yes", 16).unwrap_err();
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn text_int_happy_oid23() {
        let result = text_to_pg_value("42", 23);
        assert_eq!(result.unwrap(), Value::<Postgres>::Int(42));
    }

    #[test]
    fn text_int_happy_oid20_bigint() {
        let result = text_to_pg_value("9223372036854775807", 20);
        assert_eq!(result.unwrap(), Value::<Postgres>::Int(i64::MAX));
    }

    #[test]
    fn text_int_malformed() {
        let err = text_to_pg_value("not_a_number", 23).unwrap_err();
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn text_float_happy_oid700() {
        let result = text_to_pg_value("3.14", 700);
        assert_eq!(result.unwrap(), Value::<Postgres>::Float(3.14));
    }

    #[test]
    fn text_float_malformed() {
        let err = text_to_pg_value("nope", 701).unwrap_err();
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn text_decimal_happy() {
        let result = text_to_pg_value("123.456", 1700);
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Decimal(BigDecimal::from_str("123.456").unwrap())
        );
    }

    #[test]
    fn text_decimal_malformed() {
        let err = text_to_pg_value("bad_decimal", 1700).unwrap_err();
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn text_uuid_happy() {
        let result = text_to_pg_value("550e8400-e29b-41d4-a716-446655440000", 2950);
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Uuid(
                Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
            )
        );
    }

    #[test]
    fn text_uuid_malformed() {
        let err = text_to_pg_value("not-a-uuid", 2950).unwrap_err();
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn text_timestamp_happy() {
        let result = text_to_pg_value("2024-01-15 12:34:56", 1114);
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Timestamp(
                NaiveDateTime::parse_from_str("2024-01-15 12:34:56", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        );
    }

    #[test]
    fn text_timestamp_malformed() {
        let err = text_to_pg_value("garbage", 1114).unwrap_err();
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn text_timestamptz_happy() {
        let result = text_to_pg_value("2024-01-15 12:34:56+00:00", 1184);
        let expected = DateTime::parse_from_rfc3339("2024-01-15 12:34:56+00:00")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(result.unwrap(), Value::<Postgres>::TimestampTz(expected));
    }

    #[test]
    fn text_timestamptz_malformed() {
        let err = text_to_pg_value("garbage", 1184).unwrap_err();
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn text_date_happy() {
        let result = text_to_pg_value("2024-01-15", 1082);
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap())
        );
    }

    #[test]
    fn text_date_malformed() {
        let err = text_to_pg_value("not-a-date", 1082).unwrap_err();
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn text_time_happy_oid1083() {
        let result = text_to_pg_value("12:34:56", 1083);
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Time(NaiveTime::from_hms_opt(12, 34, 56).unwrap())
        );
    }

    #[test]
    fn text_time_happy_oid1266_timetz() {
        let result = text_to_pg_value("12:34:56", 1266);
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Time(NaiveTime::from_hms_opt(12, 34, 56).unwrap())
        );
    }

    #[test]
    fn text_time_malformed() {
        let err = text_to_pg_value("garbage", 1083).unwrap_err();
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn text_json_happy() {
        let result = text_to_pg_value(r#"{"k":"v"}"#, 114);
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Json(serde_json::json!({"k":"v"}))
        );
    }

    #[test]
    fn text_json_malformed() {
        let err = text_to_pg_value("{not_json", 114).unwrap_err();
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn text_jsonb_happy() {
        let result = text_to_pg_value(r#"{"k":"v"}"#, 3802);
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::Jsonb(serde_json::json!({"k":"v"}))
        );
    }

    #[test]
    fn text_jsonb_malformed() {
        let err = text_to_pg_value("{not_json", 3802).unwrap_err();
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn text_text_passthrough_oid25() {
        let result = text_to_pg_value("hello", 25);
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::String("hello".to_string())
        );
    }

    #[test]
    fn text_bpchar_passthrough_oid1042() {
        let result = text_to_pg_value("padded  ", 1042);
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::String("padded  ".to_string())
        );
    }

    #[test]
    fn text_fallback_unknown_oid() {
        let result = text_to_pg_value("192.168.0.1", 869);
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::String("192.168.0.1".to_string())
        );
    }

    // Sub-block C: infer_pg_value_from_json_strict

    #[test]
    fn infer_pg_null() {
        let result = infer_pg_value_from_json_strict(&serde_json::Value::Null, "f");
        assert_eq!(result.unwrap(), Value::<Postgres>::Null);
    }

    #[test]
    fn infer_pg_bool_true() {
        let result = infer_pg_value_from_json_strict(&serde_json::json!(true), "f");
        assert_eq!(result.unwrap(), Value::<Postgres>::Bool(true));
    }

    #[test]
    fn infer_pg_int_positive() {
        let result = infer_pg_value_from_json_strict(&serde_json::json!(42), "f");
        assert_eq!(result.unwrap(), Value::<Postgres>::Int(42));
    }

    #[test]
    fn infer_pg_float() {
        let result = infer_pg_value_from_json_strict(&serde_json::json!(3.14), "f");
        assert_eq!(result.unwrap(), Value::<Postgres>::Float(3.14));
    }

    #[test]
    fn infer_pg_string() {
        let result = infer_pg_value_from_json_strict(&serde_json::json!("hello"), "f");
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::String("hello".to_string())
        );
    }

    #[test]
    fn infer_pg_object_stringified() {
        let result = infer_pg_value_from_json_strict(&serde_json::json!({"k": "v"}), "f");
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::String(r#"{"k":"v"}"#.to_string())
        );
    }

    #[test]
    fn infer_pg_array_stringified() {
        let result = infer_pg_value_from_json_strict(&serde_json::json!([1, 2, 3]), "f");
        assert_eq!(
            result.unwrap(),
            Value::<Postgres>::String("[1,2,3]".to_string())
        );
    }

    // Sub-block D: infer_mysql_value_from_json_strict

    #[test]
    fn infer_mysql_null() {
        let result = infer_mysql_value_from_json_strict(&serde_json::Value::Null, "f");
        assert_eq!(result.unwrap(), Value::<MySql>::Null);
    }

    #[test]
    fn infer_mysql_bool_true() {
        let result = infer_mysql_value_from_json_strict(&serde_json::json!(true), "f");
        assert_eq!(result.unwrap(), Value::<MySql>::Bool(true));
    }

    #[test]
    fn infer_mysql_int_positive() {
        let result = infer_mysql_value_from_json_strict(&serde_json::json!(42), "f");
        assert_eq!(result.unwrap(), Value::<MySql>::Int(42));
    }

    #[test]
    fn infer_mysql_float() {
        let result = infer_mysql_value_from_json_strict(&serde_json::json!(3.14), "f");
        assert_eq!(result.unwrap(), Value::<MySql>::Float(3.14));
    }

    #[test]
    fn infer_mysql_string() {
        let result = infer_mysql_value_from_json_strict(&serde_json::json!("hello"), "f");
        assert_eq!(result.unwrap(), Value::<MySql>::String("hello".to_string()));
    }

    #[test]
    fn infer_mysql_object_stringified() {
        let result = infer_mysql_value_from_json_strict(&serde_json::json!({"k": "v"}), "f");
        assert_eq!(
            result.unwrap(),
            Value::<MySql>::String(r#"{"k":"v"}"#.to_string())
        );
    }

    #[test]
    fn infer_mysql_array_stringified() {
        let result = infer_mysql_value_from_json_strict(&serde_json::json!([1, 2, 3]), "f");
        assert_eq!(
            result.unwrap(),
            Value::<MySql>::String("[1,2,3]".to_string())
        );
    }
}

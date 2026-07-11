#![allow(clippy::match_same_arms)]
//! PostgreSQL wire encoding to typed [`Value<Postgres>`] and
//! [`Value<MySql>`] conversion.
//!
//! Shared by all JSON-based WAL parsers (wal2json, Maxwell, Debezium)
//! and the pgoutput binary wire path. Every consumer is now Phase-7
//! typed; the legacy Cell-based decoders were retired in Phase 7F.

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

/// Typed variant of [`json_value_to_cell_strict`] that produces a typed
/// [`Value<Postgres>`] shaped to the declared PG type.
///
/// Routes on the lowercased PG type string. Where the [`Cell`] version
/// fell back to `Cell::String` for uuid / timestamp / date / time /
/// numeric / json / jsonb, this variant parses those payloads into their
/// typed [`Value`] variants; genuinely text-shaped types (`text`,
/// `varchar`, `char`, unknown user-defined types, ...) still fall back to
/// [`Value::String`].
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
        "real" | "float4" | "double precision" | "float8" | "money" => {
            pg_float_value(value, field)
        }
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

/// Type-inference typed variant of [`infer_cell_from_json_strict`].
///
/// Used by wire formats that do not carry column type metadata (Maxwell,
/// Debezium). Bare JSON booleans / numbers / strings map to the natural
/// [`Value<Postgres>`] variants; JSON objects and arrays are stringified
/// into [`Value::String`] the same way the [`Cell`] path already did.
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
        1114 => parse_pg_timestamp(text).map(Value::Timestamp).ok_or_else(|| {
            WalParseError::MalformedPayload(format!(
                "invalid timestamp text value for type oid {type_oid}: {text}"
            ))
        }),
        // timestamptz
        1184 => parse_pg_timestamptz(text).map(Value::TimestampTz).ok_or_else(|| {
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
        // interval and all other types: text fallback for semantic parity with Cell
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
    BigDecimal::from_str(&text).map(Value::Decimal).map_err(|_| {
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
        WalParseError::MalformedPayload(format!(
            "invalid timestamp value in field '{field}': {s}"
        ))
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
    parse_pg_timestamptz(s).map(Value::TimestampTz).ok_or_else(|| {
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

fn pg_json_value(
    value: &serde_json::Value,
    field: &str,
) -> Result<Value<Postgres>, WalParseError> {
    if let serde_json::Value::String(s) = value {
        return serde_json::from_str(s).map(Value::Json).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid json value in field '{field}': {s}"
            ))
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
            WalParseError::MalformedPayload(format!(
                "invalid jsonb value in field '{field}': {s}"
            ))
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
// Phase 10 note: the previous `mod tests` block (~478 lines) exercised
// the retired `json_value_to_cell`, `text_to_cell_strict`, and
// `infer_cell_from_json` helpers that Phase 7 replaced with typed
// `Value<Postgres>` decoders. The typed decoders are covered by parser
// round-trip tests in `wal2json.rs`, `debezium.rs`, `maxwell.rs`, and
// `pgoutput.rs`, so the pg_type mod tests block was dropped rather
// than migrated.

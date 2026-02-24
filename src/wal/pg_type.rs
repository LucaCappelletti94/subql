//! PostgreSQL type string → [`Cell`] conversion.
//!
//! Shared by all JSON-based WAL parsers (wal2json, Maxwell, Debezium).

use std::sync::Arc;

use super::WalParseError;
use crate::Cell;

/// Test-only lossy converter retained for unit tests.
#[cfg(test)]
#[must_use]
fn json_value_to_cell(value: &serde_json::Value, pg_type: &str) -> Cell {
    json_value_to_cell_strict(value, pg_type, "value").unwrap_or_else(|_| string_cell(value))
}

/// Strict variant of [`json_value_to_cell`] that reports lossy numeric
/// conversions as parse errors.
pub(super) fn json_value_to_cell_strict(
    value: &serde_json::Value,
    pg_type: &str,
    field: &str,
) -> Result<Cell, WalParseError> {
    if value.is_null() {
        return Ok(Cell::Null);
    }

    // Normalize: lowercase, strip leading underscore (array indicator)
    let ty = pg_type.to_ascii_lowercase();
    let ty = ty.strip_prefix('_').unwrap_or(&ty);

    match ty {
        // Integer types
        "integer" | "int" | "int2" | "int4" | "int8" | "smallint" | "bigint" | "serial"
        | "bigserial" | "smallserial" | "oid" => int_cell_strict(value, field),

        // Floating-point / numeric types
        "real" | "float4" | "double precision" | "float8" | "numeric" | "decimal" | "money" => {
            float_cell_strict(value, field)
        }

        // Boolean
        "boolean" | "bool" => bool_cell_strict(value, field),

        // Everything else → String (including known text-like types)
        _ => Ok(string_cell(value)),
    }
}

/// Parse an integer cell. Handles JSON number and string encodings.
fn int_cell_strict(value: &serde_json::Value, field: &str) -> Result<Cell, WalParseError> {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return Ok(Cell::Int(i));
            }

            if let Some(u) = n.as_u64() {
                let i = i64::try_from(u).map_err(|_| WalParseError::NumericOverflow {
                    field: field.to_string(),
                    value: u.to_string(),
                    target: "i64",
                })?;
                return Ok(Cell::Int(i));
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
                return Ok(Cell::Int(f as i64));
            }

            Ok(Cell::Null)
        }
        serde_json::Value::String(s) => Ok(s.parse::<i64>().map(Cell::Int).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid integer value in field '{field}': {s}"
            ))
        })?),
        _ => Err(WalParseError::MalformedPayload(format!(
            "invalid integer value in field '{field}': {value}"
        ))),
    }
}

fn float_cell_strict(value: &serde_json::Value, field: &str) -> Result<Cell, WalParseError> {
    match value {
        serde_json::Value::Number(n) => n.as_f64().map(Cell::Float).ok_or_else(|| {
            WalParseError::MalformedPayload(format!(
                "invalid floating value in field '{field}': {n}"
            ))
        }),
        serde_json::Value::String(s) => s.parse::<f64>().map(Cell::Float).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid floating value in field '{field}': {s}"
            ))
        }),
        _ => Err(WalParseError::MalformedPayload(format!(
            "invalid floating value in field '{field}': {value}"
        ))),
    }
}

/// Parse a boolean cell strictly. Accepts JSON booleans and common textual
/// encodings; rejects all other values.
fn bool_cell_strict(value: &serde_json::Value, field: &str) -> Result<Cell, WalParseError> {
    match value {
        serde_json::Value::Bool(b) => Ok(Cell::Bool(*b)),
        serde_json::Value::String(s) => match s.as_str() {
            "t" | "true" | "TRUE" | "True" | "1" => Ok(Cell::Bool(true)),
            "f" | "false" | "FALSE" | "False" | "0" => Ok(Cell::Bool(false)),
            _ => Err(WalParseError::MalformedPayload(format!(
                "invalid boolean value in field '{field}': {s}"
            ))),
        },
        _ => Err(WalParseError::MalformedPayload(format!(
            "invalid boolean value in field '{field}': {value}"
        ))),
    }
}

/// Coerce any JSON value to a String cell.
fn string_cell(value: &serde_json::Value) -> Cell {
    match value {
        serde_json::Value::String(s) => Cell::String(Arc::from(s.as_str())),
        serde_json::Value::Null => Cell::Null,
        other => Cell::String(Arc::from(other.to_string().as_str())),
    }
}

/// Test-only lossy converter retained for unit tests.
#[cfg(test)]
#[must_use]
fn infer_cell_from_json(value: &serde_json::Value) -> Cell {
    infer_cell_from_json_strict(value, "value").unwrap_or_else(|_| string_cell(value))
}

/// Strict variant of [`infer_cell_from_json`] that errors on unsigned values
/// that do not fit into i64.
pub(super) fn infer_cell_from_json_strict(
    value: &serde_json::Value,
    field: &str,
) -> Result<Cell, WalParseError> {
    match value {
        serde_json::Value::Null => Ok(Cell::Null),
        serde_json::Value::Bool(b) => Ok(Cell::Bool(*b)),
        #[allow(clippy::option_if_let_else)]
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Cell::Int(i))
            } else if let Some(u) = n.as_u64() {
                let i = i64::try_from(u).map_err(|_| WalParseError::NumericOverflow {
                    field: field.to_string(),
                    value: u.to_string(),
                    target: "i64",
                })?;
                Ok(Cell::Int(i))
            } else {
                Ok(n.as_f64().map_or(Cell::Null, Cell::Float))
            }
        }
        serde_json::Value::String(s) => Ok(Cell::String(Arc::from(s.as_str()))),
        // Arrays and objects: JSON-serialize as fallback
        other => Ok(Cell::String(Arc::from(other.to_string().as_str()))),
    }
}

/// Text-format converter for pgoutput parsing.
///
/// Returns [`WalParseError::MalformedPayload`] when typed scalar values are not
/// valid textual encodings for the declared PostgreSQL type OID.
pub(super) fn text_to_cell_strict(text: &str, type_oid: u32) -> Result<Cell, WalParseError> {
    #[allow(clippy::match_same_arms)]
    match type_oid {
        // bool
        16 => match text {
            "t" => Ok(Cell::Bool(true)),
            "f" => Ok(Cell::Bool(false)),
            _ => Err(WalParseError::MalformedPayload(format!(
                "invalid boolean text value for type oid {type_oid}: {text}"
            ))),
        },
        // int8, int2, int4, oid
        20 | 21 | 23 | 26 => text.parse::<i64>().map(Cell::Int).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid integer text value for type oid {type_oid}: {text}"
            ))
        }),
        // float4, float8, numeric
        700 | 701 | 1700 => text.parse::<f64>().map(Cell::Float).map_err(|_| {
            WalParseError::MalformedPayload(format!(
                "invalid floating text value for type oid {type_oid}: {text}"
            ))
        }),
        // text, bpchar, varchar, name, uuid
        25 | 1042 | 1043 | 19 | 2950 => Ok(Cell::String(Arc::from(text))),
        // date, time, timestamp, timestamptz, timetz, interval
        1082 | 1083 | 1114 | 1184 | 1266 | 1186 => Ok(Cell::String(Arc::from(text))),
        // json, jsonb
        114 | 3802 => Ok(Cell::String(Arc::from(text))),
        // everything else → String fallback
        _ => Ok(Cell::String(Arc::from(text))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn null_value() {
        assert_eq!(json_value_to_cell(&json!(null), "int4"), Cell::Null);
        assert_eq!(json_value_to_cell(&json!(null), "text"), Cell::Null);
        assert_eq!(json_value_to_cell(&json!(null), "boolean"), Cell::Null);
    }

    #[test]
    fn integer_types() {
        assert_eq!(json_value_to_cell(&json!(42), "integer"), Cell::Int(42));
        assert_eq!(json_value_to_cell(&json!(-1), "int4"), Cell::Int(-1));
        assert_eq!(
            json_value_to_cell(&json!(9_999_999_999_i64), "bigint"),
            Cell::Int(9_999_999_999)
        );
        assert_eq!(json_value_to_cell(&json!(1), "smallserial"), Cell::Int(1));
        assert_eq!(json_value_to_cell(&json!(123), "oid"), Cell::Int(123));
    }

    #[test]
    fn integer_from_string() {
        assert_eq!(json_value_to_cell(&json!("42"), "int4"), Cell::Int(42));
    }

    #[test]
    fn float_types() {
        assert_eq!(json_value_to_cell(&json!(3.15), "real"), Cell::Float(3.15));
        assert_eq!(
            json_value_to_cell(&json!(2.719), "double precision"),
            Cell::Float(2.719)
        );
    }

    #[test]
    fn numeric_as_string() {
        // wal2json sometimes encodes numeric as a string
        assert_eq!(
            json_value_to_cell(&json!("99.95"), "numeric"),
            Cell::Float(99.95)
        );
    }

    #[test]
    fn boolean_types() {
        assert_eq!(
            json_value_to_cell(&json!(true), "boolean"),
            Cell::Bool(true)
        );
        assert_eq!(json_value_to_cell(&json!(false), "bool"), Cell::Bool(false));
    }

    #[test]
    fn bool_string_encoding() {
        assert_eq!(json_value_to_cell(&json!("t"), "boolean"), Cell::Bool(true));
        assert_eq!(
            json_value_to_cell(&json!("f"), "boolean"),
            Cell::Bool(false)
        );
        assert_eq!(json_value_to_cell(&json!("TRUE"), "bool"), Cell::Bool(true));
        assert_eq!(
            json_value_to_cell(&json!("FALSE"), "bool"),
            Cell::Bool(false)
        );
    }

    #[test]
    fn string_types() {
        assert_eq!(
            json_value_to_cell(&json!("hello"), "text"),
            Cell::String(Arc::from("hello"))
        );
        assert_eq!(
            json_value_to_cell(&json!("world"), "varchar"),
            Cell::String(Arc::from("world"))
        );
        assert_eq!(
            json_value_to_cell(&json!("550e8400-e29b-41d4-a716-446655440000"), "uuid"),
            Cell::String(Arc::from("550e8400-e29b-41d4-a716-446655440000"))
        );
        assert_eq!(
            json_value_to_cell(&json!("2024-01-01"), "date"),
            Cell::String(Arc::from("2024-01-01"))
        );
    }

    #[test]
    fn unknown_type_fallback() {
        assert_eq!(
            json_value_to_cell(&json!("data"), "custom_type"),
            Cell::String(Arc::from("data"))
        );
        assert_eq!(
            json_value_to_cell(&json!(42), "custom_type"),
            Cell::String(Arc::from("42"))
        );
    }

    #[test]
    fn array_type_prefix_stripped() {
        // _int4 is an int4 array type — prefix stripped, treated as int
        assert_eq!(json_value_to_cell(&json!(42), "_int4"), Cell::Int(42));
    }

    #[test]
    fn int_large_u64() {
        // Non-strict helper falls back to a lossless string representation.
        let large = u64::MAX;
        assert_eq!(
            json_value_to_cell(&json!(large), "bigint"),
            Cell::String(Arc::from(large.to_string()))
        );
    }

    #[test]
    fn int_large_u64_strict_errors() {
        let large = u64::MAX;
        let err = json_value_to_cell_strict(&json!(large), "bigint", "test.bigint")
            .expect_err("overflow should fail in strict mode");
        assert!(matches!(
            err,
            WalParseError::NumericOverflow {
                field,
                target: "i64",
                ..
            } if field == "test.bigint"
        ));
    }

    #[test]
    fn int_fractional_non_strict_falls_back_to_string() {
        // Strict conversion rejects fractional ints; non-strict helper falls back to string.
        assert_eq!(
            json_value_to_cell(&json!(3.7), "int4"),
            Cell::String(Arc::from("3.7"))
        );
    }

    #[test]
    fn int_fractional_strict_errors() {
        let err = json_value_to_cell_strict(&json!(3.7), "int4", "test.int4")
            .expect_err("fractional value should fail in strict mode");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn int_non_numeric_string() {
        // Non-numeric string in integer column falls back to Cell::String
        assert_eq!(
            json_value_to_cell(&json!("hello"), "int4"),
            Cell::String(Arc::from("hello"))
        );
    }

    #[test]
    fn int_non_numeric_string_strict_errors() {
        let err = json_value_to_cell_strict(&json!("hello"), "int4", "test.int4")
            .expect_err("non-numeric integer string should fail in strict mode");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn int_array_value_fallback() {
        // JSON array/object for integer type goes to catch-all string_cell
        assert_eq!(
            json_value_to_cell(&json!([1, 2]), "int4"),
            Cell::String(Arc::from("[1,2]"))
        );
    }

    #[test]
    fn int_non_scalar_strict_errors() {
        let err = json_value_to_cell_strict(&json!([1, 2]), "int4", "test.int4")
            .expect_err("non-scalar integer value should fail in strict mode");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn float_non_numeric_string() {
        // Non-numeric string in float column falls back to Cell::String
        assert_eq!(
            json_value_to_cell(&json!("not_a_number"), "numeric"),
            Cell::String(Arc::from("not_a_number"))
        );
    }

    #[test]
    fn float_non_numeric_string_strict_errors() {
        let err = json_value_to_cell_strict(&json!("not_a_number"), "numeric", "test.numeric")
            .expect_err("non-numeric float string should fail in strict mode");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn float_array_value_fallback() {
        // JSON array for float type goes to catch-all string_cell
        assert_eq!(
            json_value_to_cell(&json!({"x": 1}), "float8"),
            Cell::String(Arc::from(r#"{"x":1}"#))
        );
    }

    #[test]
    fn float_non_scalar_strict_errors() {
        let err = json_value_to_cell_strict(&json!({"x": 1}), "numeric", "test.numeric")
            .expect_err("non-scalar float value should fail in strict mode");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn bool_unknown_string() {
        // Unrecognized string for boolean falls back to Cell::String
        assert_eq!(
            json_value_to_cell(&json!("maybe"), "boolean"),
            Cell::String(Arc::from("maybe"))
        );
    }

    #[test]
    fn bool_unknown_string_strict_errors() {
        let err = json_value_to_cell_strict(&json!("maybe"), "boolean", "test.boolean")
            .expect_err("invalid boolean string should fail in strict mode");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn bool_non_bool_non_string_fallback() {
        // JSON number for boolean type goes to catch-all string_cell
        assert_eq!(
            json_value_to_cell(&json!(42), "boolean"),
            Cell::String(Arc::from("42"))
        );
    }

    #[test]
    fn bool_non_scalar_strict_errors() {
        let err = json_value_to_cell_strict(&json!({"value": true}), "boolean", "test.boolean")
            .expect_err("non-boolean value should fail in strict mode");
        assert!(matches!(err, WalParseError::MalformedPayload(_)));
    }

    #[test]
    fn string_cell_null_direct() {
        // Exercise the null branch in string_cell directly
        // (normally unreachable via json_value_to_cell's early return)
        assert_eq!(string_cell(&json!(null)), Cell::Null);
    }

    // ========================================================================
    // text_to_cell_strict tests (OID-based, for pgoutput)
    // ========================================================================

    #[test]
    fn text_to_cell_strict_bool() {
        assert_eq!(text_to_cell_strict("t", 16).ok(), Some(Cell::Bool(true)));
        assert_eq!(text_to_cell_strict("f", 16).ok(), Some(Cell::Bool(false)));
        assert!(matches!(
            text_to_cell_strict("yes", 16),
            Err(WalParseError::MalformedPayload(_))
        ));
    }

    #[test]
    fn text_to_cell_strict_integers() {
        assert_eq!(text_to_cell_strict("42", 21).ok(), Some(Cell::Int(42)));
        assert_eq!(text_to_cell_strict("-1", 23).ok(), Some(Cell::Int(-1)));
        assert_eq!(
            text_to_cell_strict("9999999999", 20).ok(),
            Some(Cell::Int(9_999_999_999))
        );
        assert_eq!(
            text_to_cell_strict("12345", 26).ok(),
            Some(Cell::Int(12345))
        );
        assert!(matches!(
            text_to_cell_strict("abc", 23),
            Err(WalParseError::MalformedPayload(_))
        ));
    }

    #[test]
    fn text_to_cell_strict_floats() {
        assert_eq!(
            text_to_cell_strict("3.15", 700).ok(),
            Some(Cell::Float(3.15))
        );
        assert_eq!(
            text_to_cell_strict("2.719", 701).ok(),
            Some(Cell::Float(2.719))
        );
        assert_eq!(
            text_to_cell_strict("99.95", 1700).ok(),
            Some(Cell::Float(99.95))
        );
        assert!(matches!(
            text_to_cell_strict("NaN-ish", 700),
            Err(WalParseError::MalformedPayload(_))
        ));
    }

    #[test]
    fn text_to_cell_strict_strings() {
        assert_eq!(
            text_to_cell_strict("hello", 25).ok(),
            Some(Cell::String(Arc::from("hello")))
        );
        assert_eq!(
            text_to_cell_strict("world", 1043).ok(),
            Some(Cell::String(Arc::from("world")))
        );
        assert_eq!(
            text_to_cell_strict("x", 1042).ok(),
            Some(Cell::String(Arc::from("x")))
        );
        assert_eq!(
            text_to_cell_strict("colname", 19).ok(),
            Some(Cell::String(Arc::from("colname")))
        );
        assert_eq!(
            text_to_cell_strict("550e8400-e29b-41d4-a716-446655440000", 2950).ok(),
            Some(Cell::String(Arc::from(
                "550e8400-e29b-41d4-a716-446655440000"
            )))
        );
    }

    #[test]
    fn text_to_cell_strict_temporal() {
        assert_eq!(
            text_to_cell_strict("2024-01-01", 1082).ok(),
            Some(Cell::String(Arc::from("2024-01-01")))
        );
        assert_eq!(
            text_to_cell_strict("2024-01-01 12:00:00", 1114).ok(),
            Some(Cell::String(Arc::from("2024-01-01 12:00:00")))
        );
        assert_eq!(
            text_to_cell_strict("2024-01-01 12:00:00+00", 1184).ok(),
            Some(Cell::String(Arc::from("2024-01-01 12:00:00+00")))
        );
        assert_eq!(
            text_to_cell_strict("12:00:00", 1083).ok(),
            Some(Cell::String(Arc::from("12:00:00")))
        );
        assert_eq!(
            text_to_cell_strict("12:00:00+00", 1266).ok(),
            Some(Cell::String(Arc::from("12:00:00+00")))
        );
        assert_eq!(
            text_to_cell_strict("1 day", 1186).ok(),
            Some(Cell::String(Arc::from("1 day")))
        );
    }

    #[test]
    fn text_to_cell_strict_json() {
        assert_eq!(
            text_to_cell_strict(r#"{"a":1}"#, 114).ok(),
            Some(Cell::String(Arc::from(r#"{"a":1}"#)))
        );
        assert_eq!(
            text_to_cell_strict(r"[1,2,3]", 3802).ok(),
            Some(Cell::String(Arc::from(r"[1,2,3]")))
        );
    }

    #[test]
    fn text_to_cell_strict_unknown_oid() {
        assert_eq!(
            text_to_cell_strict("anything", 99999).ok(),
            Some(Cell::String(Arc::from("anything")))
        );
    }

    // ========================================================================
    // infer_cell_from_json tests (type inference without metadata)
    // ========================================================================

    #[test]
    fn infer_null() {
        assert_eq!(infer_cell_from_json(&json!(null)), Cell::Null);
    }

    #[test]
    fn infer_bool() {
        assert_eq!(infer_cell_from_json(&json!(true)), Cell::Bool(true));
        assert_eq!(infer_cell_from_json(&json!(false)), Cell::Bool(false));
    }

    #[test]
    fn infer_integer() {
        assert_eq!(infer_cell_from_json(&json!(42)), Cell::Int(42));
        assert_eq!(infer_cell_from_json(&json!(-7)), Cell::Int(-7));
        assert_eq!(infer_cell_from_json(&json!(0)), Cell::Int(0));
    }

    #[test]
    fn infer_float() {
        assert_eq!(infer_cell_from_json(&json!(4.2341)), Cell::Float(4.2341));
        assert_eq!(infer_cell_from_json(&json!(-0.5)), Cell::Float(-0.5));
    }

    #[test]
    fn infer_string() {
        assert_eq!(
            infer_cell_from_json(&json!("hello")),
            Cell::String(Arc::from("hello"))
        );
        assert_eq!(
            infer_cell_from_json(&json!("2016-10-21 05:33:37")),
            Cell::String(Arc::from("2016-10-21 05:33:37"))
        );
    }

    #[test]
    fn infer_array_fallback() {
        assert_eq!(
            infer_cell_from_json(&json!([1, 2, 3])),
            Cell::String(Arc::from("[1,2,3]"))
        );
    }

    #[test]
    fn infer_object_fallback() {
        assert_eq!(
            infer_cell_from_json(&json!({"key": "val"})),
            Cell::String(Arc::from(r#"{"key":"val"}"#))
        );
    }

    #[test]
    fn infer_large_u64() {
        let large = u64::MAX;
        assert_eq!(
            infer_cell_from_json(&json!(large)),
            Cell::String(Arc::from(large.to_string()))
        );
    }

    #[test]
    fn infer_large_u64_strict_errors() {
        let large = u64::MAX;
        let err = infer_cell_from_json_strict(&json!(large), "test.value")
            .expect_err("overflow should fail in strict mode");
        assert!(matches!(
            err,
            WalParseError::NumericOverflow {
                field,
                target: "i64",
                ..
            } if field == "test.value"
        ));
    }
}

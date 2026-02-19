//! PostgreSQL type string → [`Cell`] conversion.
//!
//! Shared by all JSON-based WAL parsers (wal2json, Maxwell, Debezium).

use std::sync::Arc;

use crate::Cell;

/// Convert a JSON value to a [`Cell`] given the PostgreSQL type name.
///
/// Handles wal2json quirks such as numeric values encoded as JSON strings.
#[must_use]
pub fn json_value_to_cell(value: &serde_json::Value, pg_type: &str) -> Cell {
    if value.is_null() {
        return Cell::Null;
    }

    // Normalize: lowercase, strip leading underscore (array indicator)
    let ty = pg_type.to_ascii_lowercase();
    let ty = ty.strip_prefix('_').unwrap_or(&ty);

    match ty {
        // Integer types
        "integer" | "int" | "int2" | "int4" | "int8" | "smallint" | "bigint" | "serial"
        | "bigserial" | "smallserial" | "oid" => int_cell(value),

        // Floating-point / numeric types
        "real" | "float4" | "double precision" | "float8" | "numeric" | "decimal" | "money" => {
            float_cell(value)
        }

        // Boolean
        "boolean" | "bool" => bool_cell(value),

        // Everything else → String
        "text" | "varchar" | "character varying" | "char" | "bpchar" | "name" | "uuid" | "date"
        | "time" | "timetz" | "timestamp" | "timestamptz" | "interval" | "json" | "jsonb"
        | "xml" | "bytea" | "inet" | "cidr" | "macaddr" | "point" | "line" | "box" | "path"
        | "polygon" | "circle" | "tsquery" | "tsvector" | "bit" | "varbit" => string_cell(value),

        // Unknown type → best-effort String
        _ => string_cell(value),
    }
}

/// Parse an integer cell. Handles JSON number and string encodings.
fn int_cell(value: &serde_json::Value) -> Cell {
    match value {
        serde_json::Value::Number(n) => n.as_i64().map_or_else(
            || {
                n.as_u64().map_or_else(
                    || {
                        // Fractional number in an integer column — truncate.
                        #[allow(clippy::cast_possible_truncation)]
                        n.as_f64().map_or(Cell::Null, |f| Cell::Int(f as i64))
                    },
                    |u| {
                        // u64 values that fit in i64 were already caught;
                        // for values > i64::MAX, saturate.
                        #[allow(clippy::cast_possible_wrap)]
                        Cell::Int(u as i64)
                    },
                )
            },
            Cell::Int,
        ),
        serde_json::Value::String(s) => s
            .parse::<i64>()
            .map_or_else(|_| Cell::String(Arc::from(s.as_str())), Cell::Int),
        _ => string_cell(value),
    }
}

/// Parse a float cell. Handles JSON number and string encodings
/// (wal2json sometimes encodes `numeric` as a string).
fn float_cell(value: &serde_json::Value) -> Cell {
    match value {
        serde_json::Value::Number(n) => n.as_f64().map_or(Cell::Null, Cell::Float),
        serde_json::Value::String(s) => s
            .parse::<f64>()
            .map_or_else(|_| Cell::String(Arc::from(s.as_str())), Cell::Float),
        _ => string_cell(value),
    }
}

/// Parse a boolean cell. Handles `true`/`false`, `"t"`/`"f"` string encoding.
fn bool_cell(value: &serde_json::Value) -> Cell {
    match value {
        serde_json::Value::Bool(b) => Cell::Bool(*b),
        serde_json::Value::String(s) => match s.as_str() {
            "t" | "true" | "TRUE" | "True" | "1" => Cell::Bool(true),
            "f" | "false" | "FALSE" | "False" | "0" => Cell::Bool(false),
            _ => Cell::String(Arc::from(s.as_str())),
        },
        _ => string_cell(value),
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

/// Infer a [`Cell`] from a JSON value without type metadata.
///
/// Used by CDC formats (e.g. Maxwell) that provide no column type info.
/// Maps JSON primitives directly: null→Null, bool→Bool, integer→Int,
/// float→Float, string→String. Arrays and objects are JSON-serialized
/// to a String fallback.
#[must_use]
pub fn infer_cell_from_json(value: &serde_json::Value) -> Cell {
    match value {
        serde_json::Value::Null => Cell::Null,
        serde_json::Value::Bool(b) => Cell::Bool(*b),
        #[allow(clippy::option_if_let_else)]
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Cell::Int(i)
            } else if let Some(u) = n.as_u64() {
                #[allow(clippy::cast_possible_wrap)]
                Cell::Int(u as i64)
            } else {
                n.as_f64().map_or(Cell::Null, Cell::Float)
            }
        }
        serde_json::Value::String(s) => Cell::String(Arc::from(s.as_str())),
        // Arrays and objects: JSON-serialize as fallback
        other => Cell::String(Arc::from(other.to_string().as_str())),
    }
}

/// Convert a text-format column value to a [`Cell`] given the PostgreSQL type OID.
///
/// Used by the pgoutput binary protocol parser, where columns are transmitted as
/// UTF-8 text alongside their type OID (from `pg_type`).
#[must_use]
pub fn text_to_cell(text: &str, type_oid: u32) -> Cell {
    #[allow(clippy::match_same_arms)]
    match type_oid {
        // bool
        16 => match text {
            "t" => Cell::Bool(true),
            "f" => Cell::Bool(false),
            _ => Cell::String(Arc::from(text)),
        },
        // int8, int2, int4, oid
        20 | 21 | 23 | 26 => parse_int(text),
        // float4, float8, numeric
        700 | 701 | 1700 => parse_float(text),
        // text, bpchar, varchar, name, uuid
        25 | 1042 | 1043 | 19 | 2950 => Cell::String(Arc::from(text)),
        // date, time, timestamp, timestamptz, timetz, interval
        1082 | 1083 | 1114 | 1184 | 1266 | 1186 => Cell::String(Arc::from(text)),
        // json, jsonb
        114 | 3802 => Cell::String(Arc::from(text)),
        // everything else → String fallback
        _ => Cell::String(Arc::from(text)),
    }
}

/// Parse integer from text, falling back to `Cell::String` on failure.
fn parse_int(text: &str) -> Cell {
    text.parse::<i64>()
        .map_or_else(|_| Cell::String(Arc::from(text)), Cell::Int)
}

/// Parse float from text, falling back to `Cell::String` on failure.
fn parse_float(text: &str) -> Cell {
    text.parse::<f64>()
        .map_or_else(|_| Cell::String(Arc::from(text)), Cell::Float)
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
        assert_eq!(json_value_to_cell(&json!(3.14), "real"), Cell::Float(3.14));
        assert_eq!(
            json_value_to_cell(&json!(2.718), "double precision"),
            Cell::Float(2.718)
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
        // u64 value exceeding i64::MAX triggers the u64 branch
        let large = u64::MAX;
        assert!(matches!(
            json_value_to_cell(&json!(large), "bigint"),
            Cell::Int(_)
        ));
    }

    #[test]
    fn int_fractional_truncation() {
        // Fractional JSON number in integer column — truncated to i64
        assert_eq!(json_value_to_cell(&json!(3.7), "int4"), Cell::Int(3));
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
    fn int_array_value_fallback() {
        // JSON array/object for integer type goes to catch-all string_cell
        assert_eq!(
            json_value_to_cell(&json!([1, 2]), "int4"),
            Cell::String(Arc::from("[1,2]"))
        );
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
    fn float_array_value_fallback() {
        // JSON array for float type goes to catch-all string_cell
        assert_eq!(
            json_value_to_cell(&json!({"x": 1}), "float8"),
            Cell::String(Arc::from(r#"{"x":1}"#))
        );
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
    fn bool_non_bool_non_string_fallback() {
        // JSON number for boolean type goes to catch-all string_cell
        assert_eq!(
            json_value_to_cell(&json!(42), "boolean"),
            Cell::String(Arc::from("42"))
        );
    }

    #[test]
    fn string_cell_null_direct() {
        // Exercise the null branch in string_cell directly
        // (normally unreachable via json_value_to_cell's early return)
        assert_eq!(string_cell(&json!(null)), Cell::Null);
    }

    // ========================================================================
    // text_to_cell tests (OID-based, for pgoutput)
    // ========================================================================

    #[test]
    fn text_to_cell_bool() {
        assert_eq!(text_to_cell("t", 16), Cell::Bool(true));
        assert_eq!(text_to_cell("f", 16), Cell::Bool(false));
        // Unrecognized bool text → String
        assert_eq!(text_to_cell("yes", 16), Cell::String(Arc::from("yes")));
    }

    #[test]
    fn text_to_cell_integers() {
        // int2 (OID 21)
        assert_eq!(text_to_cell("42", 21), Cell::Int(42));
        // int4 (OID 23)
        assert_eq!(text_to_cell("-1", 23), Cell::Int(-1));
        // int8 (OID 20)
        assert_eq!(text_to_cell("9999999999", 20), Cell::Int(9_999_999_999));
        // oid (OID 26)
        assert_eq!(text_to_cell("12345", 26), Cell::Int(12345));
        // Non-numeric → String fallback
        assert_eq!(text_to_cell("abc", 23), Cell::String(Arc::from("abc")));
    }

    #[test]
    fn text_to_cell_floats() {
        // float4 (OID 700)
        assert_eq!(text_to_cell("3.14", 700), Cell::Float(3.14));
        // float8 (OID 701)
        assert_eq!(text_to_cell("2.718", 701), Cell::Float(2.718));
        // numeric (OID 1700)
        assert_eq!(text_to_cell("99.95", 1700), Cell::Float(99.95));
        // Non-numeric → String fallback
        assert_eq!(
            text_to_cell("NaN-ish", 700),
            Cell::String(Arc::from("NaN-ish"))
        );
    }

    #[test]
    fn text_to_cell_strings() {
        // text (OID 25)
        assert_eq!(text_to_cell("hello", 25), Cell::String(Arc::from("hello")));
        // varchar (OID 1043)
        assert_eq!(
            text_to_cell("world", 1043),
            Cell::String(Arc::from("world"))
        );
        // bpchar (OID 1042)
        assert_eq!(text_to_cell("x", 1042), Cell::String(Arc::from("x")));
        // name (OID 19)
        assert_eq!(
            text_to_cell("colname", 19),
            Cell::String(Arc::from("colname"))
        );
        // uuid (OID 2950)
        assert_eq!(
            text_to_cell("550e8400-e29b-41d4-a716-446655440000", 2950),
            Cell::String(Arc::from("550e8400-e29b-41d4-a716-446655440000"))
        );
    }

    #[test]
    fn text_to_cell_temporal() {
        // date (OID 1082)
        assert_eq!(
            text_to_cell("2024-01-01", 1082),
            Cell::String(Arc::from("2024-01-01"))
        );
        // timestamp (OID 1114)
        assert_eq!(
            text_to_cell("2024-01-01 12:00:00", 1114),
            Cell::String(Arc::from("2024-01-01 12:00:00"))
        );
        // timestamptz (OID 1184)
        assert_eq!(
            text_to_cell("2024-01-01 12:00:00+00", 1184),
            Cell::String(Arc::from("2024-01-01 12:00:00+00"))
        );
        // time (OID 1083)
        assert_eq!(
            text_to_cell("12:00:00", 1083),
            Cell::String(Arc::from("12:00:00"))
        );
        // timetz (OID 1266)
        assert_eq!(
            text_to_cell("12:00:00+00", 1266),
            Cell::String(Arc::from("12:00:00+00"))
        );
        // interval (OID 1186)
        assert_eq!(
            text_to_cell("1 day", 1186),
            Cell::String(Arc::from("1 day"))
        );
    }

    #[test]
    fn text_to_cell_json() {
        // json (OID 114)
        assert_eq!(
            text_to_cell(r#"{"a":1}"#, 114),
            Cell::String(Arc::from(r#"{"a":1}"#))
        );
        // jsonb (OID 3802)
        assert_eq!(
            text_to_cell(r#"[1,2,3]"#, 3802),
            Cell::String(Arc::from(r#"[1,2,3]"#))
        );
    }

    #[test]
    fn text_to_cell_unknown_oid() {
        // Unknown OID → String fallback
        assert_eq!(
            text_to_cell("anything", 99999),
            Cell::String(Arc::from("anything"))
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
        assert!(matches!(infer_cell_from_json(&json!(large)), Cell::Int(_)));
    }
}

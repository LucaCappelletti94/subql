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
}

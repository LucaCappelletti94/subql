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

mod backend_impls;
mod column_ref;
mod parse_helpers;

use crate::backend::{Backend, CustomScalars, Value, ValueKind, ValueKindOf};
use crate::RegisterError;
use alloc::format;
use parse_helpers::err_shape;
use sqlparser::ast::Value as SqlValue;

pub(super) use column_ref::resolve_column_ref;
pub(super) use parse_helpers::hex_upper;

/// Backend-aware parsing of sqlparser AST literals into typed [`Value`]s.
///
/// Implemented per shipped [`Backend`]. The compiler bounds its entry
/// points on `B: Backend + SqlLiteralParse` so the parser can turn any
/// sqlparser literal into a `Value<B>` targeting a known
/// [`ValueKind`].
///
/// # Contract
///
/// * `sqlparser::ast::Value::Null` maps to [`Value::Null`] regardless of
///   `target`.
/// * When the sqlparser value's shape does not fit `target` (e.g.
///   `Boolean(true)` targeting [`crate::backend::ScalarFamily::Timestamp`]) the result is
///   [`RegisterError::TypeError`] naming the mismatch.
/// * When the shape fits but the payload cannot be parsed
///   (e.g. `SingleQuotedString("not-a-uuid")` targeting
///   [`crate::backend::ScalarFamily::Uuid`]) the result is [`RegisterError::TypeError`]
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
        target: ValueKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError>;
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
        .ok_or_else(|| err_shape(sql, ValueKind::<()>::from(carrier)))?;
    <B::Custom as CustomScalars>::convert(custom, view)
        .map(Value::Custom)
        .ok_or_else(|| {
            RegisterError::TypeError(format!(
                "the custom type {custom:?} refused SQL literal {sql:?}"
            ))
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::SqlLiteralParse;
    use crate::backend::{MySql, Pg18, Postgres, SQLite, ScalarFamily, Value};
    use crate::RegisterError;
    use sqlparser::ast::Value as SqlValue;

    #[test]
    fn postgres_null_is_type_agnostic() {
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&SqlValue::Null, ScalarFamily::Int.into()).unwrap(),
            Value::Null
        );
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&SqlValue::Null, ScalarFamily::String.into()).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn postgres_bool_from_boolean() {
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&SqlValue::Boolean(true), ScalarFamily::Bool.into())
                .unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn postgres_int_from_number() {
        let sql = SqlValue::Number("42".to_string(), false);
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Int.into()).unwrap(),
            Value::Int(42)
        );
    }

    #[test]
    fn postgres_float_from_number() {
        let sql = SqlValue::Number("3.5".to_string(), false);
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Float.into()).unwrap(),
            Value::Float(3.5)
        );
    }

    #[test]
    fn postgres_string_from_quoted() {
        let sql = SqlValue::SingleQuotedString("hi".to_string());
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::String.into()).unwrap(),
            Value::String("hi".to_string())
        );
    }

    #[test]
    fn postgres_uuid_from_quoted() {
        let sql = SqlValue::SingleQuotedString("550e8400-e29b-41d4-a716-446655440000".to_string());
        let v = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Uuid.into()).unwrap();
        assert!(matches!(v, Value::Uuid(_)));
    }

    #[test]
    fn postgres_timestamp_from_iso8601() {
        let sql = SqlValue::SingleQuotedString("2024-01-02T03:04:05".to_string());
        let v = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Timestamp.into()).unwrap();
        assert!(matches!(v, Value::Timestamp(_)));
    }

    #[test]
    fn postgres_date_from_iso8601() {
        let sql = SqlValue::SingleQuotedString("2024-01-02".to_string());
        let v = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Date.into()).unwrap();
        assert!(matches!(v, Value::Date(_)));
    }

    #[test]
    fn postgres_bytes_from_hex_literal() {
        let sql = SqlValue::HexStringLiteral("deadbeef".to_string());
        let v = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Bytes.into()).unwrap();
        assert_eq!(v, Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn postgres_json_equality_is_refused() {
        let sql = SqlValue::SingleQuotedString("{\"k\":1}".to_string());
        assert!(matches!(
            Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Json.into()),
            Err(RegisterError::TypeError(_))
        ));
    }

    #[test]
    fn postgres_decimal_from_number() {
        let sql = SqlValue::Number("123.456".to_string(), false);
        let v = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Decimal.into()).unwrap();
        assert!(matches!(v, Value::Decimal(_)));
    }

    #[test]
    fn postgres_type_mismatch_returns_type_error() {
        let sql = SqlValue::Boolean(true);
        let err = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Int.into()).unwrap_err();
        assert!(matches!(err, RegisterError::TypeError(_)));
    }

    #[test]
    fn postgres_uuid_invalid_string_errors() {
        let sql = SqlValue::SingleQuotedString("not-a-uuid".to_string());
        let err = Postgres::<Pg18>::parse_literal(&sql, ScalarFamily::Uuid.into()).unwrap_err();
        assert!(matches!(err, RegisterError::TypeError(_)));
    }

    #[test]
    fn temporal_literal_maps_representative_values() {
        use chrono::{DateTime, Utc};
        let ts = SqlValue::SingleQuotedString("2026-01-01 00:00:00".to_string());
        assert!(matches!(
            Postgres::<Pg18>::parse_literal(&ts, ScalarFamily::Timestamp.into()),
            Ok(Value::Timestamp(_))
        ));
        assert!(matches!(
            SQLite::parse_literal(&ts, ScalarFamily::Timestamp.into()),
            Ok(Value::Timestamp(_))
        ));
        let tstz = SqlValue::SingleQuotedString("2025-12-31 22:00:00-02".to_string());
        let expected_utc: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(
            Postgres::<Pg18>::parse_literal(&tstz, ScalarFamily::TimestampTz.into()).unwrap(),
            Value::TimestampTz(expected_utc)
        );
        let date = SqlValue::SingleQuotedString("2026-01-01".to_string());
        assert!(matches!(
            Postgres::<Pg18>::parse_literal(&date, ScalarFamily::Date.into()),
            Ok(Value::Date(_))
        ));
        let time = SqlValue::SingleQuotedString("12:34:56.789".to_string());
        assert!(matches!(
            MySql::parse_literal(&time, ScalarFamily::Time.into()),
            Ok(Value::Time(_))
        ));
    }

    #[test]
    fn temporal_literal_rejects_key_boundaries() {
        let no_offset = SqlValue::SingleQuotedString("2026-01-01 00:00:00".to_string());
        assert!(matches!(
            Postgres::<Pg18>::parse_literal(&no_offset, ScalarFamily::TimestampTz.into()),
            Err(RegisterError::TypeError(_))
        ));
        let with_offset = SqlValue::SingleQuotedString("2026-01-01 00:00:00+00".to_string());
        assert!(matches!(
            Postgres::<Pg18>::parse_literal(&with_offset, ScalarFamily::Timestamp.into()),
            Err(RegisterError::TypeError(_))
        ));
    }
}

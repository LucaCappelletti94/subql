//! Shared SQL literal parsing helpers used by all backend impls.

use crate::backend::{BuiltinKind, NoCustom, ScalarKind};
use crate::RegisterError;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use sqlparser::ast::Value as SqlValue;

pub(super) fn err_shape<C: core::fmt::Debug + Copy>(
    sql: &SqlValue,
    target: ScalarKind<C>,
) -> RegisterError {
    RegisterError::TypeError(format!("cannot use SQL literal {sql:?} as {target:?}"))
}

pub(super) fn err_parse(
    sql: &SqlValue,
    family: BuiltinKind,
    msg: impl core::fmt::Display,
) -> RegisterError {
    err_parse_kind(sql, ScalarKind::<NoCustom>::from(family), msg)
}

pub(super) fn err_parse_kind<C: core::fmt::Debug + Copy>(
    sql: &SqlValue,
    target: ScalarKind<C>,
    msg: impl core::fmt::Display,
) -> RegisterError {
    RegisterError::TypeError(format!(
        "cannot parse SQL literal {sql:?} as {target:?}: {msg}"
    ))
}

pub(super) fn parse_i64_literal(n: &str, sql: &SqlValue) -> Result<i64, RegisterError> {
    sql_scalar_text::parse_i64(n).ok_or_else(|| match n.parse::<i64>() {
        Err(e) => err_parse(sql, BuiltinKind::Int, e),
        Ok(_) => err_parse(sql, BuiltinKind::Int, "not an integer"),
    })
}

pub(super) fn parse_f64_literal(n: &str, sql: &SqlValue) -> Result<f64, RegisterError> {
    sql_scalar_text::parse_f64(n).ok_or_else(|| match n.parse::<f64>() {
        Err(e) => err_parse(sql, BuiltinKind::Float, e),
        Ok(_) => err_parse(sql, BuiltinKind::Float, "not a float"),
    })
}

pub(super) fn parse_decimal_literal(
    n: &str,
    sql: &SqlValue,
) -> Result<bigdecimal::BigDecimal, RegisterError> {
    sql_scalar_text::parse_decimal(n).ok_or_else(|| match n.parse::<bigdecimal::BigDecimal>() {
        Err(e) => err_parse(sql, BuiltinKind::Decimal, e),
        Ok(_) => err_parse(sql, BuiltinKind::Decimal, "not a decimal"),
    })
}

pub(super) fn parse_hex_bytes(s: &str, sql: &SqlValue) -> Result<Vec<u8>, RegisterError> {
    if !s.len().is_multiple_of(2) {
        return Err(err_parse(sql, BuiltinKind::Bytes, "odd-length hex literal"));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i]).ok_or_else(|| {
            err_parse(sql, BuiltinKind::Bytes, "non-hex character in hex literal")
        })?;
        let lo = hex_nibble(bytes[i + 1]).ok_or_else(|| {
            err_parse(sql, BuiltinKind::Bytes, "non-hex character in hex literal")
        })?;
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

/// Encode bytes as uppercase hex, the spelling `SqlValue::HexStringLiteral`
/// renders as `X'...'`. Inverse of [`parse_hex_bytes`]: the pair round-trips
/// any byte vector (empty included) losslessly, since `parse_hex_bytes`
/// accepts either case.
pub fn hex_upper(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        // u8 -> usize widens losslessly. Nibbles are already 0..=15.
        out.push(char::from(HEX[usize::from(b >> 4)]));
        out.push(char::from(HEX[usize::from(b & 0x0f)]));
    }
    out
}

pub(super) fn parse_uuid(s: &str, sql: &SqlValue) -> Result<uuid::Uuid, RegisterError> {
    uuid::Uuid::parse_str(s).map_err(|e| err_parse(sql, BuiltinKind::Uuid, e))
}

pub(super) fn parse_uuid_as_string(s: &str, sql: &SqlValue) -> Result<String, RegisterError> {
    // Validate the shape before storing as a string so downstream matches
    // do not silently paper over a malformed UUID literal.
    uuid::Uuid::parse_str(s).map_err(|e| err_parse(sql, BuiltinKind::Uuid, e))?;
    Ok(s.to_string())
}

pub(super) fn parse_timestamp_literal(
    s: &str,
    sql: &SqlValue,
) -> Result<chrono::NaiveDateTime, RegisterError> {
    sql_scalar_text::parse_timestamp(s)
        .ok_or_else(|| err_parse(sql, BuiltinKind::Timestamp, "not a timestamp"))
}

pub(super) fn parse_timestamp_tz_literal(
    s: &str,
    sql: &SqlValue,
) -> Result<chrono::DateTime<chrono::Utc>, RegisterError> {
    sql_scalar_text::parse_timestamp_tz(s).ok_or_else(|| {
        err_parse(
            sql,
            BuiltinKind::TimestampTz,
            "not a timestamp with time zone",
        )
    })
}

pub(super) fn parse_date_literal(
    s: &str,
    sql: &SqlValue,
) -> Result<chrono::NaiveDate, RegisterError> {
    sql_scalar_text::parse_date(s).ok_or_else(|| err_parse(sql, BuiltinKind::Date, "not a date"))
}

pub(super) fn parse_time_literal(
    s: &str,
    sql: &SqlValue,
) -> Result<chrono::NaiveTime, RegisterError> {
    sql_scalar_text::parse_time(s).ok_or_else(|| err_parse(sql, BuiltinKind::Time, "not a time"))
}

pub(super) fn parse_json(s: &str, sql: &SqlValue) -> Result<serde_json::Value, RegisterError> {
    serde_json::from_str(s).map_err(|e| err_parse(sql, BuiltinKind::Json, e))
}

/// Extract a string payload from a sqlparser quoted-string variant.
///
/// `SingleQuotedString`, `DoubleQuotedString`, `NationalStringLiteral`,
/// and `EscapedStringLiteral` all carry a plain `String`; hex literals
/// are handled by the bytes path separately.
pub(super) fn quoted_string(sql: &SqlValue) -> Option<&str> {
    match sql {
        SqlValue::SingleQuotedString(s)
        | SqlValue::DoubleQuotedString(s)
        | SqlValue::NationalStringLiteral(s)
        | SqlValue::EscapedStringLiteral(s) => Some(s),
        _ => None,
    }
}

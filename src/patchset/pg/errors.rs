//! Bind-time error helpers for the Postgres patchset adapter.

use alloc::boxed::Box;
use alloc::string::String;
use core::fmt;

use diesel::result::Error as DieselError;
use sqlite_diff_rs::Value;

/// Name the SQLite wire shape of a value for use in error messages.
pub(super) const fn shape_of<S, B>(value: &Value<S, B>) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::Integer(_) => "INTEGER",
        Value::Real(_) => "REAL",
        Value::Text(_) => "TEXT",
        Value::Blob(_) => "BLOB",
    }
}

/// Build a [`DieselError::QueryBuilderError`] for a column whose wire value
/// does not match the expected shape.
pub(super) fn bind_error(column: &str, expected: &str, got: &str) -> DieselError {
    DieselError::QueryBuilderError(Box::new(BindTypeMismatch {
        message: alloc::format!("column `{column}` expects {expected}, got {got}"),
    }))
}

/// Bind-time type mismatch: the wire value carried a shape the adapter
/// refuses to interpret as the target column's type.
#[derive(Debug, Clone)]
struct BindTypeMismatch {
    message: String,
}

impl fmt::Display for BindTypeMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl core::error::Error for BindTypeMismatch {}

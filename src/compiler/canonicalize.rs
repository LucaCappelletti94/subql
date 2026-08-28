//! SQL normalization and hashing for predicate deduplication.

use crate::RegisterError;
use alloc::string::{String, ToString};
use sqlparser::ast::Expr;
use sqlparser::dialect::Dialect;
use sqlparser_canonicalize::CanonicalizeError;

/// Stable 128-bit predicate hash.
pub type PredicateHash = u128;

/// Normalizes a SQL predicate.
pub fn normalize_sql(sql: &str, dialect: &dyn Dialect) -> Result<String, RegisterError> {
    sqlparser_canonicalize::normalize_sql(sql, dialect).map_err(map_error)
}

pub(crate) fn normalize_where_clause(where_expr: Option<&Expr>) -> Result<String, RegisterError> {
    sqlparser_canonicalize::normalize_where_clause(where_expr).map_err(map_error)
}

/// Hashes canonical predicate text.
#[must_use]
pub fn hash_sql(normalized: &str) -> PredicateHash {
    sqlparser_canonicalize::hash_canonical(normalized)
}

fn map_error(error: CanonicalizeError) -> RegisterError {
    match error {
        CanonicalizeError::Parse {
            line,
            column,
            message,
        } => RegisterError::ParseError {
            line,
            column,
            message,
        },
        CanonicalizeError::Unsupported(message) => RegisterError::UnsupportedSql(message),
        other => RegisterError::UnsupportedSql(other.to_string()),
    }
}

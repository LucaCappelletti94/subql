use crate::{Cell, ColumnId, RegisterError, SchemaCatalog, TableId};
use sqlparser::ast::{Expr, Value};

/// Resolve simple column references used by parser/prefilter.
///
/// Supports:
/// - `col`
/// - `table.col` (table qualifier ignored after SQL-shape validation)
#[must_use]
pub(super) fn resolve_column_ref(
    expr: &Expr,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> Option<ColumnId> {
    match expr {
        Expr::Identifier(ident) => catalog.column_id(table_id, &ident.value),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            catalog.column_id(table_id, &parts[1].value)
        }
        _ => None,
    }
}

/// Parse SQL literal value to runtime cell for parser errors.
pub(super) fn sql_value_to_cell_strict(val: &Value) -> Result<Cell, RegisterError> {
    match val {
        Value::Null => Ok(Cell::Null),
        Value::Boolean(b) => Ok(Cell::Bool(*b)),
        Value::Number(n, _long) => {
            // Try parsing as i64 first, then f64.
            #[allow(clippy::option_if_let_else)]
            if let Ok(i) = n.parse::<i64>() {
                Ok(Cell::Int(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(Cell::Float(f))
            } else {
                Err(RegisterError::TypeError(format!(
                    "Cannot parse number: {n}"
                )))
            }
        }
        Value::SingleQuotedString(s)
        | Value::DoubleQuotedString(s)
        | Value::NationalStringLiteral(s)
        | Value::HexStringLiteral(s) => Ok(Cell::String(s.as_str().into())),
        _ => Err(RegisterError::UnsupportedSql(format!(
            "Value type {val:?} not supported"
        ))),
    }
}

/// Parse SQL literal value to runtime cell for planner analysis.
///
/// Returns `None` for unsupported values and unparseable numerics.
#[must_use]
pub(super) fn sql_value_to_cell_lossy(val: &Value) -> Option<Cell> {
    match val {
        Value::Null => Some(Cell::Null),
        Value::Boolean(b) => Some(Cell::Bool(*b)),
        Value::Number(n, _long) => n
            .parse::<i64>()
            .map(Cell::Int)
            .or_else(|_| n.parse::<f64>().map(Cell::Float))
            .ok(),
        Value::SingleQuotedString(s)
        | Value::DoubleQuotedString(s)
        | Value::NationalStringLiteral(s)
        | Value::HexStringLiteral(s) => Some(Cell::String(s.as_str().into())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_and_lossy_parse_common_literals() {
        let int = Value::Number("42".to_string(), false);
        assert_eq!(sql_value_to_cell_strict(&int).ok(), Some(Cell::Int(42)));
        assert_eq!(sql_value_to_cell_lossy(&int), Some(Cell::Int(42)));

        let float = Value::Number("3.5".to_string(), false);
        assert_eq!(
            sql_value_to_cell_strict(&float).ok(),
            Some(Cell::Float(3.5))
        );
        assert_eq!(sql_value_to_cell_lossy(&float), Some(Cell::Float(3.5)));

        let string = Value::SingleQuotedString("x".to_string());
        assert_eq!(
            sql_value_to_cell_strict(&string).ok(),
            Some(Cell::String("x".into()))
        );
        assert_eq!(
            sql_value_to_cell_lossy(&string),
            Some(Cell::String("x".into()))
        );
    }

    #[test]
    fn strict_rejects_unparseable_numbers_while_lossy_skips() {
        let bad = Value::Number("not_a_number".to_string(), false);
        let strict = sql_value_to_cell_strict(&bad);
        assert!(matches!(strict, Err(RegisterError::TypeError(_))));
        assert_eq!(sql_value_to_cell_lossy(&bad), None);
    }
}

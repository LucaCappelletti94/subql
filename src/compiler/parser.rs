//! SQL parser for subscription predicates
//!
//! Supports generic SQL dialects via sqlparser crate.

use sqlparser::ast::{Expr, Statement, SetExpr, TableFactor};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use crate::{RegisterError, TableId, SchemaCatalog, Cell};
use super::{BytecodeProgram, Instruction};

/// Parse and compile SQL SELECT statement to bytecode
///
/// # Arguments
/// * `sql` - SQL SELECT statement with optional WHERE clause
/// * `dialect` - SQL dialect (`PostgreSQL`, `MySQL`, `SQLite`, etc.)
/// * `catalog` - Schema catalog for table/column resolution
///
/// # Returns
/// * `Ok((table_id, program))` - Compiled bytecode for the WHERE clause
/// * `Err(RegisterError)` - Parse error, unsupported SQL, or schema error
pub fn parse_and_compile<D: Dialect>(
    sql: &str,
    dialect: &D,
    catalog: &dyn SchemaCatalog,
) -> Result<(TableId, BytecodeProgram), RegisterError> {
    // Parse SQL
    let statements = Parser::parse_sql(dialect, sql)
        .map_err(|e| RegisterError::ParseError {
            line: 1, // sqlparser doesn't provide line numbers easily
            column: 0,
            message: e.to_string(),
        })?;

    if statements.len() != 1 {
        return Err(RegisterError::UnsupportedSql(
            "Expected exactly one SELECT statement".to_string()
        ));
    }

    let stmt = &statements[0];

    // Extract SELECT ... FROM table WHERE predicate
    let (table_name, where_clause) = extract_table_and_where(stmt)?;

    // Resolve table ID
    let table_id = catalog.table_id(&table_name)
        .ok_or_else(|| RegisterError::UnknownTable(table_name.clone()))?;

    // Compile WHERE clause to bytecode
    let program = where_clause.map_or_else(
        || {
            // No WHERE clause = always match
            // Push True onto stack
            BytecodeProgram::new(vec![
                Instruction::PushLiteral(Cell::Bool(true)),
            ])
        },
        |expr| compile_expression(&expr, table_id, catalog),
    );

    Ok((table_id, program))
}

fn extract_table_and_where(stmt: &Statement)
    -> Result<(String, Option<Expr>), RegisterError>
{
    match stmt {
        Statement::Query(query) => {
            match query.body.as_ref() {
                SetExpr::Select(select) => {
                    // Must have exactly one table
                    if select.from.len() != 1 {
                        return Err(RegisterError::UnsupportedSql(
                            "Exactly one table required (no joins)".to_string()
                        ));
                    }

                    // Check for JOINs
                    if !select.from[0].joins.is_empty() {
                        return Err(RegisterError::UnsupportedSql(
                            "Joins not supported".to_string()
                        ));
                    }

                    let table_factor = &select.from[0].relation;
                    let table_name = match table_factor {
                        TableFactor::Table { name, .. } => {
                            name.0.first()
                                .ok_or_else(|| RegisterError::UnsupportedSql(
                                    "Missing table name".to_string()
                                ))?
                                .value.clone()
                        }
                        _ => return Err(RegisterError::UnsupportedSql(
                            "Joins, subqueries not supported".to_string()
                        )),
                    };

                    // Extract WHERE clause
                    let where_clause = select.selection.clone();

                    Ok((table_name, where_clause))
                }
                _ => Err(RegisterError::UnsupportedSql(
                    "Only SELECT supported (no UNION, etc.)".to_string()
                )),
            }
        }
        _ => Err(RegisterError::UnsupportedSql(
            "Only SELECT supported".to_string()
        )),
    }
}

/// Compile SQL expression to bytecode
///
/// Phase 1: Stub implementation (will be completed in Phase 2)
fn compile_expression(
    _expr: &Expr,
    _table_id: TableId,
    _catalog: &dyn SchemaCatalog,
) -> BytecodeProgram {
    // TODO: Full expression compilation in Phase 2
    // For now, just validate that it parses and return placeholder
    BytecodeProgram::new(vec![
        // Placeholder: always return Unknown
        Instruction::PushLiteral(Cell::Null),
        Instruction::IsNull,  // NULL IS NULL → True (temporary)
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::{PostgreSqlDialect, MySqlDialect, SQLiteDialect};
    use std::collections::HashMap;

    struct MockCatalog {
        tables: HashMap<String, (TableId, usize)>, // TableId, arity
        columns: HashMap<(TableId, String), u16>,
    }

    impl SchemaCatalog for MockCatalog {
        fn table_id(&self, table_name: &str) -> Option<TableId> {
            self.tables.get(table_name).map(|(id, _)| *id)
        }

        fn column_id(&self, table_id: TableId, column_name: &str) -> Option<u16> {
            self.columns.get(&(table_id, column_name.to_string())).copied()
        }

        fn table_arity(&self, table_id: TableId) -> Option<usize> {
            self.tables.values()
                .find(|(id, _)| *id == table_id)
                .map(|(_, arity)| *arity)
        }

        fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
            Some(0xABCD_1234_5678_9ABC)
        }
    }

    fn make_catalog() -> MockCatalog {
        let mut tables = HashMap::new();
        tables.insert("users".to_string(), (1, 5));
        tables.insert("orders".to_string(), (2, 7));

        let mut columns = HashMap::new();
        columns.insert((1, "id".to_string()), 0);
        columns.insert((1, "age".to_string()), 1);
        columns.insert((1, "email".to_string()), 2);

        MockCatalog { tables, columns }
    }

    #[test]
    fn test_parse_postgresql_dialect() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age > 18";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (table_id, _program) = result.unwrap();
        assert_eq!(table_id, 1);
    }

    #[test]
    fn test_parse_mysql_dialect() {
        let catalog = make_catalog();
        let dialect = MySqlDialect {};

        // MySQL allows backticks
        let sql = "SELECT * FROM `users` WHERE `age` > 18";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_sqlite_dialect() {
        let catalog = make_catalog();
        let dialect = SQLiteDialect {};

        let sql = "SELECT * FROM users WHERE age > 18";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());
    }

    #[test]
    fn test_reject_joins() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
        let result = parse_and_compile(sql, &dialect, &catalog);

        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_reject_unknown_table() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM unknown_table WHERE id = 1";
        let result = parse_and_compile(sql, &dialect, &catalog);

        assert!(matches!(result, Err(RegisterError::UnknownTable(_))));
    }

    #[test]
    fn test_no_where_clause() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (table_id, program) = result.unwrap();
        assert_eq!(table_id, 1);
        // Should have trivial "always match" program
        assert!(!program.instructions.is_empty());
    }
}

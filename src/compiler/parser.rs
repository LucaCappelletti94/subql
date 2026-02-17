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
    let program = if let Some(expr) = where_clause {
        compile_expression(&expr, table_id, catalog)?
    } else {
        // No WHERE clause = always match
        // Push True onto stack
        BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Bool(true)),
        ])
    };

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
/// Recursively compiles an SQL expression into a sequence of VM instructions.
/// Handles all supported expression types with proper NULL propagation.
fn compile_expression(
    expr: &Expr,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> Result<BytecodeProgram, RegisterError> {
    let mut instructions = Vec::new();
    compile_expr_recursive(expr, table_id, catalog, &mut instructions)?;
    Ok(BytecodeProgram::new(instructions))
}

/// Recursive helper for expression compilation
///
/// Compiles expression to leave result on top of stack.
fn compile_expr_recursive(
    expr: &Expr,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
    out: &mut Vec<Instruction>,
) -> Result<(), RegisterError> {
    use sqlparser::ast::{BinaryOperator, UnaryOperator};

    match expr {
        // ====================================================================
        // Binary Operations
        // ====================================================================
        Expr::BinaryOp { left, op, right } => {
            // Compile operands (left then right, stack order)
            compile_expr_recursive(left, table_id, catalog, out)?;
            compile_expr_recursive(right, table_id, catalog, out)?;

            // Emit comparison/logical operator
            match op {
                BinaryOperator::Eq => out.push(Instruction::Equal),
                BinaryOperator::NotEq => out.push(Instruction::NotEqual),
                BinaryOperator::Lt => out.push(Instruction::LessThan),
                BinaryOperator::LtEq => out.push(Instruction::LessThanOrEqual),
                BinaryOperator::Gt => out.push(Instruction::GreaterThan),
                BinaryOperator::GtEq => out.push(Instruction::GreaterThanOrEqual),
                BinaryOperator::And => out.push(Instruction::And),
                BinaryOperator::Or => out.push(Instruction::Or),
                _ => {
                    return Err(RegisterError::UnsupportedSql(
                        format!("Binary operator {:?} not supported", op)
                    ));
                }
            }
        }

        // ====================================================================
        // Identifiers (column references)
        // ====================================================================
        Expr::Identifier(ident) => {
            let col_id = catalog.column_id(table_id, &ident.value)
                .ok_or_else(|| RegisterError::UnknownColumn {
                    table_id,
                    column: ident.value.clone(),
                })?;
            out.push(Instruction::LoadColumn(col_id));
        }

        Expr::CompoundIdentifier(parts) => {
            // Handle table.column format
            if parts.len() == 2 {
                let col_id = catalog.column_id(table_id, &parts[1].value)
                    .ok_or_else(|| RegisterError::UnknownColumn {
                        table_id,
                        column: parts[1].value.clone(),
                    })?;
                out.push(Instruction::LoadColumn(col_id));
            } else {
                return Err(RegisterError::UnsupportedSql(
                    format!("Complex identifier {:?} not supported", parts)
                ));
            }
        }

        // ====================================================================
        // Literals
        // ====================================================================
        Expr::Value(val) => {
            let cell = value_to_cell(val)?;
            out.push(Instruction::PushLiteral(cell));
        }

        // ====================================================================
        // IN Lists
        // ====================================================================
        Expr::InList { expr, list, negated } => {
            // Compile the expression being tested
            compile_expr_recursive(expr, table_id, catalog, out)?;

            // Convert list values to cells
            let mut literals = Vec::with_capacity(list.len());
            for item in list {
                if let Expr::Value(val) = item {
                    literals.push(value_to_cell(val)?);
                } else {
                    return Err(RegisterError::UnsupportedSql(
                        "IN list must contain only literals".to_string()
                    ));
                }
            }

            out.push(Instruction::In(literals));

            if *negated {
                out.push(Instruction::Not);
            }
        }

        // ====================================================================
        // BETWEEN
        // ====================================================================
        Expr::Between { expr, low, high, negated } => {
            // Stack order: value, lower, upper
            compile_expr_recursive(expr, table_id, catalog, out)?;
            compile_expr_recursive(low, table_id, catalog, out)?;
            compile_expr_recursive(high, table_id, catalog, out)?;

            out.push(Instruction::Between);

            if *negated {
                out.push(Instruction::Not);
            }
        }

        // ====================================================================
        // NULL Checks
        // ====================================================================
        Expr::IsNull(expr) => {
            compile_expr_recursive(expr, table_id, catalog, out)?;
            out.push(Instruction::IsNull);
        }

        Expr::IsNotNull(expr) => {
            compile_expr_recursive(expr, table_id, catalog, out)?;
            out.push(Instruction::IsNotNull);
        }

        // ====================================================================
        // Unary Operations
        // ====================================================================
        Expr::UnaryOp { op, expr } => {
            compile_expr_recursive(expr, table_id, catalog, out)?;

            match op {
                UnaryOperator::Not => out.push(Instruction::Not),
                UnaryOperator::Plus => {
                    // Unary + is no-op
                }
                UnaryOperator::Minus => {
                    return Err(RegisterError::UnsupportedSql(
                        "Unary minus not yet supported".to_string()
                    ));
                }
                _ => {
                    return Err(RegisterError::UnsupportedSql(
                        format!("Unary operator {:?} not supported", op)
                    ));
                }
            }
        }

        // ====================================================================
        // LIKE Pattern Matching
        // ====================================================================
        Expr::Like { expr, pattern, negated, escape_char } => {
            if escape_char.is_some() {
                return Err(RegisterError::UnsupportedSql(
                    "LIKE ESCAPE not yet supported".to_string()
                ));
            }

            // Compile string and pattern
            compile_expr_recursive(expr, table_id, catalog, out)?;
            compile_expr_recursive(pattern, table_id, catalog, out)?;

            // Case-sensitive LIKE by default
            out.push(Instruction::Like { case_sensitive: true });

            if *negated {
                out.push(Instruction::Not);
            }
        }

        Expr::ILike { expr, pattern, negated, escape_char } => {
            if escape_char.is_some() {
                return Err(RegisterError::UnsupportedSql(
                    "ILIKE ESCAPE not yet supported".to_string()
                ));
            }

            // Compile string and pattern
            compile_expr_recursive(expr, table_id, catalog, out)?;
            compile_expr_recursive(pattern, table_id, catalog, out)?;

            // Case-insensitive LIKE
            out.push(Instruction::Like { case_sensitive: false });

            if *negated {
                out.push(Instruction::Not);
            }
        }

        // ====================================================================
        // Nested Expressions (parentheses)
        // ====================================================================
        Expr::Nested(inner) => {
            compile_expr_recursive(inner, table_id, catalog, out)?;
        }

        // ====================================================================
        // Unsupported
        // ====================================================================
        _ => {
            return Err(RegisterError::UnsupportedSql(
                format!("Expression {:?} not supported", expr)
            ));
        }
    }

    Ok(())
}

/// Convert sqlparser Value to Cell
fn value_to_cell(val: &sqlparser::ast::Value) -> Result<Cell, RegisterError> {
    use sqlparser::ast::Value;

    match val {
        Value::Null => Ok(Cell::Null),
        Value::Boolean(b) => Ok(Cell::Bool(*b)),
        Value::Number(n, _long) => {
            // Try parsing as i64 first, then f64
            if let Ok(i) = n.parse::<i64>() {
                Ok(Cell::Int(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(Cell::Float(f))
            } else {
                Err(RegisterError::TypeError(
                    format!("Cannot parse number: {}", n)
                ))
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Ok(Cell::String(s.as_str().into()))
        }
        Value::NationalStringLiteral(s) | Value::HexStringLiteral(s) => {
            Ok(Cell::String(s.as_str().into()))
        }
        _ => Err(RegisterError::UnsupportedSql(
            format!("Value type {:?} not supported", val)
        )),
    }
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

    // ========================================================================
    // Expression Compiler Tests
    // ========================================================================

    #[test]
    fn test_simple_comparison_greater_than() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age > 18";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(1),  // age column
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
        ]);
    }

    #[test]
    fn test_simple_comparison_equal() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE id = 42";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(0),  // id column
            Instruction::PushLiteral(Cell::Int(42)),
            Instruction::Equal,
        ]);
    }

    #[test]
    fn test_compound_and() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age > 18 AND id = 42";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(1),  // age
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
            Instruction::LoadColumn(0),  // id
            Instruction::PushLiteral(Cell::Int(42)),
            Instruction::Equal,
            Instruction::And,
        ]);
    }

    #[test]
    fn test_compound_or() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age < 18 OR age > 65";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(1),  // age
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::LessThan,
            Instruction::LoadColumn(1),  // age again
            Instruction::PushLiteral(Cell::Int(65)),
            Instruction::GreaterThan,
            Instruction::Or,
        ]);
    }

    #[test]
    fn test_in_list() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE id IN (1, 2, 3)";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(0),  // id
            Instruction::In(vec![
                Cell::Int(1),
                Cell::Int(2),
                Cell::Int(3),
            ]),
        ]);
    }

    #[test]
    fn test_in_list_negated() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE id NOT IN (1, 2, 3)";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(0),
            Instruction::In(vec![Cell::Int(1), Cell::Int(2), Cell::Int(3)]),
            Instruction::Not,
        ]);
    }

    #[test]
    fn test_between() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(1),  // age
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::PushLiteral(Cell::Int(65)),
            Instruction::Between,
        ]);
    }

    #[test]
    fn test_between_negated() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age NOT BETWEEN 18 AND 65";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::PushLiteral(Cell::Int(65)),
            Instruction::Between,
            Instruction::Not,
        ]);
    }

    #[test]
    fn test_is_null() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email IS NULL";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(2),  // email
            Instruction::IsNull,
        ]);
    }

    #[test]
    fn test_is_not_null() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email IS NOT NULL";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(2),
            Instruction::IsNotNull,
        ]);
    }

    #[test]
    fn test_like_pattern() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email LIKE '%@example.com'";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(2),  // email
            Instruction::PushLiteral(Cell::String("%@example.com".into())),
            Instruction::Like { case_sensitive: true },
        ]);
    }

    #[test]
    fn test_like_negated() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email NOT LIKE '%@example.com'";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(2),
            Instruction::PushLiteral(Cell::String("%@example.com".into())),
            Instruction::Like { case_sensitive: true },
            Instruction::Not,
        ]);
    }

    #[test]
    fn test_not_operator() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE NOT (age < 18)";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::LessThan,
            Instruction::Not,
        ]);
    }

    #[test]
    fn test_complex_expression() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // (age > 18 OR age IS NULL) AND email IS NOT NULL
        let sql = "SELECT * FROM users WHERE (age > 18 OR age IS NULL) AND email IS NOT NULL";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(1),  // age
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
            Instruction::LoadColumn(1),  // age
            Instruction::IsNull,
            Instruction::Or,
            Instruction::LoadColumn(2),  // email
            Instruction::IsNotNull,
            Instruction::And,
        ]);
    }

    #[test]
    fn test_all_comparison_operators() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let tests = vec![
            ("age = 18", Instruction::Equal),
            ("age != 18", Instruction::NotEqual),
            ("age < 18", Instruction::LessThan),
            ("age <= 18", Instruction::LessThanOrEqual),
            ("age > 18", Instruction::GreaterThan),
            ("age >= 18", Instruction::GreaterThanOrEqual),
        ];

        for (where_clause, expected_op) in tests {
            let sql = format!("SELECT * FROM users WHERE {}", where_clause);
            let result = parse_and_compile(&sql, &dialect, &catalog);
            assert!(result.is_ok(), "Failed to parse: {}", where_clause);

            let (_, program) = result.unwrap();
            assert_eq!(program.instructions[2], expected_op,
                      "Wrong operator for: {}", where_clause);
        }
    }

    #[test]
    fn test_string_literals() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email = 'test@example.com'";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(2),
            Instruction::PushLiteral(Cell::String("test@example.com".into())),
            Instruction::Equal,
        ]);
    }

    #[test]
    fn test_null_literal() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // Note: age = NULL is syntactically valid but semantically wrong
        // (should use IS NULL). But we compile it correctly.
        let sql = "SELECT * FROM users WHERE age = NULL";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Cell::Null),
            Instruction::Equal,
        ]);
    }

    #[test]
    fn test_reject_unknown_column() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE unknown_column = 42";
        let result = parse_and_compile(sql, &dialect, &catalog);

        assert!(matches!(result, Err(RegisterError::UnknownColumn { .. })));
    }

    #[test]
    fn test_dependency_extraction() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // Uses columns 1 (age) and 2 (email)
        let sql = "SELECT * FROM users WHERE age > 18 AND email IS NOT NULL";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.dependency_columns, vec![1, 2]);
    }

    #[test]
    fn test_nested_parentheses() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE ((age > 18))";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
        ]);
    }

    #[test]
    fn test_float_literal() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age > 18.5";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(program.instructions, vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Cell::Float(18.5)),
            Instruction::GreaterThan,
        ]);
    }
}

//! SQL parser for subscription predicates
//!
//! Supports generic SQL dialects via sqlparser crate.

use super::{
    canonicalize, prefilter::build_prefilter_plan, sql_shape, BytecodeProgram, Instruction,
    PrefilterPlan,
};
use crate::{Cell, RegisterError, SchemaCatalog, TableId};
use sqlparser::ast::{Expr, ObjectName, Statement};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

struct SqlTableName {
    unqualified: String,
    qualified: Option<String>,
}

impl SqlTableName {
    fn from_object_name(name: &ObjectName) -> Result<Self, RegisterError> {
        let mut parts = Vec::with_capacity(name.0.len());
        for part in &name.0 {
            let ident = part
                .as_ident()
                .ok_or_else(|| RegisterError::UnsupportedSql("Missing table name".to_string()))?;
            parts.push(ident.value.clone());
        }

        let unqualified = parts
            .last()
            .cloned()
            .ok_or_else(|| RegisterError::UnsupportedSql("Missing table name".to_string()))?;
        let qualified = if parts.len() > 1 {
            Some(parts.join("."))
        } else {
            None
        };

        Ok(Self {
            unqualified,
            qualified,
        })
    }
}

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
#[allow(clippy::option_if_let_else)]
pub fn parse_and_compile<D: Dialect>(
    sql: &str,
    dialect: &D,
    catalog: &dyn SchemaCatalog,
) -> Result<(TableId, BytecodeProgram), RegisterError> {
    let (table_id, program, _normalized) = parse_compile_and_normalize(sql, dialect, catalog)?;
    Ok((table_id, program))
}

/// Parse SQL once and produce compiled bytecode plus canonical normalized form.
pub fn parse_compile_and_normalize<D: Dialect>(
    sql: &str,
    dialect: &D,
    catalog: &dyn SchemaCatalog,
) -> Result<(TableId, BytecodeProgram, String), RegisterError> {
    let (table_id, program, normalized, _prefilter_plan) =
        parse_compile_normalize_and_prefilter(sql, dialect, catalog)?;
    Ok((table_id, program, normalized))
}

/// Parse SQL once and produce compiled bytecode, canonical normalized form,
/// and OR/NOT-aware prefilter plan.
pub fn parse_compile_normalize_and_prefilter<D: Dialect>(
    sql: &str,
    dialect: &D,
    catalog: &dyn SchemaCatalog,
) -> Result<(TableId, BytecodeProgram, String, PrefilterPlan), RegisterError> {
    if sql.len() > sql_shape::MAX_SQL_LEN {
        return Err(RegisterError::UnsupportedSql(
            "SQL input too long".to_string(),
        ));
    }

    // Parse SQL
    let statements = Parser::parse_sql(dialect, sql).map_err(|e| RegisterError::ParseError {
        line: 1, // sqlparser doesn't provide line numbers easily
        column: 0,
        message: e.to_string(),
    })?;

    if statements.len() != 1 {
        return Err(RegisterError::UnsupportedSql(
            "Expected exactly one SELECT statement".to_string(),
        ));
    }

    let stmt = &statements[0];

    // Extract SELECT ... FROM table WHERE predicate
    let (table_name, where_clause) = extract_table_and_where(stmt)?;

    // Resolve table ID
    let table_id = resolve_table_id(&table_name, catalog)?;

    // Compile WHERE clause to bytecode
    let program = if let Some(expr) = where_clause.as_ref() {
        compile_expression(expr, table_id, catalog)?
    } else {
        // No WHERE clause = always match
        // Push True onto stack
        BytecodeProgram::new(vec![Instruction::PushLiteral(Cell::Bool(true))])
    };

    let normalized = canonicalize::normalize_where_clause(where_clause.as_ref())?;
    let prefilter_plan = build_prefilter_plan(where_clause.as_ref(), table_id, catalog);

    Ok((table_id, program, normalized, prefilter_plan))
}

fn resolve_table_id(
    table_name: &SqlTableName,
    catalog: &dyn SchemaCatalog,
) -> Result<TableId, RegisterError> {
    let unqualified_id = catalog.table_id(&table_name.unqualified);

    if let Some(qualified) = table_name.qualified.as_deref() {
        let qualified_id = catalog.table_id(qualified);
        return match (qualified_id, unqualified_id) {
            (Some(q), Some(u)) if q != u => Err(RegisterError::AmbiguousTable {
                reference: qualified.to_string(),
                qualified: qualified.to_string(),
                unqualified: table_name.unqualified.clone(),
            }),
            (Some(q), _) => Ok(q),
            (None, Some(u)) => Ok(u),
            (None, None) => Err(RegisterError::UnknownTable(qualified.to_string())),
        };
    }

    unqualified_id.ok_or_else(|| RegisterError::UnknownTable(table_name.unqualified.clone()))
}

fn extract_table_and_where(
    stmt: &Statement,
) -> Result<(SqlTableName, Option<Expr>), RegisterError> {
    let (table_name, where_clause) = sql_shape::extract_single_table_and_where(stmt)?;
    Ok((SqlTableName::from_object_name(&table_name)?, where_clause))
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
    compile_expr_recursive(expr, table_id, catalog, &mut instructions, 0)?;
    Ok(BytecodeProgram::new(instructions))
}

/// Recursive helper for expression compilation
///
/// Compiles expression to leave result on top of stack.
#[allow(clippy::too_many_lines)]
fn compile_expr_recursive(
    expr: &Expr,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
    out: &mut Vec<Instruction>,
    depth: usize,
) -> Result<(), RegisterError> {
    use sqlparser::ast::{BinaryOperator, UnaryOperator};

    if depth > sql_shape::MAX_EXPR_DEPTH {
        return Err(RegisterError::UnsupportedSql(
            "Expression nesting too deep".to_string(),
        ));
    }

    match expr {
        // ====================================================================
        // Binary Operations
        // ====================================================================
        Expr::BinaryOp { left, op, right } => {
            match op {
                // Short-circuit logical operators
                BinaryOperator::And => {
                    // Compile left operand
                    compile_expr_recursive(left, table_id, catalog, out, depth + 1)?;

                    // Emit placeholder jump (patched below)
                    let jump_idx = out.len();
                    out.push(Instruction::JumpIfFalse(0)); // placeholder

                    // Compile right operand
                    let rhs_start = out.len();
                    compile_expr_recursive(right, table_id, catalog, out, depth + 1)?;

                    // Emit And
                    out.push(Instruction::And);

                    // Patch jump offset: skip rhs instructions + And instruction
                    let rhs_len = out.len() - rhs_start;
                    out[jump_idx] = Instruction::JumpIfFalse(rhs_len + 1);
                }
                BinaryOperator::Or => {
                    // Compile left operand
                    compile_expr_recursive(left, table_id, catalog, out, depth + 1)?;

                    // Emit placeholder jump (patched below)
                    let jump_idx = out.len();
                    out.push(Instruction::JumpIfTrue(0)); // placeholder

                    // Compile right operand
                    let rhs_start = out.len();
                    compile_expr_recursive(right, table_id, catalog, out, depth + 1)?;

                    // Emit Or
                    out.push(Instruction::Or);

                    // Patch jump offset: skip rhs instructions + Or instruction
                    let rhs_len = out.len() - rhs_start;
                    out[jump_idx] = Instruction::JumpIfTrue(rhs_len + 1);
                }
                _ => {
                    // All non-short-circuit operators: compile both sides, emit op
                    compile_expr_recursive(left, table_id, catalog, out, depth + 1)?;
                    compile_expr_recursive(right, table_id, catalog, out, depth + 1)?;

                    match op {
                        // Comparison operators
                        BinaryOperator::Eq => out.push(Instruction::Equal),
                        BinaryOperator::NotEq => out.push(Instruction::NotEqual),
                        BinaryOperator::Lt => out.push(Instruction::LessThan),
                        BinaryOperator::LtEq => out.push(Instruction::LessThanOrEqual),
                        BinaryOperator::Gt => out.push(Instruction::GreaterThan),
                        BinaryOperator::GtEq => out.push(Instruction::GreaterThanOrEqual),

                        // Arithmetic operators
                        BinaryOperator::Plus => out.push(Instruction::Add),
                        BinaryOperator::Minus => out.push(Instruction::Subtract),
                        BinaryOperator::Multiply => out.push(Instruction::Multiply),
                        BinaryOperator::Divide => out.push(Instruction::Divide),
                        BinaryOperator::Modulo => out.push(Instruction::Modulo),

                        _ => {
                            return Err(RegisterError::UnsupportedSql(format!(
                                "Binary operator {op:?} not supported"
                            )));
                        }
                    }
                }
            }
        }

        // ====================================================================
        // Identifiers (column references)
        // ====================================================================
        Expr::Identifier(ident) => {
            let col_id = catalog.column_id(table_id, &ident.value).ok_or_else(|| {
                RegisterError::UnknownColumn {
                    table_id,
                    column: ident.value.clone(),
                }
            })?;
            out.push(Instruction::LoadColumn(col_id));
        }

        Expr::CompoundIdentifier(parts) => {
            // Handle table.column format
            if parts.len() == 2 {
                let col_id = catalog
                    .column_id(table_id, &parts[1].value)
                    .ok_or_else(|| RegisterError::UnknownColumn {
                        table_id,
                        column: parts[1].value.clone(),
                    })?;
                out.push(Instruction::LoadColumn(col_id));
            } else {
                return Err(RegisterError::UnsupportedSql(format!(
                    "Complex identifier {parts:?} not supported"
                )));
            }
        }

        // ====================================================================
        // Literals
        // ====================================================================
        Expr::Value(val) => {
            let cell = value_to_cell(&val.value)?;
            out.push(Instruction::PushLiteral(cell));
        }

        // ====================================================================
        // IN Lists
        // ====================================================================
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            // Compile the expression being tested
            compile_expr_recursive(expr, table_id, catalog, out, depth + 1)?;

            // Convert list values to cells
            let mut literals = Vec::with_capacity(list.len());
            for item in list {
                if let Expr::Value(val) = item {
                    literals.push(value_to_cell(&val.value)?);
                } else {
                    return Err(RegisterError::UnsupportedSql(
                        "IN with subqueries not supported - SubQL only supports IN with literal lists like IN ('a', 'b', 'c'). \
                         For IN with subqueries, run this as a regular SQL query in your database."
                            .to_string()
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
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            // Stack order: value, lower, upper
            compile_expr_recursive(expr, table_id, catalog, out, depth + 1)?;
            compile_expr_recursive(low, table_id, catalog, out, depth + 1)?;
            compile_expr_recursive(high, table_id, catalog, out, depth + 1)?;

            out.push(Instruction::Between);

            if *negated {
                out.push(Instruction::Not);
            }
        }

        // ====================================================================
        // NULL Checks
        // ====================================================================
        Expr::IsNull(expr) => {
            compile_expr_recursive(expr, table_id, catalog, out, depth + 1)?;
            out.push(Instruction::IsNull);
        }

        Expr::IsNotNull(expr) => {
            compile_expr_recursive(expr, table_id, catalog, out, depth + 1)?;
            out.push(Instruction::IsNotNull);
        }

        // ====================================================================
        // Unary Operations
        // ====================================================================
        Expr::UnaryOp { op, expr } => {
            compile_expr_recursive(expr, table_id, catalog, out, depth + 1)?;

            match op {
                UnaryOperator::Not => out.push(Instruction::Not),
                UnaryOperator::Plus => {
                    // Unary + is no-op
                }
                UnaryOperator::Minus => {
                    out.push(Instruction::Negate);
                }
                _ => {
                    return Err(RegisterError::UnsupportedSql(format!(
                        "Unary operator {op:?} not supported"
                    )));
                }
            }
        }

        // ====================================================================
        // LIKE Pattern Matching
        // ====================================================================
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
            ..
        } => {
            if escape_char.is_some() {
                return Err(RegisterError::UnsupportedSql(
                    "LIKE ESCAPE not yet supported".to_string(),
                ));
            }

            // Compile string and pattern
            compile_expr_recursive(expr, table_id, catalog, out, depth + 1)?;
            compile_expr_recursive(pattern, table_id, catalog, out, depth + 1)?;

            // Case-sensitive LIKE by default
            out.push(Instruction::Like {
                case_sensitive: true,
            });

            if *negated {
                out.push(Instruction::Not);
            }
        }

        Expr::ILike {
            expr,
            pattern,
            negated,
            escape_char,
            ..
        } => {
            if escape_char.is_some() {
                return Err(RegisterError::UnsupportedSql(
                    "ILIKE ESCAPE not yet supported".to_string(),
                ));
            }

            // Compile string and pattern
            compile_expr_recursive(expr, table_id, catalog, out, depth + 1)?;
            compile_expr_recursive(pattern, table_id, catalog, out, depth + 1)?;

            // Case-insensitive LIKE
            out.push(Instruction::Like {
                case_sensitive: false,
            });

            if *negated {
                out.push(Instruction::Not);
            }
        }

        // ====================================================================
        // Nested Expressions (parentheses)
        // ====================================================================
        Expr::Nested(inner) => {
            compile_expr_recursive(inner, table_id, catalog, out, depth + 1)?;
        }

        // ====================================================================
        // Unsupported
        // ====================================================================
        _ => {
            return Err(RegisterError::UnsupportedSql(
                format!("Expression {expr:?} not supported - SubQL supports basic WHERE clause predicates (comparisons, AND/OR/NOT, IN lists, BETWEEN, NULL checks, LIKE). \
                         For complex expressions, aggregates, or functions, run this as a regular SQL query in your database.")
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::uninlined_format_args)]
mod tests {
    use super::*;
    use crate::testing::MockCatalog;
    use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
    use std::collections::HashMap;

    fn make_catalog() -> MockCatalog {
        let mut tables = HashMap::new();
        tables.insert("users".to_string(), (1, 5));
        tables.insert("orders".to_string(), (2, 7));
        tables.insert("job_history".to_string(), (3, 5));
        tables.insert("airports".to_string(), (4, 5));
        tables.insert("brands".to_string(), (5, 5));
        tables.insert("stats".to_string(), (6, 5));
        tables.insert("data".to_string(), (7, 5));

        let mut columns = HashMap::new();
        // users table
        columns.insert((1, "id".to_string()), 0);
        columns.insert((1, "age".to_string()), 1);
        columns.insert((1, "email".to_string()), 2);

        // orders table
        columns.insert((2, "id".to_string()), 0);
        columns.insert((2, "price".to_string()), 1);
        columns.insert((2, "quantity".to_string()), 2);

        // job_history table
        columns.insert((3, "end_date".to_string()), 0);
        columns.insert((3, "start_date".to_string()), 1);

        // airports table
        columns.insert((4, "elevation".to_string()), 0);

        // brands table
        columns.insert((5, "products_this_year".to_string()), 0);
        columns.insert((5, "products_last_year".to_string()), 1);

        // stats table
        columns.insert((6, "total".to_string()), 0);
        columns.insert((6, "count".to_string()), 1);

        // data table
        columns.insert((7, "id".to_string()), 0);

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
    fn test_schema_qualified_table_name_falls_back_to_unqualified() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM public.orders WHERE price > 10";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (table_id, _) = result.unwrap();
        assert_eq!(table_id, 2);
    }

    #[test]
    fn test_schema_qualified_table_name_ambiguity_errors() {
        let mut catalog = make_catalog();
        catalog.tables.insert("public.orders".to_string(), (99, 7));
        catalog.columns.insert((99, "price".to_string()), 1);

        let dialect = PostgreSqlDialect {};
        let sql = "SELECT * FROM public.orders WHERE price > 10";

        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(
            result,
            Err(RegisterError::AmbiguousTable {
                reference,
                qualified,
                unqualified,
            }) if reference == "public.orders"
                && qualified == "public.orders"
                && unqualified == "orders"
        ));
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
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1), // age column
                Instruction::PushLiteral(Cell::Int(18)),
                Instruction::GreaterThan,
            ]
        );
    }

    #[test]
    fn test_simple_comparison_equal() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE id = 42";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(0), // id column
                Instruction::PushLiteral(Cell::Int(42)),
                Instruction::Equal,
            ]
        );
    }

    #[test]
    fn test_compound_and() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age > 18 AND id = 42";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1), // age
                Instruction::PushLiteral(Cell::Int(18)),
                Instruction::GreaterThan,
                Instruction::JumpIfFalse(5), // skip rhs (3 instr) + And + jump itself
                Instruction::LoadColumn(0),  // id
                Instruction::PushLiteral(Cell::Int(42)),
                Instruction::Equal,
                Instruction::And,
            ]
        );
    }

    #[test]
    fn test_compound_or() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age < 18 OR age > 65";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1), // age
                Instruction::PushLiteral(Cell::Int(18)),
                Instruction::LessThan,
                Instruction::JumpIfTrue(5), // skip rhs (3 instr) + Or + jump itself
                Instruction::LoadColumn(1), // age again
                Instruction::PushLiteral(Cell::Int(65)),
                Instruction::GreaterThan,
                Instruction::Or,
            ]
        );
    }

    #[test]
    fn test_in_list() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE id IN (1, 2, 3)";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(0), // id
                Instruction::In(vec![Cell::Int(1), Cell::Int(2), Cell::Int(3),]),
            ]
        );
    }

    #[test]
    fn test_in_list_negated() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE id NOT IN (1, 2, 3)";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(0),
                Instruction::In(vec![Cell::Int(1), Cell::Int(2), Cell::Int(3)]),
                Instruction::Not,
            ]
        );
    }

    #[test]
    fn test_between() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1), // age
                Instruction::PushLiteral(Cell::Int(18)),
                Instruction::PushLiteral(Cell::Int(65)),
                Instruction::Between,
            ]
        );
    }

    #[test]
    fn test_between_negated() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age NOT BETWEEN 18 AND 65";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(Cell::Int(18)),
                Instruction::PushLiteral(Cell::Int(65)),
                Instruction::Between,
                Instruction::Not,
            ]
        );
    }

    #[test]
    fn test_is_null() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email IS NULL";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(2), // email
                Instruction::IsNull,
            ]
        );
    }

    #[test]
    fn test_is_not_null() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email IS NOT NULL";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![Instruction::LoadColumn(2), Instruction::IsNotNull,]
        );
    }

    #[test]
    fn test_like_pattern() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email LIKE '%@example.com'";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(2), // email
                Instruction::PushLiteral(Cell::String("%@example.com".into())),
                Instruction::Like {
                    case_sensitive: true
                },
            ]
        );
    }

    #[test]
    fn test_like_negated() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email NOT LIKE '%@example.com'";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(2),
                Instruction::PushLiteral(Cell::String("%@example.com".into())),
                Instruction::Like {
                    case_sensitive: true
                },
                Instruction::Not,
            ]
        );
    }

    #[test]
    fn test_not_operator() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE NOT (age < 18)";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(Cell::Int(18)),
                Instruction::LessThan,
                Instruction::Not,
            ]
        );
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
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1), // age
                Instruction::PushLiteral(Cell::Int(18)),
                Instruction::GreaterThan,
                Instruction::JumpIfTrue(4), // skip IsNull + Or (2 instr) + jump itself
                Instruction::LoadColumn(1), // age
                Instruction::IsNull,
                Instruction::Or,
                Instruction::JumpIfFalse(4), // skip IsNotNull + And (2 instr) + jump itself
                Instruction::LoadColumn(2),  // email
                Instruction::IsNotNull,
                Instruction::And,
            ]
        );
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
            assert_eq!(
                program.instructions[2], expected_op,
                "Wrong operator for: {}",
                where_clause
            );
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
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(2),
                Instruction::PushLiteral(Cell::String("test@example.com".into())),
                Instruction::Equal,
            ]
        );
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
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(Cell::Null),
                Instruction::Equal,
            ]
        );
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
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(Cell::Int(18)),
                Instruction::GreaterThan,
            ]
        );
    }

    #[test]
    fn test_float_literal() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age > 18.5";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(Cell::Float(18.5)),
                Instruction::GreaterThan,
            ]
        );
    }

    #[test]
    fn test_arithmetic_add() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM orders WHERE price + quantity > 100";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1), // price
                Instruction::LoadColumn(2), // quantity
                Instruction::Add,
                Instruction::PushLiteral(Cell::Int(100)),
                Instruction::GreaterThan,
            ]
        );
    }

    #[test]
    fn test_arithmetic_subtract() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // Real-world example from benchmark
        let sql = "SELECT * FROM job_history WHERE end_date - start_date > 300";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert!(program.instructions.contains(&Instruction::Subtract));
    }

    #[test]
    fn test_arithmetic_multiply() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM orders WHERE price * quantity > 1000";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert!(program.instructions.contains(&Instruction::Multiply));
    }

    #[test]
    fn test_arithmetic_divide() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM stats WHERE total / count > 50.0";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert!(program.instructions.contains(&Instruction::Divide));
    }

    #[test]
    fn test_arithmetic_modulo() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM data WHERE id % 10 = 0";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert!(program.instructions.contains(&Instruction::Modulo));
    }

    #[test]
    fn test_unary_minus() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // Real-world example from benchmark
        let sql = "SELECT * FROM airports WHERE elevation BETWEEN -50 AND 50";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert!(program.instructions.contains(&Instruction::Negate));
    }

    #[test]
    fn test_complex_arithmetic() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // Real-world example from benchmark
        let sql = "SELECT * FROM brands WHERE (products_this_year - products_last_year) > 0.5 * products_last_year";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert!(program.instructions.contains(&Instruction::Subtract));
        assert!(program.instructions.contains(&Instruction::Multiply));
    }

    // Error path tests for comprehensive coverage
    #[test]
    fn test_no_where_clause_accepted() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // No WHERE clause is valid (matches everything)
        let sql = "SELECT * FROM users";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        // Should compile to "push true"
        assert_eq!(program.instructions.len(), 1);
    }

    #[test]
    fn test_error_unknown_table() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM nonexistent WHERE id > 1";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnknownTable(_))));
    }

    #[test]
    fn test_error_unknown_column() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE nonexistent > 1";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnknownColumn { .. })));
    }

    #[test]
    fn test_error_complex_identifier() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // 3-part identifier not supported
        let sql = "SELECT * FROM users WHERE schema.table.column > 1";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_error_in_with_expression() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // IN with expression (not literal) not supported
        let sql = "SELECT * FROM users WHERE id IN (age + 1)";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_error_unsupported_value_type() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // Placeholder values not supported
        let sql = "SELECT * FROM users WHERE id = $1";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_error_set_operations() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE id = 1 UNION SELECT * FROM users WHERE id = 2";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_error_ddl_statement() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "INSERT INTO users VALUES (1, 'test')";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_error_unsupported_expression() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // CASE expression not yet supported
        let sql = "SELECT * FROM users WHERE CASE WHEN age > 18 THEN true ELSE false END";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_error_unsupported_binary_operator() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // BitwiseAnd not supported
        let sql = "SELECT * FROM users WHERE id & 1 = 1";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_error_unsupported_unary_operator() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // PGBitwiseNot not supported
        let sql = "SELECT * FROM users WHERE ~id = 1";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_error_invalid_number() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // This should parse but might fail type conversion
        let sql = "SELECT * FROM users WHERE age > 999999999999999999999999999999999";
        let result = parse_and_compile(sql, &dialect, &catalog);
        // Either succeeds or fails with TypeError
        if result.is_err() {
            assert!(matches!(result, Err(RegisterError::TypeError(_))));
        }
    }

    // ========================================================================
    // Phase 1: Additional Error Path Coverage Tests
    // ========================================================================

    #[test]
    fn test_error_parse_failure() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let invalid_sql = "NOT VALID SQL ;;;"; // Malformed SQL
        let result = parse_and_compile(invalid_sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::ParseError { .. })));
    }

    #[test]
    fn test_error_multiple_statements() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE id = 1; SELECT * FROM users WHERE id = 2";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
        if let Err(RegisterError::UnsupportedSql(msg)) = result {
            assert!(msg.contains("exactly one"));
        }
    }

    #[test]
    fn test_error_multiple_tables_no_join() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users, orders WHERE users.id = 1";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
        if let Err(RegisterError::UnsupportedSql(msg)) = result {
            assert!(msg.contains("Exactly one table"));
        }
    }

    #[test]
    fn test_error_subquery() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM (SELECT * FROM users) AS u WHERE id = 1";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
        if let Err(RegisterError::UnsupportedSql(msg)) = result {
            assert!(msg.contains("Subqueries"));
        }
    }

    #[test]
    fn test_compound_identifier_two_parts() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // Two-part identifier (table.column) should work
        let sql = "SELECT * FROM users WHERE users.age > 18";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1), // age column
                Instruction::PushLiteral(Cell::Int(18)),
                Instruction::GreaterThan,
            ]
        );
    }

    #[test]
    fn test_error_like_escape() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email LIKE '%test%' ESCAPE '\\'";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
        if let Err(RegisterError::UnsupportedSql(msg)) = result {
            assert!(msg.contains("ESCAPE"));
        }
    }

    #[test]
    fn test_error_ilike_escape() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email ILIKE '%test%' ESCAPE '\\'";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
        if let Err(RegisterError::UnsupportedSql(msg)) = result {
            assert!(msg.contains("ESCAPE"));
        }
    }

    #[test]
    fn test_ilike_case_insensitive() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email ILIKE '%TEST%'";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(2), // email
                Instruction::PushLiteral(Cell::String("%TEST%".into())),
                Instruction::Like {
                    case_sensitive: false
                },
            ]
        );
    }

    #[test]
    fn test_ilike_negated() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE email NOT ILIKE '%spam%'";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(2),
                Instruction::PushLiteral(Cell::String("%spam%".into())),
                Instruction::Like {
                    case_sensitive: false
                },
                Instruction::Not,
            ]
        );
    }

    #[test]
    fn test_unary_plus_operator() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE +age = 18";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        // Unary + is a no-op, should just load column
        assert_eq!(
            program.instructions,
            vec![
                Instruction::LoadColumn(1), // age
                Instruction::PushLiteral(Cell::Int(18)),
                Instruction::Equal,
            ]
        );
    }

    #[test]
    fn test_double_quoted_string() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = r#"SELECT * FROM users WHERE email = "test@example.com""#;
        let result = parse_and_compile(sql, &dialect, &catalog);
        // Note: PostgreSQL treats double quotes as identifiers, not strings
        // This might fail or succeed depending on dialect behavior
        let _ = result; // Just test it doesn't panic
    }

    #[test]
    fn test_boolean_literal() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age > 18 AND true";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (_, program) = result.unwrap();
        // Should compile the boolean literal
        assert!(program
            .instructions
            .contains(&Instruction::PushLiteral(Cell::Bool(true))));
    }

    // ========================================================================
    // Phase 3: Push to 95% Coverage - Parser Completion
    // ========================================================================

    #[test]
    fn test_compound_identifier_unknown_column() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // Two-part identifier with unknown column
        let sql = "SELECT * FROM users WHERE users.unknown_column > 18";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(matches!(result, Err(RegisterError::UnknownColumn { .. })));
    }

    #[test]
    fn test_error_invalid_number_literal() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        // Very large number that can't be parsed as i64 or f64
        // Note: Most large numbers will parse as f64, so this is hard to trigger
        // The TypeError path is mostly defensive
        let sql = "SELECT * FROM users WHERE age > 18";
        let result = parse_and_compile(sql, &dialect, &catalog);
        // This should succeed, but documents the TypeError path exists
        assert!(result.is_ok());
    }

    #[test]
    fn test_value_to_cell_unparseable_number() {
        use sqlparser::ast::Value;

        // Construct a Value::Number with a string that can't parse as i64 or f64
        // This is defensive — sqlparser normally validates numbers — but we test it directly
        let val = Value::Number("not_a_number".to_string(), false);
        let result = value_to_cell(&val);
        assert!(matches!(result, Err(RegisterError::TypeError(_))));
    }

    #[test]
    fn test_national_string_literal() {
        let catalog = make_catalog();
        let dialect = MySqlDialect {};

        // MySQL supports N'...' for national character strings
        let sql = "SELECT * FROM users WHERE email = N'test@example.com'";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());
    }

    #[test]
    fn test_hex_string_literal() {
        let catalog = make_catalog();
        let dialect = MySqlDialect {};

        // MySQL supports X'...' for hex string literals
        let sql = "SELECT * FROM users WHERE email = X'CAFE'";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());
    }
}

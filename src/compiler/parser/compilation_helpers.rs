//! Expression compilation helpers split out of the parser.

use super::{Compiling, MAX_TERMS_PER_FILTER};
use crate::backend::{Backend, BuiltinKind, Value, ValueKindOf};
use crate::compiler::bytecode::{ComparisonRef, FloatResult};
use crate::compiler::literals::{resolve_column_ref, SqlLiteralParse};
use crate::compiler::{canonicalize, sql_shape, BytecodeProgram, Instruction};
use crate::term::{term_columns, CompiledTerm};
use crate::{RegisterError, TableId};
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue};
use sqlparser_canonicalize::Canonicalizer;

/// If `expr` is a bare column reference, return what a value of that column
/// is, via the catalog. Otherwise `None`. Used to derive the target for a
/// paired literal in a comparison or an IN list.
///
/// A value kind rather than the column's declared type, because that is what
/// a literal can be parsed at: the spelling `'0.1'` says nothing about the
/// width the column declares.
fn column_scalar_of<B: Backend, DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
) -> Option<ValueKindOf<B>> {
    let col = resolve_column_ref(expr, table_id, database)?;
    crate::catalog_helpers::column_scalar_kind::<B, DB>(database, table_id, col)
        .map(|kind| kind.value_kind())
}

/// The [`crate::backend::ScalarKind`] of the first column `expr` names, looking
/// through the arithmetic and grouping a comparison side may wrap it in.
///
/// A bare column is the common case, but `amount * quantity > 100` carries its
/// columns one level down, and typing the literal against the table's first
/// text column instead refuses the number.
///
/// Stops at [`sql_shape::MAX_EXPR_DEPTH`], the ceiling compilation itself
/// refuses past, so a flat operator chain cannot walk the stack down here
/// before the compiler reports it.
fn nested_column_scalar_of<B: Backend, DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
    depth: usize,
) -> Option<ValueKindOf<B>> {
    if let Some(kind) = column_scalar_of::<B, DB>(expr, table_id, database) {
        return Some(kind);
    }
    if depth >= sql_shape::MAX_EXPR_DEPTH {
        return None;
    }
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let operand = nested_column_scalar_of::<B, DB>(left, table_id, database, depth + 1)
                .or_else(|| nested_column_scalar_of::<B, DB>(right, table_id, database, depth + 1));
            quotient_kind::<B>(op, operand)
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            nested_column_scalar_of::<B, DB>(expr, table_id, database, depth + 1)
        }
        _ => None,
    }
}

/// The kind a binary operation answers, given the kind its operands carry.
///
/// Only `/` moves it, and only where the engine's `/` answers a decimal:
/// MySQL's `qty / 3` is a decimal even though `qty` is an integer, so a
/// literal compared against it has to be read as a decimal too. A float
/// operand keeps its own kind, since a float divided there stays a double
/// rather than becoming a decimal.
fn quotient_kind<B: Backend>(
    op: &BinaryOperator,
    operand: Option<ValueKindOf<B>>,
) -> Option<ValueKindOf<B>> {
    if !matches!(op, BinaryOperator::Divide)
        || !matches!(
            B::DIVISION,
            crate::backend::DivisionRule::QuotientsAreDecimalInWords
        )
    {
        return operand;
    }
    if operand == Some(BuiltinKind::Float.into()) {
        return operand;
    }
    Some(BuiltinKind::Decimal.into())
}

/// Return `true` if `instr` produces a [`crate::compiler::Tri`] on the
/// stack. Used to detect whether a top-level WHERE program leaves a
/// boolean at TOS or needs to be wrapped with `= true`.
const fn instruction_is_tri_typed<B: Backend>(instr: &Instruction<B>) -> bool {
    matches!(
        instr,
        Instruction::Equal(_)
            | Instruction::NotEqual(_)
            | Instruction::LessThan(_)
            | Instruction::LessThanOrEqual(_)
            | Instruction::GreaterThan(_)
            | Instruction::GreaterThanOrEqual(_)
            | Instruction::IsNull
            | Instruction::IsNotNull
            | Instruction::And
            | Instruction::Or
            | Instruction::Not
            | Instruction::In { .. }
            | Instruction::Between { .. }
            | Instruction::Like { .. }
            | Instruction::JumpIfFalse(_)
            | Instruction::JumpIfTrue(_)
            | Instruction::TermTruth(_)
    )
}

/// Wrap the tail of a WHERE program that finishes with a `Value<B>`
/// (bare column ref, arithmetic result, bare literal) in an explicit
/// `= true` comparison so the VM's final-result contract holds.
///
/// A no-op when the trailing instruction already produces a Tri.
pub(super) fn wrap_bare_value_as_tri<B>(
    instructions: &mut Vec<Instruction<B>>,
    comparison: ComparisonRef,
) -> Result<(), RegisterError>
where
    B: Backend + SqlLiteralParse,
{
    match instructions.last() {
        Some(instr) if instruction_is_tri_typed(instr) => Ok(()),
        Some(_) => {
            instructions.push(Instruction::PushLiteral(B::parse_literal(
                &SqlValue::Boolean(true),
                BuiltinKind::Bool.into(),
            )?));
            instructions.push(Instruction::Equal(comparison));
            Ok(())
        }
        None => Err(RegisterError::UnsupportedSql(
            "empty WHERE clause after compilation".to_string(),
        )),
    }
}

/// Compile a SQL expression into bytecode, plus the membership terms it names.
///
/// Recursively compiles an SQL expression into a sequence of VM
/// instructions. Handles all supported expression types with proper NULL
/// propagation. Appends the bare-value rescue at the end so the VM's
/// final-result contract holds even for `WHERE bool_col` and similar.
///
/// A membership term compiles to one [`Instruction::TermTruth`] and travels out
/// beside the program: the VM is handed its truth rather than computing it,
/// because the same row admits different subscribers through it.
pub(super) fn compile_expression<B, DB>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
    canonicalizer: &Canonicalizer<'_>,
    increment: Option<crate::backend::DivisionPrecisionIncrement>,
) -> Result<(BytecodeProgram<B>, Vec<CompiledTerm>), RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let mut compiling: Compiling<B> = Compiling::new(increment);
    compile_expr_recursive::<B, DB>(
        expr,
        table_id,
        database,
        &mut compiling,
        0,
        BuiltinKind::String.into(),
    )?;
    let bare = ComparisonRef::new(compiling.intern_comparison(expr, table_id, database), None);
    wrap_bare_value_as_tri::<B>(&mut compiling.out, bare)?;
    let terms = canonicalize_term_slots(&mut compiling, canonicalizer)?;
    let columns = term_columns(&terms);
    Ok((
        BytecodeProgram::with_comparisons(compiling.out, columns, compiling.comparisons),
        terms,
    ))
}

/// Renumber the term slots into normalized-text order and rewrite the
/// program's [`Instruction::TermTruth`] operands to match.
///
/// Predicate identity is the normalized WHERE text, which sorts `AND`/`OR`
/// operands, so two spellings of one filter share one predicate. Slots were
/// assigned in source order, and a subscription binding to a shared predicate
/// seeds by its own compile's slots, so the numbering has to be a function of
/// the normalized text too, or a reversed spelling stores one column's values
/// where dispatch reads another's.
///
/// Two terms cannot normalize alike: the text carries the compared column,
/// and two terms comparing one column are refused at registration, so the
/// order below never ties on filters SubQL serves.
fn canonicalize_term_slots<B: Backend>(
    compiling: &mut Compiling<B>,
    canonicalizer: &Canonicalizer<'_>,
) -> Result<Vec<CompiledTerm>, RegisterError> {
    let terms = core::mem::take(&mut compiling.terms);
    if terms.len() < 2 {
        return Ok(terms);
    }

    let mut keyed: Vec<(String, CompiledTerm)> = terms
        .into_iter()
        .map(|term| {
            let text = canonicalize::normalize_where_clause(Some(&term.expr), canonicalizer)?;
            Ok((text, term))
        })
        .collect::<Result<_, RegisterError>>()?;
    keyed.sort_by(|left, right| left.0.cmp(&right.0));

    let mut remap = [0u16; MAX_TERMS_PER_FILTER];
    let mut sorted = Vec::with_capacity(keyed.len());
    for (new_slot, (_, mut term)) in keyed.into_iter().enumerate() {
        let new_slot = u16::try_from(new_slot).unwrap_or(u16::MAX);
        remap[usize::from(term.slot)] = new_slot;
        term.slot = new_slot;
        sorted.push(term);
    }
    for instruction in &mut compiling.out {
        if let Instruction::TermTruth(slot) = instruction {
            *slot = remap[usize::from(*slot)];
        }
    }

    Ok(sorted)
}

/// The width an expression's float result is held at, or `None` when it is
/// not float arithmetic.
///
/// Resolved bottom-up from the columns the expression names, because the
/// engines decide it per operation rather than per operand: measured,
/// PostgreSQL computes `real + real` in float4 and promotes `real * 3` to
/// double precision. A literal carries no declared width and so lands in
/// the promoting arm, which is what the server does with `3`.
///
/// Stops at [`sql_shape::MAX_EXPR_DEPTH`], the ceiling compilation itself
/// refuses past.
fn float_result_width<B: Backend, DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
    depth: usize,
) -> FloatResult {
    if let Some(column) = resolve_column_ref(expr, table_id, database) {
        return crate::catalog_helpers::column_comparison::<B, DB>(database, table_id, column)
            .and_then(|facts| facts.kind.builtin())
            .and_then(crate::backend::BuiltinType::float_width);
    }
    if depth >= sql_shape::MAX_EXPR_DEPTH {
        return None;
    }
    match expr {
        Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
            float_result_width::<B, DB>(inner, table_id, database, depth + 1)
        }
        Expr::BinaryOp {
            left,
            op:
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo,
            right,
        } => B::float_arithmetic_width(
            float_result_width::<B, DB>(left, table_id, database, depth + 1),
            float_result_width::<B, DB>(right, table_id, database, depth + 1),
        ),
        _ => None,
    }
}

/// Recursive helper for expression compilation.
///
/// Compiles an expression to leave its result on top of stack. The
/// `target_kind` argument names the [`crate::backend::ValueKind`] a standalone literal
/// leaf should coerce to; comparison / arithmetic / IN / BETWEEN /
/// LIKE arms override this per-child by peeking at whichever sibling is
/// a column reference.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn compile_expr_recursive<B, DB>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
    out: &mut Compiling<B>,
    depth: usize,
    target_kind: ValueKindOf<B>,
) -> Result<(), RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    if depth > sql_shape::MAX_EXPR_DEPTH {
        return Err(RegisterError::UnsupportedSql(
            "Expression nesting too deep".to_string(),
        ));
    }

    match expr {
        // Binary Operations
        Expr::BinaryOp { left, op, right } => {
            match op {
                // Short-circuit logical operators.
                BinaryOperator::And => {
                    compile_expr_recursive::<B, DB>(
                        left,
                        table_id,
                        database,
                        out,
                        depth + 1,
                        BuiltinKind::String.into(),
                    )?;

                    let jump_idx = out.len();
                    out.push(Instruction::JumpIfFalse(0)); // offset backfilled once rhs length is known (line 213)

                    let rhs_start = out.len();
                    compile_expr_recursive::<B, DB>(
                        right,
                        table_id,
                        database,
                        out,
                        depth + 1,
                        BuiltinKind::String.into(),
                    )?;
                    out.push(Instruction::And);

                    let rhs_len = out.len() - rhs_start;
                    out[jump_idx] = Instruction::JumpIfFalse(rhs_len + 1);
                }
                BinaryOperator::Or => {
                    compile_expr_recursive::<B, DB>(
                        left,
                        table_id,
                        database,
                        out,
                        depth + 1,
                        BuiltinKind::String.into(),
                    )?;

                    let jump_idx = out.len();
                    out.push(Instruction::JumpIfTrue(0)); // offset backfilled once rhs length is known (line 240)

                    let rhs_start = out.len();
                    compile_expr_recursive::<B, DB>(
                        right,
                        table_id,
                        database,
                        out,
                        depth + 1,
                        BuiltinKind::String.into(),
                    )?;
                    out.push(Instruction::Or);

                    let rhs_len = out.len() - rhs_start;
                    out[jump_idx] = Instruction::JumpIfTrue(rhs_len + 1);
                }
                _ => {
                    // A comparison of a column to a columnless call is the
                    // caller comparison, recognised here in every build like
                    // the membership subquery below: dispatch reads the
                    // compared column off the changed row, and the subscriber
                    // the request states is the one value the term admits.
                    // Whether the call names the caller is registration's
                    // question, since answering it needs `rls2fga`.
                    if sql_shape::is_caller_comparison(expr) {
                        let tested = if resolve_column_ref(left, table_id, database).is_some() {
                            left
                        } else {
                            right
                        };
                        if let Some(column) = resolve_column_ref(tested, table_id, database) {
                            let slot = out.term_slot(expr, alloc::vec![column])?;
                            out.push(Instruction::TermTruth(slot));
                            return Ok(());
                        }
                        // A name that resolves to no column falls through to
                        // the generic arms, whose refusal names it.
                    }

                    // Non-short-circuit operators: compile both sides,
                    // then emit the op. Target-typed literal inference
                    // picks whichever sibling is a column reference and
                    // uses its ScalarKind for the other side's literal.
                    let child_target =
                        nested_column_scalar_of::<B, DB>(left, table_id, database, depth)
                            .or_else(|| {
                                nested_column_scalar_of::<B, DB>(right, table_id, database, depth)
                            })
                            .unwrap_or_else(|| BuiltinKind::String.into());
                    compile_expr_recursive::<B, DB>(
                        left,
                        table_id,
                        database,
                        out,
                        depth + 1,
                        child_target,
                    )?;
                    compile_expr_recursive::<B, DB>(
                        right,
                        table_id,
                        database,
                        out,
                        depth + 1,
                        child_target,
                    )?;

                    // Interned inside the comparison arms only: an
                    // arithmetic instruction cannot reference a descriptor, so
                    // resolving one for its operands would persist facts no
                    // comparison reads.
                    match op {
                        BinaryOperator::Eq => {
                            let cmp = out.comparison_for(
                                left,
                                right,
                                table_id,
                                database,
                                crate::backend::TextOperation::Equality,
                            )?;
                            out.push(Instruction::Equal(cmp));
                        }
                        BinaryOperator::NotEq => {
                            let cmp = out.comparison_for(
                                left,
                                right,
                                table_id,
                                database,
                                crate::backend::TextOperation::Equality,
                            )?;
                            out.push(Instruction::NotEqual(cmp));
                        }
                        BinaryOperator::Lt => {
                            let cmp = out.comparison_for(
                                left,
                                right,
                                table_id,
                                database,
                                crate::backend::TextOperation::Ordering,
                            )?;
                            out.push(Instruction::LessThan(cmp));
                        }
                        BinaryOperator::LtEq => {
                            let cmp = out.comparison_for(
                                left,
                                right,
                                table_id,
                                database,
                                crate::backend::TextOperation::Ordering,
                            )?;
                            out.push(Instruction::LessThanOrEqual(cmp));
                        }
                        BinaryOperator::Gt => {
                            let cmp = out.comparison_for(
                                left,
                                right,
                                table_id,
                                database,
                                crate::backend::TextOperation::Ordering,
                            )?;
                            out.push(Instruction::GreaterThan(cmp));
                        }
                        BinaryOperator::GtEq => {
                            let cmp = out.comparison_for(
                                left,
                                right,
                                table_id,
                                database,
                                crate::backend::TextOperation::Ordering,
                            )?;
                            out.push(Instruction::GreaterThanOrEqual(cmp));
                        }

                        BinaryOperator::Plus
                        | BinaryOperator::Minus
                        | BinaryOperator::Multiply
                        | BinaryOperator::Divide
                        | BinaryOperator::Modulo => {
                            let width = B::float_arithmetic_width(
                                float_result_width::<B, DB>(left, table_id, database, depth),
                                float_result_width::<B, DB>(right, table_id, database, depth),
                            );
                            let instruction = match op {
                                BinaryOperator::Plus => Instruction::Add(width),
                                BinaryOperator::Minus => Instruction::Subtract(width),
                                BinaryOperator::Multiply => Instruction::Multiply(width),
                                BinaryOperator::Divide => {
                                    Instruction::Divide(width, out.quotient()?)
                                }
                                _ => Instruction::Modulo(width),
                            };
                            out.push(instruction);
                        }

                        _ => {
                            return Err(RegisterError::UnsupportedSql(format!(
                                "Binary operator {op:?} not supported"
                            )));
                        }
                    }
                }
            }
        }

        // Identifiers (column references)
        Expr::CompoundIdentifier(parts) if parts.len() != 2 => {
            return Err(RegisterError::UnsupportedSql(format!(
                "Complex identifier {parts:?} not supported"
            )));
        }

        col_expr @ (Expr::Identifier(_) | Expr::CompoundIdentifier(_)) => {
            let col_id = resolve_column_ref(col_expr, table_id, database).ok_or_else(|| {
                let col_name = match col_expr {
                    Expr::Identifier(ident) => ident.value.clone(),
                    Expr::CompoundIdentifier(parts) => parts[1].value.clone(),
                    _ => unreachable!(),
                };
                RegisterError::UnknownColumn {
                    table_id,
                    column: col_name,
                }
            })?;
            // Reject a column whose declared type the runtime decoder cannot
            // resolve against the catalog (an unsupported SQL type).
            crate::catalog_helpers::column_scalar_kind::<B, DB>(database, table_id, col_id)
                .ok_or_else(|| {
                    RegisterError::UnsupportedSql(format!(
                        "Column {col_id} of table {table_id} has an unsupported SQL type for the compiler"
                    ))
                })?;
            out.push(Instruction::LoadColumn(col_id));
        }

        // Literals
        Expr::Value(val) => {
            let value = B::parse_literal(&val.value, target_kind)?;
            out.push(Instruction::PushLiteral(value));
        }

        // IN Lists
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            // Derive target from the tested expression if it's a column
            // reference; fall back to String otherwise (best-effort).
            let list_target = column_scalar_of::<B, DB>(expr, table_id, database)
                .unwrap_or_else(|| BuiltinKind::String.into());

            compile_expr_recursive::<B, DB>(expr, table_id, database, out, depth + 1, list_target)?;

            let mut literals: Vec<Value<B>> = Vec::with_capacity(list.len());
            for item in list {
                if let Expr::Value(val) = item {
                    literals.push(B::parse_literal(&val.value, list_target)?);
                } else {
                    return Err(RegisterError::UnsupportedSql(
                        "IN requires a literal list - SubQL only supports IN with literals like IN ('a', 'b', 'c'), \
                         not column references or computed expressions. \
                         For anything else, run this as a regular SQL query in your database."
                            .to_string(),
                    ));
                }
            }

            let tested = out.intern_comparison(expr, table_id, database);
            out.push(Instruction::In {
                literals,
                comparison: ComparisonRef::new(tested, None),
            });

            if *negated {
                out.push(Instruction::Not);
            }
        }

        // Membership subqueries
        // `IN (SELECT ...)` parses to `InSubquery`, which the literal-list arm
        // above never sees. Requirement 1 of the membership term: recognise the
        // bounded form here, in every build, and let the bound itself say what
        // is wrong with anything outside it.
        //
        // A term is not a row test, so it compiles to a slot rather than to a
        // comparison: dispatch reads the compared column off the changed row,
        // looks up which subscribers that value admits, and hands the VM one
        // truth per assignment. Whether the relationship can be served at all is
        // registration's question, since answering it needs `rls2fga`.
        Expr::InSubquery {
            expr: tested,
            subquery,
            negated,
        } => {
            if *negated {
                return Err(negated_term_refusal());
            }

            // The row-value form compares several columns at once, and the
            // same relationship translates through the EXISTS spelling, so the
            // refusal names the respelling rather than a resolution problem.
            if matches!(tested.as_ref(), Expr::Tuple(_)) {
                return Err(RegisterError::MembershipTermRefused(
                    "a row-value IN compares several columns at once, which SubQL serves \
                     through the EXISTS spelling instead: EXISTS (SELECT 1 FROM member m \
                     WHERE m.k1 = t.a AND m.k2 = t.b AND m.user = current_setting(...)), \
                     with one equality per column"
                        .to_string(),
                ));
            }

            let Some(column) = resolve_column_ref(tested, table_id, database) else {
                return Err(RegisterError::UnsupportedSql(
                    "A membership subquery must test a column of the subscribed table. SubQL \
                     reads that column off each changed row to decide which subscribers the row \
                     reaches, so an expression or an unknown name leaves it nothing to read."
                        .to_string(),
                ));
            };

            sql_shape::check_membership_subquery_bound(subquery)?;

            let slot = out.term_slot(expr, alloc::vec![column])?;
            out.push(Instruction::TermTruth(slot));
        }

        // The EXISTS spelling of the same membership, and the only spelling of
        // one whose linking key spans several columns. The bounded form is
        // recognized here, in every build, and the bound itself says what is
        // wrong with anything outside it.
        Expr::Exists { subquery, negated } => {
            if *negated {
                return Err(negated_term_refusal());
            }
            let columns = sql_shape::check_membership_exists_bound(subquery, table_id, database)?;
            let slot = out.term_slot(expr, columns)?;
            out.push(Instruction::TermTruth(slot));
        }

        // BETWEEN
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let range_target = column_scalar_of::<B, DB>(expr, table_id, database)
                .unwrap_or_else(|| BuiltinKind::String.into());

            // Stack order: value, lower, upper.
            compile_expr_recursive::<B, DB>(
                expr,
                table_id,
                database,
                out,
                depth + 1,
                range_target,
            )?;
            compile_expr_recursive::<B, DB>(low, table_id, database, out, depth + 1, range_target)?;
            compile_expr_recursive::<B, DB>(
                high,
                table_id,
                database,
                out,
                depth + 1,
                range_target,
            )?;

            // Two ordered comparisons, so the same classification applies:
            // the lower bound's pair carries it for both.
            let lower = out.comparison_for(
                expr,
                low,
                table_id,
                database,
                crate::backend::TextOperation::Ordering,
            )?;
            let high_facts = out.intern_comparison(high, table_id, database);
            out.push(Instruction::Between {
                lower,
                upper: ComparisonRef::new(lower.left, high_facts),
            });

            if *negated {
                out.push(Instruction::Not);
            }
        }

        // NULL Checks
        Expr::IsNull(inner) => {
            compile_expr_recursive::<B, DB>(
                inner,
                table_id,
                database,
                out,
                depth + 1,
                BuiltinKind::String.into(),
            )?;
            out.push(Instruction::IsNull);
        }

        Expr::IsNotNull(inner) => {
            compile_expr_recursive::<B, DB>(
                inner,
                table_id,
                database,
                out,
                depth + 1,
                BuiltinKind::String.into(),
            )?;
            out.push(Instruction::IsNotNull);
        }

        // Unary Operations
        Expr::UnaryOp { op, expr: inner } => {
            // A term under `NOT` is subtraction however it is spelled, so it is
            // refused here in the same words the inline `NOT IN` gets. Checked
            // before recursing, since the arm below would otherwise report the
            // inner term as though nothing had negated it.
            if matches!(op, UnaryOperator::Not) && sql_shape::contains_membership_subquery(inner) {
                return Err(negated_term_refusal());
            }
            // A negated caller comparison is refused for its own reason: the
            // negation admits every other subscriber, including through a NULL
            // cell that SQL's three-valued logic admits for nobody.
            if matches!(op, UnaryOperator::Not) && sql_shape::contains_caller_comparison(inner) {
                return Err(negated_caller_refusal());
            }

            compile_expr_recursive::<B, DB>(
                inner,
                table_id,
                database,
                out,
                depth + 1,
                target_kind,
            )?;

            match op {
                UnaryOperator::Not => out.push(Instruction::Not),
                UnaryOperator::Plus => {
                    // Unary + is no-op.
                }
                UnaryOperator::Minus => {
                    out.push(Instruction::Negate(float_result_width::<B, DB>(
                        expr, table_id, database, depth,
                    )));
                }
                _ => {
                    return Err(RegisterError::UnsupportedSql(format!(
                        "Unary operator {op:?} not supported"
                    )));
                }
            }
        }

        // LIKE Pattern Matching
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

            compile_expr_recursive::<B, DB>(
                expr,
                table_id,
                database,
                out,
                depth + 1,
                BuiltinKind::String.into(),
            )?;
            compile_expr_recursive::<B, DB>(
                pattern,
                table_id,
                database,
                out,
                depth + 1,
                BuiltinKind::String.into(),
            )?;

            let cmp = out.comparison_for(
                expr,
                pattern,
                table_id,
                database,
                crate::backend::TextOperation::Pattern,
            )?;
            out.push(Instruction::Like { comparison: cmp });

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

            compile_expr_recursive::<B, DB>(
                expr,
                table_id,
                database,
                out,
                depth + 1,
                BuiltinKind::String.into(),
            )?;
            compile_expr_recursive::<B, DB>(
                pattern,
                table_id,
                database,
                out,
                depth + 1,
                BuiltinKind::String.into(),
            )?;

            let cmp = out.comparison_for(
                expr,
                pattern,
                table_id,
                database,
                crate::backend::TextOperation::CaseInsensitivePattern,
            )?;
            out.push(Instruction::Like { comparison: cmp });

            if *negated {
                out.push(Instruction::Not);
            }
        }

        // Nested Expressions (parentheses)
        Expr::Nested(inner) => {
            compile_expr_recursive::<B, DB>(
                inner,
                table_id,
                database,
                out,
                depth + 1,
                target_kind,
            )?;
        }

        // Unsupported
        _ => {
            return Err(RegisterError::UnsupportedSql(format!(
                "Expression {expr:?} not supported - SubQL supports basic WHERE clause predicates \
                 (comparisons, AND/OR/NOT, IN lists, BETWEEN, NULL checks, LIKE). For complex \
                 expressions, aggregates, or functions, run this as a regular SQL query in your \
                 database."
            )));
        }
    }

    Ok(())
}

/// Why a negated membership subquery is refused, in one place so that
/// `x NOT IN (SELECT ...)`, `NOT (x IN (SELECT ...))` and `NOT EXISTS (...)`,
/// which reach different arms, cannot drift into different sentences for one
/// filter.
fn negated_term_refusal() -> RegisterError {
    RegisterError::UnsupportedSql(
        "A negated membership subquery (NOT IN, NOT EXISTS) is not supported. SubQL serves \
         a membership subquery by tracking the relationship it names, and subtraction names \
         no relationship to track. Use NOT IN with a literal list, or run this as a regular \
         SQL query in your database."
            .to_string(),
    )
}

/// Why a negated caller comparison is refused, in one place for the same
/// reason [`negated_term_refusal`] is.
fn negated_caller_refusal() -> RegisterError {
    RegisterError::UnsupportedSql(
        "NOT over a comparison to the caller is not supported. SubQL serves the comparison \
         by admitting exactly the subscriber the row names, and its negation admits every \
         other subscriber, including through a NULL cell that SQL's own three-valued logic \
         admits for nobody. Run this as a regular SQL query in your database."
            .to_string(),
    )
}

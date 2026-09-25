//! Expression compilation helpers split out of the parser.

use super::{Compiling, MAX_TERMS_PER_FILTER};
use crate::backend::{Backend, NullSafeEquality, ScalarFamily, Value, ValueKindOf};
use crate::compiler::bytecode::{ComparisonRef, FloatResult};
use crate::compiler::literals::{
    coalesce_arguments, resolve_column_ref, value_column, SqlLiteralParse,
};
use crate::compiler::{canonicalize, sql_shape, BytecodeProgram, Instruction, Tri};
use crate::term::{term_columns, CompiledTerm};
use crate::{RegisterError, TableId};
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue, ValueWithSpan};
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
    let col = resolve_column_ref::<B, DB>(value_column(expr), table_id, database)?;
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
    if operand == Some(ScalarFamily::Float.into()) {
        return operand;
    }
    Some(ScalarFamily::Decimal.into())
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
            | Instruction::NotDistinct(_)
            | Instruction::IsTruth { .. }
            | Instruction::Truth
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
                ScalarFamily::Bool.into(),
            )?));
            instructions.push(Instruction::Equal(comparison));
            Ok(())
        }
        None => Err(RegisterError::UnsupportedSql(
            "empty WHERE clause after compilation".to_string(),
        )),
    }
}

/// Whether `expr`, parentheses aside, names a boolean column of the table.
fn is_boolean_column<B: Backend, DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
) -> bool {
    let mut bare = value_column(expr);
    while let Expr::Nested(inner) = bare {
        bare = inner;
    }
    resolve_column_ref::<B, DB>(bare, table_id, database).is_some_and(|column| {
        crate::catalog_helpers::column_scalar_family(database, table_id, column)
            == Some(ScalarFamily::Bool)
    })
}

/// Turn the value `expr` just compiled to into a condition, where one is read.
///
/// A boolean column reads as its own truth, which is how each engine reads
/// one there. `= true` is not that on SQLite, whose boolean column keeps the
/// stored integer and compares it with `1`. Any other bare value keeps the
/// `= true` comparison.
fn ensure_condition<B, DB>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
    out: &mut Compiling<B>,
) -> Result<(), RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    if out.out.last().is_some_and(instruction_is_tri_typed) {
        return Ok(());
    }
    if is_boolean_column::<B, DB>(expr, table_id, database) {
        out.push(Instruction::Truth);
        return Ok(());
    }
    let comparison = ComparisonRef::new(out.intern_comparison(expr, table_id, database), None);
    wrap_bare_value_as_tri::<B>(&mut out.out, comparison)
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
        ScalarFamily::String.into(),
    )?;
    ensure_condition::<B, DB>(expr, table_id, database, &mut compiling)?;
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
    if let Some(column) = resolve_column_ref::<B, DB>(value_column(expr), table_id, database) {
        return crate::catalog_helpers::column_comparison::<B, DB>(database, table_id, column)
            .and_then(|facts| facts.kind.declared_type())
            .and_then(crate::backend::DeclaredType::float_width);
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
                        ScalarFamily::String.into(),
                    )?;
                    ensure_condition::<B, DB>(left, table_id, database, out)?;

                    let jump_idx = out.len();
                    out.push(Instruction::JumpIfFalse(0)); // offset backfilled once rhs length is known (line 213)

                    let rhs_start = out.len();
                    compile_expr_recursive::<B, DB>(
                        right,
                        table_id,
                        database,
                        out,
                        depth + 1,
                        ScalarFamily::String.into(),
                    )?;
                    ensure_condition::<B, DB>(right, table_id, database, out)?;
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
                        ScalarFamily::String.into(),
                    )?;
                    ensure_condition::<B, DB>(left, table_id, database, out)?;

                    let jump_idx = out.len();
                    out.push(Instruction::JumpIfTrue(0)); // offset backfilled once rhs length is known (line 240)

                    let rhs_start = out.len();
                    compile_expr_recursive::<B, DB>(
                        right,
                        table_id,
                        database,
                        out,
                        depth + 1,
                        ScalarFamily::String.into(),
                    )?;
                    ensure_condition::<B, DB>(right, table_id, database, out)?;
                    out.push(Instruction::Or);

                    let rhs_len = out.len() - rhs_start;
                    out[jump_idx] = Instruction::JumpIfTrue(rhs_len + 1);
                }
                BinaryOperator::Spaceship => compile_null_safe_equality::<B, DB>(
                    (left, right),
                    NullSafeEquality::Spaceship,
                    false,
                    table_id,
                    database,
                    out,
                    depth,
                )?,
                _ => {
                    // A name resolving to no column falls to the generic arms, whose refusal names it.
                    if caller_term::<B, DB>(expr, table_id, database, out)? {
                        return Ok(());
                    }
                    refuse_condition_operand(left)?;
                    refuse_condition_operand(right)?;
                    refuse_literal_foreign_to_coalesce::<B, DB>(left, right, table_id, database)?;

                    // Non-short-circuit operators: compile both sides,
                    // then emit the op. Target-typed literal inference
                    // picks whichever sibling is a column reference and
                    // uses its ScalarKind for the other side's literal.
                    let child_target =
                        nested_column_scalar_of::<B, DB>(left, table_id, database, depth)
                            .or_else(|| {
                                nested_column_scalar_of::<B, DB>(right, table_id, database, depth)
                            })
                            .unwrap_or_else(|| ScalarFamily::String.into());
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
            let col_id =
                resolve_column_ref::<B, DB>(col_expr, table_id, database).ok_or_else(|| {
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
                .unwrap_or_else(|| ScalarFamily::String.into());

            compile_expr_recursive::<B, DB>(expr, table_id, database, out, depth + 1, list_target)?;

            let mut literals: Vec<Value<B>> = Vec::with_capacity(list.len());
            for item in list {
                refuse_literal_foreign_to_coalesce::<B, DB>(expr, item, table_id, database)?;
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

            let Some(column) = resolve_column_ref::<B, DB>(tested, table_id, database) else {
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
            let columns =
                sql_shape::check_membership_exists_bound::<B, DB>(subquery, table_id, database)?;
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
                .unwrap_or_else(|| ScalarFamily::String.into());
            for bound in [low, high] {
                refuse_literal_foreign_to_coalesce::<B, DB>(expr, bound, table_id, database)?;
            }

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

            // Two ordered comparisons, and each resolves its own rule.
            // The upper bound used to be built from the lower's left
            // operand and the upper's own facts with no text rule at all,
            // so it compared bytes while the lower bound compared as the
            // engine does.
            //
            // Copying the lower's rule across is not the fix either, and
            // the mutation battery is what established that. The two
            // bounds can resolve differently: measured on PostgreSQL
            // 16.15 with `free` a `text` holding `ab   `, `loose` a
            // `varchar` holding the same, and `code` a `char(5)` holding
            // `ab`, `free >= loose` is true and `free <= code` is false,
            // because converting the `char` to `text` strips its padding.
            // `free BETWEEN loose AND code` is therefore false, and only
            // a rule resolved per bound answers that.
            let lower = out.comparison_for(
                expr,
                low,
                table_id,
                database,
                crate::backend::TextOperation::Ordering,
            )?;
            let upper = out.comparison_for(
                expr,
                high,
                table_id,
                database,
                crate::backend::TextOperation::Ordering,
            )?;
            out.push(Instruction::Between { lower, upper });

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
                ScalarFamily::String.into(),
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
                ScalarFamily::String.into(),
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
                UnaryOperator::Not => {
                    ensure_condition::<B, DB>(inner, table_id, database, out)?;
                    out.push(Instruction::Not);
                }
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
        } => compile_pattern_match::<B, DB>(
            PatternMatch {
                expr,
                pattern,
                negated: *negated,
                escape: escape_char.as_deref(),
                keyword: "LIKE",
                operation: crate::backend::TextOperation::Pattern,
            },
            table_id,
            database,
            out,
            depth,
        )?,

        Expr::ILike {
            expr,
            pattern,
            negated,
            escape_char,
            ..
        } => compile_pattern_match::<B, DB>(
            PatternMatch {
                expr,
                pattern,
                negated: *negated,
                escape: escape_char.as_deref(),
                keyword: "ILIKE",
                operation: crate::backend::TextOperation::CaseInsensitivePattern,
            },
            table_id,
            database,
            out,
            depth,
        )?,

        Expr::IsTrue(condition)
        | Expr::IsNotTrue(condition)
        | Expr::IsFalse(condition)
        | Expr::IsNotFalse(condition)
        | Expr::IsUnknown(condition)
        | Expr::IsNotUnknown(condition) => {
            let (value, negated) = match expr {
                Expr::IsTrue(_) => (Tri::True, false),
                Expr::IsNotTrue(_) => (Tri::True, true),
                Expr::IsFalse(_) => (Tri::False, false),
                Expr::IsNotFalse(_) => (Tri::False, true),
                Expr::IsUnknown(_) => (Tri::Unknown, false),
                _ => (Tri::Unknown, true),
            };
            compile_truth_test::<B, DB>(condition, value, negated, table_id, database, out, depth)?;
        }

        Expr::IsNotDistinctFrom(left, right) | Expr::IsDistinctFrom(left, right) => {
            compile_null_safe_equality::<B, DB>(
                (left, right),
                NullSafeEquality::DistinctFrom,
                matches!(expr, Expr::IsDistinctFrom(..)),
                table_id,
                database,
                out,
                depth,
            )?;
        }

        Expr::Function(_) if coalesce_arguments(expr).is_some() => {
            compile_coalesce::<B, DB>(expr, table_id, database, out, depth)?;
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

        // `= ANY(<call>)` is a caller comparison over a set or nothing SubQL runs.
        Expr::AnyOp { .. } => {
            if !caller_term::<B, DB>(expr, table_id, database, out)? {
                return Err(unsupported_expression(expr));
            }
        }

        _ => return Err(unsupported_expression(expr)),
    }

    Ok(())
}

/// The refusal for a predicate shape SubQL does not serve.
fn unsupported_expression(expr: &Expr) -> RegisterError {
    RegisterError::UnsupportedSql(format!(
        "Expression {expr:?} not supported - SubQL supports basic WHERE clause predicates \
         (comparisons, AND/OR/NOT, IN lists, BETWEEN, NULL checks, LIKE). For complex \
         expressions, aggregates, or functions, run this as a regular SQL query in your \
         database."
    ))
}

/// Emit a term slot for `expr` when it compares a column of the subscribed
/// table to the caller, and report whether it did.
///
/// Recognised structurally in every build, `membership-term` or not, so one
/// build does not accept a filter another refuses. Dispatch reads the compared
/// column off the changed row, and the caller's values the request states are
/// what the term admits. Whether the call names the caller, and whether it
/// names one value or a set, is registration's question, since answering it
/// needs `rls2fga`.
fn caller_term<B, DB>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
    out: &mut Compiling<B>,
) -> Result<bool, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let Some((column, _)) = sql_shape::caller_comparison_sides(expr) else {
        return Ok(false);
    };
    let Some(column) = resolve_column_ref::<B, DB>(column, table_id, database) else {
        return Ok(false);
    };
    let slot = out.term_slot(expr, alloc::vec![column])?;
    out.push(Instruction::TermTruth(slot));
    Ok(true)
}

/// One `LIKE` or `ILIKE` node, named so the two arms lower through the same
/// procedure.
#[derive(Clone, Copy)]
struct PatternMatch<'sql> {
    expr: &'sql Expr,
    pattern: &'sql Expr,
    negated: bool,
    /// The written `ESCAPE` clause, if any.
    escape: Option<&'sql Expr>,
    /// The keyword to name in a refusal, `LIKE` or `ILIKE`.
    keyword: &'static str,
    operation: crate::backend::TextOperation,
}

/// Lower a pattern match: both operands as text, then the comparison the
/// backend resolves for `operation`.
fn compile_pattern_match<B, DB>(
    node: PatternMatch<'_>,
    table_id: TableId,
    database: &DB,
    out: &mut Compiling<B>,
    depth: usize,
) -> Result<(), RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    // The written clause replaces the engine's default escape.
    let escape = match node.escape {
        None => B::LIKE_DEFAULT_ESCAPE,
        Some(written) => match written {
            Expr::Value(ValueWithSpan {
                value: SqlValue::SingleQuotedString(character),
                ..
            }) => B::like_escape_clause(character).map_err(|reason| {
                RegisterError::UnsupportedSql(format!("{} ESCAPE: {reason}", node.keyword))
            })?,
            _ => {
                return Err(RegisterError::UnsupportedSql(format!(
                    "{} ESCAPE is served with a quoted character only",
                    node.keyword
                )))
            }
        },
    };
    for operand in [node.expr, node.pattern] {
        compile_expr_recursive::<B, DB>(
            operand,
            table_id,
            database,
            out,
            depth + 1,
            ScalarFamily::String.into(),
        )?;
    }
    let comparison =
        out.comparison_for(node.expr, node.pattern, table_id, database, node.operation)?;
    out.push(Instruction::Like { comparison, escape });
    if node.negated {
        out.push(Instruction::Not);
    }
    Ok(())
}

/// The families a served `COALESCE` may answer in: the ones whose literal
/// every engine reads alike.
const COALESCE_FAMILIES: [ScalarFamily; 5] = [
    ScalarFamily::Int,
    ScalarFamily::Float,
    ScalarFamily::Decimal,
    ScalarFamily::String,
    ScalarFamily::Bool,
];

/// Whether `literal` is written in `family`'s own form, the one every engine
/// types alike: a number for the numeric families, and an integer one for
/// `Int`, since a fraction makes the engines pick a decimal type.
fn native_literal(literal: &SqlValue, family: ScalarFamily) -> bool {
    match (literal, family) {
        (SqlValue::Null, _)
        | (SqlValue::Number(..), ScalarFamily::Float | ScalarFamily::Decimal)
        | (SqlValue::SingleQuotedString(_), ScalarFamily::String)
        | (SqlValue::Boolean(_), ScalarFamily::Bool) => true,
        (SqlValue::Number(digits, _), ScalarFamily::Int) => !digits.contains(['.', 'e', 'E']),
        _ => false,
    }
}

/// The refusal for a `COALESCE` the engines type differently.
fn coalesce_refusal(reason: &str) -> RegisterError {
    RegisterError::UnsupportedSql(format!(
        "COALESCE is served over column arguments of one declared type and collation and \
         literals written in that type's own form, and {reason}"
    ))
}

/// Compile a `COALESCE` whose arguments every engine types alike.
///
/// Its column arguments share one declared type and collation in a family
/// whose literals every engine reads alike, and each literal is written in
/// that family's form. Measured, the engines disagree past that:
/// `COALESCE(1, 2.5) / 2` is `0.5` on PostgreSQL and MySQL and `0` on SQLite,
/// and `COALESCE(1, 'a')` is an error on PostgreSQL only.
fn compile_coalesce<B, DB>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
    out: &mut Compiling<B>,
    depth: usize,
) -> Result<(), RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let arguments = coalesce_arguments(expr).unwrap_or_default();
    if arguments.len() < 2 {
        return Err(coalesce_refusal(
            "a call of one argument is an error on SQLite",
        ));
    }
    let mut facts = None;
    for argument in &arguments {
        match argument {
            Expr::Value(_) => {}
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                let column = resolve_column_ref::<B, DB>(argument, table_id, database)
                    .ok_or_else(|| coalesce_refusal("an argument names no column of the table"))?;
                let declared =
                    crate::catalog_helpers::column_comparison::<B, DB>(database, table_id, column);
                match (&facts, declared) {
                    (_, None) => return Err(coalesce_refusal("an argument has no known type")),
                    (None, Some(declared)) => facts = Some((column, declared)),
                    (Some((_, first)), Some(declared)) if *first == declared => {}
                    (Some(_), Some(_)) => {
                        return Err(coalesce_refusal(
                            "its columns differ in declared type or collation",
                        ))
                    }
                }
            }
            _ => {
                return Err(coalesce_refusal(
                    "an argument is neither a column nor a literal",
                ))
            }
        }
    }
    let Some((column, _)) = facts else {
        return Err(coalesce_refusal("no argument is a column"));
    };
    let family = crate::catalog_helpers::column_scalar_family(database, table_id, column)
        .filter(|family| COALESCE_FAMILIES.contains(family))
        .ok_or_else(|| coalesce_refusal("its type is one whose literals the engines read apart"))?;
    for argument in &arguments {
        if let Expr::Value(literal) = argument {
            if !native_literal(&literal.value, family) {
                return Err(coalesce_refusal(
                    "a literal is written in another type's form",
                ));
            }
        }
    }
    let target =
        column_scalar_of::<B, DB>(expr, table_id, database).unwrap_or_else(|| family.into());
    for argument in &arguments {
        compile_expr_recursive::<B, DB>(argument, table_id, database, out, depth + 1, target)?;
    }
    let count = u16::try_from(arguments.len())
        .map_err(|_| coalesce_refusal("it has more arguments than a program counts"))?;
    out.push(Instruction::Coalesce(count));
    Ok(())
}

/// Refuse a literal beside a `COALESCE` written in another family's form.
///
/// SQLite gives an expression no affinity, so it compares a quoted literal
/// with a `COALESCE` over integers as text and answers false, where PostgreSQL
/// and MySQL convert it: measured, `COALESCE(NULL, 1) = '1'`.
fn refuse_literal_foreign_to_coalesce<B: Backend, DB: DatabaseLike>(
    left: &Expr,
    right: &Expr,
    table_id: TableId,
    database: &DB,
) -> Result<(), RegisterError> {
    for (coalesced, other) in [(left, right), (right, left)] {
        if coalesce_arguments(coalesced).is_none() {
            continue;
        }
        let Expr::Value(literal) = other else {
            continue;
        };
        let family = resolve_column_ref::<B, DB>(value_column(coalesced), table_id, database)
            .and_then(|column| {
                crate::catalog_helpers::column_scalar_family(database, table_id, column)
            });
        if !family.is_some_and(|family| native_literal(&literal.value, family)) {
            return Err(coalesce_refusal(
                "a literal compared with it is written in another type's form",
            ));
        }
    }
    Ok(())
}

/// Refuse a condition where a value is read, as in `(a = b) = true`.
///
/// Every engine answers it, and the language compares and computes with
/// values only, so it is routed to a read rather than typed against the
/// sibling column and refused as a bad literal.
fn refuse_condition_operand(operand: &Expr) -> Result<(), RegisterError> {
    let mut bare = operand;
    while let Expr::Nested(inner) = bare {
        bare = inner;
    }
    let condition = match bare {
        Expr::BinaryOp { op, .. } => matches!(
            op,
            BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Spaceship
        ),
        Expr::UnaryOp { op, .. } => matches!(op, UnaryOperator::Not),
        _ => matches!(
            bare,
            Expr::IsNull(_)
                | Expr::IsNotNull(_)
                | Expr::IsTrue(_)
                | Expr::IsNotTrue(_)
                | Expr::IsFalse(_)
                | Expr::IsNotFalse(_)
                | Expr::IsUnknown(_)
                | Expr::IsNotUnknown(_)
                | Expr::IsDistinctFrom(..)
                | Expr::IsNotDistinctFrom(..)
                | Expr::Like { .. }
                | Expr::ILike { .. }
                | Expr::InList { .. }
                | Expr::InSubquery { .. }
                | Expr::Between { .. }
                | Expr::Exists { .. }
        ),
    };
    if condition {
        return Err(RegisterError::UnsupportedSql(
            "a condition compared or computed with as a value is not supported in process, \
             which compares and computes with values only"
                .to_string(),
        ));
    }
    Ok(())
}

/// Refuse a membership subquery or a caller comparison as the operand of a
/// truth test or a null-safe equality.
///
/// Either can negate what it wraps, as `IS NOT TRUE` and `IS DISTINCT FROM
/// true` do, which is the subtraction `NOT` over a term is refused for, and a
/// term answers per subscriber rather than as one value.
fn refuse_term_operand(operand: &Expr) -> Result<(), RegisterError> {
    if sql_shape::contains_membership_subquery(operand)
        || sql_shape::contains_caller_comparison(operand)
    {
        return Err(RegisterError::UnsupportedSql(
            "A membership subquery or a comparison to the caller under a truth test (IS TRUE, \
             IS FALSE, IS UNKNOWN) or a null-safe equality is not supported. SubQL serves the \
             relationship itself, and these can negate it. Run this as a regular SQL query in \
             your database."
                .to_string(),
        ));
    }
    Ok(())
}

/// Compile a truth test of `condition` for `value`, negated for `IS NOT`.
///
/// The operand is a condition or a boolean column, the one kind every engine
/// reads alike: PostgreSQL raises on a number where MySQL and SQLite read
/// its truth, so any other operand is left to the engine.
fn compile_truth_test<B, DB>(
    condition: &Expr,
    value: Tri,
    negated: bool,
    table_id: TableId,
    database: &DB,
    out: &mut Compiling<B>,
    depth: usize,
) -> Result<(), RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    refuse_term_operand(condition)?;
    if value == Tri::Unknown && !B::READS_IS_UNKNOWN {
        return Err(RegisterError::UnsupportedSql(
            "`IS [NOT] UNKNOWN` is not a truth test on this engine, which reads `UNKNOWN` as a \
             name"
                .to_string(),
        ));
    }
    compile_expr_recursive::<B, DB>(
        condition,
        table_id,
        database,
        out,
        depth + 1,
        ScalarFamily::Bool.into(),
    )?;
    if !out.out.last().is_some_and(instruction_is_tri_typed)
        && !is_boolean_column::<B, DB>(condition, table_id, database)
    {
        return Err(RegisterError::UnsupportedSql(
            "a truth test is served over a condition or a boolean column, and the engines \
             disagree about any other operand"
                .to_string(),
        ));
    }
    ensure_condition::<B, DB>(condition, table_id, database, out)?;
    out.push(Instruction::IsTruth { value, negated });
    Ok(())
}

/// Compile a null-safe equality written in `spelling`, negated for
/// `IS DISTINCT FROM`.
///
/// A spelling the engine rejects is not served, so the engine's own error
/// is what the caller meets. The comparison facts are the ones `=` resolves,
/// so both refuse the same operand pairs.
fn compile_null_safe_equality<B, DB>(
    (left, right): (&Expr, &Expr),
    spelling: NullSafeEquality,
    negated: bool,
    table_id: TableId,
    database: &DB,
    out: &mut Compiling<B>,
    depth: usize,
) -> Result<(), RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    if spelling != B::NULL_SAFE_EQUALITY {
        return Err(RegisterError::UnsupportedSql(format!(
            "`{}` is not this engine's null-safe equality, which it spells `{}`",
            spelling.spelling(),
            B::NULL_SAFE_EQUALITY.spelling()
        )));
    }
    refuse_term_operand(left)?;
    refuse_term_operand(right)?;
    refuse_condition_operand(left)?;
    refuse_condition_operand(right)?;
    refuse_literal_foreign_to_coalesce::<B, DB>(left, right, table_id, database)?;
    let target = nested_column_scalar_of::<B, DB>(left, table_id, database, depth)
        .or_else(|| nested_column_scalar_of::<B, DB>(right, table_id, database, depth))
        .unwrap_or_else(|| ScalarFamily::String.into());
    for operand in [left, right] {
        compile_expr_recursive::<B, DB>(operand, table_id, database, out, depth + 1, target)?;
    }
    let comparison = out.comparison_for(
        left,
        right,
        table_id,
        database,
        crate::backend::TextOperation::Equality,
    )?;
    out.push(Instruction::NotDistinct(comparison));
    if negated {
        out.push(Instruction::Not);
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

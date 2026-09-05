//! SQL parser for subscription predicates.
//!
//! Compiles subscription `SELECT` statements into a Backend-generic
//! [`BytecodeProgram<B>`] the VM interprets against any
//! `E: CdcEvent<Backend = B>`. Public entry points are parameterised on
//! `B: Backend + SqlLiteralParse`; the concrete backend supplies both the
//! [`sqlparser::dialect::Dialect`] used to parse SQL text and the
//! literal-coercion rules ([`SqlLiteralParse::parse_literal`]) used to
//! turn AST literals into typed `Value<B>`.

mod compilation_helpers;

use super::{
    canonicalize,
    literals::{hex_upper, SqlLiteralParse},
    prefilter::build_prefilter_plan,
    sql_shape, BytecodeProgram, Instruction, PredicateHash, PrefilterPlan,
};
use crate::backend::{Backend, BuiltinKind, Value};
use crate::compiler::bytecode::ComparisonRef;
use crate::compiler::sql_shape::{AggSpec, QueryProjection};
use crate::table_resolution::{resolve_table_reference, TableResolutionError};
use crate::term::CompiledTerm;
use crate::{ColumnId, RegisterError, TableId};
use alloc::borrow::ToOwned;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use compilation_helpers::{compile_expression, wrap_bare_value_as_tri};
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::{Expr, ObjectName, Statement, Value as SqlValue};
use sqlparser::dialect::Dialect;
use sqlparser_canonicalize::Canonicalizer;

/// Where a filter's instructions accumulate, with the membership terms lifted
/// out of it as they are met.
///
/// Derefs to the instruction vector so every `out.push(..)` and every recursive
/// call in the compiler reads as it did before terms existed. The alternative
/// was a nineteenth parameter threaded through nineteen recursive calls, which
/// is the same information carried less legibly.
struct Compiling<B: Backend> {
    out: Vec<Instruction<B>>,
    terms: Vec<CompiledTerm>,
    /// Comparison facts interned as they are needed, addressed by
    /// [`crate::compiler::bytecode::ComparisonRef`] index.
    comparisons: Vec<crate::backend::ColumnComparisonOf<B>>,
    /// MySQL's `div_precision_increment` as the deployment declared it, or
    /// `None` when it did not. Only an engine whose `/` answers a decimal
    /// reads it, and that engine refuses the operator without it.
    increment: Option<crate::backend::DivisionPrecisionIncrement>,
}

impl<B: Backend> Compiling<B> {
    const fn new(increment: Option<crate::backend::DivisionPrecisionIncrement>) -> Self {
        Self {
            out: Vec::new(),
            terms: Vec::new(),
            comparisons: Vec::new(),
            increment,
        }
    }

    /// The [`Quotient`](crate::compiler::bytecode::Quotient) a `/` compiles
    /// to under this backend, or the refusal when its rule needs a setting
    /// this engine was not given.
    fn quotient(&self) -> Result<crate::compiler::bytecode::Quotient, RegisterError> {
        crate::compiler::bytecode::Quotient::resolve::<B>(self.increment)
            .map_err(RegisterError::NotServedInProcess)
    }

    /// The slot for `expr`, assigning the next one if this term is new.
    ///
    /// Two structurally equal terms share a slot, because the assignment the VM
    /// evaluates is per term rather than per occurrence: the same relationship
    /// admits the same subscribers wherever the filter mentions it.
    ///
    /// The cap is what keeps the `2^k` enumeration small. Four terms is sixteen
    /// evaluations per row version, and one term, the ordinary case, is two.
    fn term_slot(&mut self, expr: &Expr, columns: Vec<ColumnId>) -> Result<u16, RegisterError> {
        if let Some(existing) = self.terms.iter().find(|term| &term.expr == expr) {
            return Ok(existing.slot);
        }
        let slot = u16::try_from(self.terms.len()).unwrap_or(u16::MAX);
        if usize::from(slot) >= MAX_TERMS_PER_FILTER {
            return Err(RegisterError::MembershipTermRefused(format!(
                "a filter may name at most {MAX_TERMS_PER_FILTER} membership terms, because \
                 SubQL answers each changed row for every combination of them and that is \
                 2^{MAX_TERMS_PER_FILTER} evaluations at the cap"
            )));
        }
        self.terms.push(CompiledTerm {
            slot,
            columns,
            expr: expr.clone(),
        });
        Ok(slot)
    }

    /// The slot holding `expr`'s comparison facts, interning them on first
    /// use, or `None` when `expr` names no single column or the catalog
    /// cannot classify it.
    ///
    /// Facts are deduplicated by value: two columns declaring the same type
    /// and collation compare the same way, so they share a slot.
    fn intern_comparison<DB: DatabaseLike>(
        &mut self,
        expr: &Expr,
        table_id: TableId,
        database: &DB,
    ) -> Option<u16> {
        let column = crate::compiler::literals::resolve_column_ref(expr, table_id, database)?;
        let facts = crate::catalog_helpers::column_comparison::<B, DB>(database, table_id, column)?;
        if let Some(slot) = self.comparisons.iter().position(|held| *held == facts) {
            return u16::try_from(slot).ok();
        }
        let slot = u16::try_from(self.comparisons.len()).ok()?;
        self.comparisons.push(facts);
        Some(slot)
    }

    /// The facts of both operands of one comparison, with its text rule
    /// resolved for `operation`, or the refusal that sends the statement
    /// to a database read.
    ///
    /// Two reasons a comparison cannot be answered in process, both
    /// resolved here so the decision is taken once, at registration:
    ///
    /// * The operand kind has no order this build reproduces. `jsonb` is
    ///   that kind: PostgreSQL's order over it is not the order of the
    ///   canonical binary form. Its equality is fine, being equivalence of
    ///   that form.
    /// * The operands' collations describe a text comparison this build
    ///   cannot reproduce, which the backend decides per operation. A
    ///   locale ordering and a case-insensitive equality are both in that
    ///   set, and answering them by byte would answer wrongly.
    fn comparison_for<DB: DatabaseLike>(
        &mut self,
        left: &Expr,
        right: &Expr,
        table_id: TableId,
        database: &DB,
        operation: crate::backend::TextOperation,
    ) -> Result<ComparisonRef, RegisterError> {
        use crate::backend::TextOperation;

        let mut text_column = None;
        let mut columns = alloc::vec::Vec::with_capacity(2);
        for side in [left, right] {
            let Some(column) =
                crate::compiler::literals::resolve_column_ref(side, table_id, database)
            else {
                continue;
            };
            let kind = crate::catalog_helpers::column_builtin_kind(database, table_id, column);
            columns.push((column, kind));
            match kind {
                Some(crate::backend::BuiltinKind::Jsonb)
                    if operation == TextOperation::Ordering =>
                {
                    return Err(RegisterError::NotServedInProcess(
                        crate::errors::Refusal::OrderNotReproducible {
                            column,
                            kind: crate::backend::BuiltinKind::Jsonb,
                        },
                    ));
                }
                Some(crate::backend::BuiltinKind::String) => text_column = Some(column),
                _ => {}
            }
        }

        // Two columns of different kinds: served only where this backend
        // has a widening for the pair. Nothing coerces silently, because a
        // coercion the engine does not perform is a wrong answer, and the
        // engines differ on which pairs they widen and how.
        if let [(left_column, Some(left_kind)), (right_column, Some(right_kind))] = columns[..] {
            if left_kind != right_kind && B::numeric_widening(left_kind, right_kind).is_none() {
                return Err(RegisterError::NotServedInProcess(
                    crate::errors::Refusal::CrossKindComparison {
                        left: left_column,
                        left_kind,
                        right: right_column,
                        right_kind,
                    },
                ));
            }
        }

        let reference = ComparisonRef::new(
            self.intern_comparison(left, table_id, database),
            self.intern_comparison(right, table_id, database),
        );
        let Some(column) = text_column else {
            return Ok(reference);
        };

        let context = crate::backend::ComparisonContext {
            left: reference
                .left
                .and_then(|slot| self.comparisons.get(usize::from(slot))),
            right: reference
                .right
                .and_then(|slot| self.comparisons.get(usize::from(slot))),
            // The rule is what this call resolves; it is not an input.
            text: None,
        };
        let rule = match B::text_rule(&context, operation) {
            crate::backend::TextResolution::Rule(rule) => rule,
            // The engine will not run the statement, so neither an
            // in-process answer nor a read produces one. Reporting this
            // as not-served would promise a read that raises.
            crate::backend::TextResolution::Refused { reason } => {
                return Err(RegisterError::RefusedByEngine {
                    engine: core::any::type_name::<B>(),
                    reason: reason.to_string(),
                });
            }
            crate::backend::TextResolution::NeedsRead => {
                // The named collation, when a side has one to name. No
                // `expect` here: a text column whose facts were never
                // interned is a refusal like any other, and panicking in
                // `register` would be worse than the refusal it sits
                // beside.
                let collation = reference
                    .left
                    .or(reference.right)
                    .and_then(|slot| self.comparisons.get(usize::from(slot)))
                    .and_then(|facts| match &facts.collation {
                        crate::backend::CollationFacts::Named { name, .. } => {
                            Some(name.name.clone())
                        }
                        crate::backend::CollationFacts::DatabaseDefault
                        | crate::backend::CollationFacts::Unknown => None,
                    });
                return Err(RegisterError::NotServedInProcess(
                    crate::errors::Refusal::CollationNotReproducible { column, collation },
                ));
            }
        };
        Ok(reference.with_text(Some(rule)))
    }
}

impl<B: Backend> core::ops::Deref for Compiling<B> {
    type Target = Vec<Instruction<B>>;

    fn deref(&self) -> &Self::Target {
        &self.out
    }
}

impl<B: Backend> core::ops::DerefMut for Compiling<B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.out
    }
}

/// How many membership subqueries one filter may name.
///
/// Dispatch evaluates the filter once per assignment over the terms, so the
/// cost is `2^k` per row version and the cap is what keeps it small.
pub const MAX_TERMS_PER_FILTER: usize = 4;

/// Everything one subscription statement compiles to.
///
/// A struct rather than a tuple because the membership terms are its sixth
/// member and a six-tuple says nothing about which field is which.
pub struct CompiledQuery<B: Backend> {
    /// The table the statement reads.
    pub table_id: TableId,
    /// The WHERE clause as bytecode.
    pub program: BytecodeProgram<B>,
    /// Canonical WHERE-clause text, which is what predicate sharing keys on.
    pub normalized: String,
    /// Planner metadata for candidate pruning.
    pub prefilter_plan: PrefilterPlan,
    /// Row events or aggregate deltas.
    pub projection: QueryProjection,
    /// The membership terms the filter names, one per slot.
    pub terms: Vec<CompiledTerm>,
}

pub(crate) struct SqlTableName {
    pub(crate) unqualified: String,
    pub(crate) qualified: Option<String>,
}

impl SqlTableName {
    pub(crate) fn from_object_name(name: &ObjectName) -> Result<Self, RegisterError> {
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

/// Parse and compile a subscription SQL statement into bytecode.
///
/// # Arguments
/// * `sql` — SQL `SELECT` statement with optional WHERE clause.
/// * `dialect` — the sqlparser dialect for `B`. Callers typically pass
///   `&<B::Dialect as Default>::default()` or a shared instance.
/// * `database` — schema catalog for table / column resolution.
///
/// # Errors
///
/// Returns [`RegisterError`] variants for parse, catalog, or literal
/// coercion failures.
pub fn parse_and_compile<B, DB>(
    sql: &str,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<(TableId, BytecodeProgram<B>), RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let compiled = parse_compile_normalize_and_prefilter(sql, dialect, database)?;
    Ok((compiled.table_id, compiled.program))
}

/// Parse SQL once and produce compiled bytecode plus canonical normalized form.
pub fn parse_compile_and_normalize<B, DB>(
    sql: &str,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<(TableId, BytecodeProgram<B>, String), RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let compiled = parse_compile_normalize_and_prefilter(sql, dialect, database)?;
    Ok((compiled.table_id, compiled.program, compiled.normalized))
}

/// Shared front-half of query parsing: parse -> extract table + WHERE -> resolve
/// table_id -> extract projection -> normalize.
struct ParsedQuery {
    table_id: TableId,
    where_clause: Option<Expr>,
    projection: QueryProjection,
    normalized: String,
}

fn parse_query_front_half<B, DB>(
    sql: &str,
    dialect: &B::Dialect,
    database: &DB,
    binds: &[Value<B>],
) -> Result<ParsedQuery, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let stmt = sql_shape::parse_single_statement(sql, dialect as &dyn Dialect)?;
    let (table_name, where_clause) = extract_table_and_where(&stmt)?;
    let where_clause = resolve_where_placeholders::<B>(where_clause, binds)?;
    let table_id = resolve_table_id(&table_name, database)?;
    let projection = sql_shape::extract_projection::<B, DB>(&stmt, table_id, database)?;
    let canonicalizer = Canonicalizer::new(dialect as &dyn Dialect);
    let normalized = canonicalize::normalize_where_clause(where_clause.as_ref(), &canonicalizer)?;
    Ok(ParsedQuery {
        table_id,
        where_clause,
        projection,
        normalized,
    })
}

/// Parse SQL once and produce compiled bytecode, canonical normalized form,
/// OR/NOT-aware prefilter plan, and projection kind.
pub fn parse_compile_normalize_and_prefilter<B, DB>(
    sql: &str,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<CompiledQuery<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    parse_compile_normalize_and_prefilter_with_binds(sql, dialect, database, &[], None)
}

/// Like [`parse_compile_normalize_and_prefilter`], but first resolves `$N`/`?`
/// placeholders in the WHERE clause against `binds` (in placeholder order).
///
/// Used by the typed diesel API, which renders parameterised SQL plus a
/// list of typed bind values. With an empty `binds` slice this is
/// identical to the plain entry point.
pub fn parse_compile_normalize_and_prefilter_with_binds<B, DB>(
    sql: &str,
    dialect: &B::Dialect,
    database: &DB,
    binds: &[Value<B>],
    increment: Option<crate::backend::DivisionPrecisionIncrement>,
) -> Result<CompiledQuery<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let pq = parse_query_front_half::<B, DB>(sql, dialect, database, binds)?;

    // Compile WHERE clause to bytecode.
    let (program, terms): (BytecodeProgram<B>, Vec<CompiledTerm>) =
        if let Some(expr) = pq.where_clause.as_ref() {
            compile_expression::<B, DB>(
                expr,
                pq.table_id,
                database,
                &Canonicalizer::new(dialect as &dyn Dialect),
                increment,
            )?
        } else {
            // No WHERE clause matches every row. Feed the bare `true` literal
            // through the same wrapper that trailing bare-value predicates use
            // so the VM sees a Tri-typed result at TOS.
            let mut instructions = alloc::vec![Instruction::PushLiteral(B::parse_literal(
                &SqlValue::Boolean(true),
                BuiltinKind::Bool.into(),
            )?)];
            wrap_bare_value_as_tri::<B>(&mut instructions, ComparisonRef::NONE)?;
            (BytecodeProgram::new(instructions), Vec::new())
        };

    let prefilter_plan =
        build_prefilter_plan::<B, DB>(pq.where_clause.as_ref(), pq.table_id, database);

    Ok(CompiledQuery {
        table_id: pq.table_id,
        program,
        normalized: pq.normalized,
        prefilter_plan,
        projection: pq.projection,
        terms,
    })
}

/// Table identity and WHERE-clause column dependencies for a single-table
/// SELECT, plus the parsed statement.
///
/// Lets callers outside the core compile path (the reexec wrapper) obtain
/// routing information for queries whose projection the engine does not
/// support, without re-parsing.
pub(crate) struct TableAndWhereDeps<B: Backend> {
    pub table_id: TableId,
    /// The WHERE clause compiled to bytecode, so callers can evaluate row
    /// membership in-process via the VM. A query with no WHERE compiles to
    /// an always-true program (matches every row).
    pub where_program: BytecodeProgram<B>,
    /// Columns the WHERE clause depends on (mirrors
    /// `where_program.dependency_columns`).
    pub where_dependency_columns: Vec<ColumnId>,
    pub statement: Statement,
}

/// Parse a single-table SELECT and return its table id, the compiled WHERE
/// clause (plus its dependency columns), and the parsed statement.
///
/// Unlike [`parse_and_compile`], this neither validates nor compiles the
/// projection, so it succeeds for queries (e.g. `MIN`/`MAX`) the core engine
/// rejects. It still enforces the single-table statement shape (no joins,
/// subqueries, or set operations) and resolves the table against the catalog.
pub(crate) fn parse_table_and_where_deps<B, DB>(
    sql: &str,
    dialect: &B::Dialect,
    database: &DB,
    binds: &[Value<B>],
    increment: Option<crate::backend::DivisionPrecisionIncrement>,
) -> Result<TableAndWhereDeps<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let stmt = sql_shape::parse_single_statement(sql, dialect as &dyn Dialect)?;
    let (table_name, where_clause) = extract_table_and_where(&stmt)?;
    let where_clause = resolve_where_placeholders::<B>(where_clause, binds)?;
    let table_id = resolve_table_id(&table_name, database)?;
    let where_program: BytecodeProgram<B> = if let Some(expr) = where_clause.as_ref() {
        let (program, terms) = compile_expression::<B, DB>(
            expr,
            table_id,
            database,
            &Canonicalizer::new(dialect as &dyn Dialect),
            increment,
        )?;
        // This path serves the queries the engine itself rejects, which are the
        // scalar aggregates. One in-process accumulator is shared by every
        // consumer of the aggregate, and a term makes their row sets differ, so
        // the count one subscriber reads would be another's. That is the same
        // reason an aggregate on a table with row-level security is refused.
        if !terms.is_empty() {
            return Err(RegisterError::MembershipTermRefused(
                "an aggregate cannot carry a membership term. SubQL keeps one accumulator \
                 per aggregate and shares it between consumers, and a membership term gives \
                 each consumer a different set of rows to aggregate."
                    .to_string(),
            ));
        }
        program
    } else {
        // No WHERE clause matches every row. Feed the bare `true` literal
        // through the same wrapper that trailing bare-value predicates use
        // so the VM sees a Tri-typed result at TOS.
        let mut instructions = alloc::vec![Instruction::PushLiteral(B::parse_literal(
            &SqlValue::Boolean(true),
            BuiltinKind::Bool.into(),
        )?)];
        wrap_bare_value_as_tri::<B>(&mut instructions, ComparisonRef::NONE)?;
        BytecodeProgram::new(instructions)
    };
    let where_dependency_columns = where_program.dependency_columns.clone();
    Ok(TableAndWhereDeps {
        table_id,
        where_program,
        where_dependency_columns,
        statement: stmt,
    })
}

fn resolve_table_id<DB: DatabaseLike>(
    table_name: &SqlTableName,
    database: &DB,
) -> Result<TableId, RegisterError> {
    resolve_table_reference(
        table_name.qualified.as_deref(),
        &table_name.unqualified,
        database,
    )
    .map_err(|err| match err {
        TableResolutionError::Ambiguous {
            qualified,
            unqualified,
            ..
        } => RegisterError::AmbiguousTable {
            reference: qualified.clone(),
            qualified,
            unqualified,
        },
        TableResolutionError::Unknown {
            qualified,
            unqualified,
        } => RegisterError::UnknownTable(qualified.unwrap_or(unqualified)),
    })
}

fn extract_table_and_where(
    stmt: &Statement,
) -> Result<(SqlTableName, Option<Expr>), RegisterError> {
    let (table_name, where_clause) = sql_shape::extract_single_table_and_where(stmt)?;
    Ok((SqlTableName::from_object_name(&table_name)?, where_clause))
}

/// Convert a resolved bind [`Value<B>`] into a sqlparser literal so
/// placeholder resolution can re-inject it into the AST as if the user
/// had written the literal inline.
///
/// Handles the scalars commonly bound through placeholders (Int, Float,
/// String, Null) plus Bytes, whose canonical `X'...'` hex spelling round
/// trips through [`SqlLiteralParse::parse_literal`]. Every other scalar
/// returns [`RegisterError::BindResolution`] until a downstream test
/// exercises it and pins a canonical round-trip format.
pub(crate) fn value_to_sql_value<B: Backend>(v: &Value<B>) -> Result<SqlValue, RegisterError> {
    match v {
        Value::Custom(_) => Err(RegisterError::BindResolution(
            "a bind of a custom type has no SQL literal spelling, so it cannot be re-rendered"
                .into(),
        )),
        Value::Missing => Err(RegisterError::BindResolution(
            "bind value is Missing (not a concrete value)".to_string(),
        )),
        Value::Null => Ok(SqlValue::Null),
        Value::Int(i) => Ok(SqlValue::Number(format!("{i:?}"), false)),
        Value::Float(f) => Ok(SqlValue::Number(format!("{f:?}"), false)),
        Value::String(s) => Ok(SqlValue::SingleQuotedString(s.as_ref().to_string())),
        // Uppercase hex matches sqlparser's `X'...'` rendering. The decode
        // leg (`parse_literal` -> `parse_hex_bytes`) accepts either case,
        // so `parse_literal(value_to_sql_value(Bytes(v))) == Bytes(v)`.
        Value::Bytes(b) => Ok(SqlValue::HexStringLiteral(hex_upper(b.as_ref()))),
        Value::Bool(_)
        | Value::Uuid(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_)
        | Value::Date(_)
        | Value::Time(_)
        | Value::Decimal(_)
        | Value::Json(_)
        | Value::Jsonb(_) => Err(RegisterError::BindResolution(format!(
            "bind value of {kind:?} scalar not yet supported through placeholder resolution",
            kind = v.scalar_kind(),
        ))),
    }
}

/// Map a placeholder token to a bind index: `$N` (1-based) by number, `?` by
/// position (consuming `next_positional`).
fn placeholder_index(
    token: &str,
    binds_len: usize,
    next_positional: &mut usize,
) -> Result<usize, RegisterError> {
    let idx = if token == "?" {
        let i = *next_positional;
        *next_positional += 1;
        i
    } else if let Some(n) = token.strip_prefix('$') {
        n.parse::<usize>()
            .map_err(|_| RegisterError::BindResolution(format!("malformed placeholder {token:?}")))?
            .checked_sub(1)
            .ok_or_else(|| RegisterError::BindResolution("placeholder $0 is invalid".to_string()))?
    } else {
        return Err(RegisterError::BindResolution(format!(
            "unsupported placeholder {token:?}"
        )));
    };
    if idx >= binds_len {
        return Err(RegisterError::BindResolution(format!(
            "placeholder {token:?} has no bind value ({binds_len} provided)"
        )));
    }
    Ok(idx)
}

/// Recursively replace `Value::Placeholder` leaves with their literal bind
/// values, in placeholder order. Walks only the expression shapes the compiler
/// supports; a placeholder in an unsupported position is left untouched and
/// rejected later by the compiler.
fn resolve_expr_placeholders<B: Backend>(
    expr: &mut Expr,
    binds: &[Value<B>],
    next_positional: &mut usize,
) -> Result<(), RegisterError> {
    match expr {
        Expr::Value(val) => {
            if let SqlValue::Placeholder(token) = &val.value {
                let idx = placeholder_index(token, binds.len(), next_positional)?;
                val.value = value_to_sql_value(&binds[idx])?;
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            resolve_expr_placeholders(left, binds, next_positional)?;
            resolve_expr_placeholders(right, binds, next_positional)?;
        }
        Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Nested(expr) => {
            resolve_expr_placeholders(expr, binds, next_positional)?;
        }
        Expr::InList { expr, list, .. } => {
            resolve_expr_placeholders(expr, binds, next_positional)?;
            for item in list.iter_mut() {
                resolve_expr_placeholders(item, binds, next_positional)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            resolve_expr_placeholders(expr, binds, next_positional)?;
            resolve_expr_placeholders(low, binds, next_positional)?;
            resolve_expr_placeholders(high, binds, next_positional)?;
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            resolve_expr_placeholders(expr, binds, next_positional)?;
            resolve_expr_placeholders(pattern, binds, next_positional)?;
        }
        _ => {}
    }
    Ok(())
}

/// Resolve `$N`/`?` placeholders in the optional WHERE clause against `binds`.
///
/// A no-op when `binds` is empty, so plain literal SQL (and the existing
/// rejection of stray placeholders at compile time) is byte-for-byte
/// unchanged.
fn resolve_where_placeholders<B: Backend>(
    where_clause: Option<Expr>,
    binds: &[Value<B>],
) -> Result<Option<Expr>, RegisterError> {
    if binds.is_empty() {
        return Ok(where_clause);
    }
    match where_clause {
        Some(mut expr) => {
            let mut next_positional = 0usize;
            resolve_expr_placeholders(&mut expr, binds, &mut next_positional)?;
            Ok(Some(expr))
        }
        None => Ok(None),
    }
}

/// Build the projection-disambiguated hash input from a normalized WHERE
/// clause and a projection kind.
///
/// Same WHERE clause with different projection (e.g. `SELECT *` vs
/// `SELECT COUNT(*)`) must hash to distinct predicates, and so must the same
/// aggregate grouped differently, since one maintains a value per group and
/// the other one value in total.
pub(crate) fn projection_hash_input(normalized: &str, projection: &QueryProjection) -> String {
    match projection {
        QueryProjection::Rows => normalized.to_owned(),
        QueryProjection::Aggregate(spec) => {
            format!("{normalized}\x00{}", agg_tag(spec))
        }
        QueryProjection::GroupedAggregate {
            groups,
            agg,
            having,
        } => {
            let mut out = format!("{normalized}\x00{}\x00BY", agg_tag(agg));
            // Ordered as written: `GROUP BY a, b` and `GROUP BY b, a` produce
            // the same groups, but a group's key encodes its values in this
            // order, so two subscriptions sharing one predicate would hand the
            // same group two different keys.
            for column in groups {
                out.push('\x00');
                let _ = core::fmt::Write::write_fmt(&mut out, format_args!("{column}"));
            }
            // The same fold filtered differently answers differently, so the
            // condition joins the identity, spelled through the enums' own
            // stable hash strings rather than AST `Debug`, which embeds
            // source offsets.
            if let Some(having) = having {
                let subject = match having.subject {
                    crate::HavingSubject::RowCount => "COUNT(*)",
                    crate::HavingSubject::Aggregate(function) => function.as_hash_str(),
                };
                let _ = core::fmt::Write::write_fmt(
                    &mut out,
                    format_args!(
                        "\x00HAVING\x00{subject}{}{}",
                        having.op.as_hash_str(),
                        having.threshold
                    ),
                );
            }
            out
        }
    }
}

/// The aggregate's spelling inside a predicate's identity. Explicit per
/// variant rather than derived, so a new aggregate has to be spelled here
/// instead of silently sharing another's identity.
fn agg_tag(spec: &AggSpec) -> String {
    match spec {
        AggSpec::CountStar => "COUNT(*)".to_owned(),
        AggSpec::CountColumn { column } => format!("COUNT({column})"),
        AggSpec::Sum { column } => format!("SUM({column})"),
        AggSpec::Avg { column } => format!("AVG({column})"),
        AggSpec::VarPop { column } => format!("VAR_POP({column})"),
        AggSpec::VarSamp { column } => format!("VAR_SAMP({column})"),
        AggSpec::StddevPop { column } => format!("STDDEV_POP({column})"),
        AggSpec::StddevSamp { column } => format!("STDDEV_SAMP({column})"),
    }
}

/// Lightweight parse path that extracts `(TableId, PredicateHash)` from SQL
/// without compiling bytecode or building a prefilter plan.
///
/// Used by `unregister_query` to find matching predicates by hash.
pub fn parse_and_resolve_hash<B, DB>(
    sql: &str,
    dialect: &B::Dialect,
    database: &DB,
) -> Result<(TableId, PredicateHash), RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let pq = parse_query_front_half::<B, DB>(sql, dialect, database, &[])?;
    let hash_input = projection_hash_input(&pq.normalized, &pq.projection);
    let hash = canonicalize::hash_sql(&hash_input);

    Ok((pq.table_id, hash))
}

/// Derive the follow-subscription SELECT for an UPDATE statement.
///
/// Parses `sql` and returns `SELECT * FROM t WHERE <the UPDATE's WHERE>`
/// (see `sql_shape::derive_update_follow_sql`), so the caller can register
/// it as a standing subscription. Any `$N`/`?` placeholders in the WHERE
/// are preserved and resolved later against the request's binds.
///
/// This function stays generic over `D: Dialect` rather than `B: Backend`
/// because it produces SQL text, not bytecode, so no literal coercion is
/// involved.
pub fn derive_update_follow_select<D: Dialect>(
    sql: &str,
    dialect: &D,
) -> Result<String, RegisterError> {
    let stmt = sql_shape::parse_single_statement(sql, dialect)?;
    sql_shape::derive_update_follow_sql(&stmt)
}

/// Like [`derive_update_follow_select`], but additionally reports the number
/// of bind placeholders (`$N` / `?`) that the discarded SET clause consumed.
///
/// The caller uses that count to trim SET binds from a diesel-collected bind
/// list before compiling the follow SELECT. `$N` placeholders in the returned
/// SELECT are already renumbered so the first surviving one is `$1`.
pub fn derive_update_follow_select_with_set_binds<D: Dialect>(
    sql: &str,
    dialect: &D,
) -> Result<(String, usize), RegisterError> {
    let stmt = sql_shape::parse_single_statement(sql, dialect)?;
    sql_shape::derive_update_follow_sql_and_set_binds(&stmt)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    //! Bind-placeholder resolution tests. The broad parser suite is not
    //! here yet. These pin the `Value::Bytes` placeholder path added
    //! alongside the existing `SqlLiteralParse::parse_literal` decode leg.
    use super::{value_to_sql_value, SqlLiteralParse};
    use crate::backend::{Backend, BuiltinKind, MySql, Postgres, SQLite, Value};
    use crate::RegisterError;
    use alloc::vec::Vec;
    use sqlparser::ast::Value as SqlValue;

    /// The pinned contract: `parse_literal(value_to_sql_value(Bytes(v)))`
    /// returns `Bytes(v)` for every byte vector, empty included. Encode
    /// and decode share the `X'...'` hex spelling, so a bytes bind behaves
    /// exactly as an inline hex literal.
    fn assert_bytes_round_trip<B: Backend + SqlLiteralParse>(bytes: Vec<u8>)
    where
        B::Bytes: From<Vec<u8>>,
    {
        let value = Value::<B>::Bytes(B::Bytes::from(bytes.clone()));
        let sql = value_to_sql_value(&value).unwrap();
        assert!(matches!(sql, SqlValue::HexStringLiteral(_)));
        let decoded = B::parse_literal(&sql, BuiltinKind::Bytes.into()).unwrap();
        assert_eq!(decoded, Value::<B>::Bytes(B::Bytes::from(bytes)));
    }

    #[test]
    fn bytes_round_trip_postgres() {
        assert_bytes_round_trip::<Postgres>(vec![0xde, 0xad, 0xbe, 0xef]);
        assert_bytes_round_trip::<Postgres>(vec![]);
    }

    /// Exhaustive per-byte guard: every value 0..=255 encodes to two hex
    /// digits and decodes back, so no nibble combination is lost.
    #[test]
    fn bytes_round_trip_all_byte_values() {
        assert_bytes_round_trip::<Postgres>((0u8..=255).collect());
    }

    #[test]
    fn bytes_round_trip_sqlite() {
        assert_bytes_round_trip::<SQLite>(vec![0xde, 0xad, 0xbe, 0xef]);
        assert_bytes_round_trip::<SQLite>(vec![]);
    }

    #[test]
    fn bytes_round_trip_mysql() {
        assert_bytes_round_trip::<MySql>(vec![0xde, 0xad, 0xbe, 0xef]);
        assert_bytes_round_trip::<MySql>(vec![]);
    }

    /// `X'...'` renders uppercase to match sqlparser's own spelling.
    #[test]
    fn bytes_encode_is_uppercase_hex() {
        let value = Value::<Postgres>::Bytes(vec![0x0a, 0xff, 0x00]);
        match value_to_sql_value(&value).unwrap() {
            SqlValue::HexStringLiteral(s) => assert_eq!(s, "0AFF00"),
            other => panic!("expected HexStringLiteral, got {other:?}"),
        }
    }

    /// The untouched arms still reject: a `Missing` bind and a `Bool` bind
    /// both surface `RegisterError::BindResolution`.
    #[test]
    fn missing_and_bool_binds_stay_rejected() {
        assert!(matches!(
            value_to_sql_value(&Value::<Postgres>::Missing),
            Err(RegisterError::BindResolution(_))
        ));
        assert!(matches!(
            value_to_sql_value(&Value::<Postgres>::Bool(true)),
            Err(RegisterError::BindResolution(_))
        ));
    }
}

//! SQL parser for subscription predicates.
//!
//! Compiles subscription `SELECT` statements into a Backend-generic
//! [`BytecodeProgram<B>`] the VM interprets against any
//! `E: CdcEvent<Backend = B>`. Public entry points are parameterised on
//! `B: Backend + SqlLiteralParse`; the concrete backend supplies both the
//! [`sqlparser::dialect::Dialect`] used to parse SQL text and the
//! literal-coercion rules ([`SqlLiteralParse::parse_literal`]) used to
//! turn AST literals into typed `Value<B>`.

use super::{
    canonicalize,
    literals::{hex_upper, resolve_column_ref, SqlLiteralParse},
    prefilter::build_prefilter_plan,
    sql_shape, BytecodeProgram, Instruction, PredicateHash, PrefilterPlan,
};
use crate::backend::{Backend, ScalarKind, ScalarKindOf, Value};
use crate::compiler::sql_shape::{AggSpec, QueryProjection};
use crate::table_resolution::{resolve_table_reference, TableResolutionError};
use crate::term::{term_columns, CompiledTerm};
use crate::{ColumnId, RegisterError, TableId};
use alloc::borrow::ToOwned;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;
use sqlparser::ast::{Expr, ObjectName, Statement, Value as SqlValue};
use sqlparser::dialect::Dialect;

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
}

impl<B: Backend> Compiling<B> {
    const fn new() -> Self {
        Self {
            out: Vec::new(),
            terms: Vec::new(),
        }
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
    let normalized = canonicalize::normalize_where_clause(where_clause.as_ref())?;
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
    parse_compile_normalize_and_prefilter_with_binds(sql, dialect, database, &[])
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
) -> Result<CompiledQuery<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let pq = parse_query_front_half::<B, DB>(sql, dialect, database, binds)?;

    // Compile WHERE clause to bytecode.
    let (program, terms): (BytecodeProgram<B>, Vec<CompiledTerm>) =
        if let Some(expr) = pq.where_clause.as_ref() {
            compile_expression::<B, DB>(expr, pq.table_id, database)?
        } else {
            // No WHERE clause matches every row. Feed the bare `true` literal
            // through the same wrapper that trailing bare-value predicates use
            // so the VM sees a Tri-typed result at TOS.
            let mut instructions = alloc::vec![Instruction::PushLiteral(B::parse_literal(
                &SqlValue::Boolean(true),
                ScalarKind::Bool,
            )?)];
            wrap_bare_value_as_tri::<B>(&mut instructions)?;
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
) -> Result<TableAndWhereDeps<B>, RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let stmt = sql_shape::parse_single_statement(sql, dialect as &dyn Dialect)?;
    let (table_name, where_clause) = extract_table_and_where(&stmt)?;
    let table_id = resolve_table_id(&table_name, database)?;
    let where_program: BytecodeProgram<B> = if let Some(expr) = where_clause.as_ref() {
        let (program, terms) = compile_expression::<B, DB>(expr, table_id, database)?;
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
            ScalarKind::Bool,
        )?)];
        wrap_bare_value_as_tri::<B>(&mut instructions)?;
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
/// because it produces SQL text, not bytecode — no literal coercion is
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

// ============================================================================
// Compilation helpers
// ============================================================================

/// If `expr` is a bare column reference, return the [`ScalarKind`] of that
/// column via the catalog. Otherwise `None`. Used to derive the target
/// type for a paired literal in a comparison or an IN list.
fn column_scalar_of<B: Backend, DB: DatabaseLike>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
) -> Option<ScalarKindOf<B>> {
    let col = resolve_column_ref(expr, table_id, database)?;
    crate::catalog_helpers::column_scalar_kind::<B, DB>(database, table_id, col)
}

/// Return `true` if `instr` produces a [`crate::compiler::Tri`] on the
/// stack. Used to detect whether a top-level WHERE program leaves a
/// boolean at TOS or needs to be wrapped with `= true`.
const fn instruction_is_tri_typed<B: Backend>(instr: &Instruction<B>) -> bool {
    matches!(
        instr,
        Instruction::Equal
            | Instruction::NotEqual
            | Instruction::LessThan
            | Instruction::LessThanOrEqual
            | Instruction::GreaterThan
            | Instruction::GreaterThanOrEqual
            | Instruction::IsNull
            | Instruction::IsNotNull
            | Instruction::And
            | Instruction::Or
            | Instruction::Not
            | Instruction::In(_)
            | Instruction::Between
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
fn wrap_bare_value_as_tri<B>(instructions: &mut Vec<Instruction<B>>) -> Result<(), RegisterError>
where
    B: Backend + SqlLiteralParse,
{
    match instructions.last() {
        Some(instr) if instruction_is_tri_typed(instr) => Ok(()),
        Some(_) => {
            instructions.push(Instruction::PushLiteral(B::parse_literal(
                &SqlValue::Boolean(true),
                ScalarKind::Bool,
            )?));
            instructions.push(Instruction::Equal);
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
fn compile_expression<B, DB>(
    expr: &Expr,
    table_id: TableId,
    database: &DB,
) -> Result<(BytecodeProgram<B>, Vec<CompiledTerm>), RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
    let mut compiling: Compiling<B> = Compiling::new();
    compile_expr_recursive::<B, DB>(
        expr,
        table_id,
        database,
        &mut compiling,
        0,
        ScalarKind::String,
    )?;
    wrap_bare_value_as_tri::<B>(&mut compiling)?;
    let terms = canonicalize_term_slots(&mut compiling)?;
    let columns = term_columns(&terms);
    Ok((BytecodeProgram::with_terms(compiling.out, columns), terms))
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
) -> Result<Vec<CompiledTerm>, RegisterError> {
    let terms = core::mem::take(&mut compiling.terms);
    if terms.len() < 2 {
        return Ok(terms);
    }

    let mut keyed: Vec<(String, CompiledTerm)> = terms
        .into_iter()
        .map(|term| Ok((canonicalize::normalize_expr(&term.expr)?, term)))
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

/// Recursive helper for expression compilation.
///
/// Compiles an expression to leave its result on top of stack. The
/// `target_kind` argument names the [`ScalarKind`] a standalone literal
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
    target_kind: ScalarKindOf<B>,
) -> Result<(), RegisterError>
where
    B: Backend + SqlLiteralParse,
    DB: DatabaseLike,
{
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
                // Short-circuit logical operators.
                BinaryOperator::And => {
                    compile_expr_recursive::<B, DB>(
                        left,
                        table_id,
                        database,
                        out,
                        depth + 1,
                        ScalarKind::String,
                    )?;

                    let jump_idx = out.len();
                    out.push(Instruction::JumpIfFalse(0)); // placeholder, patched below

                    let rhs_start = out.len();
                    compile_expr_recursive::<B, DB>(
                        right,
                        table_id,
                        database,
                        out,
                        depth + 1,
                        ScalarKind::String,
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
                        ScalarKind::String,
                    )?;

                    let jump_idx = out.len();
                    out.push(Instruction::JumpIfTrue(0)); // placeholder, patched below

                    let rhs_start = out.len();
                    compile_expr_recursive::<B, DB>(
                        right,
                        table_id,
                        database,
                        out,
                        depth + 1,
                        ScalarKind::String,
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
                    let child_target = column_scalar_of::<B, DB>(left, table_id, database)
                        .or_else(|| column_scalar_of::<B, DB>(right, table_id, database))
                        .unwrap_or(ScalarKind::String);
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

                    match op {
                        BinaryOperator::Eq => out.push(Instruction::Equal),
                        BinaryOperator::NotEq => out.push(Instruction::NotEqual),
                        BinaryOperator::Lt => out.push(Instruction::LessThan),
                        BinaryOperator::LtEq => out.push(Instruction::LessThanOrEqual),
                        BinaryOperator::Gt => out.push(Instruction::GreaterThan),
                        BinaryOperator::GtEq => out.push(Instruction::GreaterThanOrEqual),

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
                .ok_or_else(
                || {
                    RegisterError::UnsupportedSql(format!(
                        "Column {col_id} of table {table_id} has an unsupported SQL type for the compiler"
                    ))
                },
            )?;
            out.push(Instruction::LoadColumn(col_id));
        }

        // ====================================================================
        // Literals
        // ====================================================================
        Expr::Value(val) => {
            let value = B::parse_literal(&val.value, target_kind)?;
            out.push(Instruction::PushLiteral(value));
        }

        // ====================================================================
        // IN Lists
        // ====================================================================
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            // Derive target from the tested expression if it's a column
            // reference; fall back to String otherwise (best-effort).
            let list_target =
                column_scalar_of::<B, DB>(expr, table_id, database).unwrap_or(ScalarKind::String);

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

            out.push(Instruction::In(literals));

            if *negated {
                out.push(Instruction::Not);
            }
        }

        // ====================================================================
        // Membership subqueries
        // ====================================================================
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

        // ====================================================================
        // BETWEEN
        // ====================================================================
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let range_target =
                column_scalar_of::<B, DB>(expr, table_id, database).unwrap_or(ScalarKind::String);

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

            out.push(Instruction::Between);

            if *negated {
                out.push(Instruction::Not);
            }
        }

        // ====================================================================
        // NULL Checks
        // ====================================================================
        Expr::IsNull(inner) => {
            compile_expr_recursive::<B, DB>(
                inner,
                table_id,
                database,
                out,
                depth + 1,
                ScalarKind::String,
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
                ScalarKind::String,
            )?;
            out.push(Instruction::IsNotNull);
        }

        // ====================================================================
        // Unary Operations
        // ====================================================================
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

            compile_expr_recursive::<B, DB>(
                expr,
                table_id,
                database,
                out,
                depth + 1,
                ScalarKind::String,
            )?;
            compile_expr_recursive::<B, DB>(
                pattern,
                table_id,
                database,
                out,
                depth + 1,
                ScalarKind::String,
            )?;

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

            compile_expr_recursive::<B, DB>(
                expr,
                table_id,
                database,
                out,
                depth + 1,
                ScalarKind::String,
            )?;
            compile_expr_recursive::<B, DB>(
                pattern,
                table_id,
                database,
                out,
                depth + 1,
                ScalarKind::String,
            )?;

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
            compile_expr_recursive::<B, DB>(
                inner,
                table_id,
                database,
                out,
                depth + 1,
                target_kind,
            )?;
        }

        // ====================================================================
        // Unsupported
        // ====================================================================
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    //! Bind-placeholder resolution tests. The broad parser suite is not
    //! here yet. These pin the `Value::Bytes` placeholder path added
    //! alongside the existing `SqlLiteralParse::parse_literal` decode leg.
    use super::*;
    use crate::backend::{MySql, Postgres, SQLite};

    /// The pinned contract: `parse_literal(value_to_sql_value(Bytes(v)))`
    /// returns `Bytes(v)` for every byte vector, empty included. Encode
    /// and decode share the `X'...'` hex spelling, so a bytes bind behaves
    /// exactly as an inline hex literal.
    fn assert_bytes_round_trip<B: Backend + SqlLiteralParse>(bytes: alloc::vec::Vec<u8>)
    where
        B::Bytes: From<alloc::vec::Vec<u8>>,
    {
        let value = Value::<B>::Bytes(B::Bytes::from(bytes.clone()));
        let sql = value_to_sql_value(&value).unwrap();
        assert!(matches!(sql, SqlValue::HexStringLiteral(_)));
        let decoded = B::parse_literal(&sql, ScalarKind::Bytes).unwrap();
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

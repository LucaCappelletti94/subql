//! Adapter helpers between subql's compact ID types and the `sql-traits`
//! schema model.
//!
//! subql's runtime works with `TableId = u32` and `ColumnId = u16` (compact
//! integers chosen for bytecode/bitmap density). `sql-traits` exposes
//! schemas through [`DatabaseLike`], [`TableLike`] and [`ColumnLike`], which
//! identify columns and tables via `usize`. These free functions perform
//! the `usize` <-> `u32`/`u16` boundary conversion and centralize the
//! identifier-resolution rules so call sites never reinvent them.
//!
//! Functions: [`table_id`], [`table_name`], [`column_id`], [`resolve_table`],
//! [`table_arity`], [`schema_fingerprint`], [`primary_key_columns`],
//! [`column_scalar_kind`], [`column_comparison`], [`table_has_rls`].

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use sql_traits::{
    prelude::{ColumnLike, DatabaseLike, TableLike},
    structs::{FingerprintError, SchemaFingerprint, TargetName},
    utils::scalar_family::scalar_family,
};
use sqlite_diff_rs::SimpleTable;

use crate::backend::{ColumnComparison, ColumnComparisonOf, ScalarKind, ScalarKindOf};
use crate::types::{ColumnId, TableId};

/// Resolve a table name, written as SQL, to subql's compact [`TableId`].
///
/// The text is read by [`TargetName::parse`], so quoting decides case
/// sensitivity, a dot separates the qualifier only outside quotes, and a
/// doubled quote stands for one. Resolution is the catalog's own
/// [`DatabaseLike::resolve_target_table`]: an unqualified name resolves
/// through the search path, and a table stored without a schema resides
/// in the default schema.
///
/// Returns `None` when the text is not one valid identifier or a
/// qualified pair, when the catalog holds no such table, when the name is
/// ambiguous across the search path, or when the table's index exceeds
/// `u32::MAX`. The cases are indistinguishable to callers, whose question
/// is "which compact id, if any".
#[must_use]
pub fn table_id<DB: DatabaseLike>(database: &DB, table_name: &str) -> Option<TableId> {
    table_id_for_name(database, TargetName::parse(table_name).ok()?)
}

/// Resolve a name whose parts a caller already holds, quoting included.
///
/// [`table_id`] is for text somebody wrote as SQL. This is for a caller that
/// took the name off a parsed statement, where the identifier and its
/// quoting arrive separately and rendering them back to text only to read
/// them again would lose the quoting or invent a qualifier out of a dot
/// inside a name.
#[must_use]
pub fn table_id_for_name<DB: DatabaseLike>(
    database: &DB,
    target: TargetName<'_>,
) -> Option<TableId> {
    let table = database.resolve_target_table(target).ok()??;
    let id = database.table_id(table)?;
    u32::try_from(id).ok()
}

/// Resolve a table from separate schema and relation names.
#[must_use]
pub(crate) fn table_id_in_schema<DB: DatabaseLike>(
    database: &DB,
    schema: Option<&str>,
    table_name: &str,
) -> Option<TableId> {
    let table = database.table(schema, table_name)?;
    let id = database.table_id(table)?;
    u32::try_from(id).ok()
}
#[cfg(feature = "visibility-records")]
pub(crate) fn contract_table_id<DB: DatabaseLike>(
    database: &DB,
    table: &rls2fga_types::TableId,
) -> Option<TableId> {
    let table = database.table(table.schema(), table.name())?;
    let id = database.table_id(table)?;
    u32::try_from(id).ok()
}

/// The name the catalog stores for `table_id`, or [`None`] when it knows no such
/// table.
///
/// The inverse of [`table_id`] for the one caller that has to hand a table's
/// name to something outside subql. Unqualified, as the catalog stores it.
#[must_use]
pub fn table_name<DB: DatabaseLike>(database: &DB, table_id: TableId) -> Option<String> {
    let index = usize::try_from(table_id).ok()?;
    Some(database.table_by_id(index)?.table_name().to_string())
}

/// Resolve a column name within a table to subql's compact [`ColumnId`].
///
/// Identifier matching is upstream's, through
/// [`TableLike::column_id_by_name`], which applies the same quoting rule
/// `DatabaseLike::table` applies to relations: a quoted lookup of an already
/// folded spelling reaches a bare column, so `"id"` and `id` answer the same
/// ordinal, while `"ID"` answers a separately declared quoted column.
///
/// Returns `None` when the table is unknown, the column is not present, or the
/// column's ordinal exceeds `u16::MAX`, which is 65536 columns in one table
/// and unreachable in any sane schema.
///
/// **Complexity**: O(n) per call where `n = table.number_of_columns()`.
/// Upstream answers with one walk rather than the nested walk this used to
/// perform, but it is still a walk: callers needing repeated lookups should
/// keep the result.
#[must_use]
pub fn column_id<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
    column_name: &str,
) -> Option<ColumnId> {
    let table = database.table_by_id(table_id as usize)?;
    let ordinal = table.column_id_by_name(column_name, database).ok()??;
    u16::try_from(ordinal).ok()
}

/// The catalog table for `table_id`, or the typed refusal every metadata
/// helper shares.
fn lookup_table<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
) -> Result<&DB::Table, crate::CatalogError> {
    usize::try_from(table_id)
        .ok()
        .and_then(|index| database.table_by_id(index))
        .ok_or(crate::CatalogError::UnknownTable(table_id))
}

/// Number of columns in the table.
///
/// # Errors
/// [`CatalogError::UnknownTable`](crate::CatalogError::UnknownTable) for an id
/// the catalog does not know, [`CatalogError::Lookup`](crate::CatalogError::Lookup)
/// when the catalog itself fails to answer.
pub fn table_arity<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
) -> Result<usize, crate::CatalogError> {
    let table = lookup_table(database, table_id)?;
    table
        .number_of_columns(database)
        .map_err(|error| crate::CatalogError::Lookup { table_id, error })
}

/// Compute the spec-compliant [`SchemaFingerprint`] for the table.
///
/// Returns:
/// - `Ok(Some(fp))` when the table is known and its canonical model is
///   well-formed.
/// - `Ok(None)` when the table id is unknown to the database.
/// - `Err(FingerprintError)` when the table is known but its column model
///   is malformed (non-contiguous ordinals, duplicate or out-of-range
///   primary-key ordinals).
///
/// # Errors
///
/// See above. Propagates the validation errors from
/// [`compute_persistence_v1`](sql_traits::structs::canonical_bytes_v1).
pub fn schema_fingerprint<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
) -> Result<Option<SchemaFingerprint>, FingerprintError> {
    let Some(table) = database.table_by_id(table_id as usize) else {
        return Ok(None);
    };
    table.schema_fingerprint(database).map(Some)
}

/// Primary-key column ordinals for the table, in declaration order.
///
/// `Ok(vec![])` is a table with no declared primary key. Failure stays typed
/// the whole way: upstream's
/// [`TableLike::primary_key_column_ids`] answers
/// [`LookupError::ColumnNotFound`](sql_traits::errors::LookupError::ColumnNotFound)
/// for a key column that resolves to no ordinal rather than handing back a
/// shorter key, and this maps that onto
/// [`CatalogError::Lookup`](crate::CatalogError::Lookup), which is a refusal
/// and never an empty key.
///
/// # Errors
/// [`CatalogError`](crate::CatalogError) when the table is unknown, a lookup
/// fails, or a key column has no resolvable ordinal or one too wide for
/// [`ColumnId`].
pub fn primary_key_columns<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
) -> Result<Vec<ColumnId>, crate::CatalogError> {
    let table = lookup_table(database, table_id)?;
    let ordinals = table
        .primary_key_column_ids(database)
        .map_err(|error| crate::CatalogError::Lookup { table_id, error })?;
    ordinals
        .into_iter()
        .map(|ordinal| {
            u16::try_from(ordinal)
                .map_err(|_| crate::CatalogError::UnknownColumn { table_id, ordinal })
        })
        .collect()
}

/// Resolve a column's stored name from its compact [`ColumnId`].
///
/// The inverse of [`column_id`], through
/// [`TableLike::column_name_by_id`]. Returns `None` when the table or column
/// id is unknown.
#[must_use]
pub fn column_name<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<alloc::string::String> {
    use alloc::string::ToString;
    let table = database.table_by_id(usize::try_from(table_id).ok()?)?;
    let name = table
        .column_name_by_id(usize::from(column_id), database)
        .ok()??;
    Some(name.to_string())
}

/// Build a [`SimpleTable`] from the catalog for `table_id`.
///
/// Reads the column names in order and the primary-key indices from the
/// catalog, so the catalog is the authoritative source for both. The
/// outbound emit path and the inbound patchset apply path both build their
/// table shape through this one helper, so a single catalog is the source
/// of truth for the column order and the primary key on both sides.
///
/// # Errors
/// [`CatalogError`](crate::CatalogError) when the table id or any of its
/// columns cannot be resolved.
pub fn simple_table<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
) -> Result<SimpleTable, crate::CatalogError> {
    let arity = table_arity(database, table_id)?;
    let mut column_names: Vec<String> = Vec::with_capacity(arity);
    for ordinal in 0..arity {
        let column_id = ColumnId::try_from(ordinal)
            .map_err(|_| crate::CatalogError::UnknownColumn { table_id, ordinal })?;
        column_names.push(
            column_name(database, table_id, column_id)
                .ok_or(crate::CatalogError::UnknownColumn { table_id, ordinal })?,
        );
    }
    let table_name = lookup_table(database, table_id)?.table_name().to_string();
    let pk_indices: Vec<usize> = primary_key_columns(database, table_id)?
        .into_iter()
        .map(usize::from)
        .collect();
    let column_refs: Vec<&str> = column_names.iter().map(String::as_str).collect();
    Ok(SimpleTable::new(table_name, &column_refs, &pk_indices))
}

/// A table resolved to subql's compact ids, returned by [`resolve_table`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedTable {
    /// Compact id of the table.
    pub table_id: TableId,
    /// Ids of the requested columns, in the order their names were passed.
    pub column_ids: Vec<ColumnId>,
    /// Primary-key column ids, in declaration order (empty when none).
    pub primary_key: Vec<ColumnId>,
}

/// Resolve a table and a set of its columns in one call.
///
/// Bundles [`table_id`], a [`column_id`] lookup per name, and
/// [`primary_key_columns`]. `Ok(None)` is a table or column name the catalog
/// does not contain.
///
/// ```
/// use sql_traits::structs::ParserDB;
/// use sqlparser::dialect::PostgreSqlDialect;
/// use subql::catalog_helpers;
///
/// let db = ParserDB::parse::<PostgreSqlDialect>(
///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
/// )?;
/// let t = catalog_helpers::resolve_table(&db, "orders", &["id", "amount", "status"])?.unwrap();
/// assert_eq!(t.column_ids, vec![0, 1, 2]);
/// assert_eq!(t.primary_key, vec![0]);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Errors
/// [`CatalogError`](crate::CatalogError) when the catalog fails to answer a
/// lookup about a table it does contain.
pub fn resolve_table<DB: DatabaseLike, S: AsRef<str>>(
    database: &DB,
    table_name: &str,
    columns: &[S],
) -> Result<Option<ResolvedTable>, crate::CatalogError> {
    let Some(table_id) = table_id(database, table_name) else {
        return Ok(None);
    };
    let mut column_ids = Vec::with_capacity(columns.len());
    for name in columns {
        let Some(column_id) = column_id(database, table_id, name.as_ref()) else {
            return Ok(None);
        };
        column_ids.push(column_id);
    }
    let primary_key = primary_key_columns(database, table_id)?;
    Ok(Some(ResolvedTable {
        table_id,
        column_ids,
        primary_key,
    }))
}

/// Resolve a column's declared SQL type into its runtime [`ScalarKind`].
///
/// Builtin classification comes directly from sql-traits before the custom
/// fallback runs.
///
/// Returns `None` when the table / column id is unknown or when the
/// declared type doesn't match any supported scalar (compiler surfaces
/// this as [`crate::RegisterError::UnsupportedSql`]).
#[must_use]
pub fn column_scalar_kind<B: crate::backend::Backend, DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<ScalarKindOf<B>> {
    let table_index = usize::try_from(table_id).ok()?;
    let table = database.table_by_id(table_index)?;
    let column = table
        .column_by_id(usize::from(column_id), database)
        .ok()??;
    classify_scalar_kind::<B>(&column.data_type(database))
}

/// Returns the scalar and comparison facts for one group-key column.
#[must_use]
pub fn column_comparison<B: crate::backend::Backend, DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<ColumnComparisonOf<B>> {
    let table_index = usize::try_from(table_id).ok()?;
    let table = database.table_by_id(table_index)?;
    let column = table
        .column_by_id(usize::from(column_id), database)
        .ok()??;
    let declared_type = column.data_type(database).into_owned();
    let kind = classify_scalar_kind::<B>(&declared_type)?;
    let collation = column.collation(database).ok()?.into_owned();
    Some(ColumnComparison {
        kind,
        declared_type,
        collation,
    })
}

/// Classify a declared SQL type into its runtime [`ScalarKind`], builtins
/// first, then the backend's custom fallback. The type-only core of
/// [`column_scalar_kind`], for callers that already hold the column.
pub(crate) fn classify_scalar_kind<B: crate::backend::Backend>(
    declared_type: &str,
) -> Option<ScalarKindOf<B>> {
    if let Some(family) = scalar_family(declared_type) {
        // The family is upstream's coarse answer; the refinements the
        // declaration fixes are the backend's, because the spellings differ
        // per engine.
        return Some(ScalarKind::Builtin(B::refine_declared_type(
            family,
            declared_type,
        )));
    }
    <B::Custom as crate::backend::CustomScalars>::classify(declared_type).map(ScalarKind::Custom)
}

/// The builtin kind a column declares, or `None` when it declares none.
///
/// For callers whose question is genuinely about builtins: whether a column
/// can be aggregated, which wire type it emits, how a seed row decodes. A
/// column of a custom type answers `None`, which is the honest answer to
/// "which builtin is this", and each caller refuses it in its own terms.
/// Use [`column_scalar_kind`] where a custom column has to be served.
/// How a fold over `spec`'s column answers on this backend.
///
/// Resolved once at registration, because it follows the column's declared
/// type: measured, PostgreSQL sums an `int` column into `bigint` and a
/// `bigint` column into `numeric`, so the same statement over two integer
/// columns answers two different types. The mean and the division rule
/// come from the backend itself.
///
/// A spec that sums nothing (`COUNT`) still needs an answer, and
/// [`crate::backend::SumRule::Double`] is the one that carries no exact
/// total to disagree about. A column the catalog cannot type takes it too,
/// which is the same conservative reading `column_scalar_kind` gives every
/// other caller.
#[must_use]
pub fn fold_rule<B: crate::backend::Backend, DB: DatabaseLike>(
    spec: &crate::compiler::AggSpec,
    database: &DB,
    table_id: crate::TableId,
    increment: Option<crate::backend::DivisionPrecisionIncrement>,
) -> crate::runtime::aggregate::FoldRule {
    let total = total_rule::<B, DB>(spec, database, table_id);
    crate::runtime::aggregate::FoldRule {
        total,
        mean: B::MEAN,
        variance_seed: B::VARIANCE_SEED,
        float_overflow: B::FLOAT_SUM_OVERFLOW,
        // A fold that computes no mean never reads this, and one that
        // does is refused at registration when the setting is missing,
        // which `mysql_avg_without_the_declared_increment_is_refused`
        // holds in place.
        quotient: crate::compiler::bytecode::Quotient::resolve::<B>(increment)
            .unwrap_or(crate::compiler::bytecode::Quotient::FromTheOperands),
    }
}

/// What a total over `spec`'s column accumulates in, which is the half of
/// [`fold_rule`] a seed's decode kinds need.
#[must_use]
pub fn total_rule<B: crate::backend::Backend, DB: DatabaseLike>(
    spec: &crate::compiler::AggSpec,
    database: &DB,
    table_id: crate::TableId,
) -> crate::backend::SumRule {
    let rule = spec
        .column()
        .map_or(crate::backend::SumRule::Double, |column| {
            column_scalar_kind::<B, DB>(database, table_id, column)
                .as_ref()
                .and_then(crate::backend::ScalarKind::declared_type)
                .map_or(crate::backend::SumRule::Double, B::sum_rule)
        });
    // The accumulator's width belongs to the aggregate, not only to the
    // column. Measured on PostgreSQL 16.15 over a `real` column:
    // `pg_typeof(SUM(v))` is `real`, while `pg_typeof(AVG(v))` and
    // `pg_typeof(VAR_POP(v))` are both `double precision`, and
    // `AVG` over three `0.1`s answers `0.10000000149011612`, which is
    // the double sum divided by three rather than the single one. So a
    // narrower total is a `SUM`'s alone.
    match (rule, spec) {
        (crate::backend::SumRule::Single, crate::compiler::AggSpec::Sum { .. }) => rule,
        (crate::backend::SumRule::Single, _) => crate::backend::SumRule::Double,
        _ => rule,
    }
}

#[must_use]
pub fn column_scalar_family<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<crate::backend::ScalarFamily> {
    let table = database.table_by_id(table_id as usize)?;
    let column = table.column_by_id(column_id as usize, database).ok()??;
    scalar_family(&column.data_type(database))
}

/// Whether the table has row-level security enabled (per
/// [`TableLike::has_row_level_security`]).
///
/// The reexec wrapper consults this when classifying aggregator queries:
/// under RLS, different viewers observe different result rows, so a single
/// in-process IVM state would be unsafe to share across consumers. The
/// wrapper rejects such registrations until per-consumer total re-execution
/// lands. Failure is typed so a catalog that cannot answer refuses the
/// registration instead of silently authorizing a shared answer.
///
/// # Errors
/// [`CatalogError`](crate::CatalogError) when the table is unknown or the
/// lookup fails.
pub fn table_has_rls<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
) -> Result<bool, crate::CatalogError> {
    let table = lookup_table(database, table_id)?;
    table
        .has_row_level_security(database)
        .map_err(|error| crate::CatalogError::Lookup { table_id, error })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::{Postgres, ScalarFamily};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::GenericDialect;

    fn make_db() -> ParserDB {
        ParserDB::parse::<GenericDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
        )
        .expect("DDL parses")
    }

    #[test]
    fn table_id_resolves_known_table() {
        let db = make_db();
        let tid = table_id(&db, "orders").expect("orders table exists");
        assert_eq!(table_arity(&db, tid), Ok(3));
    }

    #[test]
    fn table_id_none_for_unknown_table() {
        let db = make_db();
        assert!(table_id(&db, "no_such_table").is_none());
    }

    /// The review's reproduction, kept permanently: a quoted, case-sensitive
    /// qualified name resolves by identifier semantics, never by string
    /// splitting on dots and quotes.
    #[test]
    fn table_id_resolves_quoted_qualified_name() {
        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            r#"CREATE SCHEMA "App"; CREATE TABLE "App"."Items" (id INT PRIMARY KEY);"#,
        )
        .unwrap();
        assert_eq!(table_id(&db, r#""App"."Items""#), Some(0));
    }

    /// A dot inside a quoted identifier belongs to the identifier, never a
    /// qualifier separator.
    #[test]
    fn table_id_resolves_quoted_name_containing_a_dot() {
        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            r#"CREATE TABLE "my.table" (id INT PRIMARY KEY);"#,
        )
        .unwrap();
        assert_eq!(table_id(&db, r#""my.table""#), Some(0));
    }

    /// A doubled quote inside a quoted identifier stands for one quote.
    #[test]
    fn table_id_resolves_escaped_quotes() {
        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            r#"CREATE TABLE "we""ird" (id INT PRIMARY KEY);"#,
        )
        .unwrap();
        assert_eq!(table_id(&db, r#""we""ird""#), Some(0));
    }

    /// An unqualified name resolves through the search path, exactly as the
    /// catalog resolves every other written target.
    #[test]
    fn table_id_resolves_through_the_search_path() {
        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            "CREATE SCHEMA app; SET search_path TO app; CREATE TABLE app.docs (id INT PRIMARY KEY);",
        )
        .unwrap();
        assert_eq!(table_id(&db, "docs"), Some(0));
    }

    /// A bare spelling and its default-schema qualified spelling reach the
    /// same table.
    #[test]
    fn table_id_resolves_bare_and_qualified_spellings() {
        let db = make_db();
        assert_eq!(table_id(&db, "orders"), Some(0));
        assert_eq!(table_id(&db, "public.orders"), Some(0));
    }

    /// Text that does not spell one identifier, or a qualifier and a name,
    /// resolves to nothing.
    ///
    /// The refusals are the part of this contract a caller can trip over by
    /// passing text it did not build itself, so each spelling is named
    /// rather than sampled. A three-part name is refused because subql's
    /// catalog has no cross-database resolution to hand it to, not because
    /// the text is unreadable.
    ///
    /// The fixture stores two tables whose real names are themselves
    /// unreadable as written text, a dotted one and one carrying a quote.
    /// Without them the test cannot tell a refusal from a name that simply
    /// resolves to nothing: taking the whole text as one unquoted
    /// identifier would answer `None` for every other spelling here and
    /// still reach these two.
    #[test]
    fn table_id_refuses_text_that_is_not_one_written_name() {
        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            r#"CREATE TABLE orders (id INT PRIMARY KEY);
               CREATE TABLE "a.b.c" (id INT PRIMARY KEY);
               CREATE TABLE "we""ird" (id INT PRIMARY KEY);"#,
        )
        .unwrap();
        for text in [
            "",
            " ",
            ".",
            "orders.",
            ".orders",
            "\"orders",
            "\"orders\"x",
            "orders orders",
            "orders,",
            "public.public.orders",
            "SELECT",
            // Three parts, and the catalog does hold a table named `a.b.c`,
            // which only its quoted spelling reaches.
            "a.b.c",
            // A quote where a dot or the end of the name was required, and
            // the catalog does hold a table named `we"ird`.
            "we\"ird",
        ] {
            assert_eq!(
                table_id(&db, text),
                None,
                "`{text}` does not name one table"
            );
        }
        // The spellings that do reach those two tables, so the refusals
        // above are about how the text was written and not about the
        // catalog missing them. The ids are not asserted by number: the
        // catalog orders its tables by name, not by the order the DDL
        // declared them.
        let dotted = table_id(&db, r#""a.b.c""#).expect("the quoted dotted name resolves");
        let quoted = table_id(&db, r#""we""ird""#).expect("the doubled quote resolves");
        let plain = table_id(&db, "orders").expect("the bare name resolves");
        assert_ne!(dotted, quoted);
        assert_ne!(dotted, plain);
        assert_ne!(quoted, plain);
    }

    /// A name whose parts are spelled unquoted resolves case-insensitively,
    /// and a quoted part only matches what the catalog stored.
    #[test]
    fn table_id_reads_quoting_as_sql_would() {
        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            r#"CREATE SCHEMA app; CREATE TABLE app."Items" (id INT PRIMARY KEY);"#,
        )
        .unwrap();
        // The stored name is quoted and mixed case, so only that spelling
        // reaches it.
        assert_eq!(table_id(&db, r#"app."Items""#), Some(0));
        assert_eq!(table_id(&db, r#""app"."Items""#), Some(0));
        assert_eq!(table_id(&db, r#"app."items""#), None);
        assert_eq!(table_id(&db, "app.Items"), None);
    }

    /// Whitespace around a written name, which SQL's tokenizer skips.
    ///
    /// Kept separate from the refusals because this is the case where the
    /// text is a name and only its padding is in question.
    #[test]
    fn table_id_reads_a_padded_name() {
        let db = make_db();
        assert_eq!(table_id(&db, " orders"), Some(0));
        assert_eq!(table_id(&db, "orders "), Some(0));
        assert_eq!(table_id(&db, "public . orders"), Some(0));
    }

    #[test]
    fn table_id_in_schema_keeps_the_name_parts_separate() {
        let db = ParserDB::parse::<GenericDialect>(
            "CREATE SCHEMA east;
             CREATE SCHEMA west;
             CREATE TABLE east.orders (id INT);
             CREATE TABLE west.orders (id INT);",
        )
        .expect("DDL parses");

        let east = table_id_in_schema(&db, Some("east"), "orders").expect("east orders exists");
        let west = table_id_in_schema(&db, Some("west"), "orders").expect("west orders exists");

        assert_ne!(east, west);
    }

    #[test]
    fn column_id_resolves_each_column_to_its_ordinal() {
        let db = make_db();
        let tid = table_id(&db, "orders").unwrap();
        assert_eq!(column_id(&db, tid, "id"), Some(0));
        assert_eq!(column_id(&db, tid, "amount"), Some(1));
        assert_eq!(column_id(&db, tid, "status"), Some(2));
    }

    #[test]
    fn column_name_resolves_the_requested_ordinal() {
        let db = make_db();
        let table = table_id(&db, "orders").expect("orders exists");

        assert_eq!(column_name(&db, table, 1).as_deref(), Some("amount"));
        assert_eq!(column_name(&db, table, 3), None);
    }

    #[test]
    fn column_id_is_case_insensitive_for_unquoted_lookup() {
        let db = make_db();
        let tid = table_id(&db, "orders").unwrap();
        assert_eq!(column_id(&db, tid, "AMOUNT"), Some(1));
    }

    #[test]
    fn column_id_none_for_unknown_column() {
        let db = make_db();
        let tid = table_id(&db, "orders").unwrap();
        assert!(column_id(&db, tid, "nope").is_none());
    }

    #[test]
    fn primary_key_columns_returns_pk_ordinals() {
        let db = make_db();
        let tid = table_id(&db, "orders").unwrap();
        assert_eq!(primary_key_columns(&db, tid), Ok(vec![0]));
    }

    #[test]
    fn primary_key_columns_empty_when_no_pk() {
        let db = ParserDB::parse::<GenericDialect>("CREATE TABLE t (a INT, b TEXT);")
            .expect("DDL parses");
        let tid = table_id(&db, "t").unwrap();
        assert_eq!(primary_key_columns(&db, tid), Ok(vec![]));
    }

    /// The identifier rule for a column lookup is upstream's, and it is not a
    /// string compare: the lookup text is read as SQL would read it. A quoted
    /// lookup of an already folded spelling reaches a bare column, so `"id"`
    /// and `id` answer the same ordinal, while `"ID"` answers the separately
    /// declared quoted column beside it.
    #[test]
    fn column_id_reads_the_lookup_as_sql_would() {
        let db = ParserDB::parse::<GenericDialect>(
            r#"CREATE TABLE mixed (id INT, "ID" INT, amount INT);"#,
        )
        .expect("DDL parses");
        let tid = table_id(&db, "mixed").expect("mixed exists");

        assert_eq!(column_id(&db, tid, "id"), Some(0));
        assert_eq!(column_id(&db, tid, "\"id\""), Some(0));
        assert_eq!(column_id(&db, tid, "\"ID\""), Some(1));
        assert_eq!(
            column_id(&db, tid, "ID"),
            Some(0),
            "an unquoted lookup folds, so it reaches the bare column"
        );
        // And the inverse answers the stored spelling of each.
        assert_eq!(column_name(&db, tid, 0).as_deref(), Some("id"));
        assert_eq!(column_name(&db, tid, 1).as_deref(), Some("ID"));
    }

    /// A composite key declared in an order the columns do not follow comes
    /// back in key order. Sorting the ordinals, or reading the columns rather
    /// than the key, answers `[0, 2]` and pairs every key value with the wrong
    /// column.
    #[test]
    fn primary_key_columns_keeps_the_declared_key_order() {
        let db = ParserDB::parse::<GenericDialect>(
            "CREATE TABLE lots (id INT, amount INT, status TEXT, PRIMARY KEY (status, id));",
        )
        .expect("DDL parses");
        let tid = table_id(&db, "lots").expect("lots exists");

        assert_eq!(primary_key_columns(&db, tid), Ok(alloc::vec![2, 0]));
    }
    /// A table id the catalog does not know is a typed refusal on every
    /// metadata helper, never a silent empty or false answer.
    #[test]
    fn unknown_table_id_is_a_typed_error() {
        let db = make_db();
        let missing: TableId = 99;
        assert_eq!(
            table_arity(&db, missing),
            Err(crate::CatalogError::UnknownTable(missing))
        );
        assert_eq!(
            table_has_rls(&db, missing),
            Err(crate::CatalogError::UnknownTable(missing))
        );
        assert_eq!(
            primary_key_columns(&db, missing),
            Err(crate::CatalogError::UnknownTable(missing))
        );
        assert_eq!(
            simple_table(&db, missing).unwrap_err(),
            crate::CatalogError::UnknownTable(missing)
        );
    }

    #[test]
    fn schema_fingerprint_round_trips_for_same_schema() {
        let db_a = make_db();
        let db_b = make_db();
        let tid_a = table_id(&db_a, "orders").unwrap();
        let tid_b = table_id(&db_b, "orders").unwrap();
        let fp_a = schema_fingerprint(&db_a, tid_a).unwrap().unwrap();
        let fp_b = schema_fingerprint(&db_b, tid_b).unwrap().unwrap();
        assert_eq!(fp_a, fp_b);
    }

    #[test]
    fn schema_fingerprint_differs_for_different_schemas() {
        let db_a = make_db();
        let db_b = ParserDB::parse::<GenericDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, total INT, status TEXT);",
        )
        .unwrap();
        let tid_a = table_id(&db_a, "orders").unwrap();
        let tid_b = table_id(&db_b, "orders").unwrap();
        let fp_a = schema_fingerprint(&db_a, tid_a).unwrap().unwrap();
        let fp_b = schema_fingerprint(&db_b, tid_b).unwrap().unwrap();
        assert_ne!(fp_a, fp_b);
    }

    #[test]
    fn schema_fingerprint_none_for_unknown_table() {
        let db = make_db();
        assert!(schema_fingerprint(&db, 9_999).unwrap().is_none());
    }

    #[test]
    fn column_scalar_kind_classifies_temporal_columns_through_ddl() {
        // Postgres spellings must classify precisely, because `PgAdapter`
        // dispatches native temporal binds off the resolved `ScalarKind`.
        let pg = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            "CREATE TABLE e (id INT PRIMARY KEY, ts TIMESTAMP, tstz TIMESTAMPTZ, d DATE, t TIME);",
        )
        .unwrap();
        let tid = table_id(&pg, "e").unwrap();
        assert_eq!(
            column_scalar_kind::<Postgres, _>(&pg, tid, 1),
            Some(ScalarFamily::Timestamp.into())
        );
        assert_eq!(
            column_scalar_kind::<Postgres, _>(&pg, tid, 2),
            Some(ScalarFamily::TimestampTz.into())
        );
        assert_eq!(
            column_scalar_kind::<Postgres, _>(&pg, tid, 3),
            Some(ScalarFamily::Date.into())
        );
        assert_eq!(
            column_scalar_kind::<Postgres, _>(&pg, tid, 4),
            Some(ScalarFamily::Time.into())
        );

        // MySQL spellings, including `DATETIME` and `BIGINT UNSIGNED`.
        // `DATETIME` classifies as a wall-clock `Timestamp`, and `BIGINT
        // UNSIGNED` folds into the integer family.
        let my = ParserDB::parse::<sqlparser::dialect::MySqlDialect>(
            "CREATE TABLE e (id INT PRIMARY KEY, dt DATETIME, ts TIMESTAMP, d DATE, t TIME, big BIGINT UNSIGNED);",
        )
        .unwrap();
        let tid = table_id(&my, "e").unwrap();
        assert_eq!(
            column_scalar_kind::<Postgres, _>(&my, tid, 1),
            Some(ScalarFamily::Timestamp.into())
        );
        assert_eq!(
            column_scalar_kind::<Postgres, _>(&my, tid, 2),
            Some(ScalarFamily::Timestamp.into())
        );
        assert_eq!(
            column_scalar_kind::<Postgres, _>(&my, tid, 3),
            Some(ScalarFamily::Date.into())
        );
        assert_eq!(
            column_scalar_kind::<Postgres, _>(&my, tid, 4),
            Some(ScalarFamily::Time.into())
        );
        assert_eq!(
            column_scalar_kind::<Postgres, _>(&my, tid, 5),
            Some(ScalarFamily::Int.into())
        );
    }

    #[test]
    fn column_comparison_preserves_postgres_collation_facts() {
        use crate::backend::{ColumnCollation, ColumnComparisonOf};

        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
             CREATE TABLE labels (name TEXT COLLATE ci);",
        )
        .unwrap();
        let table = table_id(&db, "labels").unwrap();

        let column: ColumnComparisonOf<Postgres> =
            column_comparison::<Postgres, _>(&db, table, 0).unwrap();
        assert_eq!(column.kind, ScalarFamily::String.into());
        assert_eq!(column.declared_type, "TEXT");
        let ColumnCollation::Named(collation) = column.collation else {
            panic!("expected named collation")
        };
        assert_eq!(collation.name().name(), "ci");
        assert_eq!(collation.postgres_deterministic(), Some(false));
    }

    #[test]
    fn column_comparison_distinguishes_default_and_unknown_collations() {
        use crate::backend::{ColumnCollation, ColumnComparisonOf};

        let default_db = ParserDB::parse::<sqlparser::dialect::SQLiteDialect>(
            "CREATE TABLE labels (name TEXT);",
        )
        .unwrap();
        let table = table_id(&default_db, "labels").unwrap();
        let column: ColumnComparisonOf<crate::backend::SQLite> =
            column_comparison::<crate::backend::SQLite, _>(&default_db, table, 0).unwrap();
        assert_eq!(column.collation, ColumnCollation::DatabaseDefault);

        let unknown_db = ParserDB::parse::<sqlparser::dialect::MySqlDialect>(
            "CREATE TABLE labels (name TEXT CHARACTER SET utf8mb4);",
        )
        .unwrap();
        let table = table_id(&unknown_db, "labels").unwrap();
        let column: ColumnComparisonOf<crate::backend::MySql> =
            column_comparison::<crate::backend::MySql, _>(&unknown_db, table, 0).unwrap();
        assert_eq!(column.collation, ColumnCollation::Unknown);
    }
}

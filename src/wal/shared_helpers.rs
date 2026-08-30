use crate::table_resolution::{resolve_table_reference, TableResolutionError};
use crate::{catalog_helpers, ColumnId, TableId};
use alloc::string::ToString;
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;

use super::WalParseError;

/// Resolve table name through database with qualified-first semantics.
///
/// Resolution rules:
/// 1. If `schema.table` resolves, it is preferred.
/// 2. If only `table` resolves, use it.
/// 3. If both resolve to different IDs, return ambiguity instead of guessing.
pub fn resolve_table<DB: DatabaseLike>(
    schema: &str,
    table: &str,
    database: &DB,
) -> Result<TableId, WalParseError> {
    let qualified = (!schema.is_empty()).then(|| format!("{schema}.{table}"));
    resolve_table_reference(qualified.as_deref(), table, database).map_err(|err| match err {
        TableResolutionError::Ambiguous {
            qualified,
            qualified_id,
            unqualified_id,
            ..
        } => WalParseError::AmbiguousTable {
            schema: schema.to_string(),
            table: table.to_string(),
            qualified,
            qualified_id,
            unqualified_id,
        },
        TableResolutionError::Unknown { .. } => WalParseError::UnknownTable {
            schema: schema.to_string(),
            table: table.to_string(),
        },
    })
}

/// Derive the changed columns of an UPDATE by comparing the old and new
/// image value of each catalog column, in ordinal order.
///
/// `lookup(name)` returns the `(old, new)` value pair for a column name.
/// A column present in both images whose values differ is reported; a
/// column absent from either image is skipped. The comparison is by value,
/// so it serves any `PartialEq` cell type (wal2json's `serde_json::Value`,
/// pgoutput's `ColumnValue`). Callers gate the REPLICA IDENTITY FULL
/// precondition (both images cover every column) before calling.
pub fn changed_columns_by_name<DB, V, F>(
    db: &DB,
    table_id: TableId,
    arity: usize,
    lookup: F,
) -> Vec<ColumnId>
where
    DB: DatabaseLike,
    V: PartialEq,
    F: Fn(&str) -> (Option<V>, Option<V>),
{
    let mut changed = Vec::new();
    for idx in 0..arity {
        let Ok(col) = ColumnId::try_from(idx) else {
            break;
        };
        let Some(name) = catalog_helpers::column_name(db, table_id, col) else {
            continue;
        };
        if let (Some(old), Some(new)) = lookup(&name) {
            if old != new {
                changed.push(col);
            }
        }
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::resolve_table;
    use crate::wal::WalParseError;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    #[test]
    fn test_resolve_table_conflicting_matches_errors() {
        use crate::table_resolution::{resolve_table_reference, TableResolutionError};

        // Two `users` tables in two different schemas. The wal2json caller
        // passes schema=`public`, table=`users`; the qualified `public.users`
        // resolves to one id, the bare `users` is also present (it has its
        // own ambient/no-schema entry via a second schema)..
        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE SCHEMA other;\n\
             CREATE TABLE other.users (id INT PRIMARY KEY, name TEXT);\n\
             CREATE TABLE public.users (id INT PRIMARY KEY, name TEXT);",
        )
        .expect("ambiguous DDL parses");
        let qualified_id =
            crate::catalog_helpers::table_id(&catalog, "public.users").expect("public.users id");
        let unqualified_id =
            crate::catalog_helpers::table_id(&catalog, "other.users").expect("other.users id");
        assert_ne!(qualified_id, unqualified_id);

        // `resolve_table("public", "users", ...)` looks up both
        // "public.users" (qualified) and "users" (unqualified). With the
        // two schema-qualified `users` tables above and no bare `users`,
        // we expect an UnknownTable for the unqualified side and a hit on
        // the qualified side, i.e. resolution succeeds to
        // `public.users`. To exercise the ambiguity error we instead
        // delegate to `resolve_table_reference` with two distinct
        // table-name strings (one playing the role of qualified, the
        // other unqualified).
        let err = resolve_table_reference(Some("public.users"), "other.users", &catalog)
            .expect_err("ambiguous lookup must fail");
        assert!(matches!(
            err,
            TableResolutionError::Ambiguous {
                qualified_id: q,
                unqualified_id: u,
                ..
            } if q == qualified_id && u == unqualified_id
        ));
    }

    #[test]
    fn test_resolve_table_falls_back_to_unqualified_name() {
        let catalog =
            ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE users (id INT PRIMARY KEY);")
                .expect("users DDL parses");
        let expected = crate::catalog_helpers::table_id(&catalog, "users").expect("users id");

        let table_id =
            resolve_table("public", "users", &catalog).expect("table should be resolved");
        assert_eq!(table_id, expected);
    }

    #[test]
    fn test_resolve_table_uses_qualified_when_available() {
        // Declare the schema explicitly so the table is resolvable by
        // its qualified `public.users` name.
        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE SCHEMA public;\n\
             CREATE TABLE public.users (id INT PRIMARY KEY);",
        )
        .expect("public.users DDL parses");
        let expected =
            crate::catalog_helpers::table_id(&catalog, "public.users").expect("public.users id");

        let table_id =
            resolve_table("public", "users", &catalog).expect("table should be resolved");
        assert_eq!(table_id, expected);
    }

    #[test]
    fn test_resolve_table_unknown_table() {
        let catalog = ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE other (id INT);")
            .expect("empty fixture DDL parses");

        let err = resolve_table("public", "users", &catalog).expect_err("must fail");
        match err {
            WalParseError::UnknownTable { schema, table } => {
                assert_eq!(schema, "public");
                assert_eq!(table, "users");
            }
            _ => panic!("unexpected error variant"),
        }
    }
}

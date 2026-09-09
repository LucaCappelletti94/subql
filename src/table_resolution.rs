use crate::{catalog_helpers, TableId};
use alloc::string::{String, ToString};
use sql_traits::prelude::DatabaseLike;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableResolutionError {
    Ambiguous {
        qualified: String,
        unqualified: String,
        qualified_id: TableId,
        unqualified_id: TableId,
    },
    Unknown {
        qualified: Option<String>,
        unqualified: String,
    },
}

pub fn resolve_table_reference<B: crate::backend::Backend, DB: DatabaseLike>(
    qualified: Option<&str>,
    unqualified: &str,
    database: &DB,
) -> Result<TableId, TableResolutionError> {
    let qualified_id =
        qualified.and_then(|name| catalog_helpers::table_id::<B, DB>(database, name));
    let unqualified_id = catalog_helpers::table_id::<B, DB>(database, unqualified);
    resolve_table_ids(qualified_id, unqualified_id, unqualified, || {
        qualified.map(ToString::to_string)
    })
}

/// Resolve table-name parts carried as wire identifier values.
pub fn resolve_table_parts<B: crate::backend::Backend, DB: DatabaseLike>(
    schema: Option<&str>,
    table: &str,
    database: &DB,
) -> Result<TableId, TableResolutionError> {
    let qualified_id = schema.and_then(|schema| {
        catalog_helpers::table_id_in_schema::<B, DB>(database, Some(schema), table)
    });
    let unqualified_id = catalog_helpers::table_id_in_schema::<B, DB>(database, None, table);
    resolve_table_ids(qualified_id, unqualified_id, table, || {
        schema.map(|schema| alloc::format!("{schema}.{table}"))
    })
}

fn resolve_table_ids(
    qualified_id: Option<TableId>,
    unqualified_id: Option<TableId>,
    unqualified: &str,
    qualified_name: impl FnOnce() -> Option<String>,
) -> Result<TableId, TableResolutionError> {
    match (qualified_id, unqualified_id) {
        (Some(q), Some(u)) if q != u => Err(TableResolutionError::Ambiguous {
            qualified: qualified_name().unwrap_or_default(),
            unqualified: unqualified.to_string(),
            qualified_id: q,
            unqualified_id: u,
        }),
        (Some(q), _) => Ok(q),
        (None, Some(u)) => Ok(u),
        (None, None) => Err(TableResolutionError::Unknown {
            qualified: qualified_name(),
            unqualified: unqualified.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::GenericDialect;

    fn parse(sql: &str) -> ParserDB {
        ParserDB::parse::<GenericDialect>(sql).expect("DDL parses")
    }

    #[test]
    fn resolves_known_table() {
        let db = parse("CREATE TABLE orders (id INT);");
        let resolved = resolve_table_reference::<crate::backend::Postgres, _>(None, "orders", &db)
            .expect("resolution should succeed");
        assert_eq!(
            catalog_helpers::table_id::<crate::backend::Postgres, _>(&db, "orders"),
            Some(resolved)
        );
    }

    /// A wire message's table name is an identifier value, so a dot inside
    /// it belongs to the name.
    ///
    /// [`resolve_table_parts`] takes what a CDC message carried, where the
    /// quoting a written name would have shown is already gone. Reading it
    /// as SQL text would make `my.table` a qualifier and a name, and the
    /// schema `my` does not exist.
    #[test]
    fn wire_parts_keep_a_dotted_name_whole() {
        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            r#"CREATE TABLE "my.table" (id INT);"#,
        )
        .expect("DDL parses");
        let expected =
            catalog_helpers::table_id::<crate::backend::Postgres, _>(&db, r#""my.table""#)
                .expect("the quoted spelling resolves");

        assert_eq!(
            resolve_table_parts::<crate::backend::Postgres, _>(None, "my.table", &db),
            Ok(expected)
        );
        assert_eq!(
            resolve_table_parts::<crate::backend::Postgres, _>(Some("public"), "my.table", &db),
            Ok(expected),
            "and the schema the catalog stores it under reaches it too"
        );
    }

    #[test]
    fn wire_parts_match_a_stored_mixed_case_name_exactly() {
        let db = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(
            r#"CREATE SCHEMA app;
               CREATE SCHEMA "App";
               CREATE TABLE app.docs (id INT);
               CREATE TABLE "App"."Docs" (id INT);"#,
        )
        .expect("DDL parses");
        let expected =
            catalog_helpers::table_id::<crate::backend::Postgres, _>(&db, r#""App"."Docs""#)
                .expect("the mixed-case table exists");
        let folded = catalog_helpers::table_id::<crate::backend::Postgres, _>(&db, "app.docs")
            .expect("the folded table exists");

        assert_ne!(expected, folded);
        assert_eq!(
            resolve_table_parts::<crate::backend::Postgres, _>(Some("App"), "Docs", &db),
            Ok(expected)
        );
    }

    #[test]
    fn mysql_wire_names_follow_lower_case_table_names() {
        use crate::backend::{MySql, NamesFoldedAtLookup, NamesStoredLowercased};

        let db = ParserDB::parse::<sqlparser::dialect::MySqlDialect>("CREATE TABLE Docs (id INT);")
            .expect("DDL parses");

        assert_eq!(
            resolve_table_parts::<MySql<NamesStoredLowercased>, _>(None, "docs", &db),
            Ok(0)
        );
        assert_eq!(
            resolve_table_parts::<MySql<NamesFoldedAtLookup>, _>(None, "docs", &db),
            Ok(0)
        );
    }

    #[test]
    fn sqlite_wire_names_fold_like_the_engine() {
        let db =
            ParserDB::parse::<sqlparser::dialect::SQLiteDialect>("CREATE TABLE Docs (id INT);")
                .expect("DDL parses");

        assert_eq!(
            resolve_table_parts::<crate::backend::SQLite, _>(None, "docs", &db),
            Ok(0)
        );
    }

    #[test]
    fn unknown_contains_best_available_reference() {
        let db = parse("CREATE TABLE elsewhere (id INT);");

        let err = resolve_table_reference::<crate::backend::Postgres, _>(
            Some("public.orders"),
            "orders",
            &db,
        )
        .expect_err("missing table should fail");
        assert!(matches!(
            err,
            TableResolutionError::Unknown {
                qualified: Some(qualified),
                unqualified,
            } if qualified == "public.orders" && unqualified == "orders"
        ));

        let err_unqualified =
            resolve_table_reference::<crate::backend::Postgres, _>(None, "orders", &db)
                .expect_err("unqualified missing table should fail");
        assert!(matches!(
            err_unqualified,
            TableResolutionError::Unknown {
                qualified: None,
                unqualified,
            } if unqualified == "orders"
        ));
    }
}

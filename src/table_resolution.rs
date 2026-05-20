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

pub fn resolve_table_reference<DB: DatabaseLike>(
    qualified: Option<&str>,
    unqualified: &str,
    database: &DB,
) -> Result<TableId, TableResolutionError> {
    let qualified_id = qualified.and_then(|name| catalog_helpers::table_id(database, name));
    let unqualified_id = catalog_helpers::table_id(database, unqualified);

    match (qualified_id, unqualified_id) {
        (Some(q), Some(u)) if q != u => Err(TableResolutionError::Ambiguous {
            qualified: qualified.unwrap_or_default().to_string(),
            unqualified: unqualified.to_string(),
            qualified_id: q,
            unqualified_id: u,
        }),
        (Some(q), _) => Ok(q),
        (None, Some(u)) => Ok(u),
        (None, None) => Err(TableResolutionError::Unknown {
            qualified: qualified.map(str::to_string),
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
        let resolved =
            resolve_table_reference(None, "orders", &db).expect("resolution should succeed");
        assert_eq!(catalog_helpers::table_id(&db, "orders"), Some(resolved));
    }

    #[test]
    fn unknown_contains_best_available_reference() {
        let db = parse("CREATE TABLE elsewhere (id INT);");

        let err = resolve_table_reference(Some("public.orders"), "orders", &db)
            .expect_err("missing table should fail");
        assert!(matches!(
            err,
            TableResolutionError::Unknown {
                qualified: Some(qualified),
                unqualified,
            } if qualified == "public.orders" && unqualified == "orders"
        ));

        let err_unqualified = resolve_table_reference(None, "orders", &db)
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

use crate::{SchemaCatalog, TableId};

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

pub fn resolve_table_reference(
    qualified: Option<&str>,
    unqualified: &str,
    catalog: &dyn SchemaCatalog,
) -> Result<TableId, TableResolutionError> {
    let qualified_id = qualified.and_then(|name| catalog.table_id(name));
    let unqualified_id = catalog.table_id(unqualified);

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
    use crate::testing::MockCatalog;
    use std::collections::HashMap;

    #[test]
    fn prefers_qualified_when_both_resolve_to_same_id() {
        let tables = HashMap::from([
            ("orders".to_string(), (1_u32, 2_usize)),
            ("public.orders".to_string(), (1_u32, 2_usize)),
        ]);
        let catalog = MockCatalog {
            tables,
            columns: HashMap::new(),
        };

        let resolved = resolve_table_reference(Some("public.orders"), "orders", &catalog)
            .expect("resolution should succeed");
        assert_eq!(resolved, 1);
    }

    #[test]
    fn errors_when_qualified_and_unqualified_are_ambiguous() {
        let tables = HashMap::from([
            ("orders".to_string(), (1_u32, 2_usize)),
            ("public.orders".to_string(), (2_u32, 2_usize)),
        ]);
        let catalog = MockCatalog {
            tables,
            columns: HashMap::new(),
        };

        let err = resolve_table_reference(Some("public.orders"), "orders", &catalog)
            .expect_err("ambiguous resolution should fail");
        assert!(matches!(
            err,
            TableResolutionError::Ambiguous {
                qualified,
                unqualified,
                qualified_id: 2,
                unqualified_id: 1,
            } if qualified == "public.orders" && unqualified == "orders"
        ));
    }

    #[test]
    fn falls_back_to_unqualified_when_qualified_missing() {
        let tables = HashMap::from([("orders".to_string(), (1_u32, 2_usize))]);
        let catalog = MockCatalog {
            tables,
            columns: HashMap::new(),
        };

        let resolved = resolve_table_reference(Some("public.orders"), "orders", &catalog)
            .expect("unqualified fallback should work");
        assert_eq!(resolved, 1);
    }

    #[test]
    fn unknown_contains_best_available_reference() {
        let catalog = MockCatalog {
            tables: HashMap::new(),
            columns: HashMap::new(),
        };

        let err = resolve_table_reference(Some("public.orders"), "orders", &catalog)
            .expect_err("missing table should fail");
        assert!(matches!(
            err,
            TableResolutionError::Unknown {
                qualified: Some(qualified),
                unqualified,
            } if qualified == "public.orders" && unqualified == "orders"
        ));

        let err_unqualified = resolve_table_reference(None, "orders", &catalog)
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

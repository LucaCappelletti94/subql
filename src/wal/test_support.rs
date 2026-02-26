use crate::{ColumnId, SchemaCatalog, TableId};
use std::collections::HashMap;

/// Shared test catalog: schema="public", db="mydb", table="orders",
/// columns: id=0, amount=1, status=2, comment=3, PK=[id].
/// Used by parsers that emit a full 4-column orders row (e.g. Debezium).
pub(super) fn orders_catalog() -> TestCatalog {
    let mut tables = HashMap::new();
    tables.insert("orders".to_string(), (1, 4));
    tables.insert("public.orders".to_string(), (1, 4));
    tables.insert("mydb.orders".to_string(), (1, 4));

    let mut columns = HashMap::new();
    columns.insert((1, "id".to_string()), 0);
    columns.insert((1, "amount".to_string()), 1);
    columns.insert((1, "status".to_string()), 2);
    columns.insert((1, "comment".to_string()), 3);

    let mut primary_keys = HashMap::new();
    primary_keys.insert(1, vec![0]); // id is PK

    TestCatalog {
        tables,
        columns,
        primary_keys,
    }
}

/// Same as `orders_catalog()` but without primary key metadata.
pub(super) fn orders_no_pk_catalog() -> TestCatalog {
    let mut cat = orders_catalog();
    cat.primary_keys.clear();
    cat
}

/// Shared test catalog: schema="public", table="orders",
/// columns: id=0, customer=1, amount=2, status=3, PK=[id].
/// Used by parsers that model customer-centric orders fixtures
/// (e.g. wal2json and pgoutput).
pub(super) fn orders_customer_catalog() -> TestCatalog {
    let mut tables = HashMap::new();
    tables.insert("orders".to_string(), (1, 4));
    tables.insert("public.orders".to_string(), (1, 4));

    let mut columns = HashMap::new();
    columns.insert((1, "id".to_string()), 0);
    columns.insert((1, "customer".to_string()), 1);
    columns.insert((1, "amount".to_string()), 2);
    columns.insert((1, "status".to_string()), 3);

    let mut primary_keys = HashMap::new();
    primary_keys.insert(1, vec![0]); // id is PK

    TestCatalog {
        tables,
        columns,
        primary_keys,
    }
}

/// Same as `orders_customer_catalog()` but without primary key metadata.
pub(super) fn orders_customer_no_pk_catalog() -> TestCatalog {
    let mut cat = orders_customer_catalog();
    cat.primary_keys.clear();
    cat
}

pub(super) struct TestCatalog {
    pub(super) tables: HashMap<String, (TableId, usize)>,
    pub(super) columns: HashMap<(TableId, String), ColumnId>,
    pub(super) primary_keys: HashMap<TableId, Vec<ColumnId>>,
}

impl SchemaCatalog for TestCatalog {
    fn table_id(&self, table_name: &str) -> Option<TableId> {
        self.tables.get(table_name).map(|(id, _)| *id)
    }

    fn column_id(&self, table_id: TableId, column_name: &str) -> Option<ColumnId> {
        self.columns
            .get(&(table_id, column_name.to_string()))
            .copied()
    }

    fn table_arity(&self, table_id: TableId) -> Option<usize> {
        self.tables
            .values()
            .find(|(id, _)| *id == table_id)
            .map(|(_, arity)| *arity)
    }

    fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
        Some(0)
    }

    fn primary_key_columns(&self, table_id: TableId) -> Option<&[ColumnId]> {
        self.primary_keys.get(&table_id).map(Vec::as_slice)
    }
}

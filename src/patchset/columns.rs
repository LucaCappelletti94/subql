//! Catalog metadata indexed once, shared by the patchset adapters.
//!
//! [`sqlite_diff_rs::Adapter`] resolves `(table_name, column_index)` for
//! every adapted cell. Walking the catalog for each cell scales with the
//! whole schema, so each adapter builds this index at construction and
//! answers every lookup with one hash lookup and one slice index.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use diesel::result::Error as DieselError;
use sql_traits::prelude::{DatabaseLike, TableLike};
use sqlite_diff_rs::Value as WireValue;

/// The columns of every catalog table, keyed by the table's bare stored
/// name.
///
/// When two schemas store the same bare name, the first table the catalog
/// yields wins, matching the linear scan this index replaced.
#[derive(Debug)]
pub struct ColumnIndex<'db, DB: DatabaseLike> {
    tables: hashbrown::HashMap<&'db str, Vec<&'db DB::Column>>,
}

impl<'db, DB: DatabaseLike> ColumnIndex<'db, DB> {
    /// Walk the catalog once and index it.
    ///
    /// # Errors
    /// [`CatalogError::Lookup`](crate::CatalogError::Lookup) when the
    /// catalog fails to yield a table's columns.
    pub fn new(catalog: &'db DB) -> Result<Self, crate::CatalogError> {
        let mut tables: hashbrown::HashMap<&'db str, Vec<&'db DB::Column>> =
            hashbrown::HashMap::new();
        for table in catalog.tables() {
            let columns = table
                .columns(catalog)
                .map_err(|error| crate::CatalogError::Lookup {
                    table_id: catalog
                        .table_id(table)
                        .and_then(|id| u32::try_from(id).ok())
                        .unwrap_or(u32::MAX),
                    error,
                })?
                .collect();
            tables.entry(table.table_name()).or_insert(columns);
        }
        Ok(Self { tables })
    }

    /// The column at `index` of `table_name`, or `None` when the catalog
    /// knows no such table or the index is out of range.
    pub fn column_at(&self, table_name: &str, index: usize) -> Option<&'db DB::Column> {
        self.tables.get(table_name)?.get(index).copied()
    }
}

/// The refusal for a bind against a column the catalog does not know.
///
/// [`sqlite_diff_rs::Adapter::column_name`] has no failure channel, so the
/// same missed lookup there can only answer an empty string. The bind path
/// carries a `Result` and refuses explicitly instead of binding a default
/// into a statement that is already wrong.
pub fn unknown_column_error(table_name: &str, column_index: usize) -> DieselError {
    refusal(alloc::format!(
        "column {column_index} of table {table_name} is not in the catalog"
    ))
}

/// The refusal for a wire value whose shape the target column cannot take.
///
/// Shared by every adapter, so one column carries one message wherever it is
/// refused.
pub fn bind_error(column: &str, expected: &str, got: &str) -> DieselError {
    refusal(alloc::format!(
        "column `{column}` expects {expected}, got {got}"
    ))
}

/// Name the SQLite wire shape of a value for use in a refusal.
pub const fn shape_of<S, B>(value: &WireValue<S, B>) -> &'static str {
    match value {
        WireValue::Null => "NULL",
        WireValue::Integer(_) => "INTEGER",
        WireValue::Real(_) => "REAL",
        WireValue::Text(_) => "TEXT",
        WireValue::Blob(_) => "BLOB",
    }
}

fn refusal(message: String) -> DieselError {
    DieselError::QueryBuilderError(Box::new(BindRefusal::Refused(message)))
}

/// A bind the adapter refuses, carrying the message the caller sees.
#[derive(Debug, Clone, thiserror::Error)]
enum BindRefusal {
    #[error("{0}")]
    Refused(String),
}

#[cfg(test)]
mod tests {
    use super::{bind_error, shape_of, unknown_column_error, ColumnIndex};
    use alloc::string::{String, ToString as _};
    use alloc::vec::Vec;
    use sql_traits::prelude::{ColumnLike, DatabaseLike, TableLike as _};
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::Value;
    use sqlparser::dialect::PostgreSqlDialect;

    /// Every adapter refuses a mismatched cell with the same sentence, naming
    /// the column, the shape it takes and the shape it got.
    #[test]
    fn a_refused_bind_names_the_column_the_expectation_and_the_shape() {
        assert_eq!(
            bind_error("active", "INTEGER or NULL", "TEXT").to_string(),
            "column `active` expects INTEGER or NULL, got TEXT"
        );
        assert_eq!(
            unknown_column_error("things", 3).to_string(),
            "column 3 of table things is not in the catalog"
        );
    }

    /// The wire shape a refusal reports, one name per SQLite storage class.
    #[test]
    fn every_wire_shape_has_a_name() {
        let shapes: [Value<String, Vec<u8>>; 5] = [
            Value::Null,
            Value::Integer(1),
            Value::Real(1.0),
            Value::Text("a".to_string()),
            Value::Blob(Vec::new()),
        ];
        let named: Vec<&str> = shapes.iter().map(shape_of).collect();
        assert_eq!(named, ["NULL", "INTEGER", "REAL", "TEXT", "BLOB"]);
    }

    /// Two schemas may declare the same bare table name. The index answers
    /// with the columns of the first such table the catalog yields, matching
    /// the linear scan it replaced, never with the last one's.
    #[test]
    fn the_first_table_with_a_bare_name_wins() {
        fn names<'db>(
            db: &'db ParserDB,
            table: &'db <ParserDB as DatabaseLike>::Table,
        ) -> Vec<&'db str> {
            table
                .columns(db)
                .expect("the fixture catalog yields its columns")
                .map(ColumnLike::column_name)
                .collect()
        }

        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE SCHEMA a; CREATE SCHEMA b; \
             CREATE TABLE a.items (id INT PRIMARY KEY, left_text TEXT); \
             CREATE TABLE b.items (id INT PRIMARY KEY, right_num INT);",
        )
        .expect("the fixture DDL parses");

        let mut items = db.tables().filter(|table| table.table_name() == "items");
        let first = names(&db, items.next().expect("the catalog holds a.items"));
        let last = names(&db, items.next().expect("the catalog holds b.items"));
        assert_ne!(
            first, last,
            "the fixture's two tables must differ for the tie-break to be visible"
        );

        let index = ColumnIndex::new(&db).expect("the catalog indexes");
        let indexed: Vec<&str> = (0..first.len())
            .map(|position| {
                index
                    .column_at("items", position)
                    .expect("the index holds one items table")
                    .column_name()
            })
            .collect();
        assert_eq!(indexed, first, "the first table yielded wins the bare name");
        assert!(
            index.column_at("items", first.len()).is_none(),
            "a position past the winning table's width is out of range"
        );
    }
}

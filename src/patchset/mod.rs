//! Apply client-uploaded SQLite session patchsets against a target
//! database via diesel.
//!
//! This module is the "digest" side of the CDC round-trip for
//! `connetto-rs`-shaped topologies: a client uploads a batch of local
//! mutations as an SQLite session-extension patchset, and the server
//! executes each op against Postgres or MySQL with native diesel binds.
//!
//! SQLite has no boolean type, so a Postgres `BOOLEAN` or MySQL
//! `TINYINT(1)` column round-trips through the wire as
//! `Value::Integer(0 | 1)`. Without dispatch, diesel would bind that
//! integer as `BigInt`, and the target rejects the coerce-free INSERT.
//! Each per-backend adapter carries the catalog and rewrites integer
//! binds for `bool`-shaped columns to native `bool` binds so no
//! `CAST` wrapper is emitted.
//!
//! # Extension
//!
//! [`PgAdapter`] and [`MysqlAdapter`] are plain structs that implement
//! [`sqlite_diff_rs::Adapter`]. Downstream users who need dispatch for
//! domain-specific types (custom enums, ranges, uuid variants, ...)
//! either wrap one of them with their own [`sqlite_diff_rs::Adapter`]
//! impl that intercepts the columns they own and delegates the rest,
//! or roll a whole new adapter.
//!
//! # Scope
//!
//! MVP dispatch on both backends: boolean columns only. `INTEGER`,
//! `TEXT`, `REAL`, and `BLOB`/`BYTEA` already work through
//! [`sqlite_diff_rs::DefaultBinder`]. UUID, timestamp, decimal, and
//! json/jsonb dispatch land as follow-up.

use alloc::string::String;
use alloc::vec::Vec;
use core::hash::Hash;

use diesel::backend::Backend;
use diesel::result::QueryResult;
use diesel::serialize::ToSql;
use diesel::sql_types::{BigInt, Binary, Double, HasSqlType, Text};
use sql_traits::prelude::DatabaseLike;
use sqlite_diff_rs::{
    Adapter, ApplyOps, ColumnNames, DiffOps, Insert, ParsedDiffSet, PatchDelete, PatchSet,
    PatchsetFormat, PatchsetOp, SchemaWithPK, SimpleTable, Update,
};

#[cfg(feature = "apply-patchset-postgres")]
pub mod pg;
#[cfg(feature = "apply-patchset-postgres")]
pub use pg::PgAdapter;

#[cfg(feature = "apply-patchset-mysql")]
pub mod mysql;
#[cfg(feature = "apply-patchset-mysql")]
pub use mysql::MysqlAdapter;

#[cfg(feature = "apply-patchset-sqlite")]
pub mod sqlite;
#[cfg(feature = "apply-patchset-sqlite")]
pub use sqlite::SqliteAdapter;

// ============================================================================
// Engine entry point (generic over any diesel Backend)
// ============================================================================

impl<E, I, DB> crate::SubscriptionEngine<E, I, DB>
where
    E: crate::backend::CdcEvent,
    E::Backend: crate::compiler::literals::SqlLiteralParse,
    I: crate::IdTypes,
    DB: DatabaseLike + 'static,
{
    /// Apply a client-uploaded SQLite session patchset against `conn`
    /// using `adapter` for native diesel bind dispatch. The batch runs
    /// inside one diesel transaction: either every op commits or none
    /// do. Returns the summed affected-row count.
    ///
    /// Generic over any diesel [`Backend`]. Concrete adapters shipped
    /// with subql are [`PgAdapter`] and [`MysqlAdapter`].
    ///
    /// # Errors
    ///
    /// Propagates the first [`diesel::result::Error`] any op produces
    /// (with the transaction rolled back).
    pub fn apply_patchset<DBend, Conn, T, S, B, A>(
        &self,
        patchset: &PatchSet<T, S, B>,
        conn: &mut Conn,
        adapter: &A,
    ) -> QueryResult<usize>
    where
        DBend: Backend
            + HasSqlType<BigInt>
            + HasSqlType<Double>
            + HasSqlType<Text>
            + HasSqlType<Binary>,
        Conn: diesel::Connection<Backend = DBend>,
        A: Adapter<DBend, S, B> + Send + Sync,
        T: SchemaWithPK + ColumnNames + Sync,
        S: AsRef<str> + Clone + Hash + Eq + Sync,
        B: AsRef<[u8]> + Clone + Hash + Eq + Sync,
        i64: ToSql<BigInt, DBend>,
        f64: ToSql<Double, DBend>,
        str: ToSql<Text, DBend>,
        [u8]: ToSql<Binary, DBend>,
    {
        patchset
            .iter()
            .map(|op| op.with_adapter::<DBend, _>(adapter))
            .apply_transactional(conn)
    }

    /// Apply a client-uploaded SQLite session patchset, still in its raw
    /// wire bytes, against `conn` using `adapter`.
    ///
    /// This is the production inbound entry point for the CDC round trip:
    /// a client uploads the bytes the SQLite session extension emitted,
    /// and subql parses them, reconstructs the batch of inserts, updates,
    /// and deletes, and applies it in one transaction with native diesel
    /// binds. It is the byte-level counterpart to [`Self::apply_patchset`],
    /// which takes an already reconstructed [`PatchSet`].
    ///
    /// The table shape (column order and primary-key indices) is resolved
    /// from subql's catalog by the table name each op carries, not from
    /// the primary-key flags embedded in the uploaded bytes. The catalog
    /// is subql's source of truth, so a client whose local SQLite replica
    /// declares a different primary key cannot steer the server's WHERE
    /// clause.
    ///
    /// # Errors
    ///
    /// Returns a [`diesel::result::Error::QueryBuilderError`] when the
    /// bytes fail to parse, carry the changeset marker instead of a
    /// patchset, or name a table absent from the catalog. Propagates the
    /// first bind or execution error any op produces, with the whole
    /// transaction rolled back.
    pub fn apply_patchset_bytes<DBend, Conn, A>(
        &self,
        bytes: &[u8],
        conn: &mut Conn,
        adapter: &A,
    ) -> QueryResult<usize>
    where
        DBend: Backend
            + HasSqlType<BigInt>
            + HasSqlType<Double>
            + HasSqlType<Text>
            + HasSqlType<Binary>,
        Conn: diesel::Connection<Backend = DBend>,
        A: Adapter<DBend, String, Vec<u8>> + Send + Sync,
        i64: ToSql<BigInt, DBend>,
        f64: ToSql<Double, DBend>,
        str: ToSql<Text, DBend>,
        [u8]: ToSql<Binary, DBend>,
    {
        let patchset = self.rebuild_patchset(bytes)?;
        self.apply_patchset(&patchset, conn, adapter)
    }

    /// Parse raw session patchset bytes into an applyable
    /// [`PatchSet<SimpleTable>`], resolving each op's table shape from the
    /// catalog.
    ///
    /// Rejects a changeset (marker `T`), since the loop uploads patchsets
    /// (marker `P`). The op reconstruction mirrors what the SQLite session
    /// extension records: an insert carries every column value, an update
    /// carries the primary key plus the new non-key values, and a delete
    /// carries only the primary key.
    fn rebuild_patchset(
        &self,
        bytes: &[u8],
    ) -> QueryResult<PatchSet<SimpleTable, String, Vec<u8>>> {
        let parsed = ParsedDiffSet::parse(bytes).map_err(|err| {
            ingest_error(alloc::format!(
                "failed to parse uploaded SQLite session patchset bytes: {err}"
            ))
        })?;
        let diff = match parsed {
            ParsedDiffSet::Patchset(diff) => diff,
            ParsedDiffSet::Changeset(_) => {
                return Err(ingest_error(
                    "uploaded diffset carries the changeset marker T, but apply_patchset_bytes requires a patchset (marker P)",
                ));
            }
        };
        let mut builder = PatchSet::<SimpleTable, String, Vec<u8>>::new();
        for op in diff.iter() {
            let table = self.catalog_table(op.table().name())?;
            match op {
                PatchsetOp::Insert { values, .. } => {
                    let mut insert = Insert::from(table);
                    for (index, value) in values.iter().enumerate() {
                        insert = insert
                            .set(index, value.clone())
                            .map_err(|err| op_error(&err))?;
                    }
                    builder = builder.insert(insert);
                }
                PatchsetOp::Update { pk, entries, .. } => {
                    let pk_indices = table.pk_indices();
                    let mut update = Update::<_, PatchsetFormat, String, Vec<u8>>::from(table);
                    for (value, &col) in pk.iter().zip(pk_indices.iter()) {
                        update = update
                            .set(col, value.clone())
                            .map_err(|err| op_error(&err))?;
                    }
                    for (index, (_unit, new)) in entries.iter().enumerate() {
                        if !pk_indices.contains(&index) {
                            if let Some(value) = new {
                                update = update
                                    .set(index, value.clone())
                                    .map_err(|err| op_error(&err))?;
                            }
                        }
                    }
                    builder = builder.update(update);
                }
                PatchsetOp::Delete { pk, .. } => {
                    builder = builder.delete(PatchDelete::new(table, pk.to_vec()));
                }
            }
        }
        Ok(builder)
    }

    /// Resolve a [`SimpleTable`] for `name` from subql's catalog.
    fn catalog_table(&self, name: &str) -> QueryResult<SimpleTable> {
        let database = self.database();
        let table_id = crate::catalog_helpers::table_id(database, name).ok_or_else(|| {
            ingest_error(alloc::format!(
                "uploaded patchset names table `{name}`, which is absent from the catalog"
            ))
        })?;
        crate::catalog_helpers::simple_table(database, table_id).ok_or_else(|| {
            ingest_error(alloc::format!(
                "catalog table `{name}` could not be resolved to a schema"
            ))
        })
    }
}

/// Build a [`diesel::result::Error::QueryBuilderError`] carrying an
/// ingest-time message (parse failure, wrong marker, or unknown table).
fn ingest_error(message: impl Into<String>) -> diesel::result::Error {
    let message: String = message.into();
    diesel::result::Error::QueryBuilderError(message.into())
}

/// Map a `sqlite-diff-rs` op reconstruction error to a diesel error.
fn op_error(err: &sqlite_diff_rs::Error) -> diesel::result::Error {
    ingest_error(alloc::format!("failed to reconstruct patchset op: {err}"))
}

#[cfg(all(test, feature = "apply-patchset-sqlite"))]
#[allow(clippy::unwrap_used)]
mod tests {
    use diesel::{sql_query, Connection, QueryableByName, RunQueryDsl, SqliteConnection};
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::{
        ChangeSet, DiffOps, Insert, PatchDelete, PatchSet, PatchsetFormat, SimpleTable, Update,
        Value,
    };
    use sqlparser::dialect::SQLiteDialect;

    use super::SqliteAdapter;
    use crate::backend::SQLite as SqliteBackend;
    use crate::testing::TestEvent;
    use crate::{DefaultIds, SubscriptionEngine};

    const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER);";

    type Engine = SubscriptionEngine<TestEvent<SqliteBackend>, DefaultIds, ParserDB>;

    #[derive(QueryableByName, Debug, PartialEq)]
    struct Item {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        id: i64,
        #[diesel(sql_type = diesel::sql_types::Text)]
        name: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        qty: i64,
    }

    fn items(conn: &mut SqliteConnection) -> Vec<Item> {
        sql_query("SELECT id, name, qty FROM items ORDER BY id")
            .load::<Item>(conn)
            .unwrap()
    }

    fn table() -> SimpleTable {
        SimpleTable::new("items", &["id", "name", "qty"], &[0])
    }

    /// Craft the raw patchset bytes the SQLite session extension would
    /// emit for a full-row insert.
    fn insert_bytes(id: i64, name: &str, qty: i64) -> Vec<u8> {
        let insert = Insert::<_, String, Vec<u8>>::from(table())
            .set(0, Value::Integer(id))
            .unwrap()
            .set(1, Value::Text(name.into()))
            .unwrap()
            .set(2, Value::Integer(qty))
            .unwrap();
        PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .insert(insert)
            .build()
    }

    /// Craft an update that carries the primary key plus a new `qty`,
    /// leaving `name` unset (unchanged), as a patchset update does.
    fn update_qty_bytes(id: i64, qty: i64) -> Vec<u8> {
        let update = Update::<_, PatchsetFormat, String, Vec<u8>>::from(table())
            .set(0, Value::Integer(id))
            .unwrap()
            .set(2, Value::Integer(qty))
            .unwrap();
        PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .update(update)
            .build()
    }

    /// Craft a delete carrying only the primary key.
    fn delete_bytes(id: i64) -> Vec<u8> {
        PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .delete(PatchDelete::new(table(), vec![Value::Integer(id)]))
            .build()
    }

    fn fixture() -> (Engine, SqliteConnection) {
        let mut conn = SqliteConnection::establish(":memory:").unwrap();
        sql_query(DDL).execute(&mut conn).unwrap();
        let catalog = ParserDB::parse::<SQLiteDialect>(DDL).unwrap();
        let engine = Engine::new(catalog, SQLiteDialect {});
        (engine, conn)
    }

    #[test]
    fn apply_patchset_bytes_inserts_updates_deletes() {
        let (engine, mut conn) = fixture();
        let adapter = SqliteAdapter::new(engine.database());

        let inserted = engine
            .apply_patchset_bytes(&insert_bytes(1, "a", 10), &mut conn, &adapter)
            .unwrap();
        assert_eq!(inserted, 1, "one row inserted");
        engine
            .apply_patchset_bytes(&insert_bytes(2, "b", 20), &mut conn, &adapter)
            .unwrap();
        assert_eq!(
            items(&mut conn),
            vec![
                Item {
                    id: 1,
                    name: "a".into(),
                    qty: 10,
                },
                Item {
                    id: 2,
                    name: "b".into(),
                    qty: 20,
                },
            ],
            "both rows present after inserts"
        );

        engine
            .apply_patchset_bytes(&update_qty_bytes(1, 99), &mut conn, &adapter)
            .unwrap();
        assert_eq!(
            items(&mut conn),
            vec![
                Item {
                    id: 1,
                    name: "a".into(),
                    qty: 99,
                },
                Item {
                    id: 2,
                    name: "b".into(),
                    qty: 20,
                },
            ],
            "row 1 qty updated, name unchanged, row 2 untouched"
        );

        engine
            .apply_patchset_bytes(&delete_bytes(2), &mut conn, &adapter)
            .unwrap();
        assert_eq!(
            items(&mut conn),
            vec![Item {
                id: 1,
                name: "a".into(),
                qty: 99,
            }],
            "row 2 deleted, matched on its primary key"
        );
    }

    #[test]
    fn apply_patchset_bytes_rejects_changeset_marker() {
        let (engine, mut conn) = fixture();
        let adapter = SqliteAdapter::new(engine.database());

        let insert = Insert::<_, String, Vec<u8>>::from(table())
            .set(0, Value::Integer(1))
            .unwrap()
            .set(1, Value::Text("a".into()))
            .unwrap()
            .set(2, Value::Integer(10))
            .unwrap();
        let changeset = ChangeSet::<SimpleTable, String, Vec<u8>>::new()
            .insert(insert)
            .build();

        let err = engine
            .apply_patchset_bytes(&changeset, &mut conn, &adapter)
            .unwrap_err();
        assert!(
            err.to_string().contains("changeset marker T"),
            "error names the changeset marker, got: {err}"
        );
        assert!(items(&mut conn).is_empty(), "nothing applied on rejection");
    }

    #[test]
    fn apply_patchset_bytes_rejects_unknown_table() {
        let (engine, mut conn) = fixture();
        let adapter = SqliteAdapter::new(engine.database());

        let ghost = SimpleTable::new("ghosts", &["id"], &[0]);
        let insert = Insert::<_, String, Vec<u8>>::from(ghost)
            .set(0, Value::Integer(1))
            .unwrap();
        let bytes = PatchSet::<SimpleTable, String, Vec<u8>>::new()
            .insert(insert)
            .build();

        let err = engine
            .apply_patchset_bytes(&bytes, &mut conn, &adapter)
            .unwrap_err();
        assert!(
            err.to_string().contains("ghosts"),
            "error names the unknown table, got: {err}"
        );
    }
}

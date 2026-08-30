//! Catalog-only patchset application entry points that require no
//! [`crate::SubscriptionEngine`] in scope.

use alloc::string::String;
use alloc::vec::Vec;

use diesel::backend::Backend;
use diesel::result::QueryResult;
use diesel::serialize::ToSql;
use diesel::sql_types::{BigInt, Binary, Double, HasSqlType, Text};
use sql_traits::prelude::DatabaseLike;
use sqlite_diff_rs::{
    Adapter, ApplyOps, ChangeDelete, ChangeSet, ChangesetFormat, ChangesetOp, DiffOps, DiffSet,
    Insert, ParsedDiffSet, PatchDelete, PatchSet, PatchsetFormat, PatchsetOp, SimpleTable,
    TableSchema, Update,
};

#[cfg(any(
    feature = "apply-patchset-postgres-async",
    feature = "apply-patchset-mysql-async"
))]
use sqlite_diff_rs::ApplyOpsAsync;

/// Owned reconstructed batch that
/// [`apply_diffset_bytes_async_with_catalog`] hands to the async execution
/// driver. Owning it keeps the returned future from borrowing `catalog`,
/// so the future stays `Send`.
#[cfg(any(
    feature = "apply-patchset-postgres-async",
    feature = "apply-patchset-mysql-async"
))]
enum ReconstructedDiff {
    Patchset(PatchSet<SimpleTable, String, Vec<u8>>),
    Changeset(ChangeSet<SimpleTable, String, Vec<u8>>),
}

/// Apply a client-uploaded SQLite session diffset from its raw wire bytes
/// against `conn` using `adapter`, resolving table shapes from `catalog`.
///
/// The catalog-only inbound entry point: apply needs a catalog (`&DB`),
/// not a whole [`SubscriptionEngine`](crate::SubscriptionEngine), which
/// delegates here. Dispatches on the format marker, so a changeset upload
/// can apply a primary-key-changing UPDATE a patchset cannot. Table shapes
/// come from `catalog` by name, never the uploaded flags, so a client
/// cannot steer the WHERE clause.
///
/// # Errors
///
/// [`QueryBuilderError`](diesel::result::Error::QueryBuilderError) when the
/// bytes fail to parse or name an unknown table, else the first bind or
/// execution error, rolling back the transaction.
pub fn apply_diffset_bytes_with_catalog<DB, DBend, Conn, A>(
    catalog: &DB,
    bytes: &[u8],
    conn: &mut Conn,
    adapter: &A,
) -> QueryResult<usize>
where
    DB: DatabaseLike,
    DBend:
        Backend + HasSqlType<BigInt> + HasSqlType<Double> + HasSqlType<Text> + HasSqlType<Binary>,
    Conn: diesel::Connection<Backend = DBend>,
    A: Adapter<DBend, String, Vec<u8>> + Send + Sync,
    i64: ToSql<BigInt, DBend>,
    f64: ToSql<Double, DBend>,
    str: ToSql<Text, DBend>,
    [u8]: ToSql<Binary, DBend>,
{
    match ParsedDiffSet::parse(bytes).map_err(|err| {
        ingest_error(alloc::format!(
            "failed to parse uploaded SQLite session diffset bytes: {err}"
        ))
    })? {
        ParsedDiffSet::Patchset(diff) => {
            let patchset = reconstruct_patchset(catalog, &diff)?;
            patchset
                .iter()
                .map(|op| op.with_adapter::<DBend, _>(adapter))
                .apply_transactional(conn)
        }
        ParsedDiffSet::Changeset(diff) => {
            let changeset = reconstruct_changeset(catalog, &diff)?;
            changeset
                .iter()
                .map(|op| op.with_adapter::<DBend, _>(adapter))
                .apply_transactional(conn)
        }
    }
}

/// Async peer of [`apply_diffset_bytes_with_catalog`], driven by
/// [`diesel_async`]. One transaction.
///
/// Parse and reconstruction run synchronously up front, so the returned
/// future owns its batch and never borrows `catalog`: it is `Send`, and a
/// shared `&catalog` (for a `Sync` catalog such as
/// [`ParserDB`](sql_traits::structs::ParserDB)) serves concurrent applies
/// on a multi-thread runtime.
///
/// # Errors
///
/// As [`apply_diffset_bytes_with_catalog`].
#[cfg(any(
    feature = "apply-patchset-postgres-async",
    feature = "apply-patchset-mysql-async"
))]
pub fn apply_diffset_bytes_async_with_catalog<'a, DB, DBend, Conn, A>(
    catalog: &DB,
    bytes: &[u8],
    conn: &'a mut Conn,
    adapter: &'a A,
) -> impl core::future::Future<Output = QueryResult<usize>> + Send + 'a
where
    DB: DatabaseLike,
    DBend:
        Backend + HasSqlType<BigInt> + HasSqlType<Double> + HasSqlType<Text> + HasSqlType<Binary>,
    Conn: diesel_async::AsyncConnection<Backend = DBend>,
    A: Adapter<DBend, String, Vec<u8>> + Send + Sync + 'a,
    i64: ToSql<BigInt, DBend>,
    f64: ToSql<Double, DBend>,
    str: ToSql<Text, DBend>,
    [u8]: ToSql<Binary, DBend>,
{
    // Reconstruct synchronously up front so the future owns its batch and
    // never borrows `catalog`.
    let reconstructed = ParsedDiffSet::parse(bytes)
        .map_err(|err| {
            ingest_error(alloc::format!(
                "failed to parse uploaded SQLite session diffset bytes: {err}"
            ))
        })
        .and_then(|parsed| match parsed {
            ParsedDiffSet::Patchset(diff) => {
                reconstruct_patchset(catalog, &diff).map(ReconstructedDiff::Patchset)
            }
            ParsedDiffSet::Changeset(diff) => {
                reconstruct_changeset(catalog, &diff).map(ReconstructedDiff::Changeset)
            }
        });
    async move {
        match reconstructed? {
            ReconstructedDiff::Patchset(patchset) => {
                patchset
                    .iter()
                    .map(|op| op.with_adapter::<DBend, _>(adapter))
                    .apply_transactional_async(conn)
                    .await
            }
            ReconstructedDiff::Changeset(changeset) => {
                changeset
                    .iter()
                    .map(|op| op.with_adapter::<DBend, _>(adapter))
                    .apply_transactional_async(conn)
                    .await
            }
        }
    }
}

/// Reconstruct an applyable [`PatchSet<SimpleTable>`] from a parsed
/// patchset, resolving each op's table shape from `catalog`.
///
/// A patchset stores one value per primary-key column, so it cannot
/// express a primary-key change (a changeset can).
fn reconstruct_patchset<DB: DatabaseLike>(
    catalog: &DB,
    diff: &DiffSet<PatchsetFormat, TableSchema<String>, String, Vec<u8>>,
) -> QueryResult<PatchSet<SimpleTable, String, Vec<u8>>> {
    let mut builder = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for op in diff.iter() {
        let table = catalog_table(catalog, op.table().name())?;
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

/// Reconstruct an applyable [`ChangeSet<SimpleTable>`] from a parsed
/// changeset, resolving each op's table shape from `catalog`.
///
/// Unlike [`reconstruct_patchset`], it keeps the old and new value of
/// every column, so a primary-key change renders the old key in the WHERE
/// and the new key in the SET. An unchanged pk column (old present, no
/// new) becomes old equal to new, staying in the WHERE and out of the SET.
fn reconstruct_changeset<DB: DatabaseLike>(
    catalog: &DB,
    diff: &DiffSet<ChangesetFormat, TableSchema<String>, String, Vec<u8>>,
) -> QueryResult<ChangeSet<SimpleTable, String, Vec<u8>>> {
    let mut builder = ChangeSet::<SimpleTable, String, Vec<u8>>::new();
    for op in diff.iter() {
        let table = catalog_table(catalog, op.table().name())?;
        match op {
            ChangesetOp::Insert { values, .. } => {
                let mut insert = Insert::from(table);
                for (index, value) in values.iter().enumerate() {
                    insert = insert
                        .set(index, value.clone())
                        .map_err(|err| op_error(&err))?;
                }
                builder = builder.insert(insert);
            }
            ChangesetOp::Update { values, .. } => {
                let mut update = Update::<_, ChangesetFormat, String, Vec<u8>>::from(table);
                for (index, (old, new)) in values.iter().enumerate() {
                    update = match (old, new) {
                        (Some(old), Some(new)) => update.set(index, old.clone(), new.clone()),
                        // An unchanged primary-key column: old present,
                        // no new. Set old equal to new so it stays in
                        // the WHERE and out of the SET.
                        (Some(old), None) => update.set(index, old.clone(), old.clone()),
                        (None, Some(new)) => update.set_new(index, new.clone()),
                        // Not part of the diff.
                        (None, None) => Ok(update),
                    }
                    .map_err(|err| op_error(&err))?;
                }
                builder = builder.update(update);
            }
            ChangesetOp::Delete { old_values, .. } => {
                let mut delete = ChangeDelete::from(table);
                for (index, value) in old_values.iter().enumerate() {
                    delete = delete
                        .set(index, value.clone())
                        .map_err(|err| op_error(&err))?;
                }
                builder = builder.delete(delete);
            }
        }
    }
    Ok(builder)
}

/// Resolve a [`SimpleTable`] for `name` from `catalog`.
fn catalog_table<DB: DatabaseLike>(catalog: &DB, name: &str) -> QueryResult<SimpleTable> {
    let table_id = crate::catalog_helpers::table_id(catalog, name).ok_or_else(|| {
        ingest_error(alloc::format!(
            "uploaded patchset names table `{name}`, which is absent from the catalog"
        ))
    })?;
    crate::catalog_helpers::simple_table(catalog, table_id).ok_or_else(|| {
        ingest_error(alloc::format!(
            "catalog table `{name}` could not be resolved to a schema"
        ))
    })
}

/// Build a [`diesel::result::Error::QueryBuilderError`] carrying an
/// ingest-time message (a parse failure or an unknown table).
fn ingest_error(message: impl Into<String>) -> diesel::result::Error {
    let message: String = message.into();
    diesel::result::Error::QueryBuilderError(message.into())
}

/// Map a `sqlite-diff-rs` op reconstruction error to a diesel error.
fn op_error(err: &sqlite_diff_rs::Error) -> diesel::result::Error {
    ingest_error(alloc::format!("failed to reconstruct patchset op: {err}"))
}

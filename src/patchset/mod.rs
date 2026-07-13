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

use core::hash::Hash;

use diesel::backend::Backend;
use diesel::result::QueryResult;
use diesel::serialize::ToSql;
use diesel::sql_types::{BigInt, Binary, Double, HasSqlType, Text};
use sql_traits::prelude::DatabaseLike;
use sqlite_diff_rs::{Adapter, ApplyOps, ColumnNames, PatchSet, SchemaWithPK};

#[cfg(feature = "apply-patchset-postgres")]
pub mod pg;
#[cfg(feature = "apply-patchset-postgres")]
pub use pg::PgAdapter;

#[cfg(feature = "apply-patchset-mysql")]
pub mod mysql;
#[cfg(feature = "apply-patchset-mysql")]
pub use mysql::MysqlAdapter;

// ============================================================================
// Engine entry point (generic over any diesel Backend)
// ============================================================================

impl<E, I, DB> crate::SubscriptionEngine<E, I, DB>
where
    E: crate::backend::CdcEvent,
    E::Backend: crate::compiler::literals::SqlLiteralParse,
    I: crate::IdTypes,
    DB: DatabaseLike,
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
}

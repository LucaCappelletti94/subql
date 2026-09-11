//! Docker-backed E2E test for
//! [`subql::SubscriptionEngine::apply_patchset_async`].
//!
//! Async twin of `tests/it/apply_patchset_pg_e2e.rs`: spins up a real
//! Postgres, creates a `things (id INT PK, active BOOL)` table, builds an
//! SQLite session patchset with INSERT / UPDATE / DELETE ops that touch the
//! `active` column, applies it through `apply_patchset_async` over a
//! `diesel-async` [`AsyncPgConnection`](diesel_async::AsyncPgConnection) with
//! the [`PgAdapter`](subql::patchset::PgAdapter), and asserts the rows land
//! in PG with the correct `bool` values (no `CAST` wrapper, no wire-format
//! bind-type mismatch). The apply runs on a multi-thread tokio runtime,
//! proving the returned future is `Send`.
//!
//! Tests are `#[ignore]`d so default `cargo test` does not require Docker.
//! Run with:
//!
//! ```sh
//! cargo test --test it apply_patchset_pg_async_e2e:: \
//!     --features apply-patchset-postgres-async -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::{sql_query, Connection, PgConnection, QueryableByName, RunQueryDsl};
use diesel_async::{AsyncConnection, AsyncPgConnection};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::patchset::PgAdapter;
use subql::{ChangeEvent, DefaultIds, SubscriptionEngine};

const DDL: &str = "CREATE TABLE things (id INT PRIMARY KEY, active BOOLEAN);";
const PG_DDL: &str = "CREATE TABLE things (id INT PRIMARY KEY, active BOOLEAN)";

#[derive(QueryableByName, Debug, PartialEq)]
struct ThingRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    active: bool,
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn apply_patchset_async_bool_roundtrip_insert_update_delete() {
    common::assert_docker_available();
    let db = common::pg_database();
    let url = db.url();

    common::multi_thread_rt().block_on(async move {
        let mut verify = PgConnection::establish(&url).expect("PG connection");
        sql_query(PG_DDL)
            .execute(&mut verify)
            .expect("create table");
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("async pg connect");

        let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse subql DDL");
        let engine: SubscriptionEngine<ChangeEvent, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});
        let adapter = PgAdapter::new(engine.database()).expect("the catalog indexes");

        let (inserts, updates, deletes) = common::patchset::bool_roundtrip_ops();

        assert_eq!(
            engine
                .apply_patchset_async(&inserts, &mut conn, &adapter)
                .await
                .expect("apply inserts"),
            2,
            "two rows inserted"
        );
        assert_eq!(
            common::patchset::load_all(&mut verify),
            common::patchset::expected_after_inserts()
        );

        assert_eq!(
            engine
                .apply_patchset_async(&updates, &mut conn, &adapter)
                .await
                .expect("apply updates"),
            1,
            "one row updated"
        );
        assert_eq!(
            common::patchset::load_all(&mut verify),
            common::patchset::expected_after_update()
        );

        assert_eq!(
            engine
                .apply_patchset_async(&deletes, &mut conn, &adapter)
                .await
                .expect("apply deletes"),
            1,
            "one row deleted"
        );
        assert_eq!(
            common::patchset::load_all(&mut verify),
            common::patchset::expected_after_delete()
        );
    });
}

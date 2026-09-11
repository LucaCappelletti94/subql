//! Docker-backed E2E test for
//! [`subql::SubscriptionEngine::apply_patchset_async`] against MySQL.
//!
//! Async twin of `tests/it/apply_patchset_mysql_e2e.rs`: spins up a real MySQL
//! 8, creates a `things (id INT PK, active BOOL)` table, builds an SQLite
//! session patchset with INSERT / UPDATE / DELETE ops that touch the
//! `active` column, applies it through `apply_patchset_async` over a
//! `diesel-async` [`AsyncMysqlConnection`](diesel_async::AsyncMysqlConnection)
//! with the [`MysqlAdapter`](subql::patchset::MysqlAdapter), and asserts the
//! rows land in MySQL with the correct `bool` values. The apply runs on a
//! multi-thread tokio runtime, proving the returned future is `Send`.
//!
//! Tests are `#[ignore]`d so default `cargo test` does not require Docker.
//! Run with:
//!
//! ```sh
//! cargo test --test it apply_patchset_mysql_async_e2e:: \
//!     --features apply-patchset-mysql-async -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::{sql_query, QueryableByName, RunQueryDsl};
use diesel_async::{AsyncConnection, AsyncMysqlConnection};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::MySqlDialect;
use subql::patchset::MysqlAdapter;
use subql::wal::MaxwellEvent;
use subql::{DefaultIds, SubscriptionEngine};

const DDL: &str = "CREATE TABLE things (id INT PRIMARY KEY, active BOOLEAN);";
const MYSQL_DDL: &str = "CREATE TABLE things (id INT PRIMARY KEY, active BOOLEAN)";

#[derive(QueryableByName, Debug, PartialEq)]
struct ThingRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    id: i32,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    active: bool,
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn apply_patchset_async_bool_roundtrip_insert_update_delete_mysql() {
    common::assert_docker_available();
    let db = common::mysql_database();
    common::multi_thread_rt().block_on(async move {
        // Sync connection for DDL and result verification.
        let mut verify = db.connect();
        sql_query(MYSQL_DDL)
            .execute(&mut verify)
            .expect("create table");

        // Async connection that drives the apply path under test.
        let mut conn = AsyncMysqlConnection::establish(&db.url())
            .await
            .expect("async mysql connect");

        // subql catalog mirrors the MySQL DDL so the adapter can resolve
        // "things.active -> BOOLEAN" for dispatch.
        let catalog = ParserDB::parse::<MySqlDialect>(DDL).expect("parse subql DDL");
        let engine: SubscriptionEngine<MaxwellEvent, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, MySqlDialect {});
        let adapter = MysqlAdapter::new(engine.database()).expect("the catalog indexes");

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

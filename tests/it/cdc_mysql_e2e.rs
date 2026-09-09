//! Standalone MySQL + Maxwell CDC end-to-end test.
//!
//! Applies a deterministic DML stream (two INSERTs, an UPDATE, a DELETE) to a
//! live MySQL 8.0, reads Maxwell's JSONL file output, parses each line with
//! [`subql::parse_maxwell`], dispatches through a [`SubscriptionEngine`] with a
//! single registered subscription, and asserts the per-consumer notifications
//! directly (an INSERT matching the WHERE lands in `inserted()`, the DELETE in
//! `deleted()`).
//!
//! This is the MySQL/Maxwell half of `tests/it/cdc_cross_db.rs`, lifted and
//! specialized: no PG, no cross-DB parity, just direct assertions on the
//! Maxwell path.
//!
//! Requires Docker and `default-libmysqlclient-dev`. Run with:
//! ```sh
//! cargo test --test it cdc_mysql_e2e:: -- --ignored --nocapture
//! ```

use diesel::prelude::*;
use sqlparser::dialect::MySqlDialect;

use crate::common::{
    assert_docker_available, maxwell_collect, mysql_networked, mysql_port, mysql_url, start_maxwell,
};
use sql_traits::structs::ParserDB;
use subql::backend::MySql;
use subql::{parse_maxwell, DefaultIds, MaxwellEvent, SubscriptionEngine, SubscriptionRequest};

/// Catalog for the `events` table. Parsed with `PostgreSqlDialect` (subql
/// parses PG-flavored DDL regardless of the live backend). Maxwell sends
/// `schema="testdb"`; with no `testdb.events` in the catalog, table resolution
/// falls back to the unqualified lookup that hits this bare table.
fn events_catalog() -> ParserDB {
    ParserDB::parse::<MySqlDialect>(
        "CREATE TABLE events (id INT PRIMARY KEY, amount DOUBLE PRECISION, label TEXT);",
    )
    .expect("events DDL parses")
}

fn setup_mysql(my: &mut MysqlConnection) {
    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS events (
            id INT PRIMARY KEY,
            amount DOUBLE,
            label VARCHAR(100)
        )",
    )
    .execute(my)
    .expect("MySQL CREATE TABLE");
}

/// Apply the deterministic DML stream. Maxwell emits one CDC row per change in
/// commit order: INSERT(1), INSERT(2), UPDATE(1), DELETE(1).
fn apply_dml(my: &mut MysqlConnection) {
    diesel::sql_query("INSERT INTO events (id, amount, label) VALUES (1, 35.0, 'big')")
        .execute(my)
        .expect("insert 1");
    diesel::sql_query("INSERT INTO events (id, amount, label) VALUES (2, 5.0, 'small')")
        .execute(my)
        .expect("insert 2");
    diesel::sql_query("UPDATE events SET amount = 40.0 WHERE id = 1")
        .execute(my)
        .expect("update 1");
    diesel::sql_query("DELETE FROM events WHERE id = 1")
        .execute(my)
        .expect("delete 1");
}

#[test]
#[ignore = "requires Docker; run with: cargo test --test it cdc_mysql_e2e:: -- --ignored"]
#[allow(clippy::print_stderr)]
fn mysql_maxwell_cdc_e2e() {
    assert_docker_available();

    let pid = std::process::id();
    let network = format!("subql-mysql-e2e-{pid}");
    let mysql_name = format!("subql-mysql-e2e-{pid}");

    // Maxwell output dir (bind-mounted). World-writable so the in-container
    // Maxwell process can write to it.
    let maxwell_dir = tempfile::tempdir().expect("create maxwell tempdir");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(maxwell_dir.path(), std::fs::Permissions::from_mode(0o777))
            .expect("chmod maxwell dir");
    }
    let maxwell_path = maxwell_dir
        .path()
        .to_str()
        .expect("tempdir path")
        .to_string();

    let mysql_container = mysql_networked(&network, &mysql_name);

    let _maxwell_container = start_maxwell(&network, &mysql_name, &maxwell_path);

    let my_url = mysql_url(mysql_port(&mysql_container));
    let mut my = MysqlConnection::establish(&my_url).expect("MySQL connection");

    setup_mysql(&mut my);

    apply_dml(&mut my);

    let messages = maxwell_collect(&maxwell_path, "events", 4);
    assert_eq!(
        messages.len(),
        4,
        "expected exactly 4 Maxwell CDC rows for `events`, got {}",
        messages.len()
    );

    let consumer: u64 = 1;
    let mut engine: SubscriptionEngine<MaxwellEvent, DefaultIds, ParserDB> =
        SubscriptionEngine::new(events_catalog(), MySqlDialect {});
    engine
        .register(SubscriptionRequest::<DefaultIds, MySql>::new(
            consumer,
            "SELECT * FROM events WHERE amount > 10",
        ))
        .expect("register subscription");

    // Per-event matched-consumer buckets, in commit order.
    let mut inserted: Vec<Vec<u64>> = Vec::new();
    let mut updated: Vec<Vec<u64>> = Vec::new();
    let mut deleted: Vec<Vec<u64>> = Vec::new();

    for (i, msg) in messages.iter().enumerate() {
        let events: Vec<MaxwellEvent> = parse_maxwell(msg.as_bytes())
            .unwrap_or_else(|e| panic!("Maxwell parse failed for message {i}: {e}"))
            .into_iter()
            .map(MaxwellEvent::new)
            .collect();
        for event in &events {
            let notifs = engine
                .consumers(event)
                .unwrap_or_else(|e| panic!("dispatch failed for event {i}: {e}"));
            inserted.push(notifs.inserted().to_vec());
            updated.push(notifs.updated().to_vec());
            deleted.push(notifs.deleted().to_vec());
        }
    }

    // INSERT (1, 35.0): amount>10 -> consumer 1 in `inserted`.
    assert!(
        inserted[0].contains(&consumer),
        "INSERT id=1 (amount=35) should match WHERE amount>10, got inserted={:?}",
        inserted[0]
    );
    // INSERT (2, 5.0): amount<=10 -> no match anywhere.
    assert!(
        !inserted[1].contains(&consumer),
        "INSERT id=2 (amount=5) should not match, got inserted={:?}",
        inserted[1]
    );
    // UPDATE id=1 amount 35->40: still >10, only amount changed -> `updated`.
    assert!(
        updated[2].contains(&consumer),
        "UPDATE id=1 (amount 35->40) should re-match WHERE amount>10, got updated={:?}",
        updated[2]
    );
    // DELETE id=1 (amount=40): old row matched -> consumer 1 in `deleted`.
    assert!(
        deleted[3].contains(&consumer),
        "DELETE id=1 (amount=40) should match WHERE amount>10, got deleted={:?}",
        deleted[3]
    );
}

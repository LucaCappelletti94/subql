//! `follow_row` + [`PgSqliteEmuSource`] end-to-end.
//!
//! Replaces the pre-Phase-8.1 `tests/follow_cdc_sqlite.rs`. The
//! shape of the old test does not port directly: it used a shared
//! temp-file database with a sibling writer connection so the
//! trigger-installed shadow log would observe the INSERT. The
//! session-extension emulator only sees writes routed through the
//! connection it owns, so the follow's INSERT must run through
//! `source.connection()` (or the [`PgSqliteEmuSource::execute_sql`]
//! helper), and there is no need for a temp file.
//!
//! The old test also drove [`SubscriptionEngine::register_follow_insert`]
//! to combine the follow registration with the INSERT execution.
//! That helper requires the diesel connection's backend to match the
//! engine's [`crate::backend::Backend`], which fails for the Postgres
//! emulator over a SQLite diesel connection. The coverage the old
//! test actually exercised (the follow's target row round-trips
//! through the CDC event stream and reaches the subscriber) is still
//! validated here by registering the follow through
//! [`SubscriptionEngine::follow_row`] directly and running the INSERT
//! through the emulator.
//!
//! Gated behind the `pg-sqlite-emu` feature.

#![cfg(feature = "pg-sqlite-emu")]
#![allow(clippy::unwrap_used)]

use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, Value};
use subql::{ChangeEvent, DefaultIds, EventKind, PgSqliteEmuSource, SubscriptionEngine};

const PG_DDL: &str = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);";

#[test]
fn follow_row_receives_inserted_row_delta() {
    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("build source");
    let mut engine: SubscriptionEngine<ChangeEvent, DefaultIds, _> =
        SubscriptionEngine::new(source.pg_catalog().clone(), PostgreSqlDialect {});

    // Registering a follow before the row exists is the interesting
    // ordering: any INSERT arriving after this call should reach
    // consumer 1, and any INSERT for a different id should not.
    let ann = engine
        .follow_row(1, "users", vec![Value::Int(1)])
        .expect("register follow for id=1");
    let bob = engine
        .follow_row(2, "users", vec![Value::Int(2)])
        .expect("register follow for id=2");
    assert_ne!(ann.subscription_id, bob.subscription_id);

    source
        .execute_sql("INSERT INTO users (id, name) VALUES (1, 'ann')")
        .expect("insert ann");

    let event = source
        .poll_next_event()
        .expect("poll succeeds")
        .expect("insert emitted an event");
    assert_eq!(event.kind(), EventKind::Insert);

    let notifs = engine.consumers(&event).expect("dispatch");

    assert!(
        notifs.inserted().contains(&1),
        "consumer 1 follows id=1 and should see the INSERT: notifs.inserted() = {:?}",
        notifs.inserted(),
    );
    assert!(
        !notifs.inserted().contains(&2),
        "consumer 2 follows id=2 and should NOT see the id=1 INSERT: notifs.inserted() = {:?}",
        notifs.inserted(),
    );
}

#[test]
fn follow_row_ignores_unrelated_deletes() {
    // Prove that after a followed row goes away, the follow does not
    // spuriously fire on later inserts targeting a different PK.
    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("build source");
    let mut engine: SubscriptionEngine<ChangeEvent, DefaultIds, _> =
        SubscriptionEngine::new(source.pg_catalog().clone(), PostgreSqlDialect {});

    engine
        .follow_row(1, "users", vec![Value::Int(7)])
        .expect("register follow for id=7");

    source
        .execute_sql("INSERT INTO users (id, name) VALUES (7, 'seven')")
        .expect("insert seven");
    let insert_event = source.poll_next_event().unwrap().expect("insert event");
    let notifs = engine.consumers(&insert_event).unwrap();
    assert!(notifs.inserted().contains(&1), "follow observes its INSERT");

    source
        .execute_sql("DELETE FROM users WHERE id = 7")
        .expect("delete seven");
    let delete_event = source.poll_next_event().unwrap().expect("delete event");
    let notifs = engine.consumers(&delete_event).unwrap();
    assert!(
        notifs.deleted().contains(&1),
        "follow observes its DELETE: notifs.deleted() = {:?}",
        notifs.deleted(),
    );

    // A subsequent INSERT targeting a different id must not fire the
    // (now-closed) follow.
    source
        .execute_sql("INSERT INTO users (id, name) VALUES (99, 'other')")
        .expect("insert other");
    let event = source.poll_next_event().unwrap().expect("insert event");
    let notifs = engine.consumers(&event).unwrap();
    assert!(
        !notifs.inserted().contains(&1),
        "closed follow must not fire on unrelated INSERT: notifs.inserted() = {:?}",
        notifs.inserted(),
    );
}

#[test]
fn deleting_a_followed_row_auto_closes_the_pk_follow() {
    // Behavioural regression test for the pk-follow lifecycle:
    // registering `follow_row` on a PK, deleting that row, and then
    // dispatching the DELETE event must unregister the follow so it
    // stops occupying a subscription slot. Pre-fix (Phase-5 TODO),
    // the auto-close was disabled and the follow leaked.
    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("build source");
    let mut engine: SubscriptionEngine<ChangeEvent, DefaultIds, _> =
        SubscriptionEngine::new(source.pg_catalog().clone(), PostgreSqlDialect {});

    source
        .execute_sql("INSERT INTO users (id, name) VALUES (11, 'eleven')")
        .expect("insert eleven");
    let insert_event = source
        .poll_next_event()
        .unwrap()
        .expect("insert event pending");

    engine
        .follow_row(1, "users", vec![Value::Int(11)])
        .expect("register follow for id=11");
    let before = engine.subscription_count();
    assert_eq!(before, 1, "follow was just registered");

    // Dispatch the pre-existing INSERT event first (irrelevant to the
    // auto-close but exercises the engine + follow pipeline).
    let _ = engine.dispatch(&insert_event).unwrap();
    assert_eq!(
        engine.subscription_count(),
        1,
        "INSERT dispatch does not close the follow",
    );

    source
        .execute_sql("DELETE FROM users WHERE id = 11")
        .expect("delete eleven");
    let delete_event = source
        .poll_next_event()
        .unwrap()
        .expect("delete event pending");
    assert_eq!(delete_event.kind(), EventKind::Delete);

    // Dispatch closes the pk-follow because its tracked row was just
    // deleted.
    let _ = engine.dispatch(&delete_event).unwrap();
    assert_eq!(
        engine.subscription_count(),
        0,
        "DELETE dispatch must close the pk-follow whose row is gone",
    );
}

#[test]
fn deleting_an_unrelated_row_leaves_the_pk_follow_open() {
    // Symmetric guard: a DELETE for a *different* PK must not close
    // a pk-follow tracking another PK. Verifies the auto-close
    // condition is a PK match, not a table-wide DELETE sweep.
    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("build source");
    let mut engine: SubscriptionEngine<ChangeEvent, DefaultIds, _> =
        SubscriptionEngine::new(source.pg_catalog().clone(), PostgreSqlDialect {});

    source
        .execute_sql("INSERT INTO users (id, name) VALUES (20, 'twenty')")
        .expect("insert twenty");
    source
        .execute_sql("INSERT INTO users (id, name) VALUES (21, 'twentyone')")
        .expect("insert twentyone");
    let _ = source.poll_next_event().unwrap().expect("first insert");
    let _ = source.poll_next_event().unwrap().expect("second insert");

    // Follow id=20; delete id=21. The follow on id=20 must survive.
    engine
        .follow_row(1, "users", vec![Value::Int(20)])
        .expect("register follow for id=20");
    assert_eq!(engine.subscription_count(), 1);

    source
        .execute_sql("DELETE FROM users WHERE id = 21")
        .expect("delete twentyone");
    let event = source
        .poll_next_event()
        .unwrap()
        .expect("delete event pending");
    assert_eq!(event.kind(), EventKind::Delete);

    let _ = engine.dispatch(&event).unwrap();
    assert_eq!(
        engine.subscription_count(),
        1,
        "unrelated DELETE must not close the pk-follow for id=20",
    );
}

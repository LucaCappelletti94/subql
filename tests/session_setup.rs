//! Docker-free coverage for the connector session-setup seam (U8).
//!
//! Uses the generic `DieselConnector` over in-memory SQLite with a recording
//! instrumentation, so the exact statement sequence a read issues is
//! observable without a server: setup runs before the caller's SQL and inside
//! the transaction that serves the read, an empty setup adds nothing, and a
//! transaction-free path opens a transaction only when the caller supplies
//! statements. Transaction-scope observation on the cursor path and the async
//! MySQL `read_page` path rides the `#[ignore]` Docker convention elsewhere.

#![cfg(feature = "executor-diesel")]
#![allow(clippy::unwrap_used)]

use diesel::connection::{Instrumentation, InstrumentationEvent};
use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
use parking_lot::Mutex;
use std::sync::Arc;
use subql::backend::{BuiltinKind, Postgres};
use subql::reexec::{Connector, DieselConnector, SessionSetup};

/// Records the ordered statement and transaction events a connection sees.
#[derive(Clone)]
struct Recorder(Arc<Mutex<Vec<String>>>);

impl Instrumentation for Recorder {
    fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
        let entry = match event {
            InstrumentationEvent::StartQuery { query, .. } => format!("Q:{query}"),
            InstrumentationEvent::BeginTransaction { .. } => "BEGIN".to_string(),
            InstrumentationEvent::CommitTransaction { .. } => "COMMIT".to_string(),
            InstrumentationEvent::RollbackTransaction { .. } => "ROLLBACK".to_string(),
            _ => return,
        };
        self.0.lock().push(entry);
    }
}

/// A borrowed-list session setup carrying one or more statements.
struct Setup(Vec<String>);

impl SessionSetup for Setup {
    fn setup_statements(&self) -> &[String] {
        &self.0
    }
}

const SETUP_SQL: &str = "PRAGMA foreign_keys = ON";
const SCALAR_SQL: &str = "SELECT amount FROM t WHERE id = 1";

/// Fresh in-memory SQLite with one row, instrumentation attached after the
/// fixture DDL so only the connector's own statements are recorded.
fn conn_with_recorder() -> (SqliteConnection, Arc<Mutex<Vec<String>>>) {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    sql_query("CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER)")
        .execute(&mut conn)
        .unwrap();
    sql_query("INSERT INTO t (id, amount) VALUES (1, 10)")
        .execute(&mut conn)
        .unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    conn.set_instrumentation(Recorder(Arc::clone(&log)));
    (conn, log)
}

fn is_txn_open(event: &str) -> bool {
    event == "BEGIN" || event.starts_with("Q:BEGIN")
}

fn is_setup(event: &str) -> bool {
    event.contains("foreign_keys")
}

fn position(events: &[String], pred: impl Fn(&str) -> bool) -> Option<usize> {
    events.iter().position(|e| pred(e))
}

#[test]
fn setup_statements_run_inside_the_read_transaction() {
    let (conn, log) = conn_with_recorder();
    let connector: DieselConnector<SqliteConnection, Postgres, Setup> =
        DieselConnector::with_session_setup(conn);
    let setup = Setup(vec![SETUP_SQL.to_string()]);
    connector
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds(SCALAR_SQL),
            BuiltinKind::Int,
            &setup,
        )
        .unwrap();

    let events = log.lock().clone();
    let txn =
        position(&events, is_txn_open).unwrap_or_else(|| panic!("no transaction: {events:?}"));
    let setup_at =
        position(&events, is_setup).unwrap_or_else(|| panic!("setup absent: {events:?}"));
    let caller_at = position(&events, |e| e.contains("amount"))
        .unwrap_or_else(|| panic!("caller absent: {events:?}"));
    assert!(txn < setup_at, "transaction opens before setup: {events:?}");
    assert!(
        setup_at < caller_at,
        "setup runs before the caller SQL: {events:?}"
    );
    assert_eq!(
        events.iter().filter(|e| is_setup(e)).count(),
        1,
        "setup runs exactly once: {events:?}"
    );
}

#[test]
fn an_empty_setup_changes_nothing() {
    // `()` setup on the transaction-free `execute_scalar` path: exactly the
    // caller's SQL, no transaction, no extra statement, as before the seam.
    let (conn, log) = conn_with_recorder();
    let connector: DieselConnector<SqliteConnection, Postgres> = DieselConnector::new(conn);
    connector
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds(SCALAR_SQL),
            BuiltinKind::Int,
            &(),
        )
        .unwrap();
    let events = log.lock().clone();
    assert!(
        !events.iter().any(|e| is_txn_open(e)),
        "no transaction is opened for an empty setup: {events:?}"
    );
    assert!(
        !events.iter().any(|e| is_setup(e)),
        "no setup statement runs: {events:?}"
    );
    assert_eq!(
        events.iter().filter(|e| e.starts_with("Q:")).count(),
        1,
        "only the caller SQL runs: {events:?}"
    );
}

#[test]
fn a_transaction_free_path_with_setup_opens_a_transaction() {
    // Parameterised over the generic connector's two transaction-free read
    // paths. The async MySQL `read_page` third path is Docker-gated below.
    #[allow(clippy::type_complexity)]
    let paths: &[(
        &str,
        fn(&DieselConnector<SqliteConnection, Postgres, Setup>, &Setup),
    )] = &[
        ("execute_scalar", |c, setup| {
            c.execute_scalar(
                &subql::reexec::ReadQuery::without_binds(SCALAR_SQL),
                BuiltinKind::Int,
                setup,
            )
            .unwrap();
        }),
        ("read_page", |c, setup| {
            c.read_page(
                &subql::reexec::ReadQuery::without_binds(SCALAR_SQL),
                4096,
                setup,
            )
            .unwrap();
        }),
    ];
    for (name, run) in paths {
        // Empty setup: no transaction.
        let (conn, log) = conn_with_recorder();
        let empty: DieselConnector<SqliteConnection, Postgres, Setup> =
            DieselConnector::with_session_setup(conn);
        run(&empty, &Setup(Vec::new()));
        let events = log.lock().clone();
        assert!(
            !events.iter().any(|e| is_txn_open(e)),
            "{name}: empty setup opens no transaction: {events:?}"
        );

        // Non-empty setup: a transaction wraps the setup and the read.
        let (conn, log) = conn_with_recorder();
        let with: DieselConnector<SqliteConnection, Postgres, Setup> =
            DieselConnector::with_session_setup(conn);
        run(&with, &Setup(vec![SETUP_SQL.to_string()]));
        let events = log.lock().clone();
        let txn =
            position(&events, is_txn_open).unwrap_or_else(|| panic!("{name}: no txn: {events:?}"));
        let setup_at =
            position(&events, is_setup).unwrap_or_else(|| panic!("{name}: no setup: {events:?}"));
        assert!(
            txn < setup_at,
            "{name}: transaction opens before setup: {events:?}"
        );
    }
}

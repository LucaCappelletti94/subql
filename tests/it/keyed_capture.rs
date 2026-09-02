//! The keyed capture tier: a filter over one table that the in-process
//! predicate language cannot evaluate, maintained by asking the database only
//! about the rows that changed.
//!
//! This is the cost model that matters. The whole-re-read tier pays for the
//! size of the answer on every change; this one pays for the size of the
//! change. Both are correct, so what these tests pin is which one you get and
//! that the cheap one actually answers the same question.
//!
//! In-memory SQLite, so no Docker: the tier needs a connector that can read a
//! page, and `DieselConnector` over SQLite is one.
#![allow(clippy::unwrap_used)]

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::SQLiteDialect;
use subql::backend::{SQLite, Value};
use subql::reexec::{AutoResolvingEngine, DieselConnector, SyncMode};
use subql::testing::TestEvent;
use subql::{
    DefaultIds, Install, MaintenanceStopReason, Registered, SubscriptionEngine,
    SubscriptionRequest, TableId, Tier, TierKind, WholeRowsInstall,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT);";

type Engine = AutoResolvingEngine<
    TestEvent<SQLite>,
    DefaultIds,
    ParserDB,
    SyncMode<DieselConnector<SqliteConnection, SQLite>>,
>;

/// Wraps the connector and records every statement, so a test can assert what
/// went to the database and not only what came back.
struct Counting {
    inner: DieselConnector<SqliteConnection, SQLite>,
    statements: parking_lot::Mutex<Vec<String>>,
    /// Reads to allow before failing. Lets a test fail a chosen batch rather
    /// than the first one, which is the only way to reach the case where an
    /// earlier batch already succeeded.
    allow: parking_lot::Mutex<usize>,
}

impl Counting {
    const fn new(conn: SqliteConnection) -> Self {
        Self {
            inner: DieselConnector::new(conn),
            statements: parking_lot::Mutex::new(Vec::new()),
            allow: parking_lot::Mutex::new(usize::MAX),
        }
    }

    fn fail_after(&self, reads: usize) {
        *self.allow.lock() = reads;
    }

    fn take(&self) -> Vec<String> {
        core::mem::take(&mut self.statements.lock())
    }
}

/// Key values named in one statement. They render into an `IN` list, so every
/// comma inside the outermost list separates two of them.
fn keys_named(sql: &str) -> usize {
    sql.rfind("IN (")
        .map_or(0, |at| sql[at..].matches(',').count() + 1)
}

impl subql::reexec::Connector for Counting {
    type AuthContext = ();
    type Error = <DieselConnector<SqliteConnection, SQLite> as subql::reexec::Connector>::Error;
    type Checkpoint =
        <DieselConnector<SqliteConnection, SQLite> as subql::reexec::Connector>::Checkpoint;
    type Backend = SQLite;

    fn execute_scalar(
        &self,
        query: &subql::reexec::ReadQuery<'_, SQLite>,
        kind: subql::backend::BuiltinKind,
        auth: &(),
    ) -> Result<(Value<SQLite>, Option<Self::Checkpoint>), Self::Error> {
        self.inner.execute_scalar(query, kind, auth)
    }

    fn read_page(
        &self,
        query: &subql::reexec::ReadQuery<'_, SQLite>,
        max_bytes: usize,
        auth: &(),
    ) -> Result<
        subql::reexec::Snapshot<subql::reexec::RowPage<SQLite>, Self::Checkpoint>,
        Self::Error,
    > {
        self.statements.lock().push(query.sql().to_string());
        {
            let mut allow = self.allow.lock();
            if *allow == 0 {
                return Err(diesel::result::Error::NotFound);
            }
            *allow = allow.saturating_sub(1);
        }
        self.inner.read_page(query, max_bytes, auth)
    }

    fn execute_scalar_row(
        &self,
        query: &subql::reexec::ReadQuery<'_, SQLite>,
        kinds: &[subql::backend::BuiltinKind],
        auth: &(),
    ) -> Result<
        (Vec<Value<SQLite>>, Option<Self::Checkpoint>),
        subql::reexec::ScalarRowError<Self::Error>,
    > {
        self.inner.execute_scalar_row(query, kinds, auth)
    }
}

type CountingEngine =
    AutoResolvingEngine<TestEvent<SQLite>, DefaultIds, ParserDB, SyncMode<Counting>>;

/// A keyed read must never name more keys in one statement than it was told to.
///
/// Statement duration tracks how many keys it names, and a caller's statement
/// timeout is per statement, so an unbounded request disables the only ceiling
/// the caller has. Measured against Postgres: at a 25 ms ceiling a 50,000 key
/// request is cancelled outright, and because a failed read gives its keys back,
/// the next ordinary change carries the whole backlog and fails again.
#[test]
fn a_keyed_read_names_no_more_keys_per_statement_than_its_budget() {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)")
        .execute(&mut conn)
        .expect("create");
    // Seven rows in the answer, so every key asked about comes back and the
    // deltas are upserts rather than removals.
    for id in 0..7 {
        diesel::sql_query(format!("INSERT INTO orders VALUES ({id}, 'paid')"))
            .execute(&mut conn)
            .expect("seed");
    }

    let catalog = ParserDB::parse::<SQLiteDialect>(DDL).expect("catalog");
    let table = subql::catalog_helpers::table_id(&catalog, "orders").expect("orders");
    let inner = SubscriptionEngine::<TestEvent<SQLite>, DefaultIds, ParserDB>::new(
        catalog,
        SQLiteDialect {},
    );
    let mut engine: CountingEngine =
        AutoResolvingEngine::new(inner, SyncMode(Counting::new(conn))).with_max_keys_per_read(3);
    engine
        .register(
            SubscriptionRequest::<DefaultIds, SQLite>::new(1u64, SQL),
            (),
        )
        .expect("captured");

    let events: Vec<_> = (0..7i64)
        .map(|id| {
            TestEvent::<SQLite>::update(table, row(id, "paid"), row(id, "paid"))
                .with_pk_columns([0u16])
        })
        .collect();
    for event in &events {
        engine.apply(event).expect("apply");
    }
    let outcome = engine.resolve_collect().expect("batch resolve");

    let statements = engine.connector().take();
    let named: Vec<usize> = statements.iter().map(|s| keys_named(s)).collect();
    assert!(
        named.iter().all(|n| *n <= 3),
        "no statement may name more than the budget of 3 keys, got {named:?}"
    );
    assert_eq!(
        named.iter().sum::<usize>(),
        7,
        "every key is asked about exactly once across the statements, got {named:?}"
    );
    assert_eq!(
        outcome.row_deltas.len(),
        7,
        "seven changed rows produce seven deltas, one per key"
    );
    let mut keys: Vec<_> = outcome
        .row_deltas
        .iter()
        .map(|d| format!("{:?}", d.key))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    assert_eq!(keys.len(), 7, "no key may be delivered twice");
}

/// `lower(status)` is a function call, which the in-process language cannot
/// evaluate, so the engine refuses and the wrapper captures. The projection is
/// a wildcard, so it carries the key.
const SQL: &str = "SELECT * FROM orders WHERE lower(status) = 'paid'";

fn setup(rows: &[(i64, &str)]) -> (Engine, TableId) {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)")
        .execute(&mut conn)
        .expect("create orders");
    for (id, status) in rows {
        diesel::sql_query(format!("INSERT INTO orders VALUES ({id}, '{status}')"))
            .execute(&mut conn)
            .expect("seed");
    }

    let catalog = ParserDB::parse::<SQLiteDialect>(DDL).expect("catalog");
    let table = subql::catalog_helpers::table_id(&catalog, "orders").expect("orders");
    let inner = SubscriptionEngine::<TestEvent<SQLite>, DefaultIds, ParserDB>::new(
        catalog,
        SQLiteDialect {},
    );
    (
        AutoResolvingEngine::new(inner, SyncMode(DieselConnector::new(conn))),
        table,
    )
}

fn register(engine: &mut Engine) -> u64 {
    match engine
        .register(
            SubscriptionRequest::<DefaultIds, SQLite>::new(1u64, SQL),
            (),
        )
        .expect("a filter outside the language is captured, not refused")
    {
        Registered {
            subscription_id,
            tier: Tier::KeyedRows { .. },
            ..
        } => subscription_id,
        other => panic!("expected a keyed capture, got {other:?}"),
    }
}

fn row(id: i64, status: &str) -> Vec<Value<SQLite>> {
    vec![Value::Int(id), Value::String(status.into())]
}

/// The tier is reported at registration, because it decides what every later
/// change costs and a caller metering its database cannot infer it.
#[test]
fn a_single_table_filter_is_captured_as_the_keyed_tier() {
    let (mut engine, _table) = setup(&[(1, "paid")]);
    let _ = register(&mut engine);
}

/// A changed row that still matches arrives as its current row, keyed by its
/// primary key. One small read, not the whole answer.
#[test]
fn a_changed_row_that_still_matches_arrives_as_itself() {
    let (mut engine, table) = setup(&[(1, "paid"), (2, "void")]);
    let subscription_id = register(&mut engine);

    let event =
        TestEvent::<SQLite>::update(table, row(1, "paid"), row(1, "paid")).with_pk_columns([0u16]);
    engine.apply(&event).expect("apply");
    let notifications = engine.resolve_collect().expect("dispatch");

    assert_eq!(notifications.row_deltas.len(), 1);
    let delta = &notifications.row_deltas[0];
    assert_eq!(delta.subscription_id, subscription_id);
    assert_eq!(delta.key, vec![Value::Int(1)]);
    assert_eq!(
        delta.row.as_deref(),
        Some(&row(1, "paid")[..]),
        "the row as it now is"
    );
    assert_eq!(delta.columns.as_ref(), ["id", "status"].map(String::from));
}

/// A row that stopped matching arrives as a removal. The database is asked and
/// answers nothing, which is the whole mechanism: membership is asked, never
/// remembered.
#[test]
fn a_row_that_stopped_matching_arrives_as_a_removal() {
    let (mut engine, table) = setup(&[(1, "void")]);
    let _ = register(&mut engine);

    let event =
        TestEvent::<SQLite>::update(table, row(1, "paid"), row(1, "void")).with_pk_columns([0u16]);
    engine.apply(&event).expect("apply");
    let notifications = engine.resolve_collect().expect("dispatch");

    assert_eq!(notifications.row_deltas.len(), 1);
    let delta = &notifications.row_deltas[0];
    assert_eq!(delta.key, vec![Value::Int(1)]);
    assert!(
        delta.row.is_none(),
        "no row came back, so it is no longer in the answer"
    );
}

/// A row deleted outright is a removal too, and it needs no previous image
/// beyond its key: the scoped read simply finds nothing.
#[test]
fn a_deleted_row_arrives_as_a_removal() {
    let (mut engine, table) = setup(&[]);
    let _ = register(&mut engine);

    let event = TestEvent::<SQLite>::delete(table, row(7, "paid")).with_pk_columns([0u16]);
    engine.apply(&event).expect("apply");
    let notifications = engine.resolve_collect().expect("dispatch");

    assert_eq!(notifications.row_deltas.len(), 1);
    assert_eq!(notifications.row_deltas[0].key, vec![Value::Int(7)]);
    assert!(notifications.row_deltas[0].row.is_none());
}

/// Several changes in one batch cost one read, and every changed row is
/// answered for. This is the property that makes the tier cheap: the cost
/// follows the change volume, not the answer size.
#[test]
fn several_changed_rows_are_answered_in_one_pass() {
    let (mut engine, table) = setup(&[(1, "paid"), (2, "paid"), (3, "void")]);
    let _ = register(&mut engine);

    let events = vec![
        TestEvent::<SQLite>::update(table, row(1, "paid"), row(1, "paid")).with_pk_columns([0u16]),
        TestEvent::<SQLite>::update(table, row(2, "paid"), row(2, "paid")).with_pk_columns([0u16]),
        TestEvent::<SQLite>::update(table, row(3, "void"), row(3, "void")).with_pk_columns([0u16]),
    ];
    for event in &events {
        engine.apply(event).expect("apply");
    }
    let outcome = engine.resolve_collect().expect("batch dispatch");

    let mut answered: Vec<(i64, bool)> = outcome
        .row_deltas
        .iter()
        .map(|d| {
            let Value::Int(id) = d.key[0] else {
                panic!("key should be an integer")
            };
            (id, d.row.is_some())
        })
        .collect();
    answered.sort_unstable();
    assert_eq!(
        answered,
        vec![(1, true), (2, true), (3, false)],
        "every changed row answered: 1 and 2 are in, 3 is not"
    );
}

/// A change carrying no readable key changes the existing subscription to a
/// complete row read under the same identity.
#[test]
fn a_change_with_no_readable_key_transitions_to_whole_rows() {
    let catalog = ParserDB::parse::<SQLiteDialect>(DDL).expect("catalog");
    let table = subql::catalog_helpers::table_id(&catalog, "orders").expect("orders");
    let mut registry = SubscriptionEngine::<TestEvent<SQLite>, DefaultIds, ParserDB>::new(
        catalog,
        SQLiteDialect {},
    );
    let bound_sql = "SELECT * FROM orders WHERE lower(status) = ?";
    let registered = registry
        .register(
            SubscriptionRequest::new(1u64, bound_sql).binds(vec![Value::String("paid".into())]),
        )
        .expect("keyed read registers");
    assert!(matches!(registered.tier, Tier::KeyedRows { .. }));

    let event = TestEvent::<SQLite>::update(table, row(1, "paid"), row(1, "paid"));
    let output = registry.dispatch(&event).expect("transition succeeds");

    assert_eq!(output.transitions().len(), 1);
    let transition = &output.transitions()[0];
    assert_eq!(transition.subscription_id, registered.subscription_id);
    assert_eq!(transition.from, TierKind::KeyedRows);
    assert_eq!(
        transition.reason,
        MaintenanceStopReason::KeyedChangeWithoutKey { table_id: table }
    );
    let Tier::WholeRows { query, .. } = &transition.to else {
        panic!("expected whole-row transition")
    };
    assert_eq!(query.sql(), bound_sql);
    assert_eq!(query.binds(), &[Value::String("paid".into())]);
    assert_eq!(
        output.triggers()[0].subscription_id,
        registered.subscription_id
    );
    let installed = Install::install(
        &mut registry,
        registered.subscription_id,
        WholeRowsInstall::<SQLite, subql::NoCheckpoint> {
            generation: 1,
            pages: Vec::new(),
        },
    )
    .expect("the original id now accepts whole-row results");
    assert!(installed.is_empty());
}

/// The delivered key must be the row's primary key, whatever order the
/// projection names its columns in.
///
/// A consumer applies these deltas by key. A key read from the wrong column
/// means the wrong row is updated and the right one is reported as removed, so
/// the consumer's view diverges from the database and stays diverged.
#[test]
fn an_explicit_projection_still_delivers_the_primary_key() {
    let (mut engine, table) = setup(&[(1, "paid")]);
    // `status` first, so a key taken by table ordinal would read the status.
    let reordered = "SELECT status, id FROM orders WHERE lower(status) = 'paid'";
    let subscription_id = match engine
        .register(
            SubscriptionRequest::<DefaultIds, SQLite>::new(9u64, reordered),
            (),
        )
        .expect("a filter outside the language is captured")
    {
        Registered {
            subscription_id,
            tier: Tier::KeyedRows { .. },
            ..
        } => subscription_id,
        other => panic!("expected a keyed capture, got {other:?}"),
    };

    let event =
        TestEvent::<SQLite>::update(table, row(1, "paid"), row(1, "paid")).with_pk_columns([0u16]);
    engine.apply(&event).expect("apply");
    let notifications = engine.resolve_collect().expect("dispatch");

    assert_eq!(
        notifications.row_deltas.len(),
        1,
        "one changed row is one delta, not an upsert plus a phantom removal"
    );
    let delta = &notifications.row_deltas[0];
    assert_eq!(delta.subscription_id, subscription_id);
    assert_eq!(
        delta.key,
        vec![Value::Int(1)],
        "the key is the primary key, not whichever column sits at the key's table ordinal"
    );
    assert!(
        delta.row.is_some(),
        "a row that still matches is an upsert, never a removal"
    );
}

/// A filter that reads a second table cannot be served by asking about the
/// changed rows of the first.
///
/// Membership then depends on rows this tier never watches, so a change over
/// there would silently never be delivered. The whole-re-read tier watches
/// every table the statement names, so this must land there instead.
#[test]
fn a_filter_reading_a_second_table_is_not_served_by_the_keyed_tier() {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    for ddl in [
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)",
        "CREATE TABLE managers (id INTEGER PRIMARY KEY)",
        "INSERT INTO orders VALUES (1, 'paid')",
        "INSERT INTO managers VALUES (1)",
    ] {
        diesel::sql_query(ddl).execute(&mut conn).unwrap();
    }
    let catalog = ParserDB::parse::<SQLiteDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT); \
         CREATE TABLE managers (id INT PRIMARY KEY);",
    )
    .expect("catalog");
    let inner = SubscriptionEngine::<TestEvent<SQLite>, DefaultIds, ParserDB>::new(
        catalog,
        SQLiteDialect {},
    );
    let mut engine: Engine = AutoResolvingEngine::new(inner, SyncMode(DieselConnector::new(conn)));

    let sql = "SELECT * FROM orders WHERE lower(status) = 'paid' \
               AND id IN (SELECT id FROM managers)";
    match engine
        .register(
            SubscriptionRequest::<DefaultIds, SQLite>::new(8u64, sql),
            (),
        )
        .expect("captured")
    {
        Registered {
            tier: Tier::WholeRows { tables, .. },
            ..
        } => {
            assert_eq!(
                tables.len(),
                2,
                "both tables must trigger, else a change to the subquery's table is never seen"
            );
        }
        other => panic!("expected a capture, got {other:?}"),
    }
}

/// A key this tier cannot spell as a SQL literal must be refused at
/// registration, not accepted and then failed on every single change.
#[test]
fn a_key_with_no_literal_spelling_is_refused_at_registration() {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    for ddl in [
        "CREATE TABLE events (at TIMESTAMP PRIMARY KEY, status TEXT)",
        "INSERT INTO events VALUES ('2026-01-01 00:00:00', 'paid')",
    ] {
        diesel::sql_query(ddl).execute(&mut conn).unwrap();
    }
    let catalog = ParserDB::parse::<SQLiteDialect>(
        "CREATE TABLE events (at TIMESTAMP PRIMARY KEY, status TEXT);",
    )
    .expect("catalog");
    let inner = SubscriptionEngine::<TestEvent<SQLite>, DefaultIds, ParserDB>::new(
        catalog,
        SQLiteDialect {},
    );
    let mut engine: Engine = AutoResolvingEngine::new(inner, SyncMode(DieselConnector::new(conn)));

    let sql = "SELECT * FROM events WHERE lower(status) = 'paid'";
    match engine
        .register(
            SubscriptionRequest::<DefaultIds, SQLite>::new(7u64, sql),
            (),
        )
        .expect("captured rather than refused outright")
    {
        Registered {
            tier: Tier::WholeRows { .. },
            ..
        } => {}
        other => panic!("expected a capture, got {other:?}"),
    }
}

/// A read that fails must not consume the changes it was asked about.
///
/// This tier is the only one that does not heal itself. A scalar or a whole
/// re-read asks the database for everything again, so a later change repairs an
/// earlier lost answer. A keyed read asks only about the rows named in it, so a
/// key dropped on the floor means that row stays wrong until its own change
/// happens again, which may be never.
#[test]
fn a_failed_read_keeps_the_keys_it_was_going_to_ask_about() {
    let dir = std::env::temp_dir().join(format!("subql_keyed_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("keyed.db");
    let url = path.to_str().expect("utf8 path");

    // Two connections to one file, so the test can break the read underneath
    // the engine. An in-memory database is private to its connection.
    let mut engine_conn = SqliteConnection::establish(url).unwrap();
    let mut saboteur = SqliteConnection::establish(url).unwrap();
    for ddl in [
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)",
        "INSERT INTO orders VALUES (1, 'paid'), (2, 'paid')",
    ] {
        diesel::sql_query(ddl).execute(&mut engine_conn).unwrap();
    }

    let catalog = ParserDB::parse::<SQLiteDialect>(DDL).expect("catalog");
    let table = subql::catalog_helpers::table_id(&catalog, "orders").expect("orders");
    let inner = SubscriptionEngine::<TestEvent<SQLite>, DefaultIds, ParserDB>::new(
        catalog,
        SQLiteDialect {},
    );
    let mut engine: Engine =
        AutoResolvingEngine::new(inner, SyncMode(DieselConnector::new(engine_conn)));
    let _ = register(&mut engine);

    // Make the scoped read fail: the catalog still names `orders`, the database
    // no longer has it.
    diesel::sql_query("ALTER TABLE orders RENAME TO orders_hidden")
        .execute(&mut saboteur)
        .expect("hide the table");
    let event =
        TestEvent::<SQLite>::update(table, row(1, "paid"), row(1, "paid")).with_pk_columns([0u16]);
    engine.apply(&event).expect("apply");
    assert!(
        engine.resolve_collect().is_err(),
        "the read must fail while the table is missing"
    );

    // Put it back and change a different row. Row 1's change was never
    // delivered, so it must still be pending rather than forgotten.
    diesel::sql_query("ALTER TABLE orders_hidden RENAME TO orders")
        .execute(&mut saboteur)
        .expect("restore the table");
    let other =
        TestEvent::<SQLite>::update(table, row(2, "paid"), row(2, "paid")).with_pk_columns([0u16]);
    engine.apply(&other).expect("apply");
    let after = engine.resolve_collect().expect("second dispatch");

    let keys: Vec<_> = after.row_deltas.iter().map(|d| d.key.clone()).collect();
    assert!(
        keys.contains(&vec![Value::Int(1)]),
        "the key from the failed read must be asked about again, got {keys:?}"
    );
    assert!(
        keys.contains(&vec![Value::Int(2)]),
        "the new change must be answered too, got {keys:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// One row changed twice in a batch is asked about once.
///
/// The read's cost is the number of distinct keys, so a row that changes
/// repeatedly must not lengthen it, and the caller must not receive the same
/// row twice under one key.
#[test]
fn a_row_changed_twice_in_one_batch_is_asked_about_once() {
    // Row 3 is in the database as `void`, so it is outside the answer and its
    // delta is a removal. A duplicate key is invisible on the upsert side, the
    // database returning one row either way, and visible on the removal side,
    // where each key not returned produces a delta of its own.
    let (mut engine, table) = setup(&[(1, "paid"), (3, "void")]);
    let _ = register(&mut engine);

    let events = [
        TestEvent::<SQLite>::update(table, row(1, "paid"), row(1, "paid")).with_pk_columns([0u16]),
        TestEvent::<SQLite>::update(table, row(3, "paid"), row(3, "void")).with_pk_columns([0u16]),
        TestEvent::<SQLite>::update(table, row(3, "void"), row(3, "void")).with_pk_columns([0u16]),
    ];
    for event in &events {
        engine.apply(event).expect("apply");
    }
    let outcome = engine.resolve_collect().expect("batch dispatch");

    let mut keys: Vec<_> = outcome
        .row_deltas
        .iter()
        .map(|d| d.key.clone())
        .collect::<Vec<_>>();
    keys.sort_by_key(|k| alloc_key(k));
    assert_eq!(
        keys,
        vec![vec![Value::Int(1)], vec![Value::Int(3)]],
        "two rows changed, so two deltas, however many events touched them"
    );
    assert_eq!(
        outcome
            .row_deltas
            .iter()
            .filter(|d| d.row.is_none())
            .count(),
        1,
        "one row left the answer, so one removal, not one per event that touched it"
    );
}

/// Sort helper: `Value` has no ordering, floats being what they are.
fn alloc_key(key: &[Value<SQLite>]) -> String {
    format!("{key:?}")
}

/// A failure on a later batch must not lose the keys the earlier ones asked
/// about.
///
/// A failed read fails the whole call, so the rows the successful batches
/// answered are never delivered. Keeping their keys would leave exactly those
/// rows stale until they happen to change again, which is the failure the
/// restore path exists to prevent. Asking about them again is the price of an
/// all-or-nothing result.
#[test]
fn a_failure_on_a_later_batch_gives_back_every_key() {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)")
        .execute(&mut conn)
        .expect("create");
    for id in 0..7 {
        diesel::sql_query(format!("INSERT INTO orders VALUES ({id}, 'paid')"))
            .execute(&mut conn)
            .expect("seed");
    }

    let catalog = ParserDB::parse::<SQLiteDialect>(DDL).expect("catalog");
    let table = subql::catalog_helpers::table_id(&catalog, "orders").expect("orders");
    let inner = SubscriptionEngine::<TestEvent<SQLite>, DefaultIds, ParserDB>::new(
        catalog,
        SQLiteDialect {},
    );
    // Three keys per statement, so seven keys are three batches.
    let mut engine: CountingEngine =
        AutoResolvingEngine::new(inner, SyncMode(Counting::new(conn))).with_max_keys_per_read(3);
    engine
        .register(
            SubscriptionRequest::<DefaultIds, SQLite>::new(1u64, SQL),
            (),
        )
        .expect("captured");

    let events: Vec<_> = (0..7i64)
        .map(|id| {
            TestEvent::<SQLite>::update(table, row(id, "paid"), row(id, "paid"))
                .with_pk_columns([0u16])
        })
        .collect();

    // Let the first batch through, fail the second.
    engine.connector().fail_after(1);
    for event in &events {
        engine.apply(event).expect("apply");
    }
    assert!(
        engine.resolve_collect().is_err(),
        "the second batch must fail"
    );
    let _ = engine.connector().take();

    // Now let everything through and change one more row. Every key from the
    // failed call must be asked about again, including the first batch's.
    engine.connector().fail_after(usize::MAX);
    let extra =
        TestEvent::<SQLite>::update(table, row(0, "paid"), row(0, "paid")).with_pk_columns([0u16]);
    engine.apply(&extra).expect("apply");
    let after = engine.resolve_collect().expect("second dispatch");

    let mut keys: Vec<i64> = after
        .row_deltas
        .iter()
        .filter_map(|d| match d.key.first() {
            Some(Value::Int(i)) => Some(*i),
            _ => None,
        })
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        (0..7).collect::<Vec<i64>>(),
        "every key of the failed call is asked about again, not just the unasked ones"
    );
}

/// A compound primary key is delivered whole, in key-column order, against a
/// real database.
///
/// Proven until now only in the renderer's unit tests, against a plan built by
/// hand. A compound key is where a position mistake is easiest to make and
/// hardest to see, since a single-column key hides any ordering error.
#[test]
fn a_compound_key_is_delivered_in_key_column_order() {
    const COMPOUND_DDL: &str =
        "CREATE TABLE lines (order_id INT, line_no INT, status TEXT, PRIMARY KEY (order_id, line_no));";
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    diesel::sql_query(
        "CREATE TABLE lines (order_id INTEGER, line_no INTEGER, status TEXT, \
         PRIMARY KEY (order_id, line_no))",
    )
    .execute(&mut conn)
    .expect("create");
    for (order, line, status) in [(7i64, 1i64, "paid"), (7, 2, "void"), (8, 1, "paid")] {
        diesel::sql_query(format!(
            "INSERT INTO lines VALUES ({order}, {line}, '{status}')"
        ))
        .execute(&mut conn)
        .expect("seed");
    }

    let catalog = ParserDB::parse::<SQLiteDialect>(COMPOUND_DDL).expect("catalog");
    let table = subql::catalog_helpers::table_id(&catalog, "lines").expect("lines");
    let inner = SubscriptionEngine::<TestEvent<SQLite>, DefaultIds, ParserDB>::new(
        catalog,
        SQLiteDialect {},
    );
    // An explicit projection in an order that is neither the table's nor the
    // key's, so a position taken from anywhere but the projection is wrong.
    let sql = "SELECT status, line_no, order_id FROM lines WHERE lower(status) = 'paid'";
    let mut engine: CountingEngine = AutoResolvingEngine::new(inner, SyncMode(Counting::new(conn)));
    engine
        .register(
            SubscriptionRequest::<DefaultIds, SQLite>::new(1u64, sql),
            (),
        )
        .expect("captured");

    let line = |order: i64, no: i64, status: &str| {
        vec![
            Value::Int(order),
            Value::Int(no),
            Value::String(status.into()),
        ]
    };
    let events = [
        // Still in the answer.
        TestEvent::<SQLite>::update(table, line(7, 1, "paid"), line(7, 1, "paid"))
            .with_pk_columns([0u16, 1u16]),
        // Not in the answer, so a removal.
        TestEvent::<SQLite>::update(table, line(7, 2, "paid"), line(7, 2, "void"))
            .with_pk_columns([0u16, 1u16]),
    ];
    for event in &events {
        engine.apply(event).expect("apply");
    }
    let outcome = engine.resolve_collect().expect("batch dispatch");

    let upserted: Vec<_> = outcome
        .row_deltas
        .iter()
        .filter(|d| d.row.is_some())
        .map(|d| d.key.clone())
        .collect();
    assert_eq!(
        upserted,
        vec![vec![Value::Int(7), Value::Int(1)]],
        "the key is (order_id, line_no) in key-column order, not projection order"
    );
    let removed: Vec<_> = outcome
        .row_deltas
        .iter()
        .filter(|d| d.row.is_none())
        .map(|d| d.key.clone())
        .collect();
    assert_eq!(
        removed,
        vec![vec![Value::Int(7), Value::Int(2)]],
        "a compound key that left the answer is reported whole"
    );
}

/// A batch whose rows do not fit one page must still answer every key exactly
/// once.
///
/// The read resumes inside the batch, excluding keys already returned. If the
/// record of what was returned resets per page, keys answered earlier re-enter
/// the next statement: they are delivered twice, and with a stable row order the
/// remaining sets oscillate between halves of the batch and the read never
/// finishes.
#[test]
fn a_batch_spanning_several_pages_answers_every_key_once() {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)")
        .execute(&mut conn)
        .expect("create");
    for id in 0..6 {
        diesel::sql_query(format!("INSERT INTO orders VALUES ({id}, 'paid')"))
            .execute(&mut conn)
            .expect("seed");
    }

    let catalog = ParserDB::parse::<SQLiteDialect>(DDL).expect("catalog");
    let table = subql::catalog_helpers::table_id(&catalog, "orders").expect("orders");
    let inner = SubscriptionEngine::<TestEvent<SQLite>, DefaultIds, ParserDB>::new(
        catalog,
        SQLiteDialect {},
    );
    // One batch holds every key, and a one-byte page budget forces that batch to
    // be read over several pages, which is the path being tested.
    let mut engine: CountingEngine = AutoResolvingEngine::new(inner, SyncMode(Counting::new(conn)))
        .with_max_keys_per_read(6)
        .with_max_page_bytes(1);
    engine
        .register(
            SubscriptionRequest::<DefaultIds, SQLite>::new(1u64, SQL),
            (),
        )
        .expect("captured");

    let events: Vec<_> = (0..6i64)
        .map(|id| {
            TestEvent::<SQLite>::update(table, row(id, "paid"), row(id, "paid"))
                .with_pk_columns([0u16])
        })
        .collect();
    for event in &events {
        engine.apply(event).expect("apply");
    }
    let outcome = engine.resolve_collect().expect("batch dispatch");

    let statements = engine.connector().take();
    assert!(
        statements.len() > 1,
        "the budget must force several pages, else this tests nothing: got {}",
        statements.len()
    );
    let mut keys: Vec<i64> = outcome
        .row_deltas
        .iter()
        .filter_map(|d| match d.key.first() {
            Some(Value::Int(i)) => Some(*i),
            _ => None,
        })
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        (0..6).collect::<Vec<i64>>(),
        "every key answered exactly once, none twice and none missing"
    );
}

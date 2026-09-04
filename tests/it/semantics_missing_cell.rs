//! A cell the event does not carry stops being a silent no-match.
//!
//! `Value::Missing` is not `Value::Null`. Null is a value the database
//! holds; missing means the change stream did not carry the cell, so the
//! predicate has no answer rather than a false one. Three decoders already
//! document that this must escalate: `src/wal/change_event.rs`,
//! `src/wal/pg_type.rs` and `src/sqlite_cdc/parser.rs`.
//!
//! Today it does not escalate. The cell reads `Missing`, the comparison
//! answers `Tri::Unknown`, and unknown collapses to no-match with no
//! trigger, no report and no error, which is indistinguishable from a row
//! the filter genuinely rejected.
//!
//! The case that produces it in production is narrow and real: PostgreSQL
//! omits an unchanged TOASTed column from a logical replication message,
//! `pg_walstream` drops it, and `REPLICA IDENTITY FULL` does not restore
//! it.
//!
//! What the core does about it is report, per subscription. It cannot
//! re-execute: it holds no connector, and an in-process subscription
//! retains no bound query to re-execute. Turning the report into a read is
//! the auto wrapper's job, which is where the connector lives.
#![allow(clippy::unwrap_used)]

use crate::common;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, Postgres, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, tag TEXT)";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine() -> (Engine, subql::TableId) {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "docs").expect("docs is in the catalog");
    (SubscriptionEngine::new(db, PostgreSqlDialect {}), table)
}

/// The finding: a predicate that reads a cell the event does not carry is
/// reported as unanswerable for that subscription, not dropped.
#[test]
fn a_missing_cell_is_reported_as_unknown() {
    let (mut engine, table) = engine();
    let subscription = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM docs WHERE body = 'keep'",
        ))
        .expect("the predicate registers")
        .subscription_id;

    // The event carries `id` and `tag` but not `body`: the row image is
    // shorter than the table, which is how an omitted cell arrives.
    let notifications = engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::Missing, Value::String("t".into())],
        ))
        .expect("dispatch succeeds");

    assert_eq!(
        notifications
            .unanswered()
            .iter()
            .map(|entry| (entry.subscription_id, entry.consumer_id, entry.column))
            .collect::<Vec<_>>(),
        vec![(subscription, 1u64, 1)],
        "the report names the subscription, its consumer and the absent column"
    );
    assert!(
        notifications.inserted().is_empty(),
        "an unanswerable predicate is not a match either"
    );
}

/// The control: a cell the event does carry answers normally, and nothing
/// is reported.
#[test]
fn a_present_cell_still_notifies() {
    let (mut engine, table) = engine();
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM docs WHERE body = 'keep'",
        ))
        .expect("the predicate registers");

    let notifications = engine
        .consumers(&TestEvent::insert(
            table,
            vec![
                Value::Int(1),
                Value::String("keep".into()),
                Value::String("t".into()),
            ],
        ))
        .expect("dispatch succeeds");
    assert!(notifications.unanswered().is_empty());
    assert_eq!(notifications.inserted(), &[1]);
}

/// `Null` is not `Missing`: a null cell is a value the database holds, the
/// comparison is unknown by SQL's own rule, and there is nothing to
/// report. Treating the two alike would report every null comparison.
#[test]
fn a_null_cell_is_not_reported() {
    let (mut engine, table) = engine();
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM docs WHERE body = 'keep'",
        ))
        .expect("the predicate registers");

    let notifications = engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::Null, Value::String("t".into())],
        ))
        .expect("dispatch succeeds");
    assert!(
        notifications.unanswered().is_empty(),
        "SQL's own unknown, not an absent cell"
    );
    assert!(notifications.inserted().is_empty());
}

/// Only a subscription whose predicate actually reads the absent cell is
/// reported. One reading another column is answered as usual, which is the
/// same per-subscription precision the arithmetic failures have.
#[test]
fn only_the_subscription_that_reads_the_absent_cell_is_reported() {
    let (mut engine, table) = engine();
    let reads_body = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM docs WHERE body = 'keep'",
        ))
        .expect("registers")
        .subscription_id;
    engine
        .register(SubscriptionRequest::new(
            2u64,
            "SELECT * FROM docs WHERE tag = 't'",
        ))
        .expect("registers");

    let notifications = engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::Missing, Value::String("t".into())],
        ))
        .expect("dispatch succeeds");

    assert_eq!(
        notifications.inserted(),
        &[2],
        "the subscription reading a present cell is answered"
    );
    assert_eq!(
        notifications
            .unanswered()
            .iter()
            .map(|entry| entry.subscription_id)
            .collect::<Vec<_>>(),
        vec![reads_body],
        "and only the one reading the absent cell is reported"
    );
}

/// A short-circuit decides it, as it does for the arithmetic failures: if
/// the predicate answers before reaching the absent cell, there is nothing
/// to report.
#[test]
fn a_short_circuit_before_the_absent_cell_reports_nothing() {
    let (mut engine, table) = engine();
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM docs WHERE tag = 't' OR body = 'keep'",
        ))
        .expect("registers");

    let notifications = engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::Missing, Value::String("t".into())],
        ))
        .expect("dispatch succeeds");
    assert_eq!(
        notifications.inserted(),
        &[1],
        "the first disjunct held, so the absent cell was never read"
    );
    assert!(notifications.unanswered().is_empty());
}

/// A decisive answer is an answer, even when the absent cell was read.
/// `body = 'x' AND tag = 'no'` reads the absent `body`, gets unknown for
/// that conjunct, and SQL's three-valued logic settles the whole predicate
/// as false because the second conjunct is false. Nothing a re-read could
/// tell the caller, so nothing is reported.
#[test]
fn a_decisive_false_after_the_absent_cell_reports_nothing() {
    let (mut engine, table) = engine();
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM docs WHERE body = 'keep' AND tag = 'no'",
        ))
        .expect("registers");

    let notifications = engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::Missing, Value::String("t".into())],
        ))
        .expect("dispatch succeeds");
    assert!(
        notifications.unanswered().is_empty(),
        "unknown AND false is false, which is decided: {:?}",
        notifications.unanswered()
    );
    assert!(notifications.inserted().is_empty());
}

/// A membership term's columns are read outside the VM, so an absent one is
/// invisible to the VM's own record of it and has to be carried separately.
/// The term cannot say who the row admits, the predicate is unknown, and
/// the subscription is reported rather than silently dropped.
#[cfg(feature = "membership-term")]
#[test]
fn an_absent_membership_term_column_is_reported() {
    use rls2fga::translator::TranslatorBuilder;
    use rls2fga::types::ConfidenceLevel;

    const DDL_TERM: &str = "CREATE TABLE projects(id INTEGER PRIMARY KEY, name TEXT);
         CREATE TABLE project_members(project_id INTEGER REFERENCES projects(id), \
         user_id TEXT, PRIMARY KEY(project_id, user_id));
         CREATE TABLE notes(id INTEGER PRIMARY KEY, project_id INTEGER, title TEXT);";
    const TERM: &str = "SELECT * FROM notes WHERE project_id IN \
         (SELECT project_id FROM project_members \
          WHERE user_id = current_setting('app.user_id', true))";

    let db = ParserDB::parse::<PostgreSqlDialect>(DDL_TERM).expect("DDL parses");
    let notes = catalog_helpers::table_id(&db, "notes").expect("notes is in the catalog");
    let project_id =
        catalog_helpers::column_id(&db, notes, "project_id").expect("project_id resolves");
    let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(
        TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build(),
    );
    let subscription = engine
        .register(
            SubscriptionRequest::new(1u64, TERM)
                .subscriber(Value::String("alice".into()))
                .term_values(vec!["project_id"], vec![vec![Value::Int(7)]]),
        )
        .expect("the term registers")
        .subscription_id;

    // The event does not carry `project_id`, which is the term's own column.
    let notifications = engine
        .consumers(&TestEvent::insert(
            notes,
            vec![Value::Int(1), Value::Missing, Value::String("t".into())],
        ))
        .expect("dispatch succeeds");

    assert_eq!(
        notifications
            .unanswered()
            .iter()
            .map(|entry| (entry.subscription_id, entry.column))
            .collect::<Vec<_>>(),
        vec![(subscription, project_id)],
        "the term cannot say who the row admits, so the answer is missing"
    );
    assert!(notifications.inserted().is_empty());
}

/// An update whose row image omits the cell is reported too, which is the
/// shape the production case takes: PostgreSQL omits an unchanged TOASTed
/// column from an update, never from an insert.
///
/// A transition needs both versions, so a subscription that cannot be
/// answered on either one is reported and left out of the transition
/// rather than counted as having stopped matching.
#[test]
fn an_update_whose_image_omits_the_cell_is_reported() {
    let (mut engine, table) = engine();
    let subscription = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM docs WHERE body = 'keep'",
        ))
        .expect("the predicate registers")
        .subscription_id;

    let old = vec![
        Value::Int(1),
        Value::String("keep".into()),
        Value::String("before".into()),
    ];
    let new = vec![Value::Int(1), Value::Missing, Value::String("after".into())];
    let notifications = engine
        .consumers(&TestEvent::update(table, old, new))
        .expect("dispatch succeeds");

    assert_eq!(
        notifications
            .unanswered()
            .iter()
            .map(|entry| (entry.subscription_id, entry.column))
            .collect::<Vec<_>>(),
        vec![(subscription, 1u16)],
        "the omitted cell is reported for the update"
    );
    assert!(
        notifications.deleted().is_empty() && notifications.updated().is_empty(),
        "and the subscriber is not told the row stopped matching"
    );
}

/// The production case, end to end on a real server: PostgreSQL omits an
/// unchanged TOASTed column from a logical replication message, so a
/// subscription whose predicate reads that column must be reported rather
/// than silently not notified.
///
/// `REPLICA IDENTITY FULL` is set deliberately. It is the strongest
/// identity available and still does not restore the column, which is why
/// the replica-identity audit cannot cover this case.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn unchanged_toast_does_not_drop_a_subscription() {
    use diesel::{sql_query, RunQueryDsl};
    use subql::{CdcSource, PgStreamingCdcSource, PgStreamingConfig};

    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    sql_query("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, tag TEXT)")
        .execute(&mut setup)
        .expect("create table");
    // The strongest identity the server offers, so the omission below is
    // not an identity gap.
    sql_query("ALTER TABLE docs REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    // Stored out of line, and stored uncompressed so the value really goes
    // to the TOAST table rather than being compressed inline.
    sql_query("ALTER TABLE docs ALTER COLUMN body SET STORAGE EXTERNAL")
        .execute(&mut setup)
        .expect("external storage");

    let publication = "subql_toast_pub";
    common::create_publication(&mut setup, publication, "docs");
    let slot = "subql_toast_slot";
    common::create_pgoutput_slot(&mut setup, slot);

    // Past the roughly two-kilobyte threshold, so the value is stored out
    // of line, while still short enough to spell in a predicate.
    let body = "x".repeat(3_000);
    sql_query("INSERT INTO docs VALUES (1, repeat('x', 3000), 'before')")
        .execute(&mut dml)
        .expect("insert the toasted row");

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&catalog, "docs").expect("docs is cataloged");
    let column = catalog_helpers::column_id(&catalog, table, "body").expect("body is cataloged");
    let config = PgStreamingConfig::new(common::pg_replication_url(port), slot, publication);

    let mut engine: SubscriptionEngine<_, DefaultIds, ParserDB> = SubscriptionEngine::new(
        ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap(),
        PostgreSqlDialect {},
    );
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            // Matches the stored row, so before this phase the omission
            // was a silent no-match on a row the database returns.
            format!("SELECT * FROM docs WHERE body = '{body}'"),
        ))
        .expect("the predicate registers in process");
    assert!(
        registered.served().is_some(),
        "the predicate is served in process, which is what makes the omission observable"
    );

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime")
        .block_on(async move {
            let mut source = PgStreamingCdcSource::connect(config, catalog)
                .await
                .expect("connect to the slot");

            // Touches `tag` only, so `body` is unchanged and PostgreSQL
            // omits it from the update message.
            sql_query("UPDATE docs SET tag = 'after' WHERE id = 1")
                .execute(&mut dml)
                .expect("update the unrelated column");

            let mut update = None;
            while update.is_none() {
                let event =
                    tokio::time::timeout(core::time::Duration::from_secs(30), source.next_event())
                        .await
                        .expect("next_event timed out")
                        .expect("next_event failed")
                        .expect("the slot closed before the update arrived");
                if event.kind() == subql::EventKind::Update {
                    update = Some(event);
                }
            }
            let update = update.expect("the update arrived");

            let notifications = engine.consumers(&update).expect("dispatch succeeds");
            assert!(
                notifications.updated().is_empty() && notifications.inserted().is_empty(),
                "the predicate cannot be answered, so nothing is notified"
            );
            let unanswered = notifications.unanswered();
            assert_eq!(
                unanswered.len(),
                1,
                "the one subscription reading the omitted column is reported, got {unanswered:?}"
            );
            assert_eq!(unanswered[0].subscription_id, registered.subscription_id);
            assert_eq!(
                unanswered[0].column, column,
                "the report names the column the stream did not carry"
            );
        });
}

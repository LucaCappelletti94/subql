//! Answers that need a database read survive a restart, come back not knowing
//! their value, and are dropped one at a time when their table moves.
#![allow(clippy::unwrap_used)]

use core::convert::Infallible;

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, ScalarFamily, Value};
use subql::reexec::{
    Connector, CursorError, CursorId, ReExecutionRead, ReadQuery, RowPage, Snapshot,
};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, DropReason, GroupedScalarSeedInstall, Install, NoCheckpoint,
    SubscriptionEngine, SubscriptionRequest, Tier,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);\
                   CREATE TABLE managers (id INT PRIMARY KEY);";

/// The same catalog with one more column on `orders`, which is what a schema
/// change looks like to the loader.
const WIDER_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT, \
                         note TEXT);\
                         CREATE TABLE managers (id INT PRIMARY KEY);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

#[derive(Default)]
struct RecordingConnector {
    queries: parking_lot::Mutex<Vec<ReadQuery<'static, Postgres>>>,
}

impl RecordingConnector {
    fn queries(&self) -> Vec<ReadQuery<'static, Postgres>> {
        self.queries.lock().clone()
    }

    fn record(&self, query: &ReadQuery<'_, Postgres>) {
        self.queries.lock().push(query.clone().into_owned());
    }
}

impl Connector for RecordingConnector {
    type AuthContext = ();
    type Error = Infallible;
    type Checkpoint = NoCheckpoint;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        query: &ReadQuery<'_, Postgres>,
        _kind: ScalarFamily,
        _auth: &(),
    ) -> Result<(Value<Postgres>, Option<NoCheckpoint>), Self::Error> {
        self.record(query);
        Ok((Value::Null, None))
    }

    fn read_page(
        &self,
        query: &ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _auth: &(),
    ) -> Result<Snapshot<RowPage<Postgres>, NoCheckpoint>, Self::Error> {
        self.record(query);
        Ok(Snapshot {
            value: RowPage {
                columns: Vec::new(),
                rows: Vec::new(),
                more: false,
            },
            checkpoint: None,
        })
    }

    fn open_cursor(
        &self,
        query: &ReadQuery<'_, Postgres>,
        _auth: &(),
    ) -> Result<CursorId, CursorError<Self::Error>> {
        self.record(query);
        Err(CursorError::Unsupported)
    }
}

fn restore_bound_reads() -> (subql::RestoredReads, [u64; 4]) {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().to_path_buf();
    let (mut engine, _) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path.clone()).expect("open store");
    let scalar = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders WHERE status = $1")
                .binds(vec![Value::String("paid".into())]),
        )
        .expect("scalar registers");
    let keyed = engine
        .register(
            SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE lower(status) = $1")
                .binds(vec![Value::String("paid".into())]),
        )
        .expect("keyed read registers");
    let grouped = engine
        .register(
            SubscriptionRequest::new(
                3u64,
                "SELECT status, MIN(price) FROM orders WHERE price > $1 GROUP BY status",
            )
            .binds(vec![Value::Float(1.0)]),
        )
        .expect("grouped read registers");
    let whole = engine
        .register(
            SubscriptionRequest::new(
                4u64,
                "SELECT * FROM orders WHERE lower(status) = $1 AND id IN (SELECT id FROM managers)",
            )
            .binds(vec![Value::String("paid".into())]),
        )
        .expect("whole read registers");
    let ids = [
        scalar.subscription_id,
        keyed.subscription_id,
        grouped.subscription_id,
        whole.subscription_id,
    ];
    drop(engine);

    let (_engine, report) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path).expect("reopen store");
    (report, ids)
}

fn catalog(ddl: &str) -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(ddl).expect("parse DDL")
}

const EXTREME: &str = "SELECT MIN(price) FROM orders";
const KEYED: &str = "SELECT * FROM orders WHERE lower(status) = 'paid'";

/// Register both re-read shapes into a fresh store, then restore from it.
fn save_then_restore(restore_ddl: &str) -> (subql::RestoredReads, Engine) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();

    let (mut engine, first) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path.clone()).expect("open store");
    assert!(
        first.restored.is_empty() && first.dropped.is_empty(),
        "an empty store restores nothing"
    );
    engine
        .register(SubscriptionRequest::new(1u64, EXTREME))
        .expect("extreme registers");
    engine
        .register(SubscriptionRequest::new(2u64, KEYED))
        .expect("keyed registers");
    assert_eq!(engine.reread_count(), 2);
    drop(engine);

    let (restored_engine, report) =
        Engine::with_storage(catalog(restore_ddl), PostgreSqlDialect {}, path)
            .expect("reopen store");
    // The directory has to outlive the reopen, so it is dropped here.
    drop(dir);
    (report, restored_engine)
}

/// Both come back, keeping their identities and their tiers.
#[test]
fn saved_answers_come_back_with_their_identities() {
    let (report, engine) = save_then_restore(DDL);

    assert!(
        report.dropped.is_empty(),
        "the schema did not move, so nothing is dropped: {:?}",
        report.dropped
    );
    assert_eq!(engine.reread_count(), 2, "both are live again");

    let mut restored: Vec<(u64, bool)> = report
        .restored
        .iter()
        .map(|r| (r.subscription_id, r.tier_changed))
        .collect();
    restored.sort_unstable();
    assert_eq!(
        restored,
        vec![(1, false), (2, false)],
        "the identities and tiers are the ones that were saved"
    );

    let tiers: Vec<&Tier> = report.restored.iter().map(|r| &r.tier).collect();
    assert!(
        tiers.iter().any(|t| matches!(t, Tier::Scalar { .. }))
            && tiers.iter().any(|t| matches!(t, Tier::KeyedRows { .. })),
        "one of each, got {tiers:?}"
    );
}

/// A per-consumer read on a row-secured table registers, so it must also
/// restore: per-consumer re-execution is exactly the mode that stays safe
/// under row-level security, and dropping it on restart silently
/// unsubscribes the consumer. Restore applies the same guard registration
/// applies, on every plan shape.
#[test]
fn per_consumer_reads_on_a_row_secured_table_survive_a_restart() {
    let rls_ddl = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);\
                   ALTER TABLE orders ENABLE ROW LEVEL SECURITY;\
                   CREATE TABLE managers (id INT PRIMARY KEY);";
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();

    let (mut engine, _) =
        Engine::with_storage(catalog(rls_ddl), PostgreSqlDialect {}, path.clone())
            .expect("open store");
    let scalar = engine
        .register(SubscriptionRequest::new(1u64, EXTREME).database_reads_per_consumer())
        .expect("a per-consumer scalar registers on the row-secured table");
    let grouped = engine
        .register(
            SubscriptionRequest::new(
                2u64,
                "SELECT status, MIN(price) FROM orders GROUP BY status",
            )
            .database_reads_per_consumer(),
        )
        .expect("a per-consumer grouped read registers on the row-secured table");
    drop(engine);

    let (restored_engine, report) =
        Engine::with_storage(catalog(rls_ddl), PostgreSqlDialect {}, path).expect("reopen store");
    drop(dir);
    assert!(
        report.dropped.is_empty(),
        "what registration accepted, restore must not drop: {:?}",
        report.dropped
    );
    let mut restored: Vec<u64> = report.restored.iter().map(|r| r.subscription_id).collect();
    restored.sort_unstable();
    assert_eq!(
        restored,
        vec![scalar.subscription_id, grouped.subscription_id],
        "both per-consumer reads come back"
    );
    assert_eq!(restored_engine.reread_count(), 2, "both are live again");
}

#[test]
fn restored_fixed_tiers_report_exact_executable_queries() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().to_path_buf();
    let (mut engine, _) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path.clone()).expect("open store");
    let bind = Value::String("paid".into());
    let scalar = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders WHERE status = $1")
                .binds(vec![bind.clone()]),
        )
        .expect("scalar registers");
    let keyed = engine
        .register(
            SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE lower(status) = $1")
                .binds(vec![bind.clone()]),
        )
        .expect("keyed read registers");
    let whole_sql = "SELECT * FROM orders WHERE lower(status) = $1 \
                     AND id IN (SELECT id FROM managers)";
    let whole = engine
        .register(SubscriptionRequest::new(3u64, whole_sql).binds(vec![bind.clone()]))
        .expect("whole read registers");
    drop(engine);

    let (_engine, report) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path).expect("reopen store");
    let expected = [
        (
            scalar.subscription_id,
            "SELECT MIN(price) AS v FROM orders WHERE status = $1",
        ),
        (
            keyed.subscription_id,
            "SELECT * FROM orders WHERE lower(status) = $1",
        ),
        (whole.subscription_id, whole_sql),
    ];
    for (subscription_id, sql) in expected {
        let restored = report
            .restored
            .iter()
            .find(|read| read.subscription_id == subscription_id)
            .expect("registered read restores");
        let query = match &restored.tier {
            Tier::Scalar { query, .. }
            | Tier::KeyedRows { query, .. }
            | Tier::WholeRows { query, .. } => query,
            other => panic!("expected fixed read tier, got {other:?}"),
        };
        assert_eq!(query.sql(), sql);
        assert_eq!(query.binds(), std::slice::from_ref(&bind));
    }
}

#[test]
fn scalar_restores_and_executes_registration_binds() {
    let (report, [scalar, _, _, _]) = restore_bound_reads();
    let restored = report
        .restored
        .iter()
        .find(|read| read.subscription_id == scalar)
        .expect("scalar restores");
    let Tier::Scalar { query, column_kind } = &restored.tier else {
        panic!("expected scalar")
    };
    let connector = RecordingConnector::default();

    connector
        .execute_scalar(&query.as_read_query(), *column_kind, &())
        .expect("connector is infallible");
    let queries = connector.queries();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT MIN(price) AS v FROM orders WHERE status = $1"
    );
    assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
}

#[test]
fn keyed_restores_and_executes_registration_binds() {
    let (report, [_, keyed, _, _]) = restore_bound_reads();
    let restored = report
        .restored
        .iter()
        .find(|read| read.subscription_id == keyed)
        .expect("keyed read restores");
    let Tier::KeyedRows { query, .. } = &restored.tier else {
        panic!("expected keyed rows")
    };
    let connector = RecordingConnector::default();

    let _ = connector.open_cursor(&query.as_read_query(), &());
    let queries = connector.queries();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT * FROM orders WHERE lower(status) = $1"
    );
    assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
}

#[test]
fn grouped_restores_and_executes_registration_binds() {
    let (report, [_, _, grouped, _]) = restore_bound_reads();
    let restored = report
        .restored
        .iter()
        .find(|read| read.subscription_id == grouped)
        .expect("grouped read restores");
    let Tier::GroupedScalar { bootstrap } = &restored.tier else {
        panic!("expected grouped scalar")
    };
    let connector = RecordingConnector::default();

    let _ = connector.open_cursor(&bootstrap.query.as_read_query(), &());
    let queries = connector.queries();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT \"status\", MIN(\"price\") AS c0, COUNT(*) AS c1 FROM orders WHERE price > $1 GROUP BY status"
    );
    assert_eq!(queries[0].binds(), &[Value::Float(1.0)]);
}

#[test]
fn whole_restores_and_executes_registration_binds() {
    let (report, [_, _, _, whole]) = restore_bound_reads();
    let restored = report
        .restored
        .iter()
        .find(|read| read.subscription_id == whole)
        .expect("whole read restores");
    let Tier::WholeRows { query, .. } = &restored.tier else {
        panic!("expected whole rows")
    };
    let connector = RecordingConnector::default();

    let _ = connector.open_cursor(&query.as_read_query(), &());
    let queries = connector.queries();
    assert_eq!(queries.len(), 1);
    assert_eq!(
        queries[0].sql(),
        "SELECT * FROM orders WHERE lower(status) = $1 AND id IN (SELECT id FROM managers)"
    );
    assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
}

/// Restored identities remain taken: the next registration must not reuse one.
#[test]
fn a_new_answer_after_restore_gets_a_new_identity() {
    let (_report, mut engine) = save_then_restore(DDL);
    let next = engine
        .register(SubscriptionRequest::new(
            3u64,
            "SELECT * FROM managers WHERE id > 0",
        ))
        .expect("a new subscription registers");
    assert!(
        next.subscription_id > 2,
        "1 and 2 came back from disk, so the counter must pass both, got {}",
        next.subscription_id
    );
}

/// A restored answer knows nothing yet, so its first change asks for a read
/// rather than reporting a value it never had.
#[test]
fn a_restored_answer_asks_before_it_answers() {
    let (report, engine) = save_then_restore(DDL);
    let extreme = report
        .restored
        .iter()
        .find(|r| matches!(r.tier, Tier::Scalar { .. }))
        .expect("the extreme came back");

    let table = catalog_helpers::table_id(&catalog(DDL), "orders").expect("orders resolves");
    let event = TestEvent::<Postgres>::insert(
        table,
        vec![
            subql::backend::Value::Int(1),
            subql::backend::Value::Float(5.0),
            subql::backend::Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16]);

    let mut engine = engine;
    let notifications = engine.dispatch(&event).expect("dispatch");

    assert!(
        notifications.scalar_updates().is_empty(),
        "a restored answer has no value to report, got {:?}",
        notifications.scalar_updates()
    );
    assert!(
        notifications
            .triggers()
            .iter()
            .any(|t| t.subscription_id == extreme.subscription_id),
        "it asks for the read that fills it in"
    );
}

/// A column added to `orders` drops the answers reading it, and says which
/// table moved. Nothing else is disturbed.
#[test]
fn an_answer_whose_table_moved_is_dropped_and_named() {
    let (report, engine) = save_then_restore(WIDER_DDL);

    assert!(
        report.restored.is_empty(),
        "both answers read `orders`, so neither fits the new shape"
    );
    assert_eq!(engine.reread_count(), 0, "and neither is live");

    let table = catalog_helpers::table_id(&catalog(WIDER_DDL), "orders").expect("orders resolves");
    let mut dropped: Vec<u64> = report.dropped.iter().map(|d| d.subscription_id).collect();
    dropped.sort_unstable();
    assert_eq!(dropped, vec![1, 2]);
    for entry in &report.dropped {
        assert_eq!(
            entry.reason,
            DropReason::TableChanged { table_id: table },
            "the reason names the table that moved"
        );
        assert!(
            entry.sql == EXTREME || entry.sql == KEYED,
            "and carries the statement, so a caller can register it again"
        );
    }
}

#[test]
fn a_grouped_scalar_read_restores_under_the_same_identity() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().to_path_buf();
    let (mut engine, _) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path.clone()).expect("open store");
    let registered = engine
        .register(SubscriptionRequest::new(
            4u64,
            "SELECT status, MIN(price) FROM orders GROUP BY status",
        ))
        .expect("grouped minimum registers");
    assert!(matches!(registered.tier, Tier::GroupedScalar { .. }));
    drop(engine);

    let (_engine, report) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path).expect("reopen store");
    assert_eq!(report.dropped, [] as [subql::DroppedRead; 0]);
    assert_eq!(report.restored.len(), 1);
    assert_eq!(
        report.restored[0].subscription_id,
        registered.subscription_id
    );
    assert!(!report.restored[0].tier_changed);
    assert!(matches!(
        report.restored[0].tier,
        Tier::GroupedScalar { .. }
    ));
}

#[test]
fn grouped_read_restores_registration_binds() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().to_path_buf();
    let (mut engine, _) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path.clone()).expect("open store");
    let registered = engine
        .register(
            SubscriptionRequest::new(
                4u64,
                "SELECT status, MIN(price) FROM orders WHERE price > $1 GROUP BY status",
            )
            .binds(vec![Value::Float(1.0)]),
        )
        .expect("grouped minimum registers");
    drop(engine);

    let (mut engine, report) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path).expect("reopen store");
    assert_eq!(report.restored.len(), 1);
    Install::install(
        &mut engine,
        registered.subscription_id,
        GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("paid".into()),
                Value::Float(5.0),
                Value::Int(2),
            ]],
            read_at: None::<subql::NoCheckpoint>,
        },
    )
    .expect("seed installs");
    let orders = catalog_helpers::table_id(&catalog(DDL), "orders").expect("orders resolves");
    let event = TestEvent::<Postgres>::delete(
        orders,
        vec![
            Value::Int(1),
            Value::Float(5.0),
            Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16]);

    let output = engine.dispatch(&event).expect("delete dispatches");
    let ReExecutionRead::GroupedScalar { query, .. } = &output.triggers()[0].read else {
        panic!("expected grouped scalar read")
    };
    assert!(query.sql().contains("price > $1"), "{query:?}");
    assert!(query.sql().contains("\"status\" = $2"));
    assert_eq!(
        query.binds(),
        &[Value::Float(1.0), Value::String("paid".into())]
    );
}

/// A grouped extreme's `HAVING` survives the restart: the restored plan
/// still installs a failing group silently.
#[test]
fn a_having_extreme_restores_with_its_condition() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().to_path_buf();
    let (mut engine, _) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path.clone()).expect("open store");
    let registered = engine
        .register(SubscriptionRequest::new(
            5u64,
            "SELECT status, MIN(price) FROM orders GROUP BY status HAVING MIN(price) < 5",
        ))
        .expect("filtered grouped minimum registers");
    drop(engine);

    let (mut engine, report) =
        Engine::with_storage(catalog(DDL), PostgreSqlDialect {}, path).expect("reopen store");
    assert_eq!(report.restored.len(), 1);
    assert!(!report.restored[0].tier_changed);
    let Tier::GroupedScalar { ref bootstrap } = report.restored[0].tier else {
        panic!("expected grouped scalar tier, got {:?}", report.restored[0]);
    };
    assert!(
        !bootstrap.query.sql().to_uppercase().contains("HAVING"),
        "the restored seed still fetches every group: {}",
        bootstrap.query.sql()
    );
    let opening = subql::Install::install(
        &mut engine,
        registered.subscription_id,
        subql::GroupedScalarSeedInstall {
            rows: vec![vec![
                subql::backend::Value::String("paid".into()),
                subql::backend::Value::Float(9.0),
                subql::backend::Value::Int(1),
            ]],
            read_at: None::<subql::NoCheckpoint>,
        },
    )
    .expect("restored plan accepts its seed");
    assert!(
        opening.updates.is_empty(),
        "minimum 9 fails the restored condition, got {:?}",
        opening.updates
    );
}

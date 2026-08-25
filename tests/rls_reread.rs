//! Row reads on an RLS table require the compile-time per-consumer request
//! type, and connector calls preserve one authorization value per subscription.
#![allow(clippy::unwrap_used)]

use core::convert::Infallible;

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{BuiltinKind, Postgres, Value};
use subql::reexec::{AutoResolvingEngine, Connector, RowPage, Snapshot, SyncMode};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, NoCheckpoint, RegisterError, SubscriptionEngine,
    SubscriptionRequest, TableId, Tier,
};

const DDL: &str = "CREATE TABLE notes (id INT PRIMARY KEY, status TEXT); \
                   ALTER TABLE notes ENABLE ROW LEVEL SECURITY;";

type Registry = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

#[derive(Default)]
struct Recording {
    calls: parking_lot::Mutex<Vec<String>>,
}

impl Connector for Recording {
    type AuthContext = String;
    type Error = Infallible;
    type Checkpoint = NoCheckpoint;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        _sql: &str,
        _kind: BuiltinKind,
        _auth: &Self::AuthContext,
    ) -> Result<(Value<Postgres>, Option<NoCheckpoint>), Self::Error> {
        unreachable!("the tests register row reads")
    }

    fn read_page(
        &self,
        _sql: &str,
        _max_bytes: usize,
        auth: &Self::AuthContext,
    ) -> Result<Snapshot<RowPage<Postgres>, NoCheckpoint>, Self::Error> {
        self.calls.lock().push(auth.clone());
        Ok(Snapshot {
            value: RowPage {
                columns: Vec::new(),
                rows: Vec::new(),
                more: false,
            },
            checkpoint: None,
        })
    }
}

fn registry() -> (Registry, TableId) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let table = catalog_helpers::table_id(&catalog, "notes").expect("notes resolves");
    (
        SubscriptionEngine::new(catalog, PostgreSqlDialect {}),
        table,
    )
}

const KEYED_SQL: &str = "SELECT * FROM notes WHERE lower(status) = 'paid'";
const WHOLE_SQL: &str = "SELECT DISTINCT status FROM notes";

#[test]
fn shared_database_reads_remain_refused_on_an_rls_table() {
    let (mut registry, table) = registry();
    for sql in [KEYED_SQL, WHOLE_SQL] {
        assert!(matches!(
            registry.register(SubscriptionRequest::new(1u64, sql)),
            Err(RegisterError::RowCaptureOnRlsTable { table_id }) if table_id == table
        ));
    }
}

#[test]
fn per_consumer_database_reads_accept_both_row_read_tiers() {
    let (mut registry, _) = registry();
    let keyed = registry
        .register(SubscriptionRequest::new(1u64, KEYED_SQL).database_reads_per_consumer())
        .expect("keyed read is isolated per consumer");
    assert!(matches!(keyed.tier, Tier::KeyedRows { .. }));

    let whole = registry
        .register(SubscriptionRequest::new(2u64, WHOLE_SQL).database_reads_per_consumer())
        .expect("whole read is isolated per consumer");
    assert!(matches!(whole.tier, Tier::WholeRows { .. }));
}

#[test]
fn two_consumers_keep_separate_connector_authorization_values() {
    let (registry, table) = registry();
    let mut engine = AutoResolvingEngine::new(registry, SyncMode(Recording::default()));
    let alice = engine
        .register(
            SubscriptionRequest::new(1u64, KEYED_SQL).database_reads_per_consumer(),
            String::from("alice"),
        )
        .expect("alice registers");
    let bob = engine
        .register(
            SubscriptionRequest::new(2u64, KEYED_SQL).database_reads_per_consumer(),
            String::from("bob"),
        )
        .expect("bob registers");
    assert_ne!(alice.subscription_id, bob.subscription_id);

    let event =
        TestEvent::<Postgres>::insert(table, vec![Value::Int(1), Value::String("paid".into())])
            .with_pk_columns([0u16]);
    let output = engine.consumers(&event).expect("both reads resolve");
    assert!(
        output.triggers.is_empty(),
        "connector mode consumes triggers"
    );

    let mut calls = engine.connector().calls.lock().clone();
    calls.sort();
    assert_eq!(calls, vec!["alice", "bob"]);
}

#[test]
fn per_consumer_marker_does_not_make_in_process_aggregates_safe() {
    let (mut registry, table) = registry();
    let result = registry.register(
        SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM notes").database_reads_per_consumer(),
    );
    assert!(matches!(
        result,
        Err(RegisterError::AggregatorOnRlsTable { table_id }) if table_id == table
    ));
}

#[test]
fn per_consumer_marker_does_not_make_grouped_extrema_safe() {
    let (mut registry, table) = registry();
    let result = registry.register(
        SubscriptionRequest::new(1u64, "SELECT status, MIN(id) FROM notes GROUP BY status")
            .database_reads_per_consumer(),
    );
    assert!(matches!(
        result,
        Err(RegisterError::AggregatorOnRlsTable { table_id }) if table_id == table
    ));
}

#[test]
fn per_consumer_read_scope_survives_persistence() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let (mut registry, first) =
        Registry::with_storage(catalog, PostgreSqlDialect {}, path.clone()).expect("open store");
    assert!(first.restored.is_empty());
    let registered = registry
        .register(SubscriptionRequest::new(1u64, KEYED_SQL).database_reads_per_consumer())
        .expect("per-consumer read registers");
    drop(registry);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let (restored, report) =
        Registry::with_storage(catalog, PostgreSqlDialect {}, path).expect("restore store");
    assert!(report.dropped.is_empty(), "{:?}", report.dropped);
    assert_eq!(restored.reread_count(), 1);
    assert_eq!(
        report.restored[0].subscription_id,
        registered.subscription_id
    );
}

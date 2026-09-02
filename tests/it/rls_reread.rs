//! Row reads on an RLS table require the compile-time per-consumer request
//! type, and connector calls preserve one authorization value per subscription.
#![allow(clippy::unwrap_used)]

use core::convert::Infallible;
use hashbrown::HashMap;

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{BuiltinKind, Postgres, Value};
use subql::reexec::{
    AsyncConnector, AsyncMode, AutoResolvingEngine, Connector, CursorError, CursorId,
    ReExecutionRead, RowPage, Snapshot, SyncMode,
};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggregateResultValue, AggregateValueChange, DefaultIds, GroupedScalarInstall,
    GroupedScalarSeedInstall, Install, NoCheckpoint, RegisterError, ScalarInstall,
    SubscriptionEngine, SubscriptionRequest, TableId, Tier,
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
        _query: &subql::reexec::ReadQuery<'_, Postgres>,
        _kind: BuiltinKind,
        _auth: &Self::AuthContext,
    ) -> Result<(Value<Postgres>, Option<NoCheckpoint>), Self::Error> {
        unreachable!("the tests register row reads")
    }

    fn read_page(
        &self,
        _query: &subql::reexec::ReadQuery<'_, Postgres>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RecordedOperation {
    Scalar,
    Page,
    Cursor,
}

#[derive(Default)]
struct AggregateRecordingState {
    calls: Vec<(RecordedOperation, String)>,
    bound_calls: Vec<(RecordedOperation, String, Vec<Value<Postgres>>)>,
    scalar_runs: HashMap<String, usize>,
    count_runs: HashMap<String, usize>,
    next_cursor: u64,
    cursors: HashMap<CursorId, RowPage<Postgres>>,
}

impl AggregateRecordingState {
    fn calls(&self) -> Vec<(RecordedOperation, String)> {
        self.calls.clone()
    }

    fn bound_calls(&self) -> Vec<(RecordedOperation, String, Vec<Value<Postgres>>)> {
        self.bound_calls.clone()
    }

    fn clear_calls(&mut self) {
        self.calls.clear();
        self.bound_calls.clear();
    }

    fn scalar_answer(
        &mut self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        auth: &str,
    ) -> Value<Postgres> {
        let run = self.scalar_runs.entry(auth.to_string()).or_default();
        let value = match (auth, *run) {
            ("alice", 0) => 5,
            ("alice", _) => 3,
            ("bob", 0) => 8,
            ("bob", _) => 4,
            _ => 0,
        };
        *run += 1;
        self.calls
            .push((RecordedOperation::Scalar, auth.to_string()));
        self.bound_calls.push((
            RecordedOperation::Scalar,
            auth.to_string(),
            query.binds().to_vec(),
        ));
        Value::Int(value)
    }

    fn page_answer(
        &mut self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        auth: &str,
    ) -> RowPage<Postgres> {
        self.calls.push((RecordedOperation::Page, auth.to_string()));
        self.bound_calls.push((
            RecordedOperation::Page,
            auth.to_string(),
            query.binds().to_vec(),
        ));
        let rows = if query.sql().contains("MIN") {
            let value = if auth == "alice" { 3 } else { 4 };
            vec![vec![Value::Int(value), Value::Int(1)]]
        } else {
            Vec::new()
        };
        RowPage {
            columns: Vec::new(),
            rows,
            more: false,
        }
    }

    fn open_cursor(
        &mut self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        auth: &str,
    ) -> CursorId {
        self.calls
            .push((RecordedOperation::Cursor, auth.to_string()));
        self.bound_calls.push((
            RecordedOperation::Cursor,
            auth.to_string(),
            query.binds().to_vec(),
        ));
        let row = if query.sql().contains("GROUP BY") {
            let value = if auth == "alice" { 5 } else { 8 };
            vec![
                Value::String("paid".into()),
                Value::Int(value),
                Value::Int(1),
            ]
        } else {
            let run = self.count_runs.entry(auth.to_string()).or_default();
            let value = match (auth, *run) {
                ("alice", 0) => 1,
                ("alice", _) => 2,
                ("bob", 0) => 6,
                ("bob", _) => 7,
                _ => 0,
            };
            *run += 1;
            vec![Value::Int(value)]
        };
        self.next_cursor += 1;
        let cursor = CursorId(self.next_cursor);
        self.cursors.insert(
            cursor,
            RowPage {
                columns: Vec::new(),
                rows: vec![row],
                more: false,
            },
        );
        cursor
    }

    fn fetch_cursor(
        &mut self,
        cursor: CursorId,
    ) -> Result<RowPage<Postgres>, CursorError<Infallible>> {
        self.cursors
            .remove(&cursor)
            .ok_or(CursorError::Unknown(cursor))
    }

    fn close_cursor(&mut self, cursor: CursorId) {
        self.cursors.remove(&cursor);
    }
}

#[derive(Default)]
struct AggregateRecording {
    state: parking_lot::Mutex<AggregateRecordingState>,
}

impl AggregateRecording {
    fn calls(&self) -> Vec<(RecordedOperation, String)> {
        self.state.lock().calls()
    }

    fn bound_calls(&self) -> Vec<(RecordedOperation, String, Vec<Value<Postgres>>)> {
        self.state.lock().bound_calls()
    }

    fn clear_calls(&self) {
        self.state.lock().clear_calls();
    }
}

impl Connector for AggregateRecording {
    type AuthContext = String;
    type Error = Infallible;
    type Checkpoint = NoCheckpoint;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        _kind: BuiltinKind,
        auth: &Self::AuthContext,
    ) -> Result<(Value<Postgres>, Option<NoCheckpoint>), Self::Error> {
        Ok((self.state.lock().scalar_answer(query, auth), None))
    }

    fn read_page(
        &self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        auth: &Self::AuthContext,
    ) -> Result<Snapshot<RowPage<Postgres>, NoCheckpoint>, Self::Error> {
        Ok(Snapshot {
            value: self.state.lock().page_answer(query, auth),
            checkpoint: None,
        })
    }

    fn open_cursor(
        &self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        auth: &Self::AuthContext,
    ) -> Result<CursorId, CursorError<Self::Error>> {
        Ok(self.state.lock().open_cursor(query, auth))
    }

    fn fetch_cursor(
        &self,
        cursor: CursorId,
        _max_bytes: usize,
    ) -> Result<Snapshot<RowPage<Postgres>, NoCheckpoint>, CursorError<Self::Error>> {
        Ok(Snapshot {
            value: self.state.lock().fetch_cursor(cursor)?,
            checkpoint: None,
        })
    }

    fn close_cursor(&self, cursor: CursorId) -> Result<(), CursorError<Self::Error>> {
        self.state.lock().close_cursor(cursor);
        Ok(())
    }
}

#[derive(Default)]
struct AsyncAggregateRecording {
    state: parking_lot::Mutex<AggregateRecordingState>,
}

impl AsyncAggregateRecording {
    fn calls(&self) -> Vec<(RecordedOperation, String)> {
        self.state.lock().calls()
    }

    fn bound_calls(&self) -> Vec<(RecordedOperation, String, Vec<Value<Postgres>>)> {
        self.state.lock().bound_calls()
    }

    fn clear_calls(&self) {
        self.state.lock().clear_calls();
    }
}

impl AsyncConnector for AsyncAggregateRecording {
    type AuthContext = String;
    type Error = Infallible;
    type Checkpoint = NoCheckpoint;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        _kind: BuiltinKind,
        auth: &Self::AuthContext,
    ) -> impl core::future::Future<
        Output = Result<(Value<Postgres>, Option<NoCheckpoint>), Self::Error>,
    > + Send {
        let answer = (self.state.lock().scalar_answer(query, auth), None);
        core::future::ready(Ok(answer))
    }

    fn read_page(
        &self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        auth: &Self::AuthContext,
    ) -> impl core::future::Future<
        Output = Result<Snapshot<RowPage<Postgres>, NoCheckpoint>, Self::Error>,
    > + Send {
        let answer = Snapshot {
            value: self.state.lock().page_answer(query, auth),
            checkpoint: None,
        };
        core::future::ready(Ok(answer))
    }

    fn open_cursor(
        &self,
        query: &subql::reexec::ReadQuery<'_, Postgres>,
        auth: &Self::AuthContext,
    ) -> impl core::future::Future<Output = Result<CursorId, CursorError<Self::Error>>> + Send {
        let cursor = self.state.lock().open_cursor(query, auth);
        core::future::ready(Ok(cursor))
    }

    fn fetch_cursor(
        &self,
        cursor: CursorId,
        _max_bytes: usize,
    ) -> impl core::future::Future<
        Output = Result<Snapshot<RowPage<Postgres>, NoCheckpoint>, CursorError<Self::Error>>,
    > + Send {
        let answer = self
            .state
            .lock()
            .fetch_cursor(cursor)
            .map(|value| Snapshot {
                value,
                checkpoint: None,
            });
        core::future::ready(answer)
    }

    fn close_cursor(
        &self,
        cursor: CursorId,
    ) -> impl core::future::Future<Output = Result<(), CursorError<Self::Error>>> + Send {
        self.state.lock().close_cursor(cursor);
        core::future::ready(Ok(()))
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

type SyncAggregateEngine =
    AutoResolvingEngine<TestEvent<Postgres>, DefaultIds, ParserDB, SyncMode<AggregateRecording>>;
type AsyncAggregateEngine = AutoResolvingEngine<
    TestEvent<Postgres>,
    DefaultIds,
    ParserDB,
    AsyncMode<AsyncAggregateRecording>,
>;

fn sync_aggregate_engine() -> (SyncAggregateEngine, TableId) {
    let (registry, table) = registry();
    (
        AutoResolvingEngine::new(registry, SyncMode(AggregateRecording::default())),
        table,
    )
}

fn register_aggregate_pair(
    engine: &mut SyncAggregateEngine,
    sql: &str,
) -> (subql::Registered, subql::Registered) {
    let alice = engine
        .register(
            SubscriptionRequest::new(1u64, sql).database_reads_per_consumer(),
            String::from("alice"),
        )
        .expect("alice registers");
    let bob = engine
        .register(
            SubscriptionRequest::new(2u64, sql).database_reads_per_consumer(),
            String::from("bob"),
        )
        .expect("bob registers");
    assert_ne!(alice.subscription_id, bob.subscription_id);
    (alice, bob)
}

fn snapshot_pair(
    engine: &mut SyncAggregateEngine,
    alice: subql::SubscriptionId,
    bob: subql::SubscriptionId,
) {
    assert!(engine
        .snapshot(alice)
        .expect("alice snapshot resolves")
        .is_some());
    assert!(engine
        .snapshot(bob)
        .expect("bob snapshot resolves")
        .is_some());
    engine.connector().clear_calls();
}

fn assert_recorded_pair(mut calls: Vec<(RecordedOperation, String)>, operation: RecordedOperation) {
    calls.sort();
    assert_eq!(
        calls,
        vec![
            (operation, String::from("alice")),
            (operation, String::from("bob"))
        ]
    );
}

fn assert_sync_scalar_authorization() {
    let (mut engine, table) = sync_aggregate_engine();
    let (alice, bob) = register_aggregate_pair(&mut engine, "SELECT MIN(id) FROM notes");
    snapshot_pair(&mut engine, alice.subscription_id, bob.subscription_id);

    let event =
        TestEvent::<Postgres>::insert(table, vec![Value::Int(3), Value::String("paid".into())])
            .with_pk_columns([0u16]);
    engine.apply(&event).expect("apply");
    let mut updates = engine
        .resolve_collect()
        .expect("scalar reads resolve")
        .scalar_updates;
    updates.sort_by_key(|update| update.consumer_id);

    assert_recorded_pair(engine.connector().calls(), RecordedOperation::Scalar);
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].subscription_id, alice.subscription_id);
    assert_eq!(updates[0].consumer_id, 1);
    assert_eq!(updates[0].value, Value::Int(3));
    assert_eq!(updates[1].subscription_id, bob.subscription_id);
    assert_eq!(updates[1].consumer_id, 2);
    assert_eq!(updates[1].value, Value::Int(4));
}

fn assert_sync_grouped_authorization() {
    let (mut engine, table) = sync_aggregate_engine();
    let (alice, bob) = register_aggregate_pair(
        &mut engine,
        "SELECT status, MIN(id) FROM notes GROUP BY status",
    );
    snapshot_pair(&mut engine, alice.subscription_id, bob.subscription_id);

    let event =
        TestEvent::<Postgres>::insert(table, vec![Value::Int(3), Value::String("paid".into())])
            .with_pk_columns([0u16]);
    engine.apply(&event).expect("apply");
    let mut updates = engine
        .resolve_collect()
        .expect("grouped reads resolve")
        .aggregate_updates;
    updates.sort_by_key(|update| update.consumer);

    assert_recorded_pair(engine.connector().calls(), RecordedOperation::Page);
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].subscription, alice.subscription_id);
    assert_eq!(updates[0].consumer, 1);
    assert_eq!(
        updates[0]
            .group
            .as_ref()
            .expect("grouped update carries identity")
            .values,
        vec![Value::String("paid".into())]
    );
    assert_eq!(
        updates[0].change,
        AggregateValueChange::Set(AggregateResultValue::Scalar(Value::Int(3)))
    );
    assert_eq!(updates[1].subscription, bob.subscription_id);
    assert_eq!(updates[1].consumer, 2);
    assert_eq!(
        updates[1]
            .group
            .as_ref()
            .expect("grouped update carries identity")
            .values,
        vec![Value::String("paid".into())]
    );
    assert_eq!(
        updates[1].change,
        AggregateValueChange::Set(AggregateResultValue::Scalar(Value::Int(4)))
    );
}

fn assert_sync_count_authorization() {
    let (mut engine, table) = sync_aggregate_engine();
    let (alice, bob) = register_aggregate_pair(&mut engine, "SELECT COUNT(*) FROM notes");
    snapshot_pair(&mut engine, alice.subscription_id, bob.subscription_id);

    let event =
        TestEvent::<Postgres>::insert(table, vec![Value::Int(3), Value::String("paid".into())])
            .with_pk_columns([0u16]);
    engine.apply(&event).expect("apply");
    let mut updates = engine
        .resolve_collect()
        .expect("whole reads resolve")
        .rows_updates;
    updates.sort_by_key(|update| update.consumer_id);

    assert_recorded_pair(engine.connector().calls(), RecordedOperation::Cursor);
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].subscription_id, alice.subscription_id);
    assert_eq!(updates[0].consumer_id, 1);
    assert_eq!(updates[0].rows, vec![vec![Value::Int(2)]]);
    assert_eq!(updates[1].subscription_id, bob.subscription_id);
    assert_eq!(updates[1].consumer_id, 2);
    assert_eq!(updates[1].rows, vec![vec![Value::Int(7)]]);
}

fn async_aggregate_engine() -> (AsyncAggregateEngine, TableId) {
    let (registry, table) = registry();
    (
        AutoResolvingEngine::new(registry, AsyncMode::new(AsyncAggregateRecording::default())),
        table,
    )
}

fn register_async_aggregate_pair(
    engine: &mut AsyncAggregateEngine,
    sql: &str,
) -> (subql::Registered, subql::Registered) {
    let alice = engine
        .register(
            SubscriptionRequest::new(1u64, sql).database_reads_per_consumer(),
            String::from("alice"),
        )
        .expect("alice registers");
    let bob = engine
        .register(
            SubscriptionRequest::new(2u64, sql).database_reads_per_consumer(),
            String::from("bob"),
        )
        .expect("bob registers");
    assert_ne!(alice.subscription_id, bob.subscription_id);
    (alice, bob)
}

async fn snapshot_async_pair(
    engine: &mut AsyncAggregateEngine,
    alice: subql::SubscriptionId,
    bob: subql::SubscriptionId,
) {
    assert!(engine
        .snapshot(alice)
        .await
        .expect("alice snapshot resolves")
        .is_some());
    assert!(engine
        .snapshot(bob)
        .await
        .expect("bob snapshot resolves")
        .is_some());
    engine.connector().clear_calls();
}

async fn assert_async_scalar_authorization() {
    let (mut engine, table) = async_aggregate_engine();
    let (alice, bob) = register_async_aggregate_pair(&mut engine, "SELECT MIN(id) FROM notes");
    snapshot_async_pair(&mut engine, alice.subscription_id, bob.subscription_id).await;

    let event =
        TestEvent::<Postgres>::insert(table, vec![Value::Int(3), Value::String("paid".into())])
            .with_pk_columns([0u16]);
    engine.apply(&event).expect("apply");
    let mut updates = engine
        .resolve_collect()
        .await
        .expect("scalar reads resolve")
        .scalar_updates;
    updates.sort_by_key(|update| update.consumer_id);

    assert_recorded_pair(engine.connector().calls(), RecordedOperation::Scalar);
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].subscription_id, alice.subscription_id);
    assert_eq!(updates[0].consumer_id, 1);
    assert_eq!(updates[0].value, Value::Int(3));
    assert_eq!(updates[1].subscription_id, bob.subscription_id);
    assert_eq!(updates[1].consumer_id, 2);
    assert_eq!(updates[1].value, Value::Int(4));
}

async fn assert_async_grouped_authorization() {
    let (mut engine, table) = async_aggregate_engine();
    let (alice, bob) = register_async_aggregate_pair(
        &mut engine,
        "SELECT status, MIN(id) FROM notes GROUP BY status",
    );
    snapshot_async_pair(&mut engine, alice.subscription_id, bob.subscription_id).await;

    let event =
        TestEvent::<Postgres>::insert(table, vec![Value::Int(3), Value::String("paid".into())])
            .with_pk_columns([0u16]);
    engine.apply(&event).expect("apply");
    let mut updates = engine
        .resolve_collect()
        .await
        .expect("grouped reads resolve")
        .aggregate_updates;
    updates.sort_by_key(|update| update.consumer);

    assert_recorded_pair(engine.connector().calls(), RecordedOperation::Page);
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].subscription, alice.subscription_id);
    assert_eq!(updates[0].consumer, 1);
    assert_eq!(
        updates[0]
            .group
            .as_ref()
            .expect("grouped update carries identity")
            .values,
        vec![Value::String("paid".into())]
    );
    assert_eq!(
        updates[0].change,
        AggregateValueChange::Set(AggregateResultValue::Scalar(Value::Int(3)))
    );
    assert_eq!(updates[1].subscription, bob.subscription_id);
    assert_eq!(updates[1].consumer, 2);
    assert_eq!(
        updates[1]
            .group
            .as_ref()
            .expect("grouped update carries identity")
            .values,
        vec![Value::String("paid".into())]
    );
    assert_eq!(
        updates[1].change,
        AggregateValueChange::Set(AggregateResultValue::Scalar(Value::Int(4)))
    );
}

async fn assert_async_count_authorization() {
    let (mut engine, table) = async_aggregate_engine();
    let (alice, bob) = register_async_aggregate_pair(&mut engine, "SELECT COUNT(*) FROM notes");
    snapshot_async_pair(&mut engine, alice.subscription_id, bob.subscription_id).await;

    let event =
        TestEvent::<Postgres>::insert(table, vec![Value::Int(3), Value::String("paid".into())])
            .with_pk_columns([0u16]);
    engine.apply(&event).expect("apply");
    let mut updates = engine
        .resolve_collect()
        .await
        .expect("whole reads resolve")
        .rows_updates;
    updates.sort_by_key(|update| update.consumer_id);

    assert_recorded_pair(engine.connector().calls(), RecordedOperation::Cursor);
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].subscription_id, alice.subscription_id);
    assert_eq!(updates[0].consumer_id, 1);
    assert_eq!(updates[0].rows, vec![vec![Value::Int(2)]]);
    assert_eq!(updates[1].subscription_id, bob.subscription_id);
    assert_eq!(updates[1].consumer_id, 2);
    assert_eq!(updates[1].rows, vec![vec![Value::Int(7)]]);
}

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
    engine.apply(&event).expect("apply");
    let _output = engine.resolve_collect().expect("both reads resolve");
    assert_eq!(engine.pending_read_count(), 0, "all pending reads resolved");

    let mut calls = engine.connector().calls.lock().clone();
    calls.sort();
    assert_eq!(calls, vec!["alice", "bob"]);
}

#[test]
fn sync_aggregate_reads_keep_authorization_per_subscription() {
    assert_sync_scalar_authorization();
    assert_sync_grouped_authorization();
    assert_sync_count_authorization();
}

#[test]
fn per_consumer_sync_read_forwards_auth_and_registration_binds() {
    let (mut engine, _) = sync_aggregate_engine();
    let sql = "SELECT MIN(id) FROM notes WHERE id > $1";
    let alice = engine
        .register(
            SubscriptionRequest::new(1u64, sql)
                .binds(vec![Value::Int(0)])
                .database_reads_per_consumer(),
            String::from("alice"),
        )
        .expect("alice registers");
    let bob = engine
        .register(
            SubscriptionRequest::new(2u64, sql)
                .binds(vec![Value::Int(0)])
                .database_reads_per_consumer(),
            String::from("bob"),
        )
        .expect("bob registers");

    engine
        .snapshot(alice.subscription_id)
        .expect("alice snapshot");
    engine.snapshot(bob.subscription_id).expect("bob snapshot");
    let mut calls = engine.connector().bound_calls();
    calls.sort_by(|left, right| left.1.cmp(&right.1));
    assert_eq!(
        calls,
        vec![
            (
                RecordedOperation::Scalar,
                String::from("alice"),
                vec![Value::Int(0)]
            ),
            (
                RecordedOperation::Scalar,
                String::from("bob"),
                vec![Value::Int(0)]
            )
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn async_aggregate_reads_keep_authorization_per_subscription() {
    assert_async_scalar_authorization().await;
    assert_async_grouped_authorization().await;
    assert_async_count_authorization().await;
}

#[tokio::test(flavor = "current_thread")]
async fn per_consumer_async_read_forwards_auth_and_registration_binds() {
    let (mut engine, _) = async_aggregate_engine();
    let sql = "SELECT MIN(id) FROM notes WHERE id > $1";
    let alice = engine
        .register(
            SubscriptionRequest::new(1u64, sql)
                .binds(vec![Value::Int(0)])
                .database_reads_per_consumer(),
            String::from("alice"),
        )
        .expect("alice registers");
    let bob = engine
        .register(
            SubscriptionRequest::new(2u64, sql)
                .binds(vec![Value::Int(0)])
                .database_reads_per_consumer(),
            String::from("bob"),
        )
        .expect("bob registers");

    engine
        .snapshot(alice.subscription_id)
        .await
        .expect("alice snapshot");
    engine
        .snapshot(bob.subscription_id)
        .await
        .expect("bob snapshot");
    let mut calls = engine.connector().bound_calls();
    calls.sort_by(|left, right| left.1.cmp(&right.1));
    assert_eq!(
        calls,
        vec![
            (
                RecordedOperation::Scalar,
                String::from("alice"),
                vec![Value::Int(0)]
            ),
            (
                RecordedOperation::Scalar,
                String::from("bob"),
                vec![Value::Int(0)]
            )
        ]
    );
}

#[test]
fn shared_extreme_reads_remain_refused_on_an_rls_table() {
    let (mut registry, table) = registry();
    for sql in [
        "SELECT MIN(id) FROM notes",
        "SELECT status, MIN(id) FROM notes GROUP BY status",
    ] {
        assert!(matches!(
            registry.register(SubscriptionRequest::new(1u64, sql)),
            Err(RegisterError::AggregatorOnRlsTable { table_id }) if table_id == table
        ));
    }
}

#[test]
fn per_consumer_database_reads_accept_extreme_read_tiers() {
    let (mut registry, _) = registry();
    let scalar = registry
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(id) FROM notes")
                .database_reads_per_consumer(),
        )
        .expect("scalar extreme is isolated per consumer");
    assert!(matches!(
        scalar.tier,
        Tier::Scalar {
            column_kind: BuiltinKind::Int,
            ..
        }
    ));

    let grouped = registry
        .register(
            SubscriptionRequest::new(2u64, "SELECT status, MIN(id) FROM notes GROUP BY status")
                .database_reads_per_consumer(),
        )
        .expect("grouped extreme is isolated per consumer");
    assert!(matches!(grouped.tier, Tier::GroupedScalar { .. }));
}

#[test]
fn per_consumer_in_process_aggregate_uses_whole_read_tier() {
    let (mut registry, _) = registry();
    let registered = registry
        .register(
            SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM notes")
                .database_reads_per_consumer(),
        )
        .expect("count is isolated by whole re-read");
    assert!(matches!(registered.tier, Tier::WholeRows { .. }));
}

#[test]
fn per_consumer_scalar_extreme_insert_asks_before_revealing_value() {
    let (mut registry, table) = registry();
    let registered = registry
        .register(
            SubscriptionRequest::new(1u64, "SELECT MIN(id) FROM notes")
                .database_reads_per_consumer(),
        )
        .expect("scalar extreme registers");
    Install::install(
        &mut registry,
        registered.subscription_id,
        ScalarInstall {
            value: Value::Int(5),
            checkpoint: None::<NoCheckpoint>,
        },
    )
    .expect("scalar seed installs");

    let event =
        TestEvent::<Postgres>::insert(table, vec![Value::Int(3), Value::String("paid".into())])
            .with_pk_columns([0u16]);
    let output = registry.dispatch(&event).expect("insert dispatches");
    assert!(output.aggregate_updates().is_empty());
    assert_eq!(output.triggers().len(), 1);
    assert_eq!(output.triggers()[0].read, ReExecutionRead::Subscription);
}

#[test]
fn per_consumer_grouped_extreme_insert_asks_before_opening_group() {
    let (mut registry, table) = registry();
    let registered = registry
        .register(
            SubscriptionRequest::new(1u64, "SELECT status, MIN(id) FROM notes GROUP BY status")
                .database_reads_per_consumer(),
        )
        .expect("grouped extreme registers");
    Install::install(
        &mut registry,
        registered.subscription_id,
        GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("paid".into()),
                Value::Int(5),
                Value::Int(1),
            ]],
            read_at: None::<NoCheckpoint>,
        },
    )
    .expect("grouped seed installs");

    let event =
        TestEvent::<Postgres>::insert(table, vec![Value::Int(3), Value::String("secret".into())])
            .with_pk_columns([0u16]);
    let output = registry.dispatch(&event).expect("insert dispatches");
    assert!(output.aggregate_updates().is_empty());
    assert_eq!(output.triggers().len(), 1);
    let ReExecutionRead::GroupedScalar { group, query, .. } = &output.triggers()[0].read else {
        panic!("expected grouped scalar read")
    };
    assert!(
        query.sql().contains("\"status\" = $1"),
        "scoped query was {query:?}"
    );
    assert_eq!(query.binds(), &[Value::String("secret".into())]);

    let installed = Install::install(
        &mut registry,
        registered.subscription_id,
        GroupedScalarInstall {
            group: group.clone(),
            row: vec![Value::Int(3), Value::Int(1)],
            checkpoint: None::<NoCheckpoint>,
        },
    )
    .expect("scoped grouped value installs");
    let identity = installed.updates[0]
        .group
        .as_ref()
        .expect("grouped update carries identity");
    assert_eq!(identity.key, *group);
    assert_eq!(identity.values, vec![Value::String("secret".into())]);
}

#[test]
fn delayed_group_read_recovers_identity_after_state_removal() {
    let (mut registry, table) = registry();
    let registered = registry
        .register(
            SubscriptionRequest::new(1u64, "SELECT status, MIN(id) FROM notes GROUP BY status")
                .database_reads_per_consumer(),
        )
        .expect("grouped extreme registers");
    Install::install(
        &mut registry,
        registered.subscription_id,
        GroupedScalarSeedInstall {
            rows: Vec::new(),
            read_at: None::<NoCheckpoint>,
        },
    )
    .expect("empty grouped seed installs");

    let first =
        TestEvent::<Postgres>::insert(table, vec![Value::Int(1), Value::String("secret".into())])
            .with_pk_columns([0u16]);
    let first = registry.dispatch(&first).expect("first insert dispatches");
    let ReExecutionRead::GroupedScalar { group, .. } = &first.triggers()[0].read else {
        panic!("expected grouped scalar read")
    };
    let group = group.clone();

    let second =
        TestEvent::<Postgres>::insert(table, vec![Value::Int(2), Value::String("secret".into())])
            .with_pk_columns([0u16]);
    let second = registry
        .dispatch(&second)
        .expect("second insert dispatches");
    assert!(matches!(
        &second.triggers()[0].read,
        ReExecutionRead::GroupedScalar {
            group: repeated,
            ..
        } if repeated == &group
    ));

    let deleted =
        TestEvent::<Postgres>::delete(table, vec![Value::Int(1), Value::String("secret".into())])
            .with_pk_columns([0u16]);
    let deleted = registry.dispatch(&deleted).expect("delete dispatches");
    assert!(deleted.aggregate_updates().is_empty());
    assert!(deleted.triggers().is_empty());

    let installed = Install::install(
        &mut registry,
        registered.subscription_id,
        GroupedScalarInstall {
            group: group.clone(),
            row: vec![Value::Int(2), Value::Int(1)],
            checkpoint: None::<NoCheckpoint>,
        },
    )
    .expect("delayed grouped value installs");
    assert_eq!(installed.updates.len(), 1);
    let identity = installed.updates[0]
        .group
        .as_ref()
        .expect("grouped update carries identity");
    assert_eq!(identity.key, group);
    assert_eq!(identity.values, vec![Value::String("secret".into())]);
    assert_eq!(
        installed.updates[0].change,
        AggregateValueChange::Set(AggregateResultValue::Scalar(Value::Int(2)))
    );
}

#[test]
fn zero_row_group_read_removes_original_identity() {
    let (mut registry, table) = registry();
    let registered = registry
        .register(
            SubscriptionRequest::new(1u64, "SELECT status, MIN(id) FROM notes GROUP BY status")
                .database_reads_per_consumer(),
        )
        .expect("grouped extreme registers");
    let seeded = Install::install(
        &mut registry,
        registered.subscription_id,
        GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("paid".into()),
                Value::Int(5),
                Value::Int(1),
            ]],
            read_at: None::<NoCheckpoint>,
        },
    )
    .expect("grouped seed installs");
    let original = seeded.updates[0]
        .group
        .clone()
        .expect("grouped update carries identity");

    let event =
        TestEvent::<Postgres>::insert(table, vec![Value::Int(3), Value::String("paid".into())])
            .with_pk_columns([0u16]);
    let output = registry.dispatch(&event).expect("insert dispatches");
    assert!(output.aggregate_updates().is_empty());
    let ReExecutionRead::GroupedScalar { group, .. } = &output.triggers()[0].read else {
        panic!("expected grouped scalar read")
    };

    let installed = Install::install(
        &mut registry,
        registered.subscription_id,
        GroupedScalarInstall {
            group: group.clone(),
            row: vec![Value::Null, Value::Int(0)],
            checkpoint: None::<NoCheckpoint>,
        },
    )
    .expect("empty grouped value installs");
    assert_eq!(installed.updates.len(), 1);
    assert_eq!(installed.updates[0].group.as_ref(), Some(&original));
    assert_eq!(installed.updates[0].change, AggregateValueChange::Remove);
}

#[test]
fn per_consumer_read_scope_survives_persistence() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let (mut registry, first) =
        Registry::with_storage(catalog, PostgreSqlDialect {}, path.clone()).expect("open store");
    assert_eq!(first.restored, [] as [subql::RestoredRead; 0]);
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

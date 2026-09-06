//! Priming a subscription answers the same thing in both modes.
//!
//! `snapshot` exists so a caller registering against a quiet database is
//! told the answer it already holds. A row filter the stream answers is
//! the one tier with nothing to prime: it keeps its own query for the
//! case the stream cannot answer, a cell the event did not carry, which
//! is a report-driven read rather than a snapshot.
//!
//! The sync engine returned `Ok(None)` for it and the async engine did
//! not. `InProcessKind::StreamServedFilter` sets `whole_result`, so the
//! async path reached its whole-result branch first and issued a read
//! the sync path never issues, answering `ReExecError::Cursor` where its
//! twin answers nothing at all.
//!
//! Both modes now read one predicate on the shared `ResolveContext`, so
//! there is no second copy to drift. These tests hold the pair together
//! by asking the same question of each, through a connector that fails
//! every read: a snapshot with nothing to prime never reaches it.
#![allow(clippy::unwrap_used)]

use core::future::Future;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, ScalarFamily, Value};
use subql::reexec::{
    AsyncConnector, AsyncMode, AutoResolvingEngine, Connector, ReadQuery, RowPage, Snapshot,
    SyncMode,
};
use subql::testing::TestEvent;
use subql::{DefaultIds, NoCheckpoint, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, amount INT)";
/// Served in process by the stream, so it registers as a
/// `StreamServedFilter` and holds its query for a report.
const SERVED: &str = "SELECT * FROM t WHERE amount > 10";

/// Every read this connector is asked for is a failure, which is the
/// point: a snapshot that reaches the database for a stream-served
/// filter is the defect, and the failure is how the test sees it.
#[derive(Debug, thiserror::Error)]
enum RefusingError {
    #[error("a stream-served filter must not be primed by a read")]
    ReadIssued,
}

struct Refusing;

impl Connector for Refusing {
    type AuthContext = ();
    type Error = RefusingError;
    type Checkpoint = NoCheckpoint;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _kind: ScalarFamily,
        _auth: &(),
    ) -> Result<(Value<Postgres>, Option<NoCheckpoint>), RefusingError> {
        Err(RefusingError::ReadIssued)
    }

    fn read_page(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _auth: &(),
    ) -> Result<Snapshot<RowPage<Postgres>, NoCheckpoint>, RefusingError> {
        Err(RefusingError::ReadIssued)
    }
}

#[allow(clippy::manual_async_fn)]
impl AsyncConnector for Refusing {
    type AuthContext = ();
    type Error = RefusingError;
    type Checkpoint = NoCheckpoint;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _kind: ScalarFamily,
        _auth: &(),
    ) -> impl Future<Output = Result<(Value<Postgres>, Option<NoCheckpoint>), RefusingError>> + Send
    {
        async move { Err(RefusingError::ReadIssued) }
    }

    fn read_page(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _auth: &(),
    ) -> impl Future<Output = Result<Snapshot<RowPage<Postgres>, NoCheckpoint>, RefusingError>> + Send
    {
        async move { Err(RefusingError::ReadIssued) }
    }
}

fn inner() -> SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> {
    let database = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("the DDL parses");
    SubscriptionEngine::new(database, PostgreSqlDialect {})
}

/// The sync engine primes nothing for a filter the stream answers.
#[test]
fn sync_snapshot_of_a_stream_served_filter_reads_nothing() {
    let mut engine: AutoResolvingEngine<
        TestEvent<Postgres>,
        DefaultIds,
        ParserDB,
        SyncMode<Refusing>,
    > = AutoResolvingEngine::new(inner(), SyncMode(Refusing));
    let registered = engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, SERVED),
            (),
        )
        .expect("the filter registers");
    assert!(
        registered.not_served_because.is_none(),
        "this filter is served in process, and it was refused: {:?}",
        registered.not_served_because
    );
    let snapshot = engine
        .snapshot(registered.subscription_id)
        .expect("a stream-served filter needs no read, so nothing can fail");
    assert!(
        snapshot.is_none(),
        "there is nothing to prime, so there is nothing to report"
    );
}

/// And so does the async engine, which is what drifted.
#[test]
fn async_snapshot_of_a_stream_served_filter_reads_nothing() {
    let mut engine: AutoResolvingEngine<
        TestEvent<Postgres>,
        DefaultIds,
        ParserDB,
        AsyncMode<Refusing>,
    > = AutoResolvingEngine::new(inner(), AsyncMode::new(Refusing));
    let registered = engine
        .register(
            SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, SERVED),
            (),
        )
        .expect("the filter registers");
    assert!(registered.not_served_because.is_none());
    let snapshot = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime builds")
        .block_on(engine.snapshot(registered.subscription_id))
        .expect("a stream-served filter needs no read, so nothing can fail");
    assert!(
        snapshot.is_none(),
        "the same answer as its twin, which is the whole point of this file"
    );
}

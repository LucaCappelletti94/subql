//! Every value the engine sets from a database read judges the stream's
//! changes against that read's snapshot, so a change the read holds is never
//! applied again and a change it missed is never lost.
//!
//! One Postgres read throughout, with snapshot `740:745:742` and WAL insert
//! position 2000 read after it. Against it, xid 741 committed at 1500 is seen
//! and arrives late. Xid 743 committed at 1800 is seen, though it committed
//! after any position read before the snapshot. Xid 742 committed at 1200 is
//! still running in the snapshot, as while it waits on a synchronous standby,
//! so the read missed it. Everything from 2000 on is beyond the read.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, ScalarFamily, Value};
use subql::reexec::{
    AsyncConnector, AsyncMode, AutoResolvingEngine, Connector, ReExecutionRead, ReadQuery, RowPage,
    ScalarInstalled, Snapshot, SyncMode,
};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, AggregateResultValue, AggregateSeedInstall, AggregateValueChange,
    DefaultIds, FenceInstall, GroupedScalarInstall, GroupedScalarSeedInstall, Install,
    PgCommitPosition, PgLsn, PgSnapshotFence, PgXid, ScalarInstall, SubscriptionEngine,
    SubscriptionRequest, TableId,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, region TEXT, amount INT, status TEXT);";

type Event = TestEvent<Postgres, PgCommitPosition>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;

fn engine() -> (Engine, TableId) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog, "orders").unwrap();
    (
        SubscriptionEngine::new(catalog, PostgreSqlDialect {}),
        orders,
    )
}

fn read_fence() -> PgSnapshotFence {
    PgSnapshotFence::parse("740:745:742", PgLsn(2000)).unwrap()
}

/// A read taken long before, which every change below passes.
fn early_fence() -> PgSnapshotFence {
    PgSnapshotFence::parse("700:701:", PgLsn(1000)).unwrap()
}

fn row(id: i64, region: &str, amount: i64) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::String(region.into()),
        Value::Int(amount),
        Value::String("paid".into()),
    ]
}

const fn at(commit_lsn: u64, xid: u32) -> PgCommitPosition {
    PgCommitPosition::new(PgLsn(commit_lsn), PgXid(xid), 1)
}

fn insert(
    orders: TableId,
    id: i64,
    region: &str,
    amount: i64,
    position: PgCommitPosition,
) -> Event {
    Event::insert(orders, row(id, region, amount))
        .with_pk_columns([0u16])
        .with_checkpoint(position)
}

fn delete(
    orders: TableId,
    id: i64,
    region: &str,
    amount: i64,
    position: PgCommitPosition,
) -> Event {
    Event::delete(orders, row(id, region, amount))
        .with_pk_columns([0u16])
        .with_checkpoint(position)
}

fn register(engine: &mut Engine, sql: &str) -> u64 {
    engine
        .register(SubscriptionRequest::new(7u64, sql))
        .unwrap()
        .subscription_id
}

fn folded(updates: &[subql::AggregateValueUpdate<DefaultIds>]) -> Vec<AggValue> {
    updates
        .iter()
        .map(|update| update.folded_value().expect("a folded value"))
        .collect()
}

#[test]
fn a_seeded_count_applies_exactly_the_changes_its_read_missed() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
    );

    // Delivered while the read runs, invisible to its snapshot.
    engine
        .aggregate_updates(&insert(orders, 1, "north", 5, at(1200, 742)))
        .unwrap();

    // The read counted 741 and 743.
    let installed = Install::install(
        &mut engine,
        sub,
        AggregateSeedInstall {
            rows: vec![vec![Value::Int(2)]],
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert_eq!(folded(&installed), vec![AggValue::CountStar(3)]);

    for held in [
        insert(orders, 2, "north", 5, at(1500, 741)),
        insert(orders, 3, "north", 5, at(1800, 743)),
    ] {
        let late = engine.aggregate_updates(&held).unwrap();
        assert!(
            late.is_empty(),
            "the read already counted it, got {:?}",
            folded(&late)
        );
    }

    let beyond = engine
        .aggregate_updates(&insert(orders, 4, "north", 5, at(2100, 746)))
        .unwrap();
    assert_eq!(folded(&beyond), vec![AggValue::CountStar(4)]);
}

#[test]
fn a_truncate_the_read_already_saw_leaves_its_count_alone() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
    );
    // The read ran after the 741 truncate and counted 743's row.
    Install::install(
        &mut engine,
        sub,
        AggregateSeedInstall {
            rows: vec![vec![Value::Int(1)]],
            fence: Some(read_fence()),
        },
    )
    .unwrap();

    let truncated = Event::truncate(orders).with_checkpoint(at(1500, 741));
    let late = engine.aggregate_updates(&truncated).unwrap();
    assert!(late.is_empty(), "got {:?}", folded(&late));
    assert_eq!(
        engine.current_aggregate_value(sub),
        Some(AggValue::CountStar(1))
    );
}

#[test]
fn a_seeded_grouped_count_drops_a_late_change_its_read_holds() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT region, COUNT(*) FROM orders WHERE status = 'paid' GROUP BY region",
    );

    engine
        .aggregate_updates(&insert(orders, 1, "north", 5, at(1200, 742)))
        .unwrap();
    let installed = Install::install(
        &mut engine,
        sub,
        AggregateSeedInstall {
            rows: vec![vec![
                Value::String("north".into()),
                Value::Int(2),
                Value::Int(2),
            ]],
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert_eq!(folded(&installed), vec![AggValue::CountStar(3)]);

    let late = engine
        .aggregate_updates(&insert(orders, 2, "north", 5, at(1800, 743)))
        .unwrap();
    assert!(
        late.is_empty(),
        "the read already counted it, got {:?}",
        folded(&late)
    );

    let beyond = engine
        .aggregate_updates(&insert(orders, 3, "north", 5, at(2100, 746)))
        .unwrap();
    assert_eq!(folded(&beyond), vec![AggValue::CountStar(4)]);
}

const fn scalar(value: i64) -> AggregateValueChange<Postgres> {
    AggregateValueChange::Set(AggregateResultValue::Scalar(Value::Int(value)))
}

#[test]
fn a_scoped_group_read_keeps_what_it_missed_and_drops_what_it_holds() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT region, MIN(amount) FROM orders WHERE status = 'paid' GROUP BY region",
    );
    // North holds amounts 4 and 3.
    let opening = Install::install(
        &mut engine,
        sub,
        GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("north".into()),
                Value::Int(3),
                Value::Int(2),
            ]],
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    let north = opening.updates[0].group.clone().unwrap();

    // Removing the extreme asks for a read of north alone.
    let displaced = engine
        .dispatch(&delete(orders, 10, "north", 3, at(1100, 720)))
        .unwrap();
    assert!(matches!(
        displaced.triggers()[0].read,
        ReExecutionRead::GroupedScalar { .. }
    ));
    // Delivered before the read lands, invisible to its snapshot.
    engine
        .dispatch(&insert(orders, 11, "north", 9, at(1200, 742)))
        .unwrap();

    // The read saw 4 and the late 741 row worth 7.
    let installed = Install::install(
        &mut engine,
        sub,
        GroupedScalarInstall {
            group: north.key,
            row: vec![Value::Int(4), Value::Int(2)],
            checkpoint: Some(at(1100, 720)),
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert_eq!(installed.updates[0].change, scalar(4));
    assert!(installed.triggers.is_empty());

    let late = engine
        .dispatch(&insert(orders, 12, "north", 7, at(1500, 741)))
        .unwrap();
    assert!(late.aggregate_updates().is_empty() && late.triggers().is_empty());

    // Three rows are left, 9, 7 and 4, so the third delete empties the group.
    for (id, amount, lsn) in [(11, 9, 2100), (12, 7, 2200)] {
        let output = engine
            .dispatch(&delete(orders, id, "north", amount, at(lsn, 750)))
            .unwrap();
        assert!(output.aggregate_updates().is_empty() && output.triggers().is_empty());
    }
    let emptied = engine
        .dispatch(&delete(orders, 13, "north", 4, at(2300, 751)))
        .unwrap();
    assert!(emptied.triggers().is_empty(), "no row is left to read");
    assert_eq!(
        emptied.aggregate_updates()[0].change,
        AggregateValueChange::Remove
    );
}

fn scalar_value(
    installed: ScalarInstalled<DefaultIds, Postgres, PgCommitPosition>,
) -> Value<Postgres> {
    let ScalarInstalled::Value(update) = installed else {
        panic!("expected a value, got {installed:?}")
    };
    update.value
}

#[test]
fn a_scalar_read_keeps_a_change_it_missed() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT MIN(amount) FROM orders WHERE status = 'paid'",
    );
    let first = Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    assert_eq!(scalar_value(first), Value::Int(4));

    let displaced = engine
        .dispatch(&delete(orders, 10, "north", 4, at(1100, 720)))
        .unwrap();
    assert_eq!(displaced.triggers().len(), 1);
    engine
        .dispatch(&insert(orders, 11, "north", 2, at(1200, 742)))
        .unwrap();

    // The read saw 5 as the smallest and not the 742 row worth 2.
    let installed = Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(5),
            checkpoint: Some(at(1100, 720)),
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert_eq!(scalar_value(installed), Value::Int(2));
}

#[test]
fn a_scalar_read_whose_answer_a_missed_change_removed_asks_again() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT MIN(amount) FROM orders WHERE status = 'paid'",
    );
    Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    engine
        .dispatch(&delete(orders, 10, "north", 4, at(1100, 720)))
        .unwrap();
    engine
        .dispatch(&delete(orders, 11, "north", 5, at(1200, 742)))
        .unwrap();

    // The read still saw the 742 row worth 5 as the smallest.
    let installed = Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(5),
            checkpoint: Some(at(1100, 720)),
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert!(
        matches!(installed, ScalarInstalled::ReadAgain(_)),
        "got {installed:?}"
    );

    // The next read holds that delete, so its answer stands.
    let reread = Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(6),
            checkpoint: None,
            fence: Some(PgSnapshotFence::parse("760:760:", PgLsn(3000)).unwrap()),
        },
    )
    .unwrap();
    assert_eq!(scalar_value(reread), Value::Int(6));
}

#[test]
fn a_scoped_group_read_whose_answer_a_missed_delete_removed_asks_again() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT region, MIN(amount) FROM orders WHERE status = 'paid' GROUP BY region",
    );
    let opening = Install::install(
        &mut engine,
        sub,
        GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("north".into()),
                Value::Int(3),
                Value::Int(3),
            ]],
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    let north = opening.updates[0].group.clone().unwrap();
    engine
        .dispatch(&delete(orders, 10, "north", 3, at(1100, 720)))
        .unwrap();
    engine
        .dispatch(&delete(orders, 11, "north", 5, at(1200, 742)))
        .unwrap();

    // The read still saw the 742 row worth 5 as the smallest.
    let installed = Install::install(
        &mut engine,
        sub,
        GroupedScalarInstall {
            group: north.key.clone(),
            row: vec![Value::Int(5), Value::Int(2)],
            checkpoint: Some(at(1100, 720)),
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert!(installed.updates.is_empty(), "{:?}", installed.updates);
    assert!(matches!(
        &installed.triggers[..],
        [trigger] if matches!(&trigger.read, ReExecutionRead::GroupedScalar { group, .. } if *group == north.key)
    ));

    // The next read holds that delete, so its answer stands.
    let reread = Install::install(
        &mut engine,
        sub,
        GroupedScalarInstall {
            group: north.key,
            row: vec![Value::Int(6), Value::Int(1)],
            checkpoint: Some(at(1100, 720)),
            fence: Some(PgSnapshotFence::parse("760:760:", PgLsn(3000)).unwrap()),
        },
    )
    .unwrap();
    assert_eq!(reread.updates[0].change, scalar(6));
    assert!(reread.triggers.is_empty());
}

#[test]
fn a_truncate_a_scoped_group_read_missed_empties_the_group_it_answers() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT region, MIN(amount) FROM orders WHERE status = 'paid' GROUP BY region",
    );
    let opening = Install::install(
        &mut engine,
        sub,
        GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("north".into()),
                Value::Int(3),
                Value::Int(2),
            ]],
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    let north = opening.updates[0].group.clone().unwrap();
    engine
        .dispatch(&delete(orders, 10, "north", 3, at(1100, 720)))
        .unwrap();
    let truncated = engine
        .dispatch(&Event::truncate(orders).with_checkpoint(at(1200, 742)))
        .unwrap();
    assert_eq!(
        truncated.aggregate_updates()[0].change,
        AggregateValueChange::Remove
    );

    // The read ran before the truncate reached its snapshot.
    let installed = Install::install(
        &mut engine,
        sub,
        GroupedScalarInstall {
            group: north.key,
            row: vec![Value::Int(4), Value::Int(1)],
            checkpoint: Some(at(1100, 720)),
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert!(
        installed.updates.is_empty() && installed.triggers.is_empty(),
        "the table is empty, got {:?}",
        installed.updates
    );
}

#[test]
fn a_scalar_read_whose_kept_changes_overflowed_asks_again() {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog, "orders").unwrap();
    let mut engine: Engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {})
        .with_max_changes_during_aggregate_read(1);
    let sub = register(
        &mut engine,
        "SELECT MIN(amount) FROM orders WHERE status = 'paid'",
    );
    Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    engine
        .dispatch(&delete(orders, 10, "north", 4, at(1100, 720)))
        .unwrap();
    engine
        .dispatch(&insert(orders, 11, "north", 9, at(1200, 742)))
        .unwrap();

    let installed = Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(5),
            checkpoint: None,
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert!(
        matches!(installed, ScalarInstalled::ReadAgain(_)),
        "got {installed:?}"
    );
}

/// Serves queued answers, each with the fence of the read that produced it.
struct FencedReads {
    answers: std::sync::Mutex<Vec<(Value<Postgres>, PgSnapshotFence)>>,
    calls: std::sync::atomic::AtomicUsize,
    fence_reads: std::sync::atomic::AtomicUsize,
}

impl FencedReads {
    /// `answers` in the order the reads get them.
    fn new(answers: Vec<(Value<Postgres>, PgSnapshotFence)>) -> Self {
        Self {
            answers: std::sync::Mutex::new(answers.into_iter().rev().collect()),
            calls: std::sync::atomic::AtomicUsize::new(0),
            fence_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn fence_reads(&self) -> usize {
        self.fence_reads.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The database now, after every transaction the tests commit.
    fn current_fence(&self) -> PgSnapshotFence {
        self.fence_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        PgSnapshotFence::parse("760:760:", PgLsn(3000)).unwrap()
    }

    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn answer(&self) -> Result<(Value<Postgres>, Option<PgSnapshotFence>), FencedReadError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (value, fence) = self
            .answers
            .lock()
            .unwrap()
            .pop()
            .ok_or(FencedReadError::Unqueued)?;
        Ok((value, Some(fence)))
    }
}

#[derive(Debug, thiserror::Error)]
enum FencedReadError {
    #[error("no answer is queued")]
    Unqueued,
    #[error("this connector reads no rows")]
    NoRows,
}

impl Connector for FencedReads {
    type AuthContext = ();
    type Error = FencedReadError;
    type Checkpoint = PgCommitPosition;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _kind: ScalarFamily,
        _auth: &(),
    ) -> Result<(Value<Postgres>, Option<PgSnapshotFence>), FencedReadError> {
        self.answer()
    }

    fn read_page(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _auth: &(),
    ) -> Result<Snapshot<RowPage<Postgres>, PgCommitPosition>, FencedReadError> {
        Err(FencedReadError::NoRows)
    }

    fn read_fence(&self, _auth: &()) -> Result<Option<PgSnapshotFence>, FencedReadError> {
        Ok(Some(self.current_fence()))
    }
}

#[allow(clippy::manual_async_fn)]
impl AsyncConnector for FencedReads {
    type AuthContext = ();
    type Error = FencedReadError;
    type Checkpoint = PgCommitPosition;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _kind: ScalarFamily,
        _auth: &(),
    ) -> impl core::future::Future<
        Output = Result<(Value<Postgres>, Option<PgSnapshotFence>), FencedReadError>,
    > + Send {
        let answer = self.answer();
        async move { answer }
    }

    fn read_page(
        &self,
        _query: &ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _auth: &(),
    ) -> impl core::future::Future<
        Output = Result<Snapshot<RowPage<Postgres>, PgCommitPosition>, FencedReadError>,
    > + Send {
        async move { Err(FencedReadError::NoRows) }
    }

    fn read_fence(
        &self,
        _auth: &(),
    ) -> impl core::future::Future<Output = Result<Option<PgSnapshotFence>, FencedReadError>> + Send
    {
        let fence = self.current_fence();
        async move { Ok(Some(fence)) }
    }
}

const MIN_PAID: &str = "SELECT MIN(amount) FROM orders WHERE status = 'paid'";

/// The read answers 5 though the 742 delete of the row worth 5 already
/// arrived, and the second read, which sees it, answers 6.
fn two_reads() -> FencedReads {
    FencedReads::new(vec![
        (Value::Int(5), read_fence()),
        (
            Value::Int(6),
            PgSnapshotFence::parse("760:760:", PgLsn(3000)).unwrap(),
        ),
    ])
}

fn fenced_engine<M: subql::reexec::ResolverMode<Postgres, AuthContext = ()>>(
    mode: M,
) -> (
    AutoResolvingEngine<Event, DefaultIds, ParserDB, M>,
    TableId,
    u64,
) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog, "orders").unwrap();
    let mut engine = AutoResolvingEngine::new(
        SubscriptionEngine::<Event, DefaultIds, ParserDB>::new(catalog, PostgreSqlDialect {}),
        mode,
    );
    let sub = engine
        .register(SubscriptionRequest::new(7u64, MIN_PAID), ())
        .unwrap()
        .subscription_id;
    Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    (engine, orders, sub)
}

/// A read that has to be asked again waits for the next drain, so a commit
/// that stays invisible cannot keep one drain reading forever.
#[test]
fn the_auto_resolving_engine_holds_a_second_read_until_the_next_drain() {
    let (mut engine, orders, _) = fenced_engine(SyncMode(two_reads()));
    drop(
        engine
            .apply(&delete(orders, 10, "north", 4, at(1100, 720)))
            .unwrap(),
    );
    drop(
        engine
            .apply(&delete(orders, 11, "north", 5, at(1200, 742)))
            .unwrap(),
    );

    let first = engine.resolve_collect().unwrap();
    assert!(
        first.scalar_updates.is_empty(),
        "{:?}",
        first.scalar_updates
    );
    assert_eq!(engine.connector().calls(), 1);
    assert_eq!(engine.pending_read_count(), 1, "the second read waits");

    let second = engine.resolve_collect().unwrap();
    assert_eq!(engine.connector().calls(), 2);
    assert_eq!(second.scalar_updates[0].value, Value::Int(6));
    assert_eq!(engine.pending_read_count(), 0);
}

#[test]
fn the_async_engine_holds_a_second_read_until_the_next_drain() {
    let (mut engine, orders, _) = fenced_engine(AsyncMode::new(two_reads()));
    drop(
        engine
            .apply(&delete(orders, 10, "north", 4, at(1100, 720)))
            .unwrap(),
    );
    drop(
        engine
            .apply(&delete(orders, 11, "north", 5, at(1200, 742)))
            .unwrap(),
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let first = runtime.block_on(engine.resolve_collect()).unwrap();
    assert!(
        first.scalar_updates.is_empty(),
        "{:?}",
        first.scalar_updates
    );
    assert_eq!(engine.connector().calls(), 1);
    assert_eq!(engine.pending_read_count(), 1, "the second read waits");

    let second = runtime.block_on(engine.resolve_collect()).unwrap();
    assert_eq!(engine.connector().calls(), 2);
    assert_eq!(second.scalar_updates[0].value, Value::Int(6));
    assert_eq!(engine.pending_read_count(), 0);
}

fn scalar_engine() -> (Engine, TableId, u64) {
    let (mut engine, orders) = engine();
    let sub = register(&mut engine, MIN_PAID);
    Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    engine
        .dispatch(&delete(orders, 10, "north", 4, at(1100, 720)))
        .unwrap();
    (engine, orders, sub)
}

fn install_min(
    engine: &mut Engine,
    sub: u64,
    value: i64,
) -> ScalarInstalled<DefaultIds, Postgres, PgCommitPosition> {
    Install::install(
        engine,
        sub,
        ScalarInstall {
            value: Value::Int(value),
            checkpoint: Some(at(1100, 720)),
            fence: Some(read_fence()),
        },
    )
    .unwrap()
}

#[test]
fn a_truncate_a_scalar_read_missed_empties_its_answer() {
    let (mut engine, orders, sub) = scalar_engine();
    engine
        .dispatch(&Event::truncate(orders).with_checkpoint(at(1200, 742)))
        .unwrap();

    assert_eq!(scalar_value(install_min(&mut engine, sub, 5)), Value::Null);
}

#[test]
fn a_missed_delete_whose_row_image_lacks_a_filtered_column_asks_again() {
    let (mut engine, orders, sub) = scalar_engine();
    let sparse = Event::delete(
        orders,
        vec![
            Value::Int(11),
            Value::String("north".into()),
            Value::Int(9),
            Value::Missing,
        ],
    )
    .with_pk_columns([0u16])
    .with_checkpoint(at(1200, 742));
    engine.dispatch(&sparse).unwrap();

    let installed = install_min(&mut engine, sub, 5);
    assert!(
        matches!(installed, ScalarInstalled::ReadAgain(_)),
        "got {installed:?}"
    );
}

/// A row the stream delivered before a read was asked for, whose
/// transaction the read's snapshot does not see yet.
fn unseen_insert(orders: TableId) -> Event {
    insert(orders, 20, "north", 2, at(1200, 742))
}

/// A delete whose row image lacks the filtered column, so only a read can say
/// what it removed.
fn sparse_delete(orders: TableId) -> Event {
    Event::delete(
        orders,
        vec![
            Value::Int(21),
            Value::String("north".into()),
            Value::Int(9),
            Value::Missing,
        ],
    )
    .with_pk_columns([0u16])
    .with_checkpoint(at(1300, 741))
}

#[test]
fn a_scalar_read_keeps_a_change_it_missed_that_arrived_before_it_was_asked_for() {
    let (mut engine, orders) = engine();
    let sub = register(&mut engine, MIN_PAID);
    Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    engine.dispatch(&unseen_insert(orders)).unwrap();
    let asked = engine.dispatch(&sparse_delete(orders)).unwrap();
    assert_eq!(asked.triggers().len(), 1);

    // The read saw 4 as the smallest and not the 742 row worth 2.
    let installed = Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: Some(at(1300, 741)),
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert_eq!(scalar_value(installed), Value::Int(2));
}

#[test]
fn a_scoped_group_read_keeps_a_change_it_missed_that_arrived_before_it_was_asked_for() {
    let (mut engine, orders) = engine();
    let sub = register(
        &mut engine,
        "SELECT region, MIN(amount) FROM orders WHERE status = 'paid' GROUP BY region",
    );
    let opening = Install::install(
        &mut engine,
        sub,
        GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("north".into()),
                Value::Int(5),
                Value::Int(2),
            ]],
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    let north = opening.updates[0].group.clone().unwrap();
    let seen = engine.dispatch(&unseen_insert(orders)).unwrap();
    assert_eq!(seen.aggregate_updates()[0].change, scalar(2));
    let asked = engine.dispatch(&sparse_delete(orders)).unwrap();
    assert_eq!(asked.triggers().len(), 1);

    // The read saw 5 as the smallest of two rows, and not the 742 row worth 2.
    let installed = Install::install(
        &mut engine,
        sub,
        GroupedScalarInstall {
            group: north.key,
            row: vec![Value::Int(5), Value::Int(2)],
            checkpoint: Some(at(1300, 741)),
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert!(
        installed.updates.is_empty() && installed.triggers.is_empty(),
        "north still holds 2, got {:?}",
        installed.updates
    );
}

/// A scalar extreme whose unseen changes are logged from its first install,
/// under a cap of four so half of it is two changes.
fn small_cap_engine() -> (Engine, TableId, u64) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog, "orders").unwrap();
    let mut engine: Engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {})
        .with_max_changes_during_aggregate_read(4);
    let sub = register(&mut engine, MIN_PAID);
    Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    (engine, orders, sub)
}

fn fence_reads(output: &subql::DispatchOutput<DefaultIds, PgCommitPosition, Postgres>) -> usize {
    output
        .triggers()
        .iter()
        .filter(|trigger| matches!(trigger.read, ReExecutionRead::Fence))
        .count()
}

#[test]
fn a_filling_unseen_log_asks_for_one_fence_read_which_trims_it() {
    let (mut engine, orders, sub) = small_cap_engine();
    let first = engine
        .dispatch(&insert(orders, 30, "north", 9, at(2100, 750)))
        .unwrap();
    assert_eq!(fence_reads(&first), 0);
    let second = engine
        .dispatch(&insert(orders, 31, "north", 9, at(2200, 751)))
        .unwrap();
    assert_eq!(fence_reads(&second), 1, "half the cap asks for a fence");
    let third = engine
        .dispatch(&insert(orders, 32, "north", 9, at(2300, 752)))
        .unwrap();
    assert_eq!(fence_reads(&third), 0, "one fence read at a time");

    Install::install(
        &mut engine,
        sub,
        FenceInstall {
            fence: Some(PgSnapshotFence::parse("760:760:", PgLsn(3000)).unwrap()),
        },
    )
    .unwrap();
    let after = engine
        .dispatch(&insert(orders, 33, "north", 9, at(3100, 770)))
        .unwrap();
    assert_eq!(fence_reads(&after), 0, "the fence emptied the log");
}

/// A fence installed while a read of a value is outstanding may be newer than
/// that read's snapshot, so it must not drop what that read has to replay.
#[test]
fn a_shared_fence_leaves_a_value_with_an_older_read_outstanding_alone() {
    let (mut engine, orders) = engine();
    let sub = register(&mut engine, MIN_PAID);
    Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    engine.dispatch(&unseen_insert(orders)).unwrap();
    engine.dispatch(&sparse_delete(orders)).unwrap();

    // Newer than the read below, and it sees the 742 row.
    Install::install(
        &mut engine,
        sub,
        FenceInstall {
            fence: Some(PgSnapshotFence::parse("760:760:", PgLsn(3000)).unwrap()),
        },
    )
    .unwrap();

    assert_eq!(
        scalar_value(install_min(&mut engine, sub, 4)),
        Value::Int(2)
    );
}

#[test]
fn the_auto_resolving_engine_serves_a_fence_read_through_its_connector() {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog, "orders").unwrap();
    let mut engine = AutoResolvingEngine::new(
        SubscriptionEngine::<Event, DefaultIds, ParserDB>::new(catalog, PostgreSqlDialect {})
            .with_max_changes_during_aggregate_read(2),
        SyncMode(FencedReads::new(Vec::new())),
    );
    let sub = engine
        .register(SubscriptionRequest::new(7u64, MIN_PAID), ())
        .unwrap()
        .subscription_id;
    Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    let settled = engine
        .apply(&insert(orders, 30, "north", 9, at(2100, 750)))
        .unwrap()
        .resolve_collect();
    settled.reads.unwrap();

    assert_eq!(engine.connector().fence_reads(), 1);
    assert_eq!(engine.connector().calls(), 0, "no query ran");
    assert_eq!(engine.pending_read_count(), 0);
}

const GROUPED_MIN: &str =
    "SELECT region, MIN(amount) FROM orders WHERE status = 'paid' GROUP BY region";

/// A grouped extreme seeded with north holding `rows` rows, the smallest
/// worth `min`, under `cap`.
fn grouped_engine(cap: usize, min: i64, rows: i64) -> (Engine, TableId, u64, Vec<u8>) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog, "orders").unwrap();
    let mut engine: Engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {})
        .with_max_changes_during_aggregate_read(cap);
    let sub = register(&mut engine, GROUPED_MIN);
    let opening = Install::install(
        &mut engine,
        sub,
        GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("north".into()),
                Value::Int(min),
                Value::Int(rows),
            ]],
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    let north = opening.updates[0].group.clone().unwrap().key;
    (engine, orders, sub, north)
}

#[test]
fn a_filling_group_log_asks_for_a_fence_read_which_trims_it_and_the_truncates() {
    let (mut engine, orders, sub, _) = grouped_engine(4, 1, 1);
    let first = engine
        .dispatch(&insert(orders, 30, "north", 9, at(2100, 750)))
        .unwrap();
    assert_eq!(fence_reads(&first), 0);
    let second = engine
        .dispatch(&insert(orders, 31, "north", 9, at(2200, 751)))
        .unwrap();
    assert_eq!(fence_reads(&second), 1, "half the cap asks for a fence");
    for (lsn, xid) in [(2300, 752), (2400, 753)] {
        engine
            .dispatch(&Event::truncate(orders).with_checkpoint(at(lsn, xid)))
            .unwrap();
    }

    Install::install(
        &mut engine,
        sub,
        FenceInstall {
            fence: Some(PgSnapshotFence::parse("760:760:", PgLsn(3000)).unwrap()),
        },
    )
    .unwrap();
    let after = engine
        .dispatch(&insert(orders, 33, "north", 9, at(3100, 770)))
        .unwrap();
    assert_eq!(fence_reads(&after), 0, "the fence emptied both logs");
}

#[test]
fn a_scoped_group_read_after_its_log_overflowed_asks_again() {
    let (mut engine, orders, sub, north) = grouped_engine(2, 5, 2);
    for (id, lsn, xid) in [(30, 2100, 750), (31, 2200, 751), (32, 2300, 752)] {
        engine
            .dispatch(&insert(orders, id, "north", 9, at(lsn, xid)))
            .unwrap();
    }
    engine.dispatch(&sparse_delete_at(orders, 2400)).unwrap();

    let installed = Install::install(
        &mut engine,
        sub,
        GroupedScalarInstall {
            group: north.clone(),
            row: vec![Value::Int(5), Value::Int(4)],
            checkpoint: Some(at(2400, 754)),
            fence: Some(PgSnapshotFence::parse("760:760:", PgLsn(3000)).unwrap()),
        },
    )
    .unwrap();
    assert!(matches!(
        &installed.triggers[..],
        [trigger] if matches!(&trigger.read, ReExecutionRead::GroupedScalar { group, .. } if *group == north)
    ));
}

/// A kept truncate lands between the kept changes around it, so a row the
/// read missed that came after the truncate survives the replay.
#[test]
fn a_scoped_group_read_replays_kept_truncates_in_stream_order() {
    let (mut engine, orders, sub, north) = grouped_engine(4096, 5, 2);
    engine.dispatch(&sparse_delete_at(orders, 1100)).unwrap();
    engine
        .dispatch(&insert(orders, 20, "north", 3, at(1200, 742)))
        .unwrap();
    engine
        .dispatch(&Event::truncate(orders).with_checkpoint(at(2100, 750)))
        .unwrap();
    engine
        .dispatch(&insert(orders, 21, "north", 8, at(2200, 751)))
        .unwrap();

    // The read saw neither 742's row, nor the truncate, nor the row after it.
    let installed = Install::install(
        &mut engine,
        sub,
        GroupedScalarInstall {
            group: north,
            row: vec![Value::Int(5), Value::Int(1)],
            checkpoint: Some(at(1100, 720)),
            fence: Some(read_fence()),
        },
    )
    .unwrap();
    assert!(
        installed.updates.is_empty() && installed.triggers.is_empty(),
        "north holds the 8 inserted after the truncate, got {:?}",
        installed.updates
    );
}

fn sparse_delete_at(orders: TableId, lsn: u64) -> Event {
    Event::delete(
        orders,
        vec![
            Value::Int(21),
            Value::String("north".into()),
            Value::Int(9),
            Value::Missing,
        ],
    )
    .with_pk_columns([0u16])
    .with_checkpoint(at(lsn, if lsn < 2000 { 720 } else { 754 }))
}

#[test]
fn the_async_engine_serves_a_fence_read_through_its_connector() {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let orders = catalog_helpers::table_id::<Postgres, _>(&catalog, "orders").unwrap();
    let mut engine = AutoResolvingEngine::new(
        SubscriptionEngine::<Event, DefaultIds, ParserDB>::new(catalog, PostgreSqlDialect {})
            .with_max_changes_during_aggregate_read(2),
        AsyncMode::new(FencedReads::new(Vec::new())),
    );
    let sub = engine
        .register(SubscriptionRequest::new(7u64, MIN_PAID), ())
        .unwrap()
        .subscription_id;
    Install::install(
        &mut engine,
        sub,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: Some(early_fence()),
        },
    )
    .unwrap();
    drop(
        engine
            .apply(&insert(orders, 30, "north", 9, at(2100, 750)))
            .unwrap(),
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(engine.resolve_collect())
        .unwrap();

    assert_eq!(engine.connector().fence_reads(), 1);
    assert_eq!(engine.connector().calls(), 0, "no query ran");
    assert_eq!(engine.pending_read_count(), 0);
}

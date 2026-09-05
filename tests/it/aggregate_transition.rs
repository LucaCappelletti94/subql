//! Aggregate maintenance failures keep `SubscriptionId`, change to
//! `Tier::WholeRows`, emit one trigger, and leave neighboring subscriptions live.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggregateSeedInstall, AggregateValueChange, DefaultIds, Install,
    InstalledPage, MaintenanceStopReason, PgLsn, SubscriptionEngine, SubscriptionRequest, TableId,
    Tier, TierKind, WholeRowsInstall,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, region TEXT, amount INT, status TEXT);";
const GROUPED_SQL: &str = "SELECT region, COUNT(*) FROM orders GROUP BY region";
const FILTERED_SQL: &str = "SELECT COUNT(*) FROM orders WHERE status = 'paid'";
const BOUND_GROUPED_SQL: &str =
    "SELECT region, COUNT(*) FROM orders WHERE amount > $1 GROUP BY region";

type Event = TestEvent<Postgres, PgLsn>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;

fn engine(group_limit: usize) -> (Engine, TableId) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let table = catalog_helpers::table_id(&catalog, "orders").expect("orders resolves");
    (
        SubscriptionEngine::new(catalog, PostgreSqlDialect {})
            .with_max_groups_per_aggregate(group_limit),
        table,
    )
}

fn row(id: i64, region: &str, status: &str) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::String(region.into()),
        Value::Int(1),
        Value::String(status.into()),
    ]
}

fn insert(table: TableId, id: i64, region: &str, lsn: u64) -> Event {
    TestEvent::insert(table, row(id, region, "paid"))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(lsn))
}

fn seed_group(engine: &mut Engine, subscription: u64, region: &str) {
    let output = Install::install(
        engine,
        subscription,
        AggregateSeedInstall {
            rows: vec![vec![
                Value::String(region.into()),
                Value::Int(1),
                Value::Int(1),
            ]],
            read_at: None,
        },
    )
    .expect("seed group");
    assert_eq!(output.updates.len(), 1);
    assert_eq!(
        output.updates[0].change,
        AggregateValueChange::Set(subql::AggregateResultValue::Folded(
            subql::AggValue::CountStar(1),
        ))
    );
}

fn assert_whole_transition(
    transition: &subql::MaintenanceTransition,
    subscription: u64,
    reason: &MaintenanceStopReason,
) {
    assert_eq!(transition.subscription_id, subscription);
    assert_eq!(transition.from, TierKind::InProcess);
    assert_eq!(&transition.reason, reason);
    match &transition.to {
        Tier::WholeRows { query, tables } => {
            assert!(
                query.sql() == GROUPED_SQL
                    || query.sql() == BOUND_GROUPED_SQL
                    || query.sql() == FILTERED_SQL,
                "original SQL survives"
            );
            assert_eq!(tables.len(), 1);
        }
        other => panic!("expected WholeRows replacement, got {other:?}"),
    }
}

#[test]
fn a_new_group_over_the_limit_changes_the_existing_subscription_to_whole_rows() {
    let (mut engine, orders) = engine(1);
    let registered = engine
        .register(SubscriptionRequest::new(7u64, BOUND_GROUPED_SQL).binds(vec![Value::Int(0)]))
        .expect("grouped count registers");
    seed_group(&mut engine, registered.subscription_id, "north");

    let output = engine
        .dispatch(&insert(orders, 2, "south", 20))
        .expect("dispatch");
    assert!(output.aggregate_updates().is_empty());
    assert_eq!(output.transitions().len(), 1);
    assert_whole_transition(
        &output.transitions()[0],
        registered.subscription_id,
        &MaintenanceStopReason::GroupLimit { limit: 1 },
    );
    let Tier::WholeRows { query, .. } = &output.transitions()[0].to else {
        panic!("expected whole-row transition")
    };
    assert_eq!(query.sql(), BOUND_GROUPED_SQL);
    assert_eq!(query.binds(), &[Value::Int(0)]);
    assert_eq!(output.triggers().len(), 1);
    assert_eq!(
        output.triggers()[0].subscription_id,
        registered.subscription_id
    );

    let installed = Install::install(
        &mut engine,
        registered.subscription_id,
        WholeRowsInstall {
            generation: 1,
            pages: vec![InstalledPage {
                columns: vec!["region".into(), "count".into()],
                rows: Vec::new(),
                more: false,
                checkpoint: Some(PgLsn(20)),
            }],
        },
    )
    .expect("the same id now accepts a whole-row result");
    assert_eq!(installed[0].subscription_id, registered.subscription_id);

    let next = engine
        .dispatch(&insert(orders, 3, "east", 30))
        .expect("dispatch");
    assert!(next.transitions().is_empty(), "the tier changed only once");
    assert!(next
        .triggers()
        .iter()
        .any(|trigger| trigger.subscription_id == registered.subscription_id));
}

#[test]
fn seed_rows_over_the_group_limit_transition_before_any_map_is_installed() {
    let (mut engine, _) = engine(1);
    let registered = engine
        .register(SubscriptionRequest::new(7u64, GROUPED_SQL))
        .expect("grouped count registers");

    let output = Install::install(
        &mut engine,
        registered.subscription_id,
        AggregateSeedInstall {
            rows: vec![
                vec![Value::String("north".into()), Value::Int(1), Value::Int(1)],
                vec![Value::String("south".into()), Value::Int(1), Value::Int(1)],
            ],
            read_at: None,
        },
    )
    .expect("the limit changes tier rather than failing installation");

    assert!(output.updates.is_empty());
    assert_eq!(output.transitions.len(), 1);
    assert_whole_transition(
        &output.transitions[0],
        registered.subscription_id,
        &MaintenanceStopReason::GroupLimit { limit: 1 },
    );
    assert_eq!(
        output.triggers[0].subscription_id,
        registered.subscription_id
    );
}

#[test]
fn missing_old_row_transitions_only_the_filtered_aggregate() {
    let (mut engine, orders) = engine(8);
    let row_subscription = engine
        .register(SubscriptionRequest::new(
            99u64,
            "SELECT * FROM orders WHERE id > 0",
        ))
        .expect("row subscription registers");
    let aggregate = engine
        .register(SubscriptionRequest::new(7u64, FILTERED_SQL))
        .expect("filtered count registers");
    let seeded = Install::install(
        &mut engine,
        aggregate.subscription_id,
        AggregateSeedInstall {
            rows: vec![vec![Value::Int(1)]],
            read_at: None,
        },
    )
    .expect("seed aggregate");
    assert_eq!(seeded.updates.len(), 1);

    let event = TestEvent::update(orders, Vec::new(), row(1, "north", "paid"))
        .with_pk_columns([0u16])
        .with_changed_columns([3u16])
        .with_checkpoint(PgLsn(40));
    let output = engine
        .dispatch(&event)
        .expect("neighboring row delivery survives");

    assert!(
        output.notified().contains(&99),
        "row subscription still receives the update"
    );
    assert!(
        output.aggregate_updates().is_empty(),
        "no wrong count is reported"
    );
    assert_eq!(output.transitions().len(), 1);
    assert_whole_transition(
        &output.transitions()[0],
        aggregate.subscription_id,
        &MaintenanceStopReason::MissingOldRow { table_id: orders },
    );
    assert_eq!(
        output.triggers()[0].subscription_id,
        aggregate.subscription_id
    );
    assert_ne!(row_subscription.subscription_id, aggregate.subscription_id);
}

/// An UPDATE whose new image omits a column the aggregate's filter reads
/// stops maintenance instead of applying a one-sided delta.
///
/// The worst shape in this whole family. The old image is complete, so
/// the old row is evaluated and contributes its removal; the new image
/// omits `status`, so the new evaluation cannot say whether the row
/// still belongs. Applying the removal alone leaves the count one too
/// low **forever**: nothing later corrects it, because every subsequent
/// event folds from the corrupted total. A refused evaluation already
/// drops its deltas here (`refused_subscriptions`), and an unanswerable
/// cell has to do the same, except that a read *can* answer it, so the
/// subscription changes tier and gets a trigger rather than only a
/// report.
#[test]
fn an_unanswerable_filter_cell_stops_maintenance_instead_of_half_folding() {
    let (mut engine, orders) = engine(8);
    let row_subscription = engine
        .register(SubscriptionRequest::new(
            99u64,
            "SELECT * FROM orders WHERE id > 0",
        ))
        .expect("row subscription registers");
    let aggregate = engine
        .register(SubscriptionRequest::new(7u64, FILTERED_SQL))
        .expect("filtered count registers");
    Install::install(
        &mut engine,
        aggregate.subscription_id,
        AggregateSeedInstall {
            rows: vec![vec![Value::Int(1)]],
            read_at: None,
        },
    )
    .expect("seed aggregate");

    // The old image is complete and matches `status = 'paid'`. The new
    // image carries every column except `status`, which is exactly what
    // an unchanged TOASTed column looks like off the wire.
    let mut new_row = row(1, "north", "paid");
    new_row[3] = Value::Missing;
    // No `changed_columns`: the trait documents them as a hint sources
    // vary on, and Maxwell reports none at all, so the aggregate has to
    // consider every candidate and actually evaluate the filter. An
    // event that *did* name only `region` as changed would rightly skip
    // this aggregate, because the source would be asserting that
    // `status` did not move.
    let event = TestEvent::update(orders, row(1, "north", "paid"), new_row)
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(41));
    let output = engine.dispatch(&event).expect("dispatch succeeds");

    assert!(
        output.aggregate_updates().is_empty(),
        "a half-folded count is never reported: {:?}",
        output.aggregate_updates()
    );
    assert_eq!(
        output.transitions().len(),
        1,
        "the aggregate leaves in-process maintenance, because a read can answer this"
    );
    assert_eq!(
        output.transitions()[0].subscription_id,
        aggregate.subscription_id
    );
    assert_eq!(
        output.transitions()[0].reason,
        MaintenanceStopReason::FilterCellMissing {
            table_id: orders,
            column: 3,
        },
        "the reason names the column, so the caller knows what the event lacked"
    );
    assert_eq!(
        output.triggers()[0].subscription_id,
        aggregate.subscription_id,
        "and a re-execution trigger, since the total has to be re-seeded"
    );
    assert!(
        output.notified().contains(&99),
        "the row subscription beside it is answered as usual"
    );
    assert_ne!(row_subscription.subscription_id, aggregate.subscription_id);
}

#[test]
fn unfiltered_count_needs_no_old_row_and_stays_in_process() {
    let (mut engine, orders) = engine(8);
    let aggregate = engine
        .register(SubscriptionRequest::new(
            7u64,
            "SELECT COUNT(*) FROM orders",
        ))
        .expect("count registers");
    Install::install(
        &mut engine,
        aggregate.subscription_id,
        AggregateSeedInstall {
            rows: vec![vec![Value::Int(1)]],
            read_at: None,
        },
    )
    .expect("seed aggregate");

    let event = TestEvent::update(orders, Vec::new(), row(1, "north", "paid"))
        .with_pk_columns([0u16])
        .with_changed_columns([3u16])
        .with_checkpoint(PgLsn(50));
    let output = engine.dispatch(&event).expect("dispatch");
    assert_eq!(output.transitions(), []);
    assert!(output.triggers().is_empty());
}

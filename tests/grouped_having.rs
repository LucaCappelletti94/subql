//! A grouped fold with a fast-path `HAVING` announces only passing groups,
//! keeps every group's numbers either way, and emits exactly the entering
//! and leaving deltas when a group crosses the threshold.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, AggregateSeedInstall, AggregateValueChange, DefaultIds, Install,
    PgLsn, SubscriptionEngine, SubscriptionRequest, TableId, Tier,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, region TEXT, amount INT, status TEXT);";

type Event = TestEvent<Postgres, PgLsn>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;

fn engine() -> (Engine, TableId) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let table = catalog_helpers::table_id(&catalog, "orders").expect("orders resolves");
    (
        SubscriptionEngine::new(catalog, PostgreSqlDialect {}),
        table,
    )
}

/// Register `sql` expecting the in-process fold tier, returning the seed
/// bundle so a test can pin what the caller is told to run.
fn register_fold(engine: &mut Engine, sql: &str) -> (u64, subql::AggregateBootstrap) {
    let registered = engine
        .register(SubscriptionRequest::new(7u64, sql))
        .unwrap_or_else(|e| panic!("`{sql}` should register as a fold, got {e:?}"));
    let Tier::InProcess(served) = registered.tier else {
        panic!(
            "`{sql}` should be an in-process fold, got {:?}",
            registered.tier
        )
    };
    (
        registered.subscription_id,
        served
            .aggregate_bootstrap
            .expect("a fold reports the query that seeds it"),
    )
}

fn row(id: i64, region: &str, amount: Value<Postgres>) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::String(region.into()),
        amount,
        Value::String("paid".into()),
    ]
}

fn insert(table: TableId, id: i64, region: &str, amount: i64, lsn: u64) -> Event {
    TestEvent::insert(table, row(id, region, Value::Int(amount)))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(lsn))
}

fn delete(table: TableId, id: i64, region: &str, amount: i64, lsn: u64) -> Event {
    TestEvent::delete(table, row(id, region, Value::Int(amount)))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(lsn))
}

fn seed(
    engine: &mut Engine,
    subscription: u64,
    rows: Vec<Vec<Value<Postgres>>>,
) -> Vec<subql::AggregateValueUpdate<DefaultIds>> {
    Install::install(
        engine,
        subscription,
        AggregateSeedInstall {
            rows,
            read_at: Some(PgLsn(5)),
        },
    )
    .expect("grouped seed rows install together")
    .updates
}

const fn folded(value: AggValue) -> AggregateValueChange<Postgres> {
    AggregateValueChange::Set(subql::AggregateResultValue::Folded(value))
}

// ---- registration ----

#[test]
fn a_having_over_the_projected_aggregate_registers_in_process() {
    let (mut engine, _) = engine();
    let (_, bootstrap) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) > 10",
    );
    assert!(
        !bootstrap.sql.to_uppercase().contains("HAVING"),
        "the seed must fetch every group, hidden ones included: {}",
        bootstrap.sql
    );
    assert!(
        bootstrap.sql.contains("AS c3"),
        "a SUM subject widens: its running value alone cannot express the \
         NULL an all-null group sums to: {}",
        bootstrap.sql
    );
}

#[test]
fn a_sibling_having_widens_the_seed_components() {
    let (mut engine, _) = engine();
    let (_, bootstrap) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING AVG(amount) > 3",
    );
    assert!(!bootstrap.sql.to_uppercase().contains("HAVING"));
    assert!(
        bootstrap.sql.contains("AS c3"),
        "a sibling HAVING seeds sum, sum of squares, count and the row count: {}",
        bootstrap.sql
    );
}

/// The mirrored spelling binds the comparison the right way around: a sum of
/// 20 satisfies `10 < SUM(amount)`, and a backwards mirror would hide it.
#[test]
fn a_reversed_operand_having_compares_the_right_way_around() {
    let (mut engine, _) = engine();
    let (subscription, _) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING 10 < SUM(amount)",
    );
    let opening = seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Float(20.0),
            Value::Float(400.0),
            Value::Int(1),
            Value::Int(1),
        ]],
    );
    assert_eq!(opening.len(), 1, "sum 20 is greater than 10");
    assert_eq!(opening[0].change, folded(AggValue::Sum(20.0)));
}

#[test]
fn a_count_star_having_needs_no_widening() {
    let (mut engine, _) = engine();
    let (_, bootstrap) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING COUNT(*) > 2",
    );
    assert!(!bootstrap.sql.to_uppercase().contains("HAVING"));
    assert!(
        !bootstrap.sql.contains("AS c2"),
        "COUNT(*) reads the row count every grouped fold already seeds: {}",
        bootstrap.sql
    );
}

/// Shapes outside the fast path keep today's behaviour: the fold refuses
/// them and the whole re-read capture answers instead.
#[test]
fn a_having_outside_the_fast_path_rides_the_capture() {
    let refused = [
        // A different column than the projection aggregates.
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(id) > 1",
        // No grouping: the ungrouped shape stays on the capture.
        "SELECT SUM(amount) FROM orders HAVING SUM(amount) > 1",
        // A grouped row subscription stays refused with or without HAVING.
        "SELECT region FROM orders GROUP BY region HAVING COUNT(*) > 1",
        // Combinations are not a single comparison.
        "SELECT region, SUM(amount) FROM orders GROUP BY region \
         HAVING SUM(amount) > 1 AND COUNT(*) > 2",
        // The compared value must be a constant.
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) > id",
        // A NULL threshold can never be crossed.
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) > NULL",
        // COUNT(*) aggregates no column, so it has no siblings.
        "SELECT region, COUNT(*) FROM orders GROUP BY region HAVING SUM(amount) > 1",
        // Siblings exist only over a numeric column.
        "SELECT region, COUNT(status) FROM orders GROUP BY region HAVING SUM(status) > 1",
    ];
    for sql in refused {
        let (mut engine, _) = engine();
        let registered = engine
            .register(SubscriptionRequest::new(7u64, sql))
            .unwrap_or_else(|e| panic!("`{sql}` should still register somewhere, got {e:?}"));
        assert!(
            matches!(registered.tier, Tier::WholeRows { .. }),
            "`{sql}` should ride the whole re-read, got {:?}",
            registered.tier
        );
    }
}

// ---- crossing ----

#[test]
fn a_group_crossing_the_threshold_emits_entering_and_leaving() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) > 10",
    );
    let opening = seed(
        &mut engine,
        subscription,
        vec![
            vec![
                Value::String("north".into()),
                Value::Float(8.0),
                Value::Float(64.0),
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                Value::String("south".into()),
                Value::Float(20.0),
                Value::Float(400.0),
                Value::Int(1),
                Value::Int(1),
            ],
        ],
    );
    assert_eq!(opening.len(), 1, "only the passing group is announced");
    assert_eq!(opening[0].change, folded(AggValue::Sum(20.0)));
    let south = opening[0].group.clone().expect("south key");

    let entering = engine
        .aggregate_updates(&insert(orders, 3, "north", 5, 10))
        .expect("insert folds");
    assert_eq!(entering.updates.len(), 1, "south stays silent");
    assert_ne!(entering.updates[0].group.as_deref(), Some(south.as_slice()));
    assert_eq!(entering.updates[0].change, folded(AggValue::Sum(13.0)));

    let leaving = engine
        .aggregate_updates(&delete(orders, 3, "north", 5, 20))
        .expect("delete folds");
    assert_eq!(leaving.updates.len(), 1);
    assert_eq!(leaving.updates[0].change, AggregateValueChange::Remove);
}

#[test]
fn a_hidden_group_folds_in_silence_and_enters_with_the_full_sum() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) > 10",
    );
    let opening = seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Float(2.0),
            Value::Float(4.0),
            Value::Int(1),
            Value::Int(1),
        ]],
    );
    assert!(opening.is_empty(), "a failing group is installed silently");

    let silent = engine
        .aggregate_updates(&insert(orders, 2, "north", 3, 10))
        .expect("insert folds");
    assert!(silent.updates.is_empty(), "still below the threshold");

    let entering = engine
        .aggregate_updates(&insert(orders, 3, "north", 6, 20))
        .expect("insert folds");
    assert_eq!(
        entering.updates[0].change,
        folded(AggValue::Sum(11.0)),
        "the entering value carries every silently folded row"
    );
}

#[test]
fn a_hidden_group_that_empties_stays_silent() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) > 10",
    );
    seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Float(2.0),
            Value::Float(4.0),
            Value::Int(1),
            Value::Int(1),
        ]],
    );
    let emptied = engine
        .aggregate_updates(&delete(orders, 1, "north", 2, 10))
        .expect("delete folds");
    assert!(
        emptied.updates.is_empty(),
        "a group never announced must not announce its removal"
    );
}

#[test]
fn entering_without_a_value_change_still_emits() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING COUNT(*) > 2",
    );
    seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Float(7.0),
            Value::Int(2),
        ]],
    );
    let entering = engine
        .aggregate_updates(&insert(orders, 3, "north", 0, 10))
        .expect("insert folds");
    assert_eq!(
        entering.updates[0].change,
        folded(AggValue::Sum(7.0)),
        "the third row crosses the row-count threshold without moving the sum"
    );
}

#[test]
fn a_sibling_having_evaluates_from_widened_components() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING AVG(amount) > 3",
    );
    // Widened layout: sum, sum of squares, non-null count, then the row count.
    let opening = seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Float(8.0),
            Value::Float(34.0),
            Value::Int(2),
            Value::Int(2),
        ]],
    );
    assert_eq!(opening.len(), 1, "average 4 passes");
    assert_eq!(opening[0].change, folded(AggValue::Sum(8.0)));

    let leaving = engine
        .aggregate_updates(&delete(orders, 1, "north", 5, 10))
        .expect("delete folds");
    assert_eq!(
        leaving.updates[0].change,
        AggregateValueChange::Remove,
        "sum 3 over one row is average 3, which no longer passes"
    );
}

#[test]
fn an_unknown_average_stays_outside_the_result() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register_fold(
        &mut engine,
        "SELECT region, AVG(amount) FROM orders GROUP BY region HAVING AVG(amount) > 3",
    );
    seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Float(0.0),
            Value::Int(0),
            Value::Int(1),
        ]],
    );
    let event = TestEvent::insert(orders, row(2, "north", Value::Null))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(10));
    let output = engine.aggregate_updates(&event).expect("insert folds");
    assert!(
        output.updates.is_empty(),
        "an all-null average is unknown, and unknown never passes"
    );
}

#[test]
fn hidden_groups_count_against_the_group_budget() {
    let (engine, orders) = engine();
    let mut engine = engine.with_max_groups_per_aggregate(1);
    let (subscription, _) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) > 10",
    );
    seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Float(20.0),
            Value::Float(400.0),
            Value::Int(1),
            Value::Int(1),
        ]],
    );
    let output = engine
        .aggregate_updates(&insert(orders, 2, "south", 1, 10))
        .expect("the budget transition dispatches");
    assert_eq!(
        output.transitions.len(),
        1,
        "a hidden group still occupies a budget slot"
    );
}

#[test]
fn a_keyed_shape_with_having_rides_the_whole_reread() {
    let (mut engine, _) = engine();
    let registered = engine
        .register(SubscriptionRequest::new(
            7u64,
            "SELECT * FROM orders WHERE lower(status) = 'paid' HAVING COUNT(*) > 1",
        ))
        .expect("the capture answers");
    assert!(
        matches!(registered.tier, Tier::WholeRows { .. }),
        "a keyed read asks about single rows and cannot honour HAVING, got {:?}",
        registered.tier
    );
}

#[test]
fn a_sibling_average_counts_its_contributions() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING AVG(amount) > 3",
    );
    seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Float(8.0),
            Value::Float(34.0),
            Value::Int(2),
            Value::Int(2),
        ]],
    );
    // Deleting the amount-3 row leaves sum 5 over one contribution, average
    // 5, still passing. A delta that failed to shrink the contribution count
    // would compute average 2.5 and wrongly announce a removal.
    let output = engine
        .aggregate_updates(&delete(orders, 1, "north", 3, 10))
        .expect("delete folds");
    assert_eq!(output.updates.len(), 1);
    assert_eq!(output.updates[0].change, folded(AggValue::Sum(5.0)));
}

#[test]
fn a_filtered_and_an_unfiltered_fold_do_not_share_an_identity() {
    let (mut engine, _) = engine();
    let (unfiltered, _) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region",
    );
    let (filtered, _) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) > 10",
    );
    assert_ne!(
        unfiltered, filtered,
        "the same consumer registering the filtered twin must get its own subscription"
    );
}

/// SQL's `SUM` over only NULLs is NULL, and a comparison over NULL is
/// UNKNOWN, so the group sits outside the result. `Sum`'s running value
/// alone cannot say this (an empty sum reads 0.0), which is why a `SUM`
/// subject widens to carry its contribution count.
#[test]
fn an_all_null_sum_stays_outside_the_result() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) < 10",
    );
    // Two rows, both with NULL amounts: the database seeds SUM NULL,
    // SUM(x*x) NULL, zero contributions, two source rows.
    let opening = seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Null,
            Value::Null,
            Value::Int(0),
            Value::Int(2),
        ]],
    );
    assert!(
        opening.is_empty(),
        "an unknown sum never passes, got {opening:?}"
    );

    let another_null = TestEvent::insert(orders, row(3, "north", Value::Null))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(10));
    let output = engine
        .aggregate_updates(&another_null)
        .expect("insert folds");
    assert!(
        output.updates.is_empty(),
        "the sum is still unknown, got {:?}",
        output.updates
    );
}

/// `TRUNCATE` says goodbye only to announced groups: a hidden group was
/// never delivered, so its disappearance announces nothing.
#[test]
fn truncate_removes_only_announced_groups() {
    let (mut engine, orders) = engine();
    let (subscription, _) = register_fold(
        &mut engine,
        "SELECT region, SUM(amount) FROM orders GROUP BY region HAVING SUM(amount) > 10",
    );
    let opening = seed(
        &mut engine,
        subscription,
        vec![
            vec![
                Value::String("north".into()),
                Value::Float(2.0),
                Value::Float(4.0),
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                Value::String("south".into()),
                Value::Float(20.0),
                Value::Float(400.0),
                Value::Int(1),
                Value::Int(1),
            ],
        ],
    );
    assert_eq!(opening.len(), 1);
    let south = opening[0].group.clone().expect("south key");

    let truncated = TestEvent::truncate(orders).with_checkpoint(PgLsn(10));
    let output = engine
        .aggregate_updates(&truncated)
        .expect("truncate empties");
    assert_eq!(output.updates.len(), 1, "north was never announced");
    assert_eq!(output.updates[0].group.as_deref(), Some(south.as_slice()));
    assert_eq!(output.updates[0].change, AggregateValueChange::Remove);
}

/// Two group columns and a `HAVING`: the key spans both columns and the
/// condition still gates announcement.
#[test]
fn a_multi_column_group_crosses_like_a_single_one() {
    let (mut engine, orders) = engine();
    let registered = engine
        .register(SubscriptionRequest::new(
            7u64,
            "SELECT region, status, SUM(amount) FROM orders \
             GROUP BY region, status HAVING SUM(amount) > 10",
        ))
        .expect("two group columns register");
    let Tier::InProcess(served) = registered.tier else {
        panic!("expected a fold, got {:?}", registered.tier)
    };
    let bootstrap = served.aggregate_bootstrap.expect("a fold seeds");
    assert_eq!(bootstrap.group_columns, 2);
    let opening = seed(
        &mut engine,
        registered.subscription_id,
        vec![vec![
            Value::String("north".into()),
            Value::String("paid".into()),
            Value::Float(8.0),
            Value::Float(64.0),
            Value::Int(1),
            Value::Int(1),
        ]],
    );
    assert!(opening.is_empty(), "sum 8 stays hidden");
    let entering = engine
        .aggregate_updates(&insert(orders, 3, "north", 5, 10))
        .expect("insert folds");
    assert_eq!(
        entering.updates[0].change,
        folded(AggValue::Sum(13.0)),
        "the north-paid group crosses on the fifth added unit"
    );
}

//! A non-finite float folds, because the engine's own answer says so.
//!
//! `Infinity` and `NaN` were routed to `AggCellRead::NonNumeric` and then
//! skipped, so the row changed the server's answer and changed nothing in
//! process. Over the rows `1.0` and `Infinity` subql reported `Sum(1.0)`
//! and then reported nothing at all, where PostgreSQL answers `Infinity`.
//!
//! Measured 2026-09-05 on PostgreSQL 16.15, MySQL 8.4.11 and SQLite
//! 3.51.1, over a `double precision` column holding `1.0` and one
//! non-finite value:
//!
//! ```text
//! aggregate    pg with Infinity   pg with NaN   sqlite with Infinity
//! SUM          Infinity           NaN           Inf
//! AVG          Infinity           NaN           Inf
//! COUNT(col)   2                  2             2
//! VAR_POP      NaN                NaN           -
//! STDDEV_POP   NaN                NaN           -
//! ```
//!
//! And `1.0`, `NaN`, `-Infinity` together sum to `NaN` on PostgreSQL, so
//! `NaN` wins over an infinity of either sign, which is IEEE arithmetic
//! rather than a rule of its own. Nothing here needs a
//! `MaintenanceStopReason`: every one of those answers is representable in
//! `f64`, which is what the float family already holds.
//!
//! MySQL never delivers one. It refuses the value while parsing the
//! statement, `ERROR 1367 (22007) Illegal double '1e400' value found
//! during parsing`, which is the plan's expectation with a different
//! message than it recorded, so `mysql_refuses_a_non_finite_double`
//! asserts the refusal itself against a real server.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Backend, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, DefaultIds, NumericValue, PgLsn, SubscriptionEngine,
    SubscriptionRequest, TableId,
};

const PG_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, approx DOUBLE PRECISION)";
const SQLITE_DDL: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, approx REAL)";

/// A backend on the standard carriers.
trait Folding:
    Backend<Int = i64, Float = f64, Decimal = bigdecimal::BigDecimal> + subql::compiler::SqlLiteralParse
{
}

impl<B> Folding for B where
    B: Backend<Int = i64, Float = f64, Decimal = bigdecimal::BigDecimal>
        + subql::compiler::SqlLiteralParse
{
}

/// Register `sql`, seed it empty, then fold `values` in order and answer
/// the last value the engine reported.
fn folded<B, D>(
    ddl: &str,
    dialect: D,
    sql: &str,
    values: &[f64],
    components: usize,
) -> Option<AggValue>
where
    B: Folding<Dialect = D>,
    D: sqlparser::dialect::Dialect + Default,
{
    let database = ParserDB::parse::<D>(ddl).unwrap();
    let table: TableId = catalog_helpers::table_id(&database, "t").unwrap();
    let mut engine: SubscriptionEngine<TestEvent<B, PgLsn>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, dialect);
    let registered = engine.register(SubscriptionRequest::new(7, sql)).unwrap();
    assert!(
        registered.not_served_because.is_none(),
        "this aggregate is served in process, and it was refused"
    );
    let mut seed: Vec<Value<B>> = (0..components).map(|_| Value::Null).collect();
    if let Some(last) = seed.last_mut() {
        *last = Value::Int(0);
    }
    subql::Install::install(
        &mut engine,
        registered.subscription_id,
        subql::AggregateSeedInstall {
            rows: vec![seed],
            read_at: Some(PgLsn(5)),
        },
    )
    .expect("the empty seed lands");

    let mut last = None;
    for (index, value) in values.iter().enumerate() {
        let lsn = 10 + u64::try_from(index).unwrap() * 10;
        let event = TestEvent::insert(
            table,
            vec![
                Value::Int(i64::try_from(lsn).unwrap()),
                Value::Float(*value),
            ],
        )
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(lsn));
        let output = engine.aggregate_updates(&event).expect("the event folds");
        assert!(
            output.transitions.is_empty(),
            "a non-finite value is representable, so no tier changes"
        );
        if let Some(value) = output
            .updates
            .first()
            .and_then(subql::AggregateValueUpdate::folded_value)
        {
            last = Some(value);
        }
    }
    last
}

fn pg(sql: &str, values: &[f64], components: usize) -> Option<AggValue> {
    folded::<Postgres, _>(PG_DDL, PostgreSqlDialect {}, sql, values, components)
}

/// The headline: an infinity reaches the fold and the total is infinite,
/// which is what the server answers.
#[test]
fn pg_sum_of_infinity_is_infinity() {
    assert_eq!(
        pg("SELECT SUM(approx) FROM t", &[1.0, f64::INFINITY], 2),
        Some(AggValue::Sum(Some(NumericValue::Double(f64::INFINITY)))),
        "measured: PostgreSQL answers Infinity, not 1"
    );
}

/// And a `NaN` makes the total `NaN` rather than leaving the previous
/// number standing.
#[test]
fn pg_sum_with_a_nan_is_nan() {
    let total = pg("SELECT SUM(approx) FROM t", &[1.0, f64::NAN], 2);
    let Some(AggValue::Sum(Some(NumericValue::Double(value)))) = total else {
        panic!("expected a double total, got {total:?}")
    };
    assert!(
        value.is_nan(),
        "measured: PostgreSQL answers NaN, got {value}"
    );
}

/// `NaN` wins over an infinity of either sign, which is IEEE arithmetic
/// and is what the server answers for the three rows together.
#[test]
fn pg_nan_wins_over_an_infinity() {
    let total = pg(
        "SELECT SUM(approx) FROM t",
        &[1.0, f64::NAN, f64::NEG_INFINITY],
        2,
    );
    let Some(AggValue::Sum(Some(NumericValue::Double(value)))) = total else {
        panic!("expected a double total, got {total:?}")
    };
    assert!(value.is_nan(), "measured: 1, NaN, -Infinity sums to NaN");
}

/// The mean follows the total, measured.
#[test]
fn pg_avg_of_infinity_is_infinity() {
    assert_eq!(
        pg("SELECT AVG(approx) FROM t", &[1.0, f64::INFINITY], 2),
        Some(AggValue::Avg(Some(NumericValue::Double(f64::INFINITY)))),
        "measured: PostgreSQL answers Infinity"
    );
}

/// A non-finite value still counts, since it is a value and not a NULL.
#[test]
fn pg_count_includes_a_non_finite_value() {
    assert_eq!(
        pg("SELECT COUNT(approx) FROM t", &[1.0, f64::INFINITY], 1),
        Some(AggValue::CountColumn(2)),
        "measured: count(f) is 2"
    );
}

/// The variance family answers `NaN` over an infinity, measured, which is
/// the difference of two infinities.
#[test]
fn pg_variance_with_an_infinity_is_nan() {
    let value = pg("SELECT VAR_POP(approx) FROM t", &[1.0, f64::INFINITY], 3);
    let Some(AggValue::VarPop(Some(value))) = value else {
        panic!("expected a real, got {value:?}")
    };
    assert!(
        value.is_nan(),
        "measured: var_pop over 1 and Infinity is NaN, got {value}"
    );
}

/// SQLite folds it too, answering `Inf`.
#[test]
fn sqlite_folds_infinity() {
    assert_eq!(
        folded::<SQLite, _>(
            SQLITE_DDL,
            SQLiteDialect {},
            "SELECT SUM(approx) FROM t",
            &[1.0, f64::INFINITY],
            2,
        ),
        Some(AggValue::Sum(Some(NumericValue::Double(f64::INFINITY)))),
        "measured: SQLite answers Inf"
    );
}

/// MySQL never delivers a non-finite double, because it refuses the value
/// while parsing the statement. Asserted against a real server, since
/// that is the only place the refusal exists.
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-postgres-r2d2",
    feature = "executor-diesel-mysql",
    feature = "executor-diesel-async-mysql",
    feature = "diesel-typed-mysql",
    feature = "apply-patchset-mysql",
    feature = "apply-patchset-mysql-async",
))]
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn mysql_refuses_a_non_finite_double() {
    use diesel::connection::SimpleConnection as _;

    let container = crate::common::mysql_8();
    let port = crate::common::mysql_port(&container);
    let mut connection = crate::common::mysql_connect(port);
    connection
        .batch_execute("CREATE TABLE non_finite (f DOUBLE)")
        .expect("the table is created");
    connection
        .batch_execute("INSERT INTO non_finite VALUES (1.0)")
        .expect("a finite double inserts");
    let refused = connection
        .batch_execute("INSERT INTO non_finite VALUES (1e400)")
        .expect_err("MySQL refuses a non-finite double");
    let message = refused.to_string();
    assert!(
        message.contains("Illegal double"),
        "measured: `Illegal double '1e400' value found during parsing`, got {message}"
    );
}

/// One change in a stream: a row arriving, or the same row leaving.
#[derive(Clone, Copy)]
enum Change {
    Inserted(f64),
    Deleted(f64),
    /// One row's value replaced, which reaches the fold as a single event
    /// carrying both contributions, so the two deltas merge before the
    /// total sees either.
    Updated(f64, f64),
}

/// Fold `changes` in order against `sql` and answer the value the engine
/// last reported, so a removal can be asked about as well as an arrival.
fn pg_stream(sql: &str, changes: &[Change], components: usize) -> Option<AggValue> {
    let database = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap();
    let table: TableId = catalog_helpers::table_id(&database, "t").unwrap();
    let mut engine: SubscriptionEngine<TestEvent<Postgres, PgLsn>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, PostgreSqlDialect {});
    let registered = engine.register(SubscriptionRequest::new(7, sql)).unwrap();
    assert!(registered.not_served_because.is_none());
    let mut seed: Vec<Value<Postgres>> = (0..components).map(|_| Value::Null).collect();
    if let Some(last) = seed.last_mut() {
        *last = Value::Int(0);
    }
    subql::Install::install(
        &mut engine,
        registered.subscription_id,
        subql::AggregateSeedInstall {
            rows: vec![seed],
            read_at: Some(PgLsn(5)),
        },
    )
    .expect("the empty seed lands");

    let mut last = None;
    for (index, change) in changes.iter().enumerate() {
        let lsn = 10 + u64::try_from(index).unwrap() * 10;
        let row = |value: f64| vec![Value::Int(i64::try_from(lsn).unwrap()), Value::Float(value)];
        let event = match *change {
            Change::Inserted(value) => TestEvent::insert(table, row(value)),
            Change::Deleted(value) => TestEvent::delete(table, row(value)),
            Change::Updated(old, new) => TestEvent::update(table, row(old), row(new)),
        }
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(lsn));
        let output = engine.aggregate_updates(&event).expect("the event folds");
        assert!(
            output.transitions.is_empty(),
            "every answer here is representable, so no tier changes"
        );
        if let Some(value) = output
            .updates
            .first()
            .and_then(subql::AggregateValueUpdate::folded_value)
        {
            last = Some(value);
        }
    }
    last
}

/// A non-finite contribution that leaves takes its effect with it.
///
/// The delta path represents a removal as the negated value, so removing
/// an `Infinity` computed `Infinity + -Infinity`, which is `NaN`, and no
/// later row escaped it. Measured on PostgreSQL 16.15 with a
/// `double precision` column:
///
/// ```text
/// rows                              SUM        AVG       VAR_POP
/// Infinity, 1, 2                    Infinity   Infinity  NaN
/// after the Infinity is deleted     3          1.5       0.25
/// ```
///
/// So the engine simply answers over the rows that remain, and an
/// in-process total has to as well.
#[test]
fn pg_sum_recovers_when_an_infinity_leaves() {
    assert_eq!(
        pg_stream(
            "SELECT SUM(approx) FROM t",
            &[
                Change::Inserted(f64::INFINITY),
                Change::Inserted(1.0),
                Change::Inserted(2.0),
                Change::Deleted(f64::INFINITY),
            ],
            2,
        ),
        Some(AggValue::Sum(Some(NumericValue::Double(3.0)))),
        "measured: PostgreSQL answers 3 over the rows that remain"
    );
}

/// A `NaN` that leaves does the same, and this is the direction the old
/// arithmetic could not express at all: `NaN + -NaN` is `NaN`.
#[test]
fn pg_sum_recovers_when_a_nan_leaves() {
    assert_eq!(
        pg_stream(
            "SELECT SUM(approx) FROM t",
            &[
                Change::Inserted(f64::NAN),
                Change::Inserted(1.0),
                Change::Inserted(2.0),
                Change::Deleted(f64::NAN),
            ],
            2,
        ),
        Some(AggValue::Sum(Some(NumericValue::Double(3.0)))),
        "measured: PostgreSQL answers 3, and NaN cannot be subtracted back out"
    );
}

/// Two infinities, one leaving: the total is still infinite, because the
/// other one still stands.
///
/// Measured: `Infinity, Infinity, 1` sums to `Infinity`, and after one
/// infinity is deleted it is still `Infinity`. So the state is a count of
/// live non-finite contributions, not a flag.
#[test]
fn pg_one_of_two_infinities_leaving_stays_infinite() {
    assert_eq!(
        pg_stream(
            "SELECT SUM(approx) FROM t",
            &[
                Change::Inserted(f64::INFINITY),
                Change::Inserted(f64::INFINITY),
                Change::Inserted(1.0),
                Change::Deleted(f64::INFINITY),
            ],
            2,
        ),
        Some(AggValue::Sum(Some(NumericValue::Double(f64::INFINITY)))),
        "measured: PostgreSQL still answers Infinity"
    );
}

/// An infinity of each sign is `NaN` while both are live, and the total
/// recovers when one leaves.
///
/// Measured: `{Infinity, -Infinity}` sums to `NaN` on PostgreSQL, which
/// is IEEE, and deleting the negative one leaves `Infinity`.
#[test]
fn pg_opposite_infinities_are_nan_until_one_leaves() {
    let both = pg_stream(
        "SELECT SUM(approx) FROM t",
        &[
            Change::Inserted(f64::INFINITY),
            Change::Inserted(f64::NEG_INFINITY),
        ],
        2,
    );
    let Some(AggValue::Sum(Some(NumericValue::Double(value)))) = both else {
        panic!("expected a double total, got {both:?}")
    };
    assert!(value.is_nan(), "both signs live is NaN, measured");

    assert_eq!(
        pg_stream(
            "SELECT SUM(approx) FROM t",
            &[
                Change::Inserted(f64::INFINITY),
                Change::Inserted(f64::NEG_INFINITY),
                Change::Deleted(f64::NEG_INFINITY),
            ],
            2,
        ),
        Some(AggValue::Sum(Some(NumericValue::Double(f64::INFINITY)))),
        "and one leaving restores the other's infinity"
    );
}

/// The mean recovers too, since it divides the same total.
///
/// Measured: `Infinity, 1, 2` averages to `Infinity`, and after the
/// infinity leaves, to `1.5`.
#[test]
fn pg_mean_recovers_when_an_infinity_leaves() {
    assert_eq!(
        pg_stream(
            "SELECT AVG(approx) FROM t",
            &[
                Change::Inserted(f64::INFINITY),
                Change::Inserted(1.0),
                Change::Inserted(2.0),
                Change::Deleted(f64::INFINITY),
            ],
            2,
        ),
        Some(AggValue::Avg(Some(NumericValue::Double(1.5)))),
        "measured: PostgreSQL answers 1.5"
    );
}

/// The spread recovers as well, so a variance answers over the rows
/// that remain.
///
/// Measured on PostgreSQL 16.15: `Infinity, 1, 2` gives `VAR_POP` of
/// `NaN`, and after the infinity is deleted, `0.25`. The spread
/// accumulates squared deviations, and a non-finite row makes each of
/// them non-finite, so it is the same irreversibility the total had.
#[test]
fn pg_variance_recovers_when_an_infinity_leaves() {
    let after = pg_stream(
        "SELECT VAR_POP(approx) FROM t",
        &[
            Change::Inserted(f64::INFINITY),
            Change::Inserted(1.0),
            Change::Inserted(2.0),
            Change::Deleted(f64::INFINITY),
        ],
        3,
    );
    assert_eq!(
        after,
        Some(AggValue::VarPop(Some(0.25))),
        "measured: PostgreSQL answers 0.25 over the two rows that remain"
    );
}

/// An update away from a non-finite value recovers too, and this is the
/// case where both contributions arrive in one event.
///
/// The two deltas merge before the total sees either, so the merge has to
/// keep the parts apart as well: collapsing them to a number first gives
/// `-Infinity + 4`, which is `-Infinity`. The mutation battery is why
/// this test exists, since deleting and inserting in separate events
/// never exercises the merge.
///
/// Measured on PostgreSQL 16.15: `Infinity, 1, 2` with the infinity
/// updated to `4` answers `SUM` 7, `AVG` 2.3333333333333335 and
/// `VAR_POP` 1.5555555555555556.
#[test]
fn pg_sum_recovers_when_an_infinity_is_updated_away() {
    assert_eq!(
        pg_stream(
            "SELECT SUM(approx) FROM t",
            &[
                Change::Inserted(f64::INFINITY),
                Change::Inserted(1.0),
                Change::Inserted(2.0),
                Change::Updated(f64::INFINITY, 4.0),
            ],
            2,
        ),
        Some(AggValue::Sum(Some(NumericValue::Double(7.0)))),
        "measured: PostgreSQL answers 7"
    );
}

/// And the mean over the same stream, which divides that total.
#[test]
fn pg_mean_recovers_when_an_infinity_is_updated_away() {
    assert_eq!(
        pg_stream(
            "SELECT AVG(approx) FROM t",
            &[
                Change::Inserted(f64::INFINITY),
                Change::Inserted(1.0),
                Change::Inserted(2.0),
                Change::Updated(f64::INFINITY, 4.0),
            ],
            2,
        ),
        Some(AggValue::Avg(Some(NumericValue::Double(
            2.333_333_333_333_333_5
        )))),
        "measured: PostgreSQL answers 2.3333333333333335"
    );
}

//! Variance stops cancelling.
//!
//! `sum_sq / n - (sum / n)^2` is two large numbers subtracted, so at a
//! large mean it keeps almost no significant digits. Over `100000000.0`,
//! `100000001.0` and `100000002.0` in a `double precision` column,
//! `sum(x*x)` is `3.0000000600000004e+16` and the identity answers `2.0`,
//! where the value is `0.6666666666666666`.
//!
//! Measured 2026-09-05 on PostgreSQL 16.15 and MySQL 8.4.11, which agree
//! digit for digit over exactly those three rows:
//!
//! ```text
//! aggregate      three rows            third row deleted   one row
//! VAR_POP        0.6666666666666666    0.25                0
//! VAR_SAMP       1                     -                   NULL
//! STDDEV_POP     0.816496580927726     0.5                 0
//! STDDEV_SAMP    1                     -                   -
//! VAR_POP*COUNT  2                     -                   -
//! ```
//!
//! That last row is the accumulator's own state: the sum of squared
//! deviations, which each server computes stably and can therefore hand
//! back as a seed. So the fold keeps `(count, sum, sum_of_squared_
//! deviations)` and the seed reads the engine's own variance rather than
//! re-deriving it from a sum of squares, which would put the cancellation
//! back at seed time.
#![allow(
    clippy::unwrap_used,
    // Row ids are derived from the measured amounts, which are whole
    // numbers, and every assertion here compares a measured value that
    // the engines answer exactly.
    clippy::cast_possible_truncation,
    clippy::float_cmp
)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};
use subql::backend::{Backend, DivisionPrecisionIncrement, MySql, Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, DefaultIds, PgLsn, SubscriptionEngine, SubscriptionRequest, TableId,
    Tier,
};

const PG_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, x DOUBLE PRECISION)";
const MYSQL_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, x DOUBLE)";

/// The three rows every engine was measured over.
const ROWS: [f64; 3] = [100_000_000.0, 100_000_001.0, 100_000_002.0];

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

struct Folding2<B: Folding> {
    engine: SubscriptionEngine<TestEvent<B, PgLsn>, DefaultIds, ParserDB>,
    table: TableId,
    lsn: u64,
}

impl<B: Folding> Folding2<B> {
    /// Insert one row and answer the value the engine now reports.
    fn insert(&mut self, x: f64) -> Option<AggValue> {
        self.lsn += 10;
        self.dispatch(TestEvent::insert(
            self.table,
            vec![Value::Int(x as i64), Value::Float(x)],
        ))
    }

    /// Delete one row and answer the value the engine now reports.
    fn delete(&mut self, x: f64) -> Option<AggValue> {
        self.lsn += 10;
        self.dispatch(TestEvent::delete(
            self.table,
            vec![Value::Int(x as i64), Value::Float(x)],
        ))
    }

    fn dispatch(&mut self, event: TestEvent<B, PgLsn>) -> Option<AggValue> {
        let event = event
            .with_pk_columns([0u16])
            .with_checkpoint(PgLsn(self.lsn));
        let output = self
            .engine
            .aggregate_updates(&event)
            .expect("the event folds");
        assert!(
            output.transitions.is_empty(),
            "a variance fold reports a number, and this one changed tier"
        );
        output
            .updates
            .first()
            .and_then(subql::AggregateValueUpdate::folded_value)
    }

    /// The real this aggregate reports right now.
    fn real(&mut self, x: f64, inserting: bool) -> f64 {
        let value = if inserting {
            self.insert(x)
        } else {
            self.delete(x)
        };
        let value = match value {
            Some(
                AggValue::VarPop(Some(value))
                | AggValue::VarSamp(Some(value))
                | AggValue::StddevPop(Some(value))
                | AggValue::StddevSamp(Some(value)),
            ) => value,
            other => panic!("the variance family reports a real, got {other:?}"),
        };
        value
    }
}

/// Register `sql`, seed it from `components`, and answer the fold plus the
/// seed query the engine rendered.
fn folding<B, D>(
    ddl: &str,
    dialect: D,
    sql: &str,
    components: Vec<Value<B>>,
    increment: Option<DivisionPrecisionIncrement>,
) -> (Folding2<B>, String)
where
    B: Folding<Dialect = D>,
    D: sqlparser::dialect::Dialect + Default,
{
    let database = ParserDB::parse::<D>(ddl).unwrap();
    let table = catalog_helpers::table_id(&database, "t").unwrap();
    let mut engine: SubscriptionEngine<TestEvent<B, PgLsn>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, dialect);
    if let Some(increment) = increment {
        engine = engine.with_division_precision_increment(increment);
    }
    let registered = engine.register(SubscriptionRequest::new(7, sql)).unwrap();
    let Tier::InProcess(served) = &registered.tier else {
        panic!("a variance aggregate folds in process")
    };
    let seed_sql = served
        .aggregate_bootstrap
        .as_ref()
        .expect("a fold seeds")
        .query
        .sql()
        .to_string();
    subql::Install::install(
        &mut engine,
        registered.subscription_id,
        subql::AggregateSeedInstall {
            rows: vec![components],
            read_at: Some(PgLsn(5)),
        },
    )
    .expect("the seed lands");
    (
        Folding2 {
            engine,
            table,
            lsn: 5,
        },
        seed_sql,
    )
}

/// An empty seed: no sum, no deviation, no rows.
fn empty<B: Folding>() -> Vec<Value<B>> {
    vec![Value::Null, Value::Null, Value::Int(0)]
}

fn pg(sql: &str) -> Folding2<Postgres> {
    folding(PG_DDL, PostgreSqlDialect {}, sql, empty(), None).0
}

/// The finding: at a large mean the answer is not the difference of two
/// large numbers.
#[test]
fn large_mean_small_variance_matches_the_engine() {
    let mut folding = pg("SELECT VAR_POP(x) FROM t");
    folding.insert(ROWS[0]);
    folding.insert(ROWS[1]);
    assert_eq!(
        folding.real(ROWS[2], true),
        0.666_666_666_666_666_6,
        "measured on PostgreSQL 16.15 and MySQL 8.4.11, digit for digit"
    );
}

/// And removal keeps it stable, where subtracting the squares answered
/// zero.
#[test]
fn variance_after_removal_matches_the_engine() {
    let mut folding = pg("SELECT VAR_POP(x) FROM t");
    for row in ROWS {
        folding.insert(row);
    }
    assert_eq!(
        folding.real(ROWS[2], false),
        0.25,
        "measured: the two remaining rows have variance 0.25, not 0"
    );
}

/// A standard deviation takes a square root, so a numerator that rounding
/// pushed below zero would surface as `NaN`. Walking a large-mean set all
/// the way back down never produces one.
#[test]
fn stddev_never_reports_nan() {
    let mut folding = pg("SELECT STDDEV_POP(x) FROM t");
    for row in ROWS {
        folding.insert(row);
    }
    assert_eq!(
        folding.real(ROWS[2], false),
        0.5,
        "measured: two rows one apart have a standard deviation of 0.5"
    );
    let one_row = folding.real(ROWS[1], false);
    assert!(
        !one_row.is_nan() && one_row == 0.0,
        "measured: one row has no spread, so 0 rather than NaN, got {one_row}"
    );
}

/// The sample forms, measured on the same three rows.
#[test]
fn the_sample_forms_match_the_engine() {
    let mut folding = pg("SELECT VAR_SAMP(x) FROM t");
    folding.insert(ROWS[0]);
    folding.insert(ROWS[1]);
    assert_eq!(folding.real(ROWS[2], true), 1.0, "measured: var_samp is 1");

    let mut stddev = pg("SELECT STDDEV_POP(x) FROM t");
    stddev.insert(ROWS[0]);
    stddev.insert(ROWS[1]);
    assert_eq!(
        stddev.real(ROWS[2], true),
        0.816_496_580_927_726,
        "measured: stddev_pop is 0.816496580927726"
    );
}

/// A sample variance is undefined below two rows, which is the engines'
/// NULL.
#[test]
fn one_row_has_no_sample_variance() {
    let mut folding = pg("SELECT VAR_SAMP(x) FROM t");
    assert_eq!(
        folding.insert(ROWS[0]),
        Some(AggValue::VarSamp(None)),
        "measured: var_samp over one row is NULL"
    );
}

/// MySQL answers the same numbers, measured, so the same fold serves it.
#[test]
fn mysql_variance_matches_the_engine() {
    let increment = DivisionPrecisionIncrement::new(4).expect("4 is in range");
    let (mut folding, _) = folding::<MySql, _>(
        MYSQL_DDL,
        MySqlDialect {},
        "SELECT VAR_POP(x) FROM t",
        empty(),
        Some(increment),
    );
    folding.insert(ROWS[0]);
    folding.insert(ROWS[1]);
    assert_eq!(folding.real(ROWS[2], true), 0.666_666_666_666_666_6);
}

/// The seed reads the engine's own variance rather than a sum of squares,
/// because deriving the deviation from squares would put the cancellation
/// back at seed time.
#[test]
fn the_seed_carries_the_engines_own_variance() {
    let (_, sql) = folding::<Postgres, _>(
        PG_DDL,
        PostgreSqlDialect {},
        "SELECT VAR_POP(x) FROM t",
        empty(),
        None,
    );
    assert!(
        sql.contains("VAR_POP(x) * COUNT(x)"),
        "the seed projects the server's own sum of squared deviations, got `{sql}`"
    );
}

/// And a seeded fold answers what the server answered, at the same large
/// mean: `(sum, var_pop * count, count)` read back over the three rows.
#[test]
fn a_seeded_variance_matches_the_engine() {
    let (mut folding, _) = folding::<Postgres, _>(
        PG_DDL,
        PostgreSqlDialect {},
        "SELECT VAR_POP(x) FROM t",
        vec![
            Value::Float(300_000_003.0),
            Value::Float(2.0),
            Value::Int(3),
        ],
        None,
    );
    // A fourth row at the same magnitude keeps the answer stable, which a
    // seed carrying a sum of squares could not.
    assert_eq!(
        folding.real(100_000_003.0, true),
        1.25,
        "the seeded state folds on stably"
    );
}

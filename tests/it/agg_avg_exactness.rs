//! A mean is a quotient, and each engine takes its own.
//!
//! `AVG` divided an `f64` sum by an `f64` count, so it lost digits before
//! it divided and then answered in a type no engine reports.
//!
//! Measured 2026-09-05 on PostgreSQL 16.15, MySQL 8.4.11 and SQLite
//! 3.51.1. The reported type follows the column, and the value follows
//! that engine's own division rule:
//!
//! ```text
//! column           pg                          mysql            sqlite
//! int              numeric                     decimal(14,4)    real
//! bigint           numeric                     decimal(23,4)    real
//! numeric/decimal  numeric                     decimal(24,6)    no decimal type
//! double           double precision            double           real
//! ```
//!
//! PostgreSQL's mean is its `numeric` division of the exact total by the
//! count, which is the sixteen-significant-digit rule Phase C12 measured.
//! Verified against the formula on three cases: `avg(int)` of 1 and 2 is
//! `1.5000000000000000` at scale 16, `avg(bigint)` of two rows of
//! `9007199254740993` is `9007199254740993.0000` at scale 4, and
//! `avg(numeric)` of `0.10` and `0.20` is `0.15000000000000000000` at
//! scale 20.
//!
//! MySQL's mean is its own `/`, word-quantised and truncated, and the
//! display misleads exactly as it does for the operator: `avg` over 1, 2,
//! 2 *shows* `1.6667`, and the value it compares against is
//! `1.666666666`, nine digits, truncated. Bisected: at increment 4 it
//! equals `1.666666666` and is not greater than it, and at increment 10 it
//! equals `1.666666666666666666`. So MySQL's `AVG` needs the same declared
//! `div_precision_increment` its `/` needs.
//!
//! SQLite has one answer: a real. Measured, `avg` of a single row of
//! `9007199254740993` is `9.00719925474099e+15`, so its mean is inexact
//! there and reproducing it means staying in `f64`.
#![allow(clippy::unwrap_used)]

use bigdecimal::BigDecimal;
use core::str::FromStr as _;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Backend, DivisionPrecisionIncrement, MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, DefaultIds, NumericValue, PgLsn, SubscriptionEngine,
    SubscriptionRequest, TableId,
};

const PG_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, small INT, big BIGINT, \
                      exact NUMERIC, approx DOUBLE PRECISION)";
const MYSQL_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, small INT, big BIGINT, \
                         exact DECIMAL(20,2), approx DOUBLE)";
const SQLITE_DDL: &str =
    "CREATE TABLE t (id INTEGER PRIMARY KEY, small INTEGER, big INTEGER, approx REAL)";

const SMALL: usize = 1;
const BIG: usize = 2;
const EXACT: usize = 3;

/// A backend on the standard carriers, which is what a running value is
/// written in.
trait Averaging:
    Backend<Int = i64, Float = f64, Decimal = BigDecimal> + subql::compiler::SqlLiteralParse
{
}

impl<B> Averaging for B where
    B: Backend<Int = i64, Float = f64, Decimal = BigDecimal> + subql::compiler::SqlLiteralParse
{
}

/// One seeded `AVG` subscription, seeded empty so every number came from a
/// fold.
struct Folding<B: Averaging> {
    engine: SubscriptionEngine<TestEvent<B, PgLsn>, DefaultIds, ParserDB>,
    table: TableId,
    arity: usize,
    lsn: u64,
}

impl<B: Averaging> Folding<B> {
    /// Fold one row and answer the mean the engine now reports.
    fn fold(&mut self, at: usize, cell: Value<B>) -> Option<AggValue> {
        self.lsn += 10;
        let mut cells: Vec<Value<B>> = (0..self.arity).map(|_| Value::Null).collect();
        cells[0] = Value::Int(i64::try_from(self.lsn).unwrap());
        cells[at] = cell;
        let event = TestEvent::insert(self.table, cells)
            .with_pk_columns([0u16])
            .with_checkpoint(PgLsn(self.lsn));
        self.engine
            .aggregate_updates(&event)
            .expect("the event folds")
            .updates
            .first()
            .and_then(subql::AggregateValueUpdate::folded_value)
    }
}

fn folding<B, D>(
    ddl: &str,
    dialect: D,
    column: &str,
    arity: usize,
    increment: Option<DivisionPrecisionIncrement>,
) -> Folding<B>
where
    B: Averaging<Dialect = D>,
    D: sqlparser::dialect::Dialect + Default,
{
    let database = ParserDB::parse::<D>(ddl).unwrap();
    let table = catalog_helpers::table_id(&database, "t").unwrap();
    let mut engine: SubscriptionEngine<TestEvent<B, PgLsn>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, dialect);
    if let Some(increment) = increment {
        engine = engine.with_division_precision_increment(increment);
    }
    let registered = engine
        .register(SubscriptionRequest::new(
            7,
            format!("SELECT AVG({column}) FROM t"),
        ))
        .unwrap();
    assert!(
        registered.not_served_because.is_none(),
        "`AVG({column})` is served in process, and this one was refused"
    );
    subql::Install::install(
        &mut engine,
        registered.subscription_id,
        subql::AggregateSeedInstall {
            rows: vec![vec![Value::Null, Value::Int(0)]],
            read_at: Some(PgLsn(5)),
        },
    )
    .expect("the empty seed lands");
    Folding {
        engine,
        table,
        arity,
        lsn: 5,
    }
}

/// The default `div_precision_increment`, which is what an unmodified
/// MySQL reports.
const fn four() -> DivisionPrecisionIncrement {
    DivisionPrecisionIncrement::new(4).expect("4 is in range")
}

fn pg(column: &str) -> Folding<Postgres> {
    folding(PG_DDL, PostgreSqlDialect {}, column, 5, None)
}

fn mysql(column: &str) -> Folding<MySql> {
    folding(MYSQL_DDL, MySqlDialect {}, column, 5, Some(four()))
}

fn sqlite(column: &str) -> Folding<SQLite> {
    folding(SQLITE_DDL, SQLiteDialect {}, column, 4, None)
}

fn decimal(text: &str) -> AggValue {
    AggValue::Avg(Some(NumericValue::Decimal(
        BigDecimal::from_str(text).unwrap(),
    )))
}

/// PostgreSQL's mean of an integer column is a `numeric`, at the scale its
/// own division picks.
#[test]
fn pg_avg_of_int_is_exact() {
    let mut folding = pg("small");
    folding.fold(SMALL, Value::Int(1));
    assert_eq!(
        folding.fold(SMALL, Value::Int(2)),
        Some(decimal("1.5000000000000000")),
        "measured: sixteen significant digits, which is scale 16 here"
    );
}

/// MySQL's mean of an integer column is a decimal, and it is the value it
/// compares rather than the value it displays.
#[test]
fn mysql_avg_of_int_is_decimal() {
    let mut folding = mysql("small");
    folding.fold(SMALL, Value::Int(1));
    folding.fold(SMALL, Value::Int(2));
    assert_eq!(
        folding.fold(SMALL, Value::Int(2)),
        Some(decimal("1.666666666")),
        "measured by bisection: nine digits, truncated, not the 1.6667 it prints"
    );
}

/// And that mean follows the declared increment, which is why MySQL needs
/// one for `AVG` as much as for `/`.
#[test]
fn mysql_avg_follows_the_declared_increment() {
    let ten = DivisionPrecisionIncrement::new(10).expect("10 is in range");
    let mut folding: Folding<MySql> = folding(MYSQL_DDL, MySqlDialect {}, "small", 5, Some(ten));
    folding.fold(SMALL, Value::Int(1));
    folding.fold(SMALL, Value::Int(2));
    assert_eq!(
        folding.fold(SMALL, Value::Int(2)),
        Some(decimal("1.666666666666666666")),
        "measured: eighteen digits at increment 10"
    );
}

/// Without that setting the mean is unknowable, so the subscription is
/// refused rather than answered from a guess. Same rule as `/`.
#[test]
fn mysql_avg_without_the_declared_increment_is_refused() {
    let database = ParserDB::parse::<MySqlDialect>(MYSQL_DDL).unwrap();
    let mut engine: SubscriptionEngine<TestEvent<MySql, PgLsn>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, MySqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(7, "SELECT AVG(small) FROM t"))
        .expect("it registers on a re-read tier");
    assert!(
        matches!(
            registered.not_served_because,
            Some(subql::NotServed::DivisionPrecisionNotDeclared)
        ),
        "expected the undeclared setting as the reason, got {:?}",
        registered.not_served_because
    );
}

/// The digits an `f64` mean lost: two rows of `9007199254740993` average
/// to themselves, and `f64` answers `9007199254740992`.
#[test]
fn avg_past_2_53_is_exact() {
    let mut pg_folding = pg("big");
    pg_folding.fold(BIG, Value::Int(9_007_199_254_740_993));
    assert_eq!(
        pg_folding.fold(BIG, Value::Int(9_007_199_254_740_993)),
        Some(decimal("9007199254740993.0000")),
        "measured: scale 4, because the quotient's leading word is wide"
    );

    let mut mysql_folding = mysql("big");
    mysql_folding.fold(BIG, Value::Int(9_007_199_254_740_993));
    assert_eq!(
        mysql_folding.fold(BIG, Value::Int(9_007_199_254_740_993)),
        Some(decimal("9007199254740993.000000000")),
        "and nine digits under MySQL's own rule"
    );
}

/// A decimal column's mean keeps the scale that division picks, which is
/// wider than the column's own.
#[test]
fn pg_avg_of_numeric_takes_the_division_scale() {
    let mut folding = pg("exact");
    folding.fold(EXACT, Value::Decimal(BigDecimal::from_str("0.10").unwrap()));
    assert_eq!(
        folding.fold(EXACT, Value::Decimal(BigDecimal::from_str("0.20").unwrap())),
        Some(decimal("0.15000000000000000000")),
        "measured: scale 20"
    );
}

/// A floating column still averages into a double, on every engine.
#[test]
fn a_float_column_averages_into_a_double() {
    let mut folding = pg("approx");
    folding.fold(4, Value::Float(1.5));
    assert_eq!(
        folding.fold(4, Value::Float(2.5)),
        Some(AggValue::Avg(Some(NumericValue::Double(2.0)))),
        "measured: avg(double precision) is a double"
    );
}

/// SQLite answers a real whatever the column, measured, so its mean stays
/// in `f64` and is inexact past `2^53` exactly as the server is.
#[test]
fn sqlite_avg_is_a_double() {
    let mut folding = sqlite("small");
    folding.fold(SMALL, Value::Int(1));
    assert_eq!(
        folding.fold(SMALL, Value::Int(2)),
        Some(AggValue::Avg(Some(NumericValue::Double(1.5)))),
        "measured: typeof(avg(x)) is real"
    );
}

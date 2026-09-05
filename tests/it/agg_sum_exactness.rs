//! A sum is exact, in the type the engine sums into.
//!
//! Measured 2026-09-05 on PostgreSQL 16.15, MySQL 8.4.11 and SQLite 3.51.1.
//! No universal rule exists, because the engines disagree about what a sum
//! even returns:
//!
//! ```text
//! column           pg                     mysql            sqlite
//! smallint, int    bigint                 decimal(32,0)    integer
//! bigint           numeric                decimal(41,0)    integer
//! numeric/decimal  numeric, scale kept    decimal, scale kept   no decimal type
//! real, double     double precision       double           real
//! ```
//!
//! And none of them is unbounded, but they run out in different places:
//!
//! ```text
//! engine   boundary                                    answer
//! pg       numeric past 131072 integer digits          ERROR value overflows numeric format
//! mysql    decimal sum past 65 digits, in a SELECT     widens: 68 digits at 200 rows,
//!                                                      70 at 51200. No error.
//! mysql    that result stored into a DECIMAL column    ERROR 1264 out of range
//! sqlite   integer sum past 64 bits                    ERROR integer overflow
//! sqlite   a non-integer joins an integer sum           the sum becomes real
//! ```
//!
//! subql accumulated everything in `f64`, so a single row of
//! `9007199254740993` folded to `9007199254740992`, which is a wrong answer
//! on all three engines, and two rows of `i64::MAX` folded to
//! `1.8446744073709552e19` where two engines answer `18446744073709551614`
//! exactly and the third refuses.
#![allow(clippy::unwrap_used)]

use bigdecimal::BigDecimal;
use core::str::FromStr as _;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Backend, MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, DefaultIds, MaintenanceStopReason, NumericValue, PgLsn,
    SubscriptionEngine, SubscriptionRequest, TableId,
};

const PG_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, small INT, big BIGINT, \
                      exact NUMERIC, approx DOUBLE PRECISION)";
const MYSQL_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, small INT, big BIGINT, \
                         exact DECIMAL(65,0), approx DOUBLE)";
const SQLITE_DDL: &str =
    "CREATE TABLE t (id INTEGER PRIMARY KEY, small INTEGER, big INTEGER, approx REAL)";

/// Column ordinals shared by the three schemas above.
const SMALL: usize = 1;
const BIG: usize = 2;

/// A backend on the standard carriers, which is what an aggregate's
/// running value is written in.
trait Summing:
    Backend<Int = i64, Float = f64, Decimal = BigDecimal> + subql::compiler::SqlLiteralParse
{
}

impl<B> Summing for B where
    B: Backend<Int = i64, Float = f64, Decimal = BigDecimal> + subql::compiler::SqlLiteralParse
{
}

/// One seeded `SUM` subscription over `column`, seeded empty.
struct Folding<B: Summing> {
    engine: SubscriptionEngine<TestEvent<B, PgLsn>, DefaultIds, ParserDB>,
    table: TableId,
    arity: usize,
    lsn: u64,
}

impl<B: Summing> Folding<B> {
    /// Fold one row whose summed column holds `cell`, and answer the value
    /// the engine now reports, or `None` when it stopped maintaining the
    /// total instead.
    fn fold(&mut self, at: usize, cell: Value<B>) -> Option<AggValue> {
        self.lsn += 10;
        let mut cells = empty_row(self.arity);
        cells[0] = Value::Int(i64::try_from(self.lsn).unwrap());
        cells[at] = cell;
        let event = TestEvent::insert(self.table, cells)
            .with_pk_columns([0u16])
            .with_checkpoint(PgLsn(self.lsn));
        let output = self.engine.aggregate_updates(&event).unwrap();
        if !output.transitions.is_empty() {
            return None;
        }
        output
            .updates
            .first()
            .and_then(subql::AggregateValueUpdate::folded_value)
    }

    /// Why the engine stopped maintaining the total, if it did. A stop
    /// leaves the subscription on a re-read tier, which the transition
    /// reports.
    fn stop(&mut self, at: usize, cell: Value<B>) -> Option<MaintenanceStopReason> {
        self.lsn += 10;
        let mut cells = empty_row(self.arity);
        cells[0] = Value::Int(i64::try_from(self.lsn).unwrap());
        cells[at] = cell;
        let event = TestEvent::insert(self.table, cells)
            .with_pk_columns([0u16])
            .with_checkpoint(PgLsn(self.lsn));
        self.engine
            .aggregate_updates(&event)
            .unwrap()
            .transitions
            .first()
            .map(|transition| transition.reason.clone())
    }
}

fn empty_row<B: Summing>(arity: usize) -> Vec<Value<B>> {
    (0..arity).map(|_| Value::Null).collect()
}

/// Register `SELECT SUM(column)` over `ddl` and seed it empty, so every
/// number the test sees came from the fold rather than from a seed.
fn folding<B, D>(ddl: &str, dialect: D, column: &str, arity: usize) -> Folding<B>
where
    B: Summing<Dialect = D>,
    D: sqlparser::dialect::Dialect + Default,
{
    let database = ParserDB::parse::<D>(ddl).unwrap();
    let table = catalog_helpers::table_id(&database, "t").unwrap();
    let mut engine: SubscriptionEngine<TestEvent<B, PgLsn>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, dialect);
    let subscription = engine
        .register(SubscriptionRequest::new(
            7,
            format!("SELECT SUM({column}) FROM t"),
        ))
        .unwrap()
        .subscription_id;
    install(&mut engine, subscription, vec![Value::Null, Value::Int(0)])
        .expect("the empty seed lands");
    Folding {
        engine,
        table,
        arity,
        lsn: 5,
    }
}

/// Install one seed component row and answer the value it reports.
fn install<B: Summing>(
    engine: &mut SubscriptionEngine<TestEvent<B, PgLsn>, DefaultIds, ParserDB>,
    subscription: u64,
    row: Vec<Value<B>>,
) -> Option<AggValue> {
    subql::Install::install(
        engine,
        subscription,
        subql::AggregateSeedInstall {
            rows: vec![row],
            read_at: Some(PgLsn(5)),
        },
    )
    .expect("the seed lands")
    .updates
    .first()
    .and_then(subql::AggregateValueUpdate::folded_value)
}

fn pg(column: &str) -> Folding<Postgres> {
    folding(PG_DDL, PostgreSqlDialect {}, column, 5)
}

fn mysql(column: &str) -> Folding<MySql> {
    folding(MYSQL_DDL, MySqlDialect {}, column, 5)
}

fn sqlite(column: &str) -> Folding<SQLite> {
    folding(SQLITE_DDL, SQLiteDialect {}, column, 4)
}

fn decimal(text: &str) -> NumericValue {
    NumericValue::Decimal(BigDecimal::from_str(text).unwrap())
}

/// The finding, on every engine: one row above `2^53` is itself, not the
/// nearest double.
#[test]
fn bigint_sum_past_2_53_is_exact() {
    assert_eq!(
        pg("big").fold(BIG, Value::Int(9_007_199_254_740_993)),
        Some(AggValue::Sum(Some(decimal("9007199254740993")))),
        "PostgreSQL sums a bigint into numeric, which is exact"
    );
    assert_eq!(
        mysql("big").fold(BIG, Value::Int(9_007_199_254_740_993)),
        Some(AggValue::Sum(Some(decimal("9007199254740993")))),
        "MySQL sums into decimal(41,0)"
    );
    assert_eq!(
        sqlite("big").fold(BIG, Value::Int(9_007_199_254_740_993)),
        Some(AggValue::Sum(Some(NumericValue::Integer(
            9_007_199_254_740_993
        )))),
        "SQLite sums integers as integers"
    );
}

/// PostgreSQL sums a `bigint` into `numeric`, so the total passes
/// `i64::MAX` without any boundary at all.
#[test]
fn pg_bigint_sum_past_i64_max_is_exact() {
    let mut folding = pg("big");
    folding.fold(BIG, Value::Int(i64::MAX));
    assert_eq!(
        folding.fold(BIG, Value::Int(i64::MAX)),
        Some(AggValue::Sum(Some(decimal("18446744073709551614")))),
        "measured: PostgreSQL answers 18446744073709551614"
    );
}

/// A `smallint` or `int` column sums into `bigint` there, not into
/// `numeric`, so the reported total is an integer.
#[test]
fn pg_int_sum_is_a_bigint() {
    assert_eq!(
        pg("small").fold(SMALL, Value::Int(2_147_483_647)),
        Some(AggValue::Sum(Some(NumericValue::Integer(2_147_483_647)))),
        "measured: pg_typeof(sum(int)) is bigint"
    );
}

/// MySQL sums every integer width into a decimal, so the same two rows
/// that PostgreSQL answers exactly are exact there too, by another route.
#[test]
fn mysql_integer_sum_is_decimal() {
    let mut folding = mysql("small");
    folding.fold(SMALL, Value::Int(2_147_483_647));
    assert_eq!(
        folding.fold(SMALL, Value::Int(2_147_483_647)),
        Some(AggValue::Sum(Some(decimal("4294967294")))),
        "measured: sum(int) is decimal(32,0) on MySQL"
    );
}

/// MySQL's decimal sum widens rather than failing, measured: two rows of
/// sixty-five nines answer a sixty-six digit total in a `SELECT`, and the
/// out-of-range error belongs to storing that result in a column.
#[test]
fn mysql_decimal_sum_widens_past_its_column_type() {
    let nines = "9".repeat(65);
    let mut folding = mysql("exact");
    folding.fold(3, Value::Decimal(BigDecimal::from_str(&nines).unwrap()));
    assert_eq!(
        folding.fold(3, Value::Decimal(BigDecimal::from_str(&nines).unwrap())),
        Some(AggValue::Sum(Some(decimal(
            "199999999999999999999999999999999999999999999999999999999999999998"
        )))),
        "measured on 8.4.11: the SELECT answers sixty-six digits without an error"
    );
}

/// PostgreSQL's `numeric` is bounded at 131072 integer digits and raises
/// past it, measured, so the fold stops rather than reporting a total the
/// server would refuse to compute.
#[test]
fn pg_numeric_sum_past_the_server_limit_fails_typed() {
    let huge = BigDecimal::from_str(&"9".repeat(131_072)).unwrap();
    let mut folding = pg("exact");
    folding.fold(3, Value::Decimal(huge.clone()));
    let stop = folding.stop(3, Value::Decimal(huge));
    assert!(
        matches!(stop, Some(MaintenanceStopReason::SumOutOfRange { .. })),
        "expected the server's own refusal, got {stop:?}"
    );
}

/// SQLite raises `integer overflow` past 64 bits rather than promoting,
/// measured, so the fold stops there too.
#[test]
fn sqlite_integer_sum_overflow_raises() {
    let mut folding = sqlite("big");
    folding.fold(BIG, Value::Int(i64::MAX));
    let stop = folding.stop(BIG, Value::Int(i64::MAX));
    assert!(
        matches!(stop, Some(MaintenanceStopReason::SumOutOfRange { .. })),
        "measured: SQLite answers `integer overflow`, got {stop:?}"
    );
}

/// But a non-integer joining the sum turns it real, measured, which is a
/// promotion rather than a refusal.
#[test]
fn sqlite_integer_sum_promotes_on_a_real_value() {
    let mut folding = sqlite("big");
    assert_eq!(
        folding.fold(BIG, Value::Int(2)),
        Some(AggValue::Sum(Some(NumericValue::Integer(2))))
    );
    assert_eq!(
        folding.fold(BIG, Value::Float(0.5)),
        Some(AggValue::Sum(Some(NumericValue::Double(2.5)))),
        "measured: typeof(sum(v)) is real once a real participates"
    );
}

/// A decimal sum keeps the scale its values carry, measured: `0.10 + 0.20`
/// is `0.30` at scale two rather than `0.3`.
#[test]
fn numeric_sum_keeps_its_scale() {
    let mut folding = pg("exact");
    folding.fold(3, Value::Decimal(BigDecimal::from_str("0.10").unwrap()));
    let total = folding.fold(3, Value::Decimal(BigDecimal::from_str("0.20").unwrap()));
    assert_eq!(
        total,
        Some(AggValue::Sum(Some(decimal("0.30")))),
        "the total is 0.30"
    );
    let AggValue::Sum(Some(NumericValue::Decimal(value))) = total.unwrap() else {
        panic!("a numeric sum reports a decimal")
    };
    assert_eq!(value.as_bigint_and_exponent().1, 2, "at scale two");
}

/// A floating column still sums into a double, on every engine.
#[test]
fn float_sum_stays_double() {
    assert_eq!(
        pg("approx").fold(4, Value::Float(1.5)),
        Some(AggValue::Sum(Some(NumericValue::Double(1.5))))
    );
    assert_eq!(
        sqlite("approx").fold(3, Value::Float(1.5)),
        Some(AggValue::Sum(Some(NumericValue::Double(1.5))))
    );
}

/// A seed carries the total in the same type the fold keeps it in, so a
/// subscription that starts from a database read starts exact.
///
/// The value is chosen to be unrepresentable in `f64`: decoding the
/// component as a double would answer `9007199254740992.2`, and the digit
/// it loses is the one this asserts.
#[test]
fn a_decimal_seed_decodes_exactly() {
    let database = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap();
    let mut engine: SubscriptionEngine<TestEvent<Postgres, PgLsn>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(7, "SELECT SUM(exact) FROM t"))
        .unwrap();
    let subql::Tier::InProcess(served) = &registered.tier else {
        panic!(
            "an aggregate over a numeric column folds, got {:?}",
            registered.tier
        )
    };
    let bootstrap = served
        .aggregate_bootstrap
        .clone()
        .expect("an in-process fold seeds");
    assert_eq!(
        bootstrap.kinds,
        vec![
            subql::backend::BuiltinKind::Decimal,
            subql::backend::BuiltinKind::Int
        ],
        "a numeric total is read back as a decimal, not as a double"
    );

    let value = install(
        &mut engine,
        registered.subscription_id,
        vec![
            Value::Decimal(BigDecimal::from_str("9007199254740993.25").unwrap()),
            Value::Int(3),
        ],
    );
    assert_eq!(
        value,
        Some(AggValue::Sum(Some(decimal("9007199254740993.25")))),
        "every digit the read returned survives the seed"
    );
}

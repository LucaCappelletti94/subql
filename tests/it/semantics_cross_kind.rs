//! Comparing two columns of different scalar kinds, measured.
//!
//! Nothing coerces in process today: a comparison whose operands are two
//! different `Value` variants answers no-match, while every engine answers
//! the comparison. `qty > price` with `qty` 5 and `price` 1.0 is a row in
//! all three.
//!
//! The numeric rule is per backend, and the differences are not cosmetic.
//! Measured 2026-09-04 on PostgreSQL 16.11, MySQL 8.4.11 and SQLite 3.51.1,
//! using the pair `9007199254740993` and `9007199254740992`, which is the
//! smallest place `f64` cannot tell two integers apart:
//!
//! ```text
//! pair                    pg                  mysql               sqlite
//! integer vs float        equal, so lossy     equal, so lossy     unequal, exact
//! integer vs decimal      unequal, exact      unequal, exact      no decimal type
//! decimal vs float        equal, so lossy     equal, so lossy     no decimal type
//! integer vs text         ERROR, no operator  coerces the text    number sorts first
//! ```
//!
//! So PostgreSQL and MySQL cast the integer to `double precision` when the
//! other side is a float, and compare exactly against `numeric`, while
//! SQLite compares an integer against a real exactly. A comparator that
//! widened everything through `f64` would answer the SQLite row wrongly,
//! and one that compared everything exactly would answer the other two
//! wrongly.
//!
//! A cross-kind pair outside the numeric family is not served: PostgreSQL
//! has no operator for it at all, and MySQL's string-to-number coercion is
//! its own semantics rather than a widening. Those take a database read.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{MySql, Postgres, SQLite, ScalarFamily, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, NotServed, SubscriptionEngine, SubscriptionRequest};

macro_rules! notifies {
    ($backend:ty, $dialect:ty, $ddl:expr, $predicate:expr, $cells:expr) => {{
        let db = ParserDB::parse::<$dialect>($ddl).expect("DDL parses");
        let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
        let mut engine: SubscriptionEngine<TestEvent<$backend>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, <$dialect>::default());
        engine
            .register(SubscriptionRequest::new(1u64, $predicate))
            .expect("registration succeeds");
        let notifications = engine
            .consumers(&TestEvent::insert(table, $cells))
            .expect("dispatch succeeds");
        !notifications.inserted().is_empty()
    }};
}

macro_rules! registered {
    ($backend:ty, $dialect:ty, $ddl:expr, $predicate:expr) => {{
        let db = ParserDB::parse::<$dialect>($ddl).expect("DDL parses");
        let mut engine: SubscriptionEngine<TestEvent<$backend>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, <$dialect>::default());
        engine
            .register(SubscriptionRequest::new(1u64, $predicate))
            .expect("a read answers it, so registration succeeds")
    }};
}

const PG_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, qty BIGINT, \
                      price DOUBLE PRECISION, amount NUMERIC, label TEXT)";
const SQLITE_DDL: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, qty INTEGER, price REAL)";

/// `(id, qty, price, amount, label)`.
fn row(qty: i64, price: f64, amount: &str, label: &str) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(1),
        Value::Int(qty),
        Value::Float(price),
        Value::Decimal(amount.parse().expect("a decimal literal")),
        Value::String(label.to_string()),
    ]
}

/// The finding: an integer column against a float column is a comparison
/// every engine answers, and subql answered no-match.
#[test]
fn int_column_compares_against_real_column() {
    assert!(
        notifies!(
            Postgres,
            PostgreSqlDialect,
            PG_DDL,
            "SELECT * FROM t WHERE qty > price",
            row(5, 1.0, "0", "")
        ),
        "5 is above 1.0"
    );
    assert!(
        !notifies!(
            Postgres,
            PostgreSqlDialect,
            PG_DDL,
            "SELECT * FROM t WHERE qty > price",
            row(1, 5.0, "0", "")
        ),
        "and 1 is not above 5.0"
    );
}

/// An integer against a decimal is compared exactly on both engines that
/// have a decimal type, which is why the widening cannot go through `f64`.
#[test]
fn numeric_column_equals_int_column() {
    assert!(
        notifies!(
            Postgres,
            PostgreSqlDialect,
            PG_DDL,
            "SELECT * FROM t WHERE amount = qty",
            row(10, 0.0, "10", "")
        ),
        "10 equals 10 across the two kinds"
    );
    assert!(
        !notifies!(
            Postgres,
            PostgreSqlDialect,
            PG_DDL,
            "SELECT * FROM t WHERE amount = qty",
            row(9_007_199_254_740_993, 0.0, "9007199254740992", "")
        ),
        "the comparison is exact, so these two do not collapse"
    );
}

/// PostgreSQL casts the integer when the other side is a float, so two
/// integers `f64` cannot separate compare equal. This is the vector that
/// forbids comparing the numeric family exactly across the board.
#[test]
fn pg_int_against_float_compares_at_float_width() {
    assert!(
        notifies!(
            Postgres,
            PostgreSqlDialect,
            PG_DDL,
            "SELECT * FROM t WHERE qty = price",
            row(9_007_199_254_740_993, 9_007_199_254_740_992.0, "0", "")
        ),
        "measured as t: the integer is cast to double precision"
    );
}

/// SQLite compares an integer against a real exactly, so the same pair is
/// unequal there. One rule cannot serve both engines.
#[test]
fn sqlite_int_against_real_compares_exactly() {
    let cells = |qty: i64, price: f64| {
        vec![
            Value::<SQLite>::Int(1),
            Value::Int(qty),
            Value::Float(price),
        ]
    };
    assert!(
        !notifies!(
            SQLite,
            SQLiteDialect,
            SQLITE_DDL,
            "SELECT * FROM t WHERE qty = price",
            cells(9_007_199_254_740_993, 9_007_199_254_740_992.0)
        ),
        "measured as 0: SQLite does not round the integer to compare it"
    );
    assert!(
        notifies!(
            SQLite,
            SQLiteDialect,
            SQLITE_DDL,
            "SELECT * FROM t WHERE qty > price",
            cells(9_007_199_254_740_993, 9_007_199_254_740_992.0)
        ),
        "measured as 1: it is above, exactly"
    );
    assert!(
        notifies!(
            SQLite,
            SQLiteDialect,
            SQLITE_DDL,
            "SELECT * FROM t WHERE qty > price",
            cells(5, 1.0)
        ),
        "and the ordinary case still answers"
    );
}

/// MySQL matches PostgreSQL on the numeric family, so its own test pins
/// that rather than inheriting it by assumption.
#[test]
fn mysql_matches_the_numeric_widening() {
    let ddl = "CREATE TABLE t (id INT PRIMARY KEY, qty BIGINT, price DOUBLE, \
               amount DECIMAL(20, 0))";
    let cells = |qty: i64, price: f64, amount: &str| {
        vec![
            Value::<MySql>::Int(1),
            Value::Int(qty),
            Value::Float(price),
            Value::Decimal(amount.parse().expect("a decimal literal")),
        ]
    };
    assert!(
        notifies!(
            MySql,
            MySqlDialect,
            ddl,
            "SELECT * FROM t WHERE qty = price",
            cells(9_007_199_254_740_993, 9_007_199_254_740_992.0, "0")
        ),
        "measured as 1: lossy against a float"
    );
    assert!(
        !notifies!(
            MySql,
            MySqlDialect,
            ddl,
            "SELECT * FROM t WHERE qty = amount",
            cells(9_007_199_254_740_993, 0.0, "9007199254740992")
        ),
        "measured as 0: exact against a decimal"
    );
}

/// A cross-kind pair outside the numeric family is not served in process.
/// PostgreSQL has no operator for it, so there is nothing to reproduce, and
/// the cause names both operands.
#[test]
fn non_numeric_cross_kind_is_not_served_in_process() {
    let db = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
    let qty = catalog_helpers::column_id(&db, table, "qty").expect("qty is in the catalog");
    let label = catalog_helpers::column_id(&db, table, "label").expect("label is in the catalog");

    let registered = registered!(
        Postgres,
        PostgreSqlDialect,
        PG_DDL,
        "SELECT * FROM t WHERE qty = label"
    );
    assert!(
        registered.served().is_none(),
        "there is no in-process answer to reproduce"
    );
    assert_eq!(
        registered.not_served_because,
        Some(NotServed::CrossKindComparison {
            left: qty,
            left_kind: ScalarFamily::Int.into(),
            right: label,
            right_kind: ScalarFamily::String.into(),
        }),
        "the cause names both operands and both kinds"
    );
}

/// The control: a same-kind comparison is untouched by any of this.
#[test]
fn same_kind_comparison_still_folds_in_process() {
    let registered = registered!(
        Postgres,
        PostgreSqlDialect,
        PG_DDL,
        "SELECT * FROM t WHERE qty > 3"
    );
    assert!(registered.served().is_some());
    assert!(notifies!(
        Postgres,
        PostgreSqlDialect,
        PG_DDL,
        "SELECT * FROM t WHERE qty > 3",
        row(5, 0.0, "0", "")
    ));
}

/// An exact cross-kind comparison still orders an infinity, which has no
/// decimal to be exact about.
///
/// Found by a maintainer review of the series, not by the differential
/// sweep, and the two are complementary: the sweep writes its rows as
/// SQL literals, and `'Infinity'` is not a numeric literal in SQLite, so
/// the generator cannot put one in a `REAL` column at all. Arithmetic
/// can: measured on SQLite 3.51.1,
///
/// ```text
/// INSERT INTO t VALUES (9e307 * 10, 1)
/// SELECT r, typeof(r)   inf, real
/// SELECT r > i          1
/// SELECT i > r          0
/// ```
///
/// `NumericWidening::Exact` compared through `BigDecimal`, which has no
/// representation for an infinity, so `from_f64` answered `None`, the
/// `?` propagated it and the row was dropped as unknown. Widening loses
/// nothing here: an infinity outranks every finite number whatever the
/// other side's precision.
#[test]
fn sqlite_orders_an_infinity_against_an_integer() {
    let cells = |qty: i64, price: f64| {
        vec![
            Value::<SQLite>::Int(1),
            Value::Int(qty),
            Value::Float(price),
        ]
    };
    assert!(
        notifies!(
            SQLite,
            SQLiteDialect,
            SQLITE_DDL,
            "SELECT * FROM t WHERE price > qty",
            cells(1, f64::INFINITY)
        ),
        "measured as 1: an infinity is above every integer"
    );
    assert!(
        !notifies!(
            SQLite,
            SQLiteDialect,
            SQLITE_DDL,
            "SELECT * FROM t WHERE qty > price",
            cells(1, f64::INFINITY)
        ),
        "measured as 0, which is the same fact from the other side"
    );
    assert!(
        notifies!(
            SQLite,
            SQLiteDialect,
            SQLITE_DDL,
            "SELECT * FROM t WHERE qty > price",
            cells(1, f64::NEG_INFINITY)
        ),
        "and a negative infinity is below every integer"
    );
}

/// A decimal too large for `double precision` refuses the comparison
/// rather than answering it.
///
/// The `AtFloatWidth` rule casts the exact side to a double, which is what
/// PostgreSQL does, and a `NUMERIC` has no bound while a double does.
/// Measured on PostgreSQL 16, the server does not answer such a pair at
/// all:
///
/// ```text
/// 1e300::numeric > 1.5::float8      t
/// 1e309::numeric > 1.5::float8      ERROR: out of range for type double precision
/// (-1e309)::numeric > 1.5::float8   ERROR: out of range for type double precision
/// 1e309::numeric::float8            ERROR: out of range for type double precision
/// ```
///
/// So this is a per-row refusal and not a no-match. Answering `Unknown`
/// dropped the row in silence, which is a subscriber quietly missing an
/// answer the database would have refused to give; answering an ordering
/// by treating the value as an infinity would be worse, since it invents a
/// result the server declines to produce.
///
/// PostgreSQL only. MySQL's `DECIMAL` holds at most 65 digits, far inside
/// the double range, and SQLite compares an integer against a real through
/// the exact rule instead.
#[test]
fn a_decimal_past_the_double_range_refuses_the_comparison() {
    let db = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    let subscription = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM t WHERE amount > price",
        ))
        .expect("the comparison registers")
        .subscription_id;

    let huge = "1".to_string() + &"0".repeat(309);
    let refused = engine
        .consumers(&TestEvent::insert(table, row(5, 1.5, &huge, "paid")))
        .expect("dispatch succeeds");
    assert_eq!(
        refused
            .evaluation_failures()
            .iter()
            .map(|failure| (failure.subscription_id, failure.refusal))
            .collect::<Vec<_>>(),
        vec![(
            subscription,
            subql::EvaluationRefusal::DecimalOutsideFloatRange
        )],
        "the report names the subscription and the operand the server will not cast"
    );
    assert!(
        refused.inserted().is_empty(),
        "a refusal is not a match, and not a no-match either"
    );

    // Both signs, because the server raises for both and a check that
    // looked only at one would pass the test above while letting the
    // other through.
    let huge_negative = "-".to_string() + &huge;
    let refused_negative = engine
        .consumers(&TestEvent::insert(
            table,
            row(5, 1.5, &huge_negative, "paid"),
        ))
        .expect("dispatch succeeds");
    assert_eq!(
        refused_negative
            .evaluation_failures()
            .iter()
            .map(|failure| failure.refusal)
            .collect::<Vec<_>>(),
        vec![subql::EvaluationRefusal::DecimalOutsideFloatRange],
        "measured: PostgreSQL raises for the negative overflow too"
    );

    // A decimal the double range holds is answered, so the refusal is
    // about the range and not about the pairing.
    assert!(
        notifies!(
            Postgres,
            PostgreSqlDialect,
            PG_DDL,
            "SELECT * FROM t WHERE amount > price",
            row(5, 1.5, "1e300", "paid")
        ),
        "measured as true: 1e300 is inside the double range"
    );
}

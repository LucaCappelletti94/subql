//! Division answers a scale, not just a number, and each engine picks it
//! differently.
//!
//! Two divergences, both measured 2026-09-05 on PostgreSQL 16.15 and MySQL
//! 8.4.11 and both read back to the servers' own source.
//!
//! MySQL's `/` is decimal division whatever its operands are, so it never
//! truncates to an integer, and the quotient's fractional digits are
//! quantised to nine-digit words and truncated (`do_div_mod`,
//! `mysys/decimal.cc:2266,2296`):
//!
//! ```text
//! frame1 = ceil(dividend_scale / 9) * 9
//! frame2 = ceil(divisor_scale  / 9) * 9
//! adj    = max(0, div_precision_increment - padding_from_that_rounding)
//! digits = ceil((frame1 + frame2 + adj) / 9) * 9
//! ```
//!
//! ```text
//! expression                   pg      mysql               sqlite
//! 7 / 2                        3       3.5000              3
//! 1 / 3                        0       0.333333333         0
//! 2 / 3                        0       0.666666666         0
//! 7 DIV 2                      -       3                   -
//! ```
//!
//! PostgreSQL diverges on the other side: its `numeric` quotient takes the
//! scale that gives sixteen significant digits and is rounded half away
//! from zero (`select_div_scale`, `numeric.c:9756-9814`):
//!
//! ```text
//! expression              pg
//! 1::numeric / 3          0.33333333333333333333    scale 20
//! 2::numeric / 3          0.66666666666666666667    scale 20, rounded up
//! 7.00::numeric / 3       2.3333333333333333        scale 16
//! ```
//!
//! subql divided decimals at `bigdecimal`'s own precision, which is no
//! engine's rule, and truncated MySQL's integer quotient, which is
//! PostgreSQL's rule rather than MySQL's.
#![allow(clippy::unwrap_used)]

use bigdecimal::BigDecimal;
use core::str::FromStr as _;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{DivisionPrecisionIncrement, MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

/// `qty` is the integer side, `d2` and `d6` the two declared scales that
/// separate MySQL's word quantisation.
const MYSQL_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, qty BIGINT, \
                         d2 DECIMAL(20,2), d6 DECIMAL(20,6))";
const PG_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, qty BIGINT, \
                      d2 NUMERIC, d6 NUMERIC)";
const SQLITE_DDL: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, qty INTEGER)";

/// The default `div_precision_increment`, which is what an unmodified
/// server reports.
const fn default_increment() -> DivisionPrecisionIncrement {
    DivisionPrecisionIncrement::new(4).expect("4 is in range")
}

/// Register `predicate` against MySQL with `increment` declared, dispatch
/// one row, and answer whether the row matched.
fn mysql_matches(
    predicate: &str,
    increment: DivisionPrecisionIncrement,
    cells: Vec<Value<MySql>>,
) -> bool {
    let db = ParserDB::parse::<MySqlDialect>(MYSQL_DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<MySql>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, MySqlDialect {}).with_division_precision_increment(increment);
    engine
        .register(SubscriptionRequest::new(1u64, predicate))
        .expect("the predicate registers");
    !engine
        .consumers(&TestEvent::insert(table, cells))
        .expect("dispatch succeeds")
        .inserted()
        .is_empty()
}

/// `(id, qty, d2, d6)` under MySQL, with both decimals at their declared
/// scale, which is what the wire carries for a fixed-point column.
fn mysql_row(qty: i64, d2: &str, d6: &str) -> Vec<Value<MySql>> {
    vec![
        Value::Int(1),
        Value::Int(qty),
        Value::Decimal(BigDecimal::from_str(d2).unwrap()),
        Value::Decimal(BigDecimal::from_str(d6).unwrap()),
    ]
}

/// As [`mysql_matches`], for PostgreSQL, which needs no declared setting.
fn pg_matches(predicate: &str, cells: Vec<Value<Postgres>>) -> bool {
    let db = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::new(1u64, predicate))
        .expect("the predicate registers");
    !engine
        .consumers(&TestEvent::insert(table, cells))
        .expect("dispatch succeeds")
        .inserted()
        .is_empty()
}

/// `(id, qty, d2, d6)` under PostgreSQL. A `numeric` value carries its own
/// scale rather than the column's, which is what its rule reads.
fn pg_row(qty: i64, d2: &str, d6: &str) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(1),
        Value::Int(qty),
        Value::Decimal(BigDecimal::from_str(d2).unwrap()),
        Value::Decimal(BigDecimal::from_str(d6).unwrap()),
    ]
}

/// The headline wrong answer: `7 / 2 > 3` is true on MySQL, because the
/// quotient is `3.5000` and not `3`.
#[test]
fn mysql_integer_division_is_not_integer_division() {
    assert!(
        mysql_matches(
            "SELECT * FROM t WHERE qty / 2 > 3",
            default_increment(),
            mysql_row(7, "0.00", "0.000000"),
        ),
        "MySQL divides 7 by 2 as 3.5000, which is greater than 3"
    );
}

/// The quotient is truncated at nine digits, not rounded: the server
/// answers 1 for `2 / 3 = 0.666666666` and 0 for the rounded spelling.
#[test]
fn mysql_quotient_truncates_at_nine_digits() {
    assert!(
        mysql_matches(
            "SELECT * FROM t WHERE qty / 3 = 0.666666666",
            default_increment(),
            mysql_row(2, "0.00", "0.000000"),
        ),
        "two thirds compares as exactly 0.666666666"
    );
    assert!(
        !mysql_matches(
            "SELECT * FROM t WHERE qty / 3 = 0.666666667",
            default_increment(),
            mysql_row(2, "0.00", "0.000000"),
        ),
        "the last digit is truncated, so the rounded spelling does not match"
    );
}

/// Nine digits and no more, at the default increment: the ninth decimal is
/// the last one the server keeps.
#[test]
fn mysql_integer_quotient_stops_at_nine_digits() {
    assert!(
        mysql_matches(
            "SELECT * FROM t WHERE qty / 3 > 0.33333333",
            default_increment(),
            mysql_row(1, "0.00", "0.000000"),
        ),
        "one third exceeds eight digits of threes"
    );
    assert!(
        !mysql_matches(
            "SELECT * FROM t WHERE qty / 3 > 0.333333333",
            default_increment(),
            mysql_row(1, "0.00", "0.000000"),
        ),
        "and equals nine of them, so it is not greater"
    );
}

/// The declared scale of the dividend moves the quotient across a word
/// boundary. At the default increment `DECIMAL(20,2)` keeps nine digits
/// while `DECIMAL(20,6)` gets eighteen, measured.
#[test]
fn mysql_dividend_scale_picks_the_word_count() {
    assert!(
        !mysql_matches(
            "SELECT * FROM t WHERE d2 / 3 > 2.333333333",
            default_increment(),
            mysql_row(0, "7.00", "7.000000"),
        ),
        "scale 2 pads a nine-digit word fully, so the quotient stays at nine digits"
    );
    assert!(
        mysql_matches(
            "SELECT * FROM t WHERE d6 / 3 > 2.333333333",
            default_increment(),
            mysql_row(0, "7.00", "7.000000"),
        ),
        "scale 6 leaves three of the four increment digits unpaid, which buys a second word"
    );
    assert!(
        !mysql_matches(
            "SELECT * FROM t WHERE d6 / 3 > 2.333333333333333333",
            default_increment(),
            mysql_row(0, "7.00", "7.000000"),
        ),
        "and stops at eighteen digits"
    );
}

/// The declared increment is answer-visible, which is why it has to be
/// declared: at 10 an integer quotient carries eighteen digits where at 4
/// it carries nine.
#[test]
fn mysql_declared_increment_changes_the_answer() {
    let wider = DivisionPrecisionIncrement::new(10).expect("10 is in range");
    assert!(
        mysql_matches(
            "SELECT * FROM t WHERE qty / 3 > 0.333333333",
            wider,
            mysql_row(1, "0.00", "0.000000"),
        ),
        "at increment 10 the quotient is 0.333333333333333333, which is greater"
    );
    assert!(
        !mysql_matches(
            "SELECT * FROM t WHERE qty / 3 > 0.333333333333333333",
            wider,
            mysql_row(1, "0.00", "0.000000"),
        ),
        "and no wider than eighteen digits"
    );
}

/// Increment zero leaves no fractional word at all, so `1 / 3` compares
/// equal to zero. Measured, and the reason a default may not be assumed.
#[test]
fn mysql_increment_zero_has_no_fractional_digits() {
    let none = DivisionPrecisionIncrement::new(0).expect("0 is in range");
    assert!(
        mysql_matches(
            "SELECT * FROM t WHERE qty / 3 = 0",
            none,
            mysql_row(1, "0.00", "0.000000"),
        ),
        "at increment 0 one third compares equal to zero"
    );
}

/// Without the declared setting the scale is unknowable, so the
/// subscription is not served in process: it is registered as a re-executed
/// tier that names the missing setting, rather than answered from a guess.
#[test]
fn mysql_division_without_the_declared_increment_is_refused() {
    let db = ParserDB::parse::<MySqlDialect>(MYSQL_DDL).expect("DDL parses");
    let mut engine: SubscriptionEngine<TestEvent<MySql>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, MySqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM t WHERE qty / 2 > 3",
        ))
        .expect("the subscription registers, to be answered by a read");
    assert!(
        matches!(
            registered.not_served_because,
            Some(subql::NotServed::DivisionPrecisionNotDeclared)
        ),
        "expected the undeclared setting as the reason, got {:?}",
        registered.not_served_because
    );
}

/// A predicate with no division registers under MySQL whether or not the
/// setting was declared: the refusal is about the operator, not the engine.
#[test]
fn mysql_without_division_needs_no_declared_increment() {
    let db = ParserDB::parse::<MySqlDialect>(MYSQL_DDL).expect("DDL parses");
    let mut engine: SubscriptionEngine<TestEvent<MySql>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, MySqlDialect {});
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM t WHERE qty * 2 > 3",
        ))
        .expect("multiplication needs no declared setting");
}

/// The one integer quotient that overflows is not a failure on MySQL,
/// because the quotient is a decimal: `bigint min / -1` answers
/// `9223372036854775808.0000`, which is greater than `bigint max`.
#[test]
fn mysql_min_bigint_over_minus_one_is_a_decimal() {
    assert!(
        mysql_matches(
            "SELECT * FROM t WHERE qty / -1 > 9223372036854775807",
            default_increment(),
            mysql_row(i64::MIN, "0.00", "0.000000"),
        ),
        "the decimal quotient exceeds bigint max instead of overflowing"
    );
}

/// PostgreSQL's `numeric` quotient carries the scale that gives sixteen
/// significant digits, so one third is twenty digits and no more.
#[test]
fn pg_numeric_quotient_takes_the_significant_digit_scale() {
    assert!(
        pg_matches(
            "SELECT * FROM t WHERE d2 / 3 > 0.3333333333333333333",
            pg_row(0, "1", "1"),
        ),
        "one third exceeds nineteen digits of threes"
    );
    assert!(
        !pg_matches(
            "SELECT * FROM t WHERE d2 / 3 > 0.33333333333333333333",
            pg_row(0, "1", "1"),
        ),
        "and equals twenty of them"
    );
}

/// And it is rounded half away from zero, not truncated: two thirds ends
/// in a seven.
#[test]
fn pg_numeric_quotient_rounds_half_up() {
    assert!(
        pg_matches(
            "SELECT * FROM t WHERE d2 / 3 = 0.66666666666666666667",
            pg_row(0, "2", "2"),
        ),
        "two thirds rounds up at the twentieth digit"
    );
    assert!(
        !pg_matches(
            "SELECT * FROM t WHERE d2 / 3 = 0.66666666666666666666",
            pg_row(0, "2", "2"),
        ),
        "so the truncated spelling does not match"
    );
}

/// The dividend's own scale raises the quotient's scale only when it
/// exceeds the significant-digit floor. `7.00 / 3` is sixteen digits,
/// measured, and the value's scale of 2 does not lift that.
#[test]
fn pg_quotient_scale_follows_the_leading_digits() {
    assert!(
        pg_matches(
            "SELECT * FROM t WHERE d2 / 3 = 2.3333333333333333",
            pg_row(0, "7.00", "7.000000"),
        ),
        "seven thirds is sixteen digits because the quotient's leading digit is bigger"
    );
}

/// PostgreSQL's integer division is untouched: it truncates, so `7 / 2`
/// is 3 and not greater than 3.
#[test]
fn pg_integer_division_still_truncates() {
    assert!(
        !pg_matches("SELECT * FROM t WHERE qty / 2 > 3", pg_row(7, "0", "0"),),
        "PostgreSQL answers 3 for 7 / 2"
    );
    assert!(
        pg_matches("SELECT * FROM t WHERE qty / 2 = 3", pg_row(7, "0", "0")),
        "and it is exactly 3"
    );
}

/// So is SQLite's, which has no decimal type at all.
#[test]
fn sqlite_integer_division_still_truncates() {
    let db = ParserDB::parse::<SQLiteDialect>(SQLITE_DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<SQLite>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, SQLiteDialect {});
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM t WHERE qty / 2 = 3",
        ))
        .expect("the predicate registers");
    assert!(
        !engine
            .consumers(&TestEvent::insert(
                table,
                vec![Value::Int(1), Value::Int(7)]
            ))
            .expect("dispatch succeeds")
            .inserted()
            .is_empty(),
        "SQLite answers 3 for 7 / 2"
    );
}

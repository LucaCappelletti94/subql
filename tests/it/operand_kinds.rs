//! What subql reads as text and as a number.
//!
//! MySQL and SQLite match a number or a boolean by its text rendering, read
//! text as the number it starts with and a boolean as its integer, and
//! PostgreSQL refuses all three. subql matches text and computes over
//! numbers only, so a `LIKE` or arithmetic over any other operand is routed
//! to a read, where the engine answers it.

use subql::backend::{MySql, Postgres, SQLite};

const DDL: &str =
    "CREATE TABLE t (id INT PRIMARY KEY, a BIGINT, r DOUBLE PRECISION, s TEXT, f BOOLEAN)";

/// Whether `predicate` over `t` is served in process on PostgreSQL, MySQL and
/// SQLite.
fn served_everywhere(predicate: &str) -> [bool; 3] {
    let sql = format!("SELECT * FROM t WHERE {predicate}");
    [
        crate::common::semantics::served::<Postgres>(DDL, &sql),
        crate::common::semantics::served::<MySql>(DDL, &sql),
        crate::common::semantics::served::<SQLite>(DDL, &sql),
    ]
}

/// A number, a boolean or a computed value is not text to subql, on either
/// side of the match.
#[test]
fn a_pattern_match_over_anything_but_text_is_routed() {
    for predicate in [
        "a LIKE '1%'",
        "f LIKE '0'",
        "r LIKE r",
        "'1' LIKE a",
        "(- s) LIKE 'a'",
        "s LIKE (a + 1)",
        "a NOT LIKE '' ESCAPE '!'",
    ] {
        assert_eq!(
            served_everywhere(predicate),
            [false; 3],
            "{predicate} is routed on every backend"
        );
    }
}

/// Text columns, strings and `NULL` stay served. MySQL's default text
/// collation is case-insensitive, which routes it there for that reason.
#[test]
fn a_pattern_match_over_text_is_served() {
    for predicate in ["s LIKE 'a%'", "'x' LIKE s", "s NOT LIKE s", "(s) LIKE NULL"] {
        assert_eq!(
            served_everywhere(predicate),
            [true, false, true],
            "{predicate} is served on PostgreSQL and SQLite"
        );
    }
}

/// Text or a boolean under arithmetic is routed on every backend.
#[test]
fn arithmetic_over_anything_but_numbers_is_routed() {
    for predicate in [
        "(- s) IS NULL",
        "(- f) IS NULL",
        "('ab' * 'x') IS NOT NULL",
        "(a - s) > 0",
        "(f + 1) = 1",
        "(a * ('1' + 1)) > 0",
    ] {
        assert_eq!(
            served_everywhere(predicate),
            [false; 3],
            "{predicate} is routed on every backend"
        );
    }
}

/// Two numeric kinds under one operator, or a `%` over anything but
/// integers, is routed. Every engine widens `1 * 2.0` to a float and subql
/// computes each kind with itself only, and `%` over a float is an error on
/// PostgreSQL, a truncation on SQLite and a remainder on MySQL.
#[test]
fn arithmetic_across_kinds_is_routed() {
    for predicate in [
        "(a + r) IS NULL",
        "(a * r) > 0",
        "(r % r) = 0",
        "(a % r) IS NULL",
        "(r % 2) = 0",
    ] {
        assert_eq!(
            served_everywhere(predicate),
            [false; 3],
            "{predicate} is routed on every backend"
        );
    }
}

/// Arithmetic over numeric columns, numbers and `NULL` stays served.
#[test]
fn arithmetic_over_numbers_is_served() {
    for predicate in [
        "(- a) > 0",
        "(a * 2) > r",
        "(a + NULL) IS NULL",
        "(- (a % 3)) = 0",
    ] {
        assert_eq!(
            served_everywhere(predicate),
            [true; 3],
            "{predicate} is served on every backend"
        );
    }
}

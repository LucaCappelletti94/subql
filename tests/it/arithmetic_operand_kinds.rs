//! Arithmetic over operands whose kinds it is not computed over.
//!
//! Registration refuses such a pair, so it reaches evaluation only through a
//! cell whose kind contradicts its column's declared type, or a promoted
//! integer beside a float on a backend that cannot convert one. Neither has
//! the answer `NULL`, so the subscription is refused rather than answered.
#![allow(clippy::unwrap_used)]

use crate::common::postgres_like::postgres_like_backend;
use sql_traits::structs::ParserDB;
use sqlparser::ast::Value as SqlValue;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Backend, NoCustomScalars, Pg18, Postgres, Value, ValueKind, ValueKindOf};
use subql::compiler::vm::refusal::{ArithmeticOp, EvaluationRefusal, IntegerOverflow};
use subql::compiler::SqlLiteralParse;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, ConsumerNotifications, DefaultIds, RegisterError, SubscriptionEngine,
    SubscriptionRequest,
};

/// Register `predicate` over `t` and insert `row`.
fn dispatch<B>(
    predicate: &str,
    row: Vec<Value<B>>,
) -> ConsumerNotifications<DefaultIds, subql::NoCheckpoint, B>
where
    B: Backend<Dialect = PostgreSqlDialect> + SqlLiteralParse,
{
    let db = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE t (id INT PRIMARY KEY, n BIGINT, d NUMERIC)",
    )
    .expect("DDL parses");
    let table = catalog_helpers::table_id::<Postgres, _>(&db, "t").expect("t is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<B>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            format!("SELECT * FROM t WHERE {predicate}"),
        ))
        .expect("the predicate registers");
    assert!(
        registered.not_served_because.is_none(),
        "{predicate} is served in process"
    );
    engine
        .consumers(&TestEvent::insert(table, row))
        .expect("dispatch succeeds")
}

fn refused<B: Backend>(
    notifications: &ConsumerNotifications<DefaultIds, subql::NoCheckpoint, B>,
) -> Vec<EvaluationRefusal> {
    notifications
        .evaluation_failures()
        .iter()
        .map(|failure| failure.refusal)
        .collect()
}

/// A cell of the wrong kind under each operator is refused, never read as
/// `NULL`, which `IS NULL` would select.
#[test]
fn a_cell_of_the_wrong_kind_is_refused_under_every_operator() {
    let decimal = || Value::Decimal("5".parse().unwrap());
    for (predicate, row, operation) in [
        (
            "(d + 1) IS NULL",
            vec![Value::Int(1), Value::Int(2), Value::Int(5)],
            ArithmeticOp::Add,
        ),
        (
            "(d - 1) IS NULL",
            vec![Value::Int(1), Value::Int(2), Value::Int(5)],
            ArithmeticOp::Subtract,
        ),
        (
            "(d * 2) IS NULL",
            vec![Value::Int(1), Value::Int(2), Value::Int(5)],
            ArithmeticOp::Multiply,
        ),
        (
            "(d / 2) IS NULL",
            vec![Value::Int(1), Value::Int(2), Value::Int(5)],
            ArithmeticOp::Divide,
        ),
        (
            "(n % 2) IS NULL",
            vec![Value::Int(1), decimal(), decimal()],
            ArithmeticOp::Modulo,
        ),
        (
            "(- n) IS NULL",
            vec![Value::Int(1), Value::String("5".into()), decimal()],
            ArithmeticOp::Negate,
        ),
    ] {
        let notifications = dispatch::<Postgres>(predicate, row);
        assert_eq!(
            refused(&notifications),
            vec![EvaluationRefusal::OperandKinds { operation }],
            "{predicate}"
        );
        assert!(notifications.inserted().is_empty(), "{predicate}");
    }
}

/// PostgreSQL's rules with an overflow that promotes, and no way to read an
/// integer as a float.
#[derive(Debug)]
struct Unconverting;

postgres_like_backend!(
    Unconverting,
    custom = NoCustomScalars<Self>,
    overflow = IntegerOverflow::PromotesToFloat,
    {}
);

impl SqlLiteralParse for Unconverting {
    fn parse_literal(
        sql: &SqlValue,
        target: ValueKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        let builtin = target.family().expect("no custom kinds");
        Ok(Self::from_postgres(Postgres::<Pg18>::parse_literal(
            sql,
            ValueKind::from(builtin),
        )?))
    }
}

/// `- n` promotes to a float, and the integer beside it, on either side of
/// any operator, cannot be read as one, so the result is refused rather than
/// answered `NULL`.
#[test]
fn a_promoted_integer_the_backend_cannot_widen_beside_is_refused() {
    for (predicate, operation) in [
        ("((- n) + 1) IS NULL", ArithmeticOp::Add),
        ("(1 + (- n)) IS NULL", ArithmeticOp::Add),
        ("((- n) - 1) IS NULL", ArithmeticOp::Subtract),
        ("(1 - (- n)) IS NULL", ArithmeticOp::Subtract),
        ("((- n) * 2) IS NULL", ArithmeticOp::Multiply),
        ("(2 * (- n)) IS NULL", ArithmeticOp::Multiply),
        ("((- n) / 2) IS NULL", ArithmeticOp::Divide),
        ("(2 / (- n)) IS NULL", ArithmeticOp::Divide),
    ] {
        let notifications = dispatch::<Unconverting>(
            predicate,
            vec![Value::Int(1), Value::Int(i64::MIN), Value::Null],
        );
        assert_eq!(
            refused(&notifications),
            vec![EvaluationRefusal::OperandKinds { operation }],
            "{predicate}"
        );
        assert!(notifications.inserted().is_empty(), "{predicate}");
    }
}

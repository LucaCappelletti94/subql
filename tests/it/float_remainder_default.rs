//! `%` over a promoted integer on a backend that states no rule for it.
//!
//! A backend whose integer overflow promotes to a float reaches
//! `Backend::float_remainder` whenever `%` reads the promoted value. The
//! default answers `None`, so the subscription reports the overflow it came
//! from rather than an answer the backend never stated. This backend takes
//! PostgreSQL's rules except that it promotes, and leaves the hook alone.
#![allow(clippy::unwrap_used)]

use crate::common::postgres_like::postgres_like_backend;
use sql_traits::structs::ParserDB;
use sqlparser::ast::Value as SqlValue;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{NoCustomScalars, Pg18, Postgres, Value, ValueKind, ValueKindOf};
use subql::compiler::vm::refusal::{ArithmeticOp, EvaluationRefusal, IntegerOverflow};
use subql::compiler::SqlLiteralParse;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, ConsumerNotifications, DefaultIds, RegisterError, SubscriptionEngine,
    SubscriptionRequest,
};

/// PostgreSQL's rules, with SQLite's promotion of an overflowed integer.
#[derive(Debug)]
struct Promoting;

postgres_like_backend!(
    Promoting,
    custom = NoCustomScalars<Self>,
    overflow = IntegerOverflow::PromotesToFloat,
    {
        fn int_as_float(value: &i64) -> Option<f64> {
            Some(subql::backend::widen_i64_to_f64(*value))
        }
    }
);

impl SqlLiteralParse for Promoting {
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

/// Register `predicate` over a row whose `qty` is the smallest integer and
/// dispatch that row.
fn dispatch(predicate: &str) -> ConsumerNotifications<DefaultIds, subql::NoCheckpoint, Promoting> {
    let db =
        ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE t (id INT PRIMARY KEY, qty BIGINT)")
            .expect("DDL parses");
    let table = catalog_helpers::table_id::<Postgres, _>(&db, "t").expect("t is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<Promoting>, DefaultIds, ParserDB> =
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
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::Int(i64::MIN)],
        ))
        .expect("dispatch succeeds")
}

/// `- qty` promotes and the arithmetic after it answers, so the backend does
/// promote. `%` over the promoted value is the overflow, reported.
#[test]
fn a_remainder_the_backend_states_no_rule_for_reports_the_overflow() {
    let promoted = dispatch("((- qty) + 1) IS NOT NULL");
    assert_eq!(promoted.evaluation_failures(), []);
    assert_eq!(promoted.inserted(), &[1], "the promoted sum is a value");

    let remainder = dispatch("((- qty) % 10) IS NOT NULL");
    assert_eq!(
        remainder
            .evaluation_failures()
            .iter()
            .map(|failure| failure.refusal)
            .collect::<Vec<_>>(),
        vec![EvaluationRefusal::IntegerOverflow {
            operation: ArithmeticOp::Modulo,
        }]
    );
    assert_eq!(remainder.inserted(), &[] as &[u64]);
}

//! `%` over a promoted integer on a backend that states no rule for it.
//!
//! A backend whose integer overflow promotes to a float reaches
//! `Backend::float_remainder` whenever `%` reads the promoted value. The
//! default answers `None`, so the subscription reports the overflow it came
//! from rather than an answer the backend never stated. This backend takes
//! PostgreSQL's rules except that it promotes, and leaves the hook alone.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::ast::Value as SqlValue;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Backend, NoCustomScalars, Pg18, Postgres, ScalarFamily, Value, ValueKind};
use subql::backend::{NumericWidening, TextOperation, TextRule, ValueKindOf};
use subql::compiler::vm::arithmetic::{checked_integer_binary, checked_integer_negate};
use subql::compiler::vm::refusal::{
    ArithmeticOp, DanglingEscape, DivisionByZero, EvaluationRefusal, IntegerOverflow,
};
use subql::compiler::SqlLiteralParse;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, ConsumerNotifications, DefaultIds, RegisterError, SubscriptionEngine,
    SubscriptionRequest,
};

/// PostgreSQL's rules, with SQLite's promotion of an overflowed integer.
#[derive(Debug)]
struct Promoting;

impl Backend for Promoting {
    const COLUMN_NAME_CASE: sql_traits::structs::IdentifierCase =
        sql_traits::structs::IdentifierCase::AsWritten;

    const WRITTEN_TABLE_NAME_CASE: sql_traits::structs::IdentifierCase =
        sql_traits::structs::IdentifierCase::AsWritten;

    const WIRE_TABLE_NAME_CASE: sql_traits::structs::IdentifierCase =
        sql_traits::structs::IdentifierCase::Exact;

    const LIKE_DEFAULT_ESCAPE: Option<char> = Some('\\');

    const LIKE_DANGLING_ESCAPE: DanglingEscape = DanglingEscape::Fails;

    fn like_escape_clause(written: &str) -> Result<Option<char>, &'static str> {
        <Postgres as Backend>::like_escape_clause(written)
    }

    const DIVISION_BY_ZERO: DivisionByZero = DivisionByZero::Fails;

    const INTEGER_OVERFLOW: IntegerOverflow = IntegerOverflow::PromotesToFloat;

    fn integer_binary(
        operation: ArithmeticOp,
        left: i64,
        right: i64,
    ) -> Result<Value<Self>, EvaluationRefusal> {
        checked_integer_binary(Self::INTEGER_OVERFLOW, operation, left, right)
    }

    fn integer_negate(value: i64) -> Result<Value<Self>, EvaluationRefusal> {
        checked_integer_negate(Self::INTEGER_OVERFLOW, value)
    }

    fn int_as_float(value: &i64) -> Option<f64> {
        Some(subql::backend::widen_i64_to_f64(*value))
    }

    fn numeric_widening(_left: ScalarFamily, _right: ScalarFamily) -> Option<NumericWidening> {
        None
    }

    const FLOAT_SUM_OVERFLOW: subql::backend::FloatSumOverflow =
        subql::backend::FloatSumOverflow::Raises;

    const FLOAT_ORDER: subql::backend::FloatOrder = subql::backend::FloatOrder::NanIsGreatest;

    const VARIANCE_SEED: subql::backend::VarianceSeed = subql::backend::VarianceSeed::EnginesOwn;

    const MEAN: subql::backend::MeanRule = subql::backend::MeanRule::Exact;

    fn sum_rule(_column: subql::backend::DeclaredType) -> subql::backend::SumRule {
        subql::backend::SumRule::Double
    }

    const DIVISION: subql::backend::DivisionRule = subql::backend::DivisionRule::IntegersTruncate;

    const NULL_SAFE_EQUALITY: subql::backend::NullSafeEquality =
        subql::backend::NullSafeEquality::DistinctFrom;

    const READS_IS_UNKNOWN: bool = true;

    const RANGE_WITH_NULL_BOUND_COMPARES_DOUBLES: bool = false;

    fn decimal_quotient(
        dividend: bigdecimal::BigDecimal,
        divisor: bigdecimal::BigDecimal,
        quotient: subql::compiler::bytecode::Quotient,
    ) -> bigdecimal::BigDecimal {
        subql::compiler::vm::arithmetic::quotient_by_rule(&dividend, &divisor, quotient)
    }

    fn integer_quotient(
        dividend: i64,
        divisor: i64,
        increment: subql::backend::DivisionPrecisionIncrement,
    ) -> bigdecimal::BigDecimal {
        subql::compiler::vm::arithmetic::integer_quotient_in_words(dividend, divisor, increment)
    }

    fn hold_float_at_single(value: f64) -> f64 {
        subql::backend::at_float4(value)
    }

    fn float_arithmetic_width(
        left: Option<subql::backend::FloatWidth>,
        right: Option<subql::backend::FloatWidth>,
    ) -> Option<subql::backend::FloatWidth> {
        left.or(right).map(|_| subql::backend::FloatWidth::Double)
    }

    fn refine_declared_type(
        family: subql::backend::ScalarFamily,
        declared_type: &str,
    ) -> subql::backend::DeclaredType {
        subql::backend::declared_type_of(
            family,
            subql::backend::declares_sixty_four_bit_int(declared_type),
            subql::backend::FloatWidth::Double,
            subql::backend::TextWidth::Varying,
        )
    }

    fn text_rule(
        _comparison: &subql::backend::ComparisonContext<'_, Self>,
        _operation: TextOperation,
    ) -> subql::backend::TextResolution {
        subql::backend::TextResolution::Rule(TextRule::EXACT)
    }

    type Dialect = PostgreSqlDialect;
    type Custom = NoCustomScalars<Self>;
    type Bool = bool;
    type Int = i64;
    type Float = f64;
    type String = String;
    type Bytes = Vec<u8>;
    type Uuid = uuid::Uuid;
    type Timestamp = chrono::NaiveDateTime;
    type TimestampTz = chrono::DateTime<chrono::Utc>;
    type Date = chrono::NaiveDate;
    type Time = chrono::NaiveTime;
    type Decimal = bigdecimal::BigDecimal;
    type Json = serde_json::Value;
    type Jsonb = serde_json::Value;
    type JsonbVersion = Pg18;
}

impl SqlLiteralParse for Promoting {
    fn parse_literal(
        sql: &SqlValue,
        target: ValueKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        let builtin = target.family().expect("no custom kinds");
        Ok(
            match Postgres::<Pg18>::parse_literal(sql, ValueKind::from(builtin))? {
                Value::Missing => Value::Missing,
                Value::Null => Value::Null,
                Value::Bool(v) => Value::Bool(v),
                Value::Int(v) => Value::Int(v),
                Value::Float(v) => Value::Float(v),
                Value::String(v) => Value::String(v),
                Value::Bytes(v) => Value::Bytes(v),
                Value::Uuid(v) => Value::Uuid(v),
                Value::Timestamp(v) => Value::Timestamp(v),
                Value::TimestampTz(v) => Value::TimestampTz(v),
                Value::Date(v) => Value::Date(v),
                Value::Time(v) => Value::Time(v),
                Value::Decimal(v) => Value::Decimal(v),
                Value::Json(v) => Value::Json(v),
                Value::Jsonb(v) => Value::Jsonb(v),
                Value::Custom(none) => match none {},
            },
        )
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
        "{predicate} is served in process, got {:?}",
        registered.not_served_because
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
    assert!(remainder.inserted().is_empty());
}

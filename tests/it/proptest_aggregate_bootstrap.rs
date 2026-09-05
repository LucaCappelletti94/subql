//! Property: for every `AggSpec` and every starting table, the value the engine
//! holds after being given the bootstrap component row equals a direct
//! aggregate recompute over that table. This pins the component-to-slot
//! mapping against textbook aggregate math and catches any future `AggSpec`
//! variant that decodes its components wrong.
//!
//! subql has no database, so "run the bootstrap SQL" is modeled by
//! computing the component cells (`c`, `s`, `sq`) a correct executor would
//! return, exactly as the rendered SQL projects them. The exact rendered
//! SQL itself is pinned by `tests/it/aggregate_bootstrap.rs`.

#![allow(clippy::unwrap_used)]

use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{AggSpec, AggValue, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, amount INT);";

/// Component cells the bootstrap query returns for `amounts` (None = NULL
/// `amount`). Bounded amounts keep `sum` and `sum_sq` exact in `f64`.
struct Components {
    count_star: i64,
    count_col: i64,
    sum: i64,
    sum_sq: i64,
    numeric: i64,
}

fn components(amounts: &[Option<i64>]) -> Components {
    let mut c = Components {
        count_star: 0,
        count_col: 0,
        sum: 0,
        sum_sq: 0,
        numeric: 0,
    };
    c.count_star = i64::try_from(amounts.len()).unwrap();
    for a in amounts.iter().flatten() {
        c.count_col += 1;
        c.numeric += 1;
        c.sum += *a;
        c.sum_sq += *a * *a;
    }
    c
}

/// SUM/SUM(sq) components arrive as NULL when no non-NULL row matched.
const fn sum_cell(value: i64, numeric: i64) -> Value<Postgres> {
    if numeric == 0 {
        Value::Null
    } else {
        Value::Int(value)
    }
}

/// The seed row per spec, mirroring the rendered bootstrap projection.
fn seed_row(spec: &AggSpec, c: &Components) -> Vec<Value<Postgres>> {
    match spec {
        AggSpec::CountStar => vec![Value::Int(c.count_star)],
        AggSpec::CountColumn { .. } => vec![Value::Int(c.count_col)],
        // SUM and AVG read the same pair: the total and its contributors.
        AggSpec::Sum { .. } | AggSpec::Avg { .. } => {
            vec![sum_cell(c.sum, c.numeric), Value::Int(c.numeric)]
        }
        AggSpec::VarPop { .. }
        | AggSpec::VarSamp { .. }
        | AggSpec::StddevPop { .. }
        | AggSpec::StddevSamp { .. } => vec![
            sum_cell(c.sum, c.numeric),
            sum_cell(c.sum_sq, c.numeric),
            Value::Int(c.numeric),
        ],
        _ => unreachable!("all_specs enumerates every AggSpec variant"),
    }
}

/// Textbook aggregate value over the components, independent of the
/// accumulator's own arithmetic path where possible.
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
fn oracle(spec: &AggSpec, c: &Components) -> AggValue {
    let n = c.numeric as f64;
    let sum = c.sum as f64;
    let sum_sq = c.sum_sq as f64;
    let var_pop = (c.numeric > 0).then(|| sum_sq / n - (sum / n).powi(2));
    let var_samp = (c.numeric >= 2).then(|| (sum_sq - sum.powi(2) / n) / (n - 1.0));
    match spec {
        AggSpec::CountStar => AggValue::Count(c.count_star),
        AggSpec::CountColumn { .. } => AggValue::Count(c.count_col),
        // The fixture sums an `INT` column under Postgres, whose sum is a
        // `bigint`, so the oracle's total is an exact integer.
        AggSpec::Sum { .. } => {
            AggValue::Sum((c.numeric > 0).then_some(subql::NumericValue::Integer(c.sum)))
        }
        // Postgres divides the exact total by the count as `numeric`.
        AggSpec::Avg { .. } => AggValue::Avg((c.numeric > 0).then(|| {
            subql::NumericValue::Decimal(
                subql::compiler::vm::arithmetic::quotient_at_significant_digits(
                    &bigdecimal::BigDecimal::from(c.sum),
                    &bigdecimal::BigDecimal::from(c.numeric),
                ),
            )
        })),
        AggSpec::VarPop { .. } => AggValue::Real(var_pop),
        AggSpec::VarSamp { .. } => AggValue::Real(var_samp),
        AggSpec::StddevPop { .. } => AggValue::Real(var_pop.map(f64::sqrt)),
        AggSpec::StddevSamp { .. } => AggValue::Real(var_samp.map(f64::sqrt)),
        _ => unreachable!("all_specs enumerates every AggSpec variant"),
    }
}

/// Every aggregate the in-process family maintains, as the SQL that registers
/// it paired with the spec the oracle reads.
fn all_specs() -> Vec<(&'static str, AggSpec)> {
    vec![
        ("SELECT COUNT(*) FROM t", AggSpec::CountStar),
        (
            "SELECT COUNT(amount) FROM t",
            AggSpec::CountColumn { column: 1 },
        ),
        ("SELECT SUM(amount) FROM t", AggSpec::Sum { column: 1 }),
        ("SELECT AVG(amount) FROM t", AggSpec::Avg { column: 1 }),
        (
            "SELECT VAR_POP(amount) FROM t",
            AggSpec::VarPop { column: 1 },
        ),
        (
            "SELECT VAR_SAMP(amount) FROM t",
            AggSpec::VarSamp { column: 1 },
        ),
        (
            "SELECT STDDEV_POP(amount) FROM t",
            AggSpec::StddevPop { column: 1 },
        ),
        (
            "SELECT STDDEV_SAMP(amount) FROM t",
            AggSpec::StddevSamp { column: 1 },
        ),
    ]
}

/// Register `sql` on `db`, hand it `row` as its starting numbers, and answer
/// with the value it then holds. Nothing has been folded against it, so the
/// read cannot have raced a change and needs no stream position.
fn seeded_value(db: &ParserDB, sql: &str, row: &[Value<Postgres>]) -> AggValue {
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db.clone(), PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, sql))
        .unwrap();
    let updates = subql::Install::install(
        &mut engine,
        registered.subscription_id,
        subql::AggregateSeedInstall {
            rows: vec![row.to_vec()],
            read_at: None,
        },
    )
    .expect("the starting numbers land");
    assert_eq!(updates.len(), 1);
    updates[0]
        .folded_value()
        .expect("ungrouped install sets a value")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn seed_matches_recompute(
        amounts in proptest::collection::vec(
            proptest::option::of(-1000i64..=1000),
            0..=50,
        ),
    ) {
        let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
        let c = components(&amounts);
        for (sql, spec) in all_specs() {
            prop_assert_eq!(
                seeded_value(&db, sql, &seed_row(&spec, &c)),
                oracle(&spec, &c),
                "seed mismatch for {:?} over {:?}", spec, amounts
            );
        }
    }
}

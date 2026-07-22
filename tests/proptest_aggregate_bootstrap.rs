//! Property: for every `AggSpec` and every starting table, seeding an
//! accumulator from the bootstrap component row equals a direct aggregate
//! recompute over that table. This pins `AggAccumulator::seed_from_row`'s
//! component-to-slot mapping against textbook aggregate math and catches
//! any future `AggSpec` variant that decodes its components wrong.
//!
//! subql has no database, so "run the bootstrap SQL" is modeled by
//! computing the component cells (`c`, `s`, `sq`) a correct executor would
//! return, exactly as the rendered SQL projects them. The exact rendered
//! SQL itself is pinned by `tests/aggregate_bootstrap.rs`.

#![allow(clippy::unwrap_used)]

use proptest::prelude::*;
use subql::backend::{Postgres, Value};
use subql::{AggAccumulator, AggSpec, AggValue};

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
        AggSpec::Sum { .. } => vec![sum_cell(c.sum, c.numeric)],
        AggSpec::Avg { .. } => vec![sum_cell(c.sum, c.numeric), Value::Int(c.numeric)],
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
        AggSpec::Sum { .. } => AggValue::Sum(sum),
        AggSpec::Avg { .. } => AggValue::Real((c.numeric > 0).then(|| sum / n)),
        AggSpec::VarPop { .. } => AggValue::Real(var_pop),
        AggSpec::VarSamp { .. } => AggValue::Real(var_samp),
        AggSpec::StddevPop { .. } => AggValue::Real(var_pop.map(f64::sqrt)),
        AggSpec::StddevSamp { .. } => AggValue::Real(var_samp.map(f64::sqrt)),
        _ => unreachable!("all_specs enumerates every AggSpec variant"),
    }
}

fn all_specs() -> Vec<AggSpec> {
    vec![
        AggSpec::CountStar,
        AggSpec::CountColumn { column: 1 },
        AggSpec::Sum { column: 1 },
        AggSpec::Avg { column: 1 },
        AggSpec::VarPop { column: 1 },
        AggSpec::VarSamp { column: 1 },
        AggSpec::StddevPop { column: 1 },
        AggSpec::StddevSamp { column: 1 },
    ]
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
        let c = components(&amounts);
        for spec in all_specs() {
            let seeded = AggAccumulator::seed_from_row(&spec, &seed_row(&spec, &c));
            prop_assert_eq!(
                seeded.value(),
                oracle(&spec, &c),
                "seed mismatch for {:?} over {:?}", spec, amounts
            );
        }
    }
}

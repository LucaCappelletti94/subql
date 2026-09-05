#![allow(
    clippy::manual_let_else,
    clippy::too_long_first_doc_paragraph,
    clippy::items_after_statements,
    clippy::needless_pass_by_value,
    clippy::map_entry,
    clippy::match_same_arms
)]

use std::collections::BTreeMap;

use arbitrary::{Arbitrary, Unstructured};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use crate::backend::{Postgres, Value};
use crate::runtime::aggregate::AggAccumulator;
use crate::testing::TestEvent;
use crate::{
    catalog_helpers, AggSpec, AggValue, DefaultIds, RegisterError, Registered, SubscriptionEngine,
    SubscriptionRequest, Tier,
};

/// Mutation operation against the virtual `orders` table used by
/// [`harness_aggregate_consistency`]. `Truncate` is absent because the engine
/// answers it from the held totals rather than from row images, which the unit
/// tests in `tests/it/aggregate_totals.rs` cover directly.
///
/// `amount` is bounded to `i16` (not `i32`) so that squared values stay
/// well inside f64's exact-integer range (2^53). Streaming variance over
/// widely varying magnitudes hits unavoidable catastrophic-cancellation
/// noise when squared values approach or exceed 2^53, and that noise is
/// not a routing or correctness bug in the engine. The harness's purpose
/// is to catch routing/semantic drift, so the bound removes the
/// f64-precision confounder.
#[derive(Debug, Arbitrary)]
pub(super) enum AggOp {
    Insert {
        id: u8,
        amount: Option<i16>,
        status: Option<u8>,
    },
    Update {
        id: u8,
        amount: Option<i16>,
        status: Option<u8>,
    },
    Delete {
        id: u8,
    },
}

/// In-virtual-table representation of one row.
#[derive(Clone, Debug)]
pub struct VirtRow {
    pub amount: Option<i64>,
    pub status: Option<String>,
}

impl VirtRow {
    pub fn from_op(amount: Option<i16>, status: Option<u8>) -> Self {
        Self {
            amount: amount.map(i64::from),
            status: status.map(|b| match b % 4 {
                0 => "open".into(),
                1 => "closed".into(),
                2 => "shipped".into(),
                _ => "pending".into(),
            }),
        }
    }
}

/// Build the 3-cell row image `(id, amount, status)` matching
/// `agg_catalog()`'s schema, using `Value<Postgres>` variants.
pub fn agg_row_values(id: i64, row: &VirtRow) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        row.amount.map_or(Value::Null, Value::Int),
        row.status
            .as_deref()
            .map_or(Value::Null, |s| Value::String(s.to_string())),
    ]
}

/// Build the `agg_catalog()` `ParserDB` once: three columns, single-
/// column INT PK. Distinct from `fuzz_catalog()` so the column-id
/// mapping is predictable (id=0, amount=1, status=2).
pub fn agg_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    )
    .expect("agg fuzz fixture DDL parses")
}

/// Pre-built engine + table metadata shared across every iteration of
/// [`harness_aggregate_consistency`] within a fuzz worker. Reusing the
/// engine across iterations is the load-bearing reason this exists: the
/// harness would otherwise drop and re-create a `SubscriptionEngine` on
/// every call, and under ASAN that allocator churn drifts the worker's
/// RSS past libFuzzer's default limit after tens of thousands of
/// iterations even though no individual iteration leaks.
///
/// Cargo-fuzz runs single-threaded, so the `thread_local!` cell is
/// shared by every iteration on the only worker thread. Re-entrancy is
/// not possible.
struct AggEngineCell {
    engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>,
    table_id: crate::TableId,
    pk_col: crate::ColumnId,
    /// Every registered aggregate in consumer order, with the id the engine
    /// files its running total under and the function that total maintains.
    totals: Vec<(u64, crate::SubscriptionId, AggSpec)>,
}

impl AggEngineCell {
    fn new() -> Self {
        let database = agg_catalog();
        let table_id = catalog_helpers::table_id(&database, "orders")
            .expect("agg_catalog must expose an `orders` table");
        let pk_col = catalog_helpers::column_id(&database, table_id, "id")
            .expect("agg_catalog `orders` must expose an `id` column");
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(database, PostgreSqlDialect {});
        // One consumer per flavor. The four VAR/STDDEV flavors share one
        // kernel and hold identical running numbers, so registering all four
        // catches per-variant routing and hash-collision bugs as well as
        // kernel correctness.
        let mut totals = Vec::new();
        for (cid, sql) in [
            (1_u64, "SELECT COUNT(*) FROM orders"),
            (2_u64, "SELECT SUM(amount) FROM orders"),
            (3_u64, "SELECT VAR_POP(amount) FROM orders"),
            (4_u64, "SELECT VAR_SAMP(amount) FROM orders"),
            (5_u64, "SELECT STDDEV_POP(amount) FROM orders"),
            (6_u64, "SELECT STDDEV_SAMP(amount) FROM orders"),
            (7_u64, "SELECT AVG(amount) FROM orders"),
        ] {
            let registered = engine
                .register(SubscriptionRequest::<DefaultIds>::new(cid, sql))
                .expect("registering an aggregate consumer should succeed against agg_catalog");
            let Tier::InProcess(served) = &registered.tier else {
                panic!("an aggregate the engine maintains registers in process")
            };
            let spec = served
                .aggregate_spec()
                .expect("an aggregate registration carries its spec")
                .clone();
            totals.push((cid, registered.subscription_id, spec));
        }
        Self {
            engine,
            table_id,
            pk_col,
            totals,
        }
    }
}

std::thread_local! {
    static AGG_ENGINE: std::cell::RefCell<AggEngineCell> =
        std::cell::RefCell::new(AggEngineCell::new());
}

/// Every aggregate flavor spanning both families (in-process delta and
/// captured `MIN`/`MAX`), used by the RLS-guard invariant in
/// [`harness_aggregate_consistency`].
const RLS_GUARD_FLAVORS: &[&str] = &[
    "SELECT COUNT(*) FROM orders",
    "SELECT COUNT(amount) FROM orders",
    "SELECT SUM(amount) FROM orders",
    "SELECT AVG(amount) FROM orders",
    "SELECT VAR_POP(amount) FROM orders",
    "SELECT VAR_SAMP(amount) FROM orders",
    "SELECT STDDEV_POP(amount) FROM orders",
    "SELECT STDDEV_SAMP(amount) FROM orders",
    "SELECT MIN(amount) FROM orders",
    "SELECT MAX(amount) FROM orders",
];

/// `agg_catalog()`'s `orders` table with row-level security enabled, so
/// [`catalog_helpers::table_has_rls`] returns true for it.
fn rls_agg_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT); \
         ALTER TABLE orders ENABLE ROW LEVEL SECURITY;",
    )
    .expect("rls agg fuzz fixture DDL parses")
}

/// Pre-built reexec wrappers over an RLS and a non-RLS `orders` catalog,
/// shared across iterations of [`harness_aggregate_consistency`] so the
/// invariant check adds no per-call engine allocation (same reasoning as
/// [`AggEngineCell`]).
struct RlsGuardCell {
    rls_engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>,
    plain_engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>,
    rls_table_id: crate::TableId,
}

impl RlsGuardCell {
    fn new() -> Self {
        let rls_db = rls_agg_catalog();
        let rls_table_id = catalog_helpers::table_id(&rls_db, "orders")
            .expect("rls_agg_catalog must expose an `orders` table");
        let rls_engine = SubscriptionEngine::new(rls_db, PostgreSqlDialect {});
        let plain_engine = SubscriptionEngine::new(agg_catalog(), PostgreSqlDialect {});
        Self {
            rls_engine,
            plain_engine,
            rls_table_id,
        }
    }

    /// Assert the RLS guard: every aggregate flavor is rejected on the
    /// RLS table with `AggregatorOnRlsTable` (never an in-process tier),
    /// while `plain_flavor` is accepted on the non-RLS table. RLS
    /// registration errors before mutating engine state, so looping all
    /// flavors adds no state growth. The non-RLS acceptance is
    /// register-then-unregister so the plain engine stays bounded.
    fn check(&mut self, plain_flavor: &str) {
        let consumer = 1u64;
        for flavor in RLS_GUARD_FLAVORS {
            match self
                .rls_engine
                .register(SubscriptionRequest::<DefaultIds>::new(consumer, *flavor))
            {
                Err(RegisterError::AggregatorOnRlsTable { table_id }) => {
                    assert_eq!(
                        table_id, self.rls_table_id,
                        "`{flavor}` rejected for the wrong table id"
                    );
                }
                other => panic!("`{flavor}` on RLS table must be rejected, got {other:?}"),
            }
        }
        match self
            .plain_engine
            .register(SubscriptionRequest::<DefaultIds>::new(
                consumer,
                plain_flavor,
            )) {
            Ok(Registered {
                tier: Tier::InProcess(_),
                ..
            }) => {
                let _ = self.plain_engine.unregister_query(consumer, plain_flavor);
            }
            Ok(Registered {
                subscription_id, ..
            }) => {
                assert!(self.plain_engine.unregister_reread(subscription_id));
            }
            Err(e) => panic!("`{plain_flavor}` without RLS must be accepted, got Err({e:?})"),
        }
    }
}

std::thread_local! {
    static RLS_GUARD: std::cell::RefCell<RlsGuardCell> =
        std::cell::RefCell::new(RlsGuardCell::new());
}

/// Bootstrap seed components over the virtual table, mirroring what
/// [`crate::AggregateBootstrap`] projects (`c0`, `c1`, `c2`).
struct AggComponents {
    count_star: i64,
    count_col: i64,
    sum: i64,
    sum_sq: i64,
    numeric: i64,
}

impl AggComponents {
    #[allow(clippy::cast_precision_loss)]
    const fn sum_sq_f64(&self) -> f64 {
        self.sum_sq as f64
    }
    /// SUM / SUM(sq) components read back as NULL when no non-NULL row matched.
    const fn sum_cell(&self) -> Value<Postgres> {
        if self.numeric == 0 {
            Value::Null
        } else {
            Value::Int(self.sum)
        }
    }
    const fn sum_sq_cell(&self) -> Value<Postgres> {
        if self.numeric == 0 {
            Value::Null
        } else {
            Value::Int(self.sum_sq)
        }
    }
}

fn agg_components(virt: &BTreeMap<i64, VirtRow>) -> AggComponents {
    let mut c = AggComponents {
        count_star: i64::try_from(virt.len()).unwrap_or(i64::MAX),
        count_col: 0,
        sum: 0,
        sum_sq: 0,
        numeric: 0,
    };
    for a in virt.values().filter_map(|r| r.amount) {
        c.count_col += 1;
        c.numeric += 1;
        c.sum += a;
        c.sum_sq += a * a;
    }
    c
}

/// How Postgres folds the fixtures' `i16` amount column: its sum is a
/// `bigint`, its mean an exact `numeric`, and its division needs no
/// declared setting.
const FOLD_RULE: crate::runtime::aggregate::FoldRule = crate::runtime::aggregate::FoldRule {
    total: crate::backend::SumRule::Integer,
    mean: crate::backend::MeanRule::Exact,
    quotient: crate::compiler::bytecode::Quotient::FromTheOperands,
};

/// The oracle's exact total, in the type that rule answers.
const fn exact_total(sum: i64) -> crate::NumericValue {
    crate::NumericValue::Integer(sum)
}

/// Textbook aggregate value over the components, using the same formulas
/// as [`AggAccumulator::value`], so seeding matches it exactly.
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
fn oracle_agg_value(spec: &AggSpec, c: &AggComponents) -> AggValue {
    let n = c.numeric as f64;
    let sum = c.sum as f64;
    let sum_sq = c.sum_sq as f64;
    let var_pop = (c.numeric > 0).then(|| sum_sq / n - (sum / n).powi(2));
    let var_samp = (c.numeric >= 2).then(|| (sum_sq - sum.powi(2) / n) / (n - 1.0));
    match spec {
        AggSpec::CountStar => AggValue::Count(c.count_star),
        AggSpec::CountColumn { .. } => AggValue::Count(c.count_col),
        // The fixtures sum an `i16` column under Postgres, whose sum is a
        // `bigint`, so the oracle's exact total is an integer.
        AggSpec::Sum { .. } => AggValue::Sum((c.numeric > 0).then(|| exact_total(c.sum))),
        // Postgres divides the exact total by the count as `numeric`.
        AggSpec::Avg { .. } => AggValue::Avg((c.numeric > 0).then(|| {
            crate::NumericValue::Decimal(
                crate::compiler::vm::arithmetic::quotient_at_significant_digits(
                    &bigdecimal::BigDecimal::from(c.sum),
                    &bigdecimal::BigDecimal::from(c.numeric),
                ),
            )
        })),
        AggSpec::VarPop { .. } => AggValue::Real(var_pop),
        AggSpec::VarSamp { .. } => AggValue::Real(var_samp),
        AggSpec::StddevPop { .. } => AggValue::Real(var_pop.map(f64::sqrt)),
        AggSpec::StddevSamp { .. } => AggValue::Real(var_samp.map(f64::sqrt)),
    }
}

/// Whether a value the engine folded event by event agrees with one
/// recomputed from scratch.
///
/// Tolerance scales with the components in play. A variance is a difference of
/// two large numbers, so where they nearly cancel the value keeps far fewer
/// significant digits than its inputs, and a standard deviation takes a square
/// root of that. Still orders of magnitude tighter than any dropped or
/// misrouted delta, which moves these values by whole units, because the
/// amounts driving them are `i16`.
#[allow(clippy::cast_precision_loss)]
fn agg_values_agree(engine: &AggValue, oracle: &AggValue, c: &AggComponents) -> bool {
    match (engine, oracle) {
        (AggValue::Count(a), AggValue::Count(b)) => a == b,
        (AggValue::Sum(None), AggValue::Sum(None)) => true,
        // An exact total agrees exactly, which is the whole point of it.
        (AggValue::Sum(Some(a)), AggValue::Sum(Some(b))) => a == b,
        (AggValue::Avg(None), AggValue::Avg(None)) => true,
        // A mean is exact here too, being the engine's own division of the
        // exact total by the count.
        (AggValue::Avg(Some(a)), AggValue::Avg(Some(b))) => a == b,
        (AggValue::Real(None), AggValue::Real(None)) => true,
        (AggValue::Real(Some(a)), AggValue::Real(Some(b))) => {
            (a - b).abs() <= 1e-3_f64.max(c.sum_sq_f64().abs().sqrt() * 1e-5)
        }
        _ => false,
    }
}

/// The bootstrap component row for `spec` over `c`, in the column order
/// [`crate::AggregateBootstrap`] projects.
fn seed_row(spec: &AggSpec, c: &AggComponents) -> Vec<Value<Postgres>> {
    match spec {
        AggSpec::CountStar => alloc::vec![Value::Int(c.count_star)],
        AggSpec::CountColumn { .. } => alloc::vec![Value::Int(c.count_col)],
        // SUM and AVG read the same pair: the total and its contributors.
        AggSpec::Sum { .. } | AggSpec::Avg { .. } => {
            alloc::vec![c.sum_cell(), Value::Int(c.numeric)]
        }
        AggSpec::VarPop { .. }
        | AggSpec::VarSamp { .. }
        | AggSpec::StddevPop { .. }
        | AggSpec::StddevSamp { .. } => {
            alloc::vec![c.sum_cell(), c.sum_sq_cell(), Value::Int(c.numeric)]
        }
    }
}

/// Seeding from the bootstrap component row must equal a direct recompute
/// for every `AggSpec`. Exact: seed and oracle share f64 inputs and math.
fn assert_seed_matches_oracle(c: &AggComponents) {
    let specs = [
        AggSpec::CountStar,
        AggSpec::CountColumn { column: 1 },
        AggSpec::Sum { column: 1 },
        AggSpec::Avg { column: 1 },
        AggSpec::VarPop { column: 1 },
        AggSpec::VarSamp { column: 1 },
        AggSpec::StddevPop { column: 1 },
        AggSpec::StddevSamp { column: 1 },
    ];
    for spec in specs {
        assert_eq!(
            AggAccumulator::seed_from_row(&spec, FOLD_RULE, &seed_row(&spec, c)).value(),
            oracle_agg_value(&spec, c),
            "seed decode drift for {spec:?}",
        );
    }
}

/// Drive an arbitrary sequence of insert/update/delete operations against a
/// fixed agg-only consumer set and assert that the value the engine holds for
/// every subscription equals a from-scratch oracle after every event.
///
/// Two properties. The held value equals the oracle after each event, and a
/// value that moved was reported. The second is the one a silent engine
/// breaks, and it holds because a value cannot move without a non-zero delta.
///
/// Covers `COUNT(*)`, `SUM`, `AVG`, and all four of
/// `VAR_POP`/`VAR_SAMP`/`STDDEV_POP`/`STDDEV_SAMP`, one consumer each.
///
/// Contract: panics are bugs. Assertion failures are bugs.
#[allow(clippy::too_many_lines)]
pub fn harness_aggregate_consistency(data: &[u8]) {
    // RLS guard invariant, independent of the ops stream below so it does
    // not perturb the aggregate-consistency coverage: registering any
    // aggregate flavor against an RLS-marked table is rejected with
    // AggregatorOnRlsTable (never an in-process tier), while the flavor
    // chosen from the raw input is accepted on the non-RLS table.
    RLS_GUARD.with(|cell| {
        let idx = usize::from(data.first().copied().unwrap_or(0)) % RLS_GUARD_FLAVORS.len();
        cell.borrow_mut().check(RLS_GUARD_FLAVORS[idx]);
    });

    let mut u = Unstructured::new(data);
    type PrepopRow = (u8, Option<i16>, Option<u8>);
    let Ok((prepop, ops)): arbitrary::Result<(Vec<PrepopRow>, Vec<AggOp>)> = (|| {
        let k = u.int_in_range(0usize..=16)?;
        let prepop = (0..k)
            .map(|_| Ok((u.arbitrary()?, u.arbitrary()?, u.arbitrary()?)))
            .collect::<arbitrary::Result<Vec<PrepopRow>>>()?;
        let n = u.int_in_range(0usize..=64)?;
        let ops = (0..n)
            .map(|_| AggOp::arbitrary(&mut u))
            .collect::<arbitrary::Result<Vec<AggOp>>>()?;
        Ok((prepop, ops))
    })() else {
        return;
    };

    AGG_ENGINE.with(|cell| {
        let mut cell = cell.borrow_mut();
        let AggEngineCell {
            engine,
            table_id,
            pk_col,
            totals,
        } = &mut *cell;
        let table_id = *table_id;
        let pk_col = *pk_col;

        // Virtual table (id -> row), the source of truth for the oracle.
        // Pre-populate an arbitrary S0 so the accumulators start from a
        // bootstrap seed over a non-empty table, not from empty.
        let mut virt: BTreeMap<i64, VirtRow> = BTreeMap::new();
        for (id, amount, status) in prepop {
            virt.entry(i64::from(id))
                .or_insert_with(|| VirtRow::from_op(amount, status));
        }
        let s0 = agg_components(&virt);

        // Exercise the seed decode over the arbitrary S0: seeding from the
        // bootstrap component row must equal a direct recompute.
        assert_seed_matches_oracle(&s0);

        // The engine holds the running values now, so each iteration resets
        // every total and seeds it from S0. The cell is reused across
        // iterations, so without the reset the second iteration would be
        // refused as already seeded.
        for (consumer, subscription, spec) in totals.iter() {
            assert!(
                engine.reset_aggregate_value(*subscription),
                "subscription {subscription} should be a live aggregate",
            );
            let seeded = crate::Install::install(
                engine,
                *subscription,
                crate::AggregateSeedInstall {
                    rows: vec![seed_row(spec, &s0)],
                    read_at: None,
                },
            )
            .expect("a seed with nothing folded against it lands");
            assert_eq!(seeded.len(), 1, "one ungrouped opening value");
            assert_eq!(seeded[0].subscription, *subscription);
            assert_eq!(seeded[0].consumer, *consumer);
            assert_eq!(seeded[0].group, None);
            assert_eq!(
                seeded[0].change,
                crate::AggregateValueChange::Set(crate::AggregateResultValue::Folded(
                    oracle_agg_value(spec, &s0),
                )),
                "seed value drift for {spec:?}",
            );
        }

        // The value each subscription held before the event about to be
        // dispatched, so "a value that moved was reported" can be checked.
        let mut previous: Vec<AggValue> = totals
            .iter()
            .map(|(_, _, spec)| oracle_agg_value(spec, &s0))
            .collect();

        for op in ops {
            let (event, mutated): (Option<TestEvent<Postgres>>, bool) = match op {
                AggOp::Insert { id, amount, status } => {
                    let id = i64::from(id);
                    if virt.contains_key(&id) {
                        (None, false)
                    } else {
                        let row = VirtRow::from_op(amount, status);
                        let values = agg_row_values(id, &row);
                        virt.insert(id, row);
                        let event = TestEvent::<Postgres>::insert(table_id, values)
                            .with_pk_columns([pk_col]);
                        (Some(event), true)
                    }
                }
                AggOp::Update { id, amount, status } => {
                    let id = i64::from(id);
                    let Some(old) = virt.get(&id).cloned() else {
                        continue;
                    };
                    let new_row = VirtRow::from_op(amount, status);
                    let old_values = agg_row_values(id, &old);
                    let new_values = agg_row_values(id, &new_row);
                    virt.insert(id, new_row);
                    let event = TestEvent::<Postgres>::update(table_id, old_values, new_values)
                        .with_pk_columns([pk_col]);
                    (Some(event), true)
                }
                AggOp::Delete { id } => {
                    let id = i64::from(id);
                    let Some(old) = virt.remove(&id) else {
                        continue;
                    };
                    let old_values = agg_row_values(id, &old);
                    let event = TestEvent::<Postgres>::delete(table_id, old_values)
                        .with_pk_columns([pk_col]);
                    (Some(event), true)
                }
            };

            if !mutated {
                continue;
            }
            let Some(event) = event else { continue };

            let updates = match engine.aggregate_updates(&event) {
                Ok(u) => u,
                Err(_) => return,
            };
            let reported: BTreeMap<crate::SubscriptionId, AggValue> = updates
                .iter()
                .map(|u| {
                    let crate::AggregateValueChange::Set(crate::AggregateResultValue::Folded(
                        value,
                    )) = &u.change
                    else {
                        panic!("ungrouped aggregate cannot remove a group")
                    };
                    (u.subscription, value.clone())
                })
                .collect();

            let now = agg_components(&virt);
            for (slot, (cid, subscription, spec)) in totals.iter().enumerate() {
                let oracle = oracle_agg_value(spec, &now);
                let held = engine
                    .current_aggregate_value(*subscription)
                    .expect("a seeded aggregate holds a value");
                assert!(
                    agg_values_agree(&held, &oracle, &now),
                    "consumer {cid} value drift: engine={held:?} oracle={oracle:?}",
                );
                match reported.get(subscription) {
                    Some(value) => assert_eq!(
                        value, &held,
                        "consumer {cid} reported a value it does not hold",
                    ),
                    None => assert!(
                        previous[slot] == oracle,
                        "consumer {cid} moved from {:?} to {oracle:?} without reporting",
                        previous[slot],
                    ),
                }
                previous[slot] = oracle;
            }
        }
    });
}

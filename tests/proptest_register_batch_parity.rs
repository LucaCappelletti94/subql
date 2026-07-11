//! Property-based parity test: `register_batch(specs)` and a `for`-loop
//! of `register(spec)` on a fresh engine with the same configuration
//! must produce equal per-index `Result`s and equal final registry
//! state.
//!
//! `register_batch` has its own bulk-dedup / single-COW path
//! (`src/runtime/engine.rs` lines 832 onward) which diverges from the
//! sequential `register` path. Any divergence (more or less
//! permissive, different evicted targets, different `SubscriptionId`
//! assignment, different normalised SQL) is a bug.
//!
//! Scope. We test parity across the full eviction-policy surface plus
//! a `with_custom_eviction` closure:
//! 1. Default config (uncapped registry).
//! 2. `EvictionPolicy::Reject` with a small cap.
//! 3. `EvictionPolicy::EvictOldest` with a small cap.
//! 4. `EvictionPolicy::EvictLeastActive` with a small cap.
//! 5. `EvictionPolicy::EvictColdest` with a small cap.
//! 6. `EvictionPolicy::EvictBySession` with mixed Durable/Session specs.
//! 7. `EvictionPolicy::EvictByConsumer` with consumer-id variation.
//! 8. A `with_custom_eviction` closure (lowest consumer id wins).
//!
//! Policies 4 and 5 require activity tracking. The engine auto-installs
//! a `StdClock` when the policy needs it (`ensure_activity_clock_for_strategy`),
//! so no extra wiring is needed here. With no dispatch happening
//! between registrations, every subscription has the same activity
//! stats (`last_dispatch_at = None`, `dispatch_count = 0`), so the tie
//! breaker (oldest `SubscriptionId`) decides every eviction. That keeps
//! the test focused on *registration-path* parity rather than dispatch
//! interleavings, which is what `register_batch` actually owns.

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::cast_possible_truncation
)]

use proptest::collection::vec;
use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{
    DefaultIds, EvictionPolicy, RegisterError, RegisterResult, SubscriptionEngine,
    SubscriptionRequest, SubscriptionScope, SubscriptionsView,
};

const CATALOG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn fresh(cap: Option<usize>, policy: Option<EvictionPolicy>) -> Engine {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(CATALOG_DDL).unwrap();
    let mut engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {});
    if let (Some(cap), Some(policy)) = (cap, policy) {
        engine = engine.with_max_subscriptions(cap, policy);
    }
    engine
}

fn fresh_custom(cap: usize) -> Engine {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(CATALOG_DDL).unwrap();
    SubscriptionEngine::new(catalog, PostgreSqlDialect {})
        .with_custom_eviction(cap, lowest_consumer_evictor)
}

/// Distinct SQL strings per `(consumer, tag)` so registrations are not
/// constantly hitting the dedup path. Some collisions still happen
/// inside a batch and that is part of what we test.
fn spec_sql(consumer_id: u64, tag: u32) -> String {
    format!(
        "SELECT * FROM orders WHERE id != {} AND amount > {}",
        consumer_id * 1_000 + u64::from(tag),
        tag
    )
}

fn build_spec(consumer_id: u64, tag: u32) -> SubscriptionRequest<DefaultIds, Postgres> {
    SubscriptionRequest::new(consumer_id, spec_sql(consumer_id, tag))
}

fn arb_spec_pairs() -> impl Strategy<Value = Vec<(u64, u32)>> {
    vec((0u64..6, 0u32..32), 0..16)
}

fn materialise(pairs: &[(u64, u32)]) -> Vec<SubscriptionRequest<DefaultIds, Postgres>> {
    pairs.iter().map(|(c, t)| build_spec(*c, *t)).collect()
}

/// Scope marker that the generator picks per spec. `None` means
/// `Durable` (the default). `Some(n)` means `Session(n)`.
type ScopeChoice = Option<u64>;

fn arb_scoped_spec_triples() -> impl Strategy<Value = Vec<(u64, u32, ScopeChoice)>> {
    vec(
        (
            0u64..6,
            0u32..32,
            prop_oneof![Just(None), (0u64..4).prop_map(Some)],
        ),
        0..16,
    )
}

fn materialise_scoped(triples: &[(u64, u32, ScopeChoice)]) -> Vec<SubscriptionRequest<DefaultIds, Postgres>> {
    triples
        .iter()
        .map(|(c, t, scope)| {
            let req = build_spec(*c, *t);
            match scope {
                None => req.scope(SubscriptionScope::Durable),
                Some(sess) => req.scope(SubscriptionScope::Session(*sess)),
            }
        })
        .collect()
}

/// Deterministic, non-capturing eviction closure: pick the
/// subscription belonging to the lowest consumer id, tie-broken by the
/// lowest `SubscriptionId`. The same closure is installed on both
/// engines under test, so parity is well-defined.
fn lowest_consumer_evictor(
    view: &SubscriptionsView<'_, DefaultIds>,
) -> Option<subql::SubscriptionId> {
    view.iter()
        .min_by_key(|m| (m.consumer_id, m.subscription_id))
        .map(|m| m.subscription_id)
}

/// Per-index equality check. `RegisterResult` derives `PartialEq` and
/// is compared directly. `RegisterError` does NOT derive `PartialEq`,
/// so we project it to a `(discriminant, payload)` tuple that captures
/// every variant + interesting field.
fn err_signature(e: &RegisterError) -> (u8, String) {
    match e {
        RegisterError::ParseError {
            line,
            column,
            message,
        } => (1, format!("parse:{line}:{column}:{message}")),
        RegisterError::UnsupportedSql(s) => (2, format!("unsupported:{s}")),
        RegisterError::UnknownTable(s) => (3, format!("unknown_table:{s}")),
        RegisterError::AmbiguousTable {
            reference,
            qualified,
            unqualified,
        } => (
            4,
            format!("ambiguous:{reference}:{qualified}:{unqualified}"),
        ),
        RegisterError::UnknownColumn { table_id, column } => {
            (5, format!("unknown_column:{table_id}:{column}"))
        }
        RegisterError::TypeError(s) => (6, format!("type_error:{s}")),
        RegisterError::Schema(s) => (7, format!("schema:{s}")),
        RegisterError::RegistryFull { cap } => (8, format!("registry_full:{cap}")),
        RegisterError::AggregatorOnRlsTable { table_id } => {
            (9, format!("aggregator_on_rls:{table_id}"))
        }
        RegisterError::Storage(s) => (10, format!("storage:{s}")),
        // Future variants get caught by the assertion below since
        // `#[non_exhaustive]` will land them here as a different
        // discriminant range.
        _ => (255, "unknown_variant".to_string()),
    }
}

fn assert_results_match(
    batch_results: &[Result<RegisterResult, RegisterError>],
    seq_results: &[Result<RegisterResult, RegisterError>],
) -> Result<(), TestCaseError> {
    prop_assert_eq!(
        batch_results.len(),
        seq_results.len(),
        "batch and sequential paths returned different result counts"
    );
    for (i, (b, s)) in batch_results.iter().zip(seq_results.iter()).enumerate() {
        match (b, s) {
            (Ok(br), Ok(sr)) => {
                prop_assert_eq!(br, sr, "RegisterResult divergence at index {}", i);
            }
            (Err(be), Err(se)) => {
                prop_assert_eq!(
                    err_signature(be),
                    err_signature(se),
                    "RegisterError divergence at index {}: batch={:?} seq={:?}",
                    i,
                    be,
                    se
                );
            }
            (Ok(br), Err(se)) => {
                return Err(TestCaseError::Fail(
                    format!(
                        "result variant divergence at {i}: batch Ok({br:?}) vs sequential Err({se:?})"
                    )
                    .into(),
                ));
            }
            (Err(be), Ok(sr)) => {
                return Err(TestCaseError::Fail(
                    format!(
                        "result variant divergence at {i}: batch Err({be:?}) vs sequential Ok({sr:?})"
                    )
                    .into(),
                ));
            }
        }
    }
    Ok(())
}

fn assert_engine_state_match(
    engine_batch: &Engine,
    engine_seq: &Engine,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(
        engine_batch.subscription_count(),
        engine_seq.subscription_count(),
        "final subscription_count divergence"
    );
    Ok(())
}

fn run_sequential(
    engine: &mut Engine,
    specs: Vec<SubscriptionRequest<DefaultIds, Postgres>>,
) -> Vec<Result<RegisterResult, RegisterError>> {
    specs.into_iter().map(|s| engine.register(s)).collect()
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        ..ProptestConfig::default()
    })]

    /// Default config (no cap): batch and sequential agree per index
    /// and on final state.
    #[test]
    fn parity_default_config(pairs in arb_spec_pairs()) {
        let mut engine_batch = fresh(None, None);
        let mut engine_seq = fresh(None, None);

        let batch_results = engine_batch.register_batch(materialise(&pairs));
        let seq_results = run_sequential(&mut engine_seq, materialise(&pairs));

        assert_results_match(&batch_results, &seq_results)?;
        assert_engine_state_match(&engine_batch, &engine_seq)?;
    }

    /// `Reject` cap: batch and sequential must produce the same
    /// pattern of `Ok` vs `RegistryFull` errors.
    #[test]
    fn parity_reject_cap(
        cap in 1usize..=4,
        pairs in arb_spec_pairs(),
    ) {
        let mut engine_batch = fresh(Some(cap), Some(EvictionPolicy::Reject));
        let mut engine_seq = fresh(Some(cap), Some(EvictionPolicy::Reject));

        let batch_results = engine_batch.register_batch(materialise(&pairs));
        let seq_results = run_sequential(&mut engine_seq, materialise(&pairs));

        assert_results_match(&batch_results, &seq_results)?;
        assert_engine_state_match(&engine_batch, &engine_seq)?;
    }

    /// `EvictOldest` cap: batch and sequential must agree on which
    /// `SubscriptionId`s end up evicted at every step. This is the
    /// richest source of potential divergence because the batch path
    /// processes evictions in one pass while the sequential path
    /// re-derives the victim per spec.
    #[test]
    fn parity_evict_oldest_cap(
        cap in 1usize..=4,
        pairs in arb_spec_pairs(),
    ) {
        let mut engine_batch = fresh(Some(cap), Some(EvictionPolicy::EvictOldest));
        let mut engine_seq = fresh(Some(cap), Some(EvictionPolicy::EvictOldest));

        let batch_results = engine_batch.register_batch(materialise(&pairs));
        let seq_results = run_sequential(&mut engine_seq, materialise(&pairs));

        assert_results_match(&batch_results, &seq_results)?;
        assert_engine_state_match(&engine_batch, &engine_seq)?;
    }

    /// `EvictLeastActive` cap. Activity stats start at zero for every
    /// subscription (no dispatch fires during these tests), so the
    /// policy degrades to its tie breaker: oldest `SubscriptionId`.
    /// Parity is still meaningful because the *fallback to a
    /// sequential register loop* triggered by an active policy
    /// (see the bug fix in `register_batch`) is what's under test here.
    #[test]
    fn parity_evict_least_active_cap(
        cap in 1usize..=4,
        pairs in arb_spec_pairs(),
    ) {
        let mut engine_batch = fresh(Some(cap), Some(EvictionPolicy::EvictLeastActive));
        let mut engine_seq = fresh(Some(cap), Some(EvictionPolicy::EvictLeastActive));

        let batch_results = engine_batch.register_batch(materialise(&pairs));
        let seq_results = run_sequential(&mut engine_seq, materialise(&pairs));

        assert_results_match(&batch_results, &seq_results)?;
        assert_engine_state_match(&engine_batch, &engine_seq)?;
    }

    /// `EvictColdest` cap. Same shape as the least-active case:
    /// `dispatch_count = 0` for every sub here, so victim selection
    /// falls back to oldest `SubscriptionId`. The interesting bit is
    /// that the sequential-fallback path inside `register_batch` must
    /// agree with the plain sequential loop, which is exactly what
    /// commit `95b435d` corrected.
    #[test]
    fn parity_evict_coldest_cap(
        cap in 1usize..=4,
        pairs in arb_spec_pairs(),
    ) {
        let mut engine_batch = fresh(Some(cap), Some(EvictionPolicy::EvictColdest));
        let mut engine_seq = fresh(Some(cap), Some(EvictionPolicy::EvictColdest));

        let batch_results = engine_batch.register_batch(materialise(&pairs));
        let seq_results = run_sequential(&mut engine_seq, materialise(&pairs));

        assert_results_match(&batch_results, &seq_results)?;
        assert_engine_state_match(&engine_batch, &engine_seq)?;
    }

    /// `EvictBySession` cap with a mix of `Durable` and `Session`
    /// specs. Scope-aware policies are interesting under batching
    /// because the within-batch fallback must surface the scope of
    /// pending subs the same way the sequential loop sees freshly
    /// committed subs.
    #[test]
    fn parity_evict_by_session_cap(
        cap in 1usize..=4,
        triples in arb_scoped_spec_triples(),
    ) {
        let mut engine_batch = fresh(Some(cap), Some(EvictionPolicy::EvictBySession));
        let mut engine_seq = fresh(Some(cap), Some(EvictionPolicy::EvictBySession));

        let batch_results = engine_batch.register_batch(materialise_scoped(&triples));
        let seq_results = run_sequential(&mut engine_seq, materialise_scoped(&triples));

        assert_results_match(&batch_results, &seq_results)?;
        assert_engine_state_match(&engine_batch, &engine_seq)?;
    }

    /// `EvictByConsumer` cap. Consumer-id varies across specs, so the
    /// "biggest hog" metric is non-trivial. Parity asserts the batch
    /// path computes the same hog ranking as the sequential loop after
    /// every registration.
    #[test]
    fn parity_evict_by_consumer_cap(
        cap in 1usize..=4,
        pairs in arb_spec_pairs(),
    ) {
        let mut engine_batch = fresh(Some(cap), Some(EvictionPolicy::EvictByConsumer));
        let mut engine_seq = fresh(Some(cap), Some(EvictionPolicy::EvictByConsumer));

        let batch_results = engine_batch.register_batch(materialise(&pairs));
        let seq_results = run_sequential(&mut engine_seq, materialise(&pairs));

        assert_results_match(&batch_results, &seq_results)?;
        assert_engine_state_match(&engine_batch, &engine_seq)?;
    }

    /// Custom eviction closure (lowest consumer id wins). Confirms the
    /// `with_custom_eviction` builder is honoured by `register_batch`'s
    /// fallback path the same way the sequential loop honours it.
    #[test]
    fn parity_custom_eviction_lowest_consumer(
        cap in 1usize..=4,
        pairs in arb_spec_pairs(),
    ) {
        let mut engine_batch = fresh_custom(cap);
        let mut engine_seq = fresh_custom(cap);

        let batch_results = engine_batch.register_batch(materialise(&pairs));
        let seq_results = run_sequential(&mut engine_seq, materialise(&pairs));

        assert_results_match(&batch_results, &seq_results)?;
        assert_engine_state_match(&engine_batch, &engine_seq)?;
    }
}

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
//! Scope. We test parity under three configurations:
//! 1. Default config (uncapped registry).
//! 2. `EvictionPolicy::Reject` with a small cap.
//! 3. `EvictionPolicy::EvictOldest` with a small cap (the eviction
//!    path is the richest source of divergence risk).

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::cast_possible_truncation
)]

use std::sync::Arc;

use proptest::collection::vec;
use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    DefaultIds, EvictionPolicy, RegisterError, RegisterResult, SubscriptionEngine,
    SubscriptionRequest,
};

const CATALOG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

type Engine = SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>;

fn fresh(cap: Option<usize>, policy: Option<EvictionPolicy>) -> Engine {
    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(CATALOG_DDL).unwrap());
    let mut engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {});
    if let (Some(cap), Some(policy)) = (cap, policy) {
        engine = engine.with_max_subscriptions(cap, policy);
    }
    engine
}

/// Distinct SQL strings per `(consumer, tag)` so registrations are not
/// constantly hitting the dedup path; some collisions still happen
/// inside a batch and that is part of what we test.
fn spec_sql(consumer_id: u64, tag: u32) -> String {
    format!(
        "SELECT * FROM orders WHERE id != {} AND amount > {}",
        consumer_id * 1_000 + u64::from(tag),
        tag
    )
}

fn build_spec(consumer_id: u64, tag: u32) -> SubscriptionRequest<DefaultIds> {
    SubscriptionRequest::new(consumer_id, spec_sql(consumer_id, tag))
}

fn arb_spec_pairs() -> impl Strategy<Value = Vec<(u64, u32)>> {
    vec((0u64..6, 0u32..32), 0..16)
}

fn materialise(pairs: &[(u64, u32)]) -> Vec<SubscriptionRequest<DefaultIds>> {
    pairs.iter().map(|(c, t)| build_spec(*c, *t)).collect()
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
    specs: Vec<SubscriptionRequest<DefaultIds>>,
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
}

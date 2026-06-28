//! Property-based tests for the subscription-registry cap and the
//! `EvictionPolicy` semantics that landed alongside the activity-aware
//! eviction work.
//!
//! The invariants tested here hold across every action sequence the
//! generator can produce. Each test fixes the cap and the policy at
//! the start, then drives randomized `register` / `unregister`
//! sequences and asserts the post-condition.
//!
//! Properties tested
//! 1. **Cap invariant** (any policy): `subscription_count()` is always
//!    `<= cap` after any sequence of operations.
//! 2. **Reject is the strict cap**: with `EvictionPolicy::Reject` at
//!    cap, the next `register` fails with `RegisterError::RegistryFull`
//!    and the count is unchanged.
//! 3. **EvictOldest determinism**: with `EvictionPolicy::EvictOldest`,
//!    when an eviction fires it removes exactly the smallest
//!    `SubscriptionId` currently live.

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::collections::HashSet;

use proptest::collection::vec;
use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    DefaultIds, EvictionPolicy, RegisterError, SubscriptionEngine, SubscriptionId,
    SubscriptionRequest,
};

const CATALOG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

type Engine = SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>;

fn fresh_engine(cap: usize, policy: EvictionPolicy) -> Engine {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(CATALOG_DDL).unwrap();
    SubscriptionEngine::new(catalog, PostgreSqlDialect {}).with_max_subscriptions(cap, policy)
}

/// Distinct SQL strings per (consumer, predicate-tag) combination so
/// registrations are never deduplicated against existing bindings.
fn predicate_sql(consumer_id: u64, tag: u32) -> String {
    // `id != N` is a structurally simple predicate that always evaluates
    // to the engine path and produces a new subscription per `tag`.
    format!(
        "SELECT * FROM orders WHERE id != {} AND amount > {}",
        consumer_id * 1_000 + u64::from(tag),
        tag
    )
}

#[derive(Debug, Clone)]
enum Action {
    Register(u64, u32),
    UnregisterByIndex(u8),
}

fn arb_action() -> impl Strategy<Value = Action> {
    prop_oneof![
        6 => (0u64..6, 0u32..32).prop_map(|(c, t)| Action::Register(c, t)),
        1 => any::<u8>().prop_map(Action::UnregisterByIndex),
    ]
}

/// Track which subscription ids are live in the engine's view of the
/// world. Used by every property test as the source of truth for
/// "what should be in the engine right now."
#[derive(Default)]
struct LiveSet(std::collections::BTreeSet<SubscriptionId>);

impl LiveSet {
    fn insert(&mut self, id: SubscriptionId) {
        self.0.insert(id);
    }
    fn remove(&mut self, id: SubscriptionId) -> bool {
        self.0.remove(&id)
    }
    fn min_id(&self) -> Option<SubscriptionId> {
        self.0.iter().next().copied()
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn nth(&self, index: usize) -> Option<SubscriptionId> {
        self.0.iter().nth(index).copied()
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        ..ProptestConfig::default()
    })]

    /// Cap invariant: `subscription_count()` is always `<= cap`,
    /// regardless of the policy or the action sequence.
    #[test]
    fn subscription_count_never_exceeds_cap_with_evict_oldest(
        cap in 1usize..=4,
        actions in vec(arb_action(), 0..40),
    ) {
        let mut engine = fresh_engine(cap, EvictionPolicy::EvictOldest);
        let mut live = LiveSet::default();
        let mut used: HashSet<(u64, u32)> = HashSet::new();

        for action in actions {
            match action {
                Action::Register(consumer, tag) => {
                    if !used.insert((consumer, tag)) {
                        // Already registered; skip to avoid the dedup
                        // path which returns the existing id without
                        // eviction.
                        continue;
                    }
                    let sql = predicate_sql(consumer, tag);
                    let spec = SubscriptionRequest::new(consumer, sql);
                    if let Ok(result) = engine.register(spec) {
                        for evicted in &result.evicted {
                            live.remove(*evicted);
                        }
                        live.insert(result.subscription_id);
                    }
                    prop_assert!(
                        engine.subscription_count() <= cap,
                        "count {} exceeded cap {}",
                        engine.subscription_count(),
                        cap
                    );
                }
                Action::UnregisterByIndex(idx) => {
                    if live.len() > 0 {
                        let target = live.nth(usize::from(idx) % live.len()).unwrap();
                        let removed = engine.unregister_subscription(target);
                        if removed {
                            live.remove(target);
                        }
                    }
                }
            }
            prop_assert_eq!(engine.subscription_count(), live.len());
        }
    }

    /// Reject policy is the strict cap: at-cap, the next `register`
    /// for an unseen `(consumer, tag)` returns `RegistryFull` and the
    /// count is unchanged.
    #[test]
    fn reject_policy_returns_registry_full_at_cap(
        cap in 1usize..=4,
        // Enough distinct registrations to fill the cap, plus one to
        // observe the rejection.
        seeds in vec((0u64..6, 0u32..32), 1..20),
    ) {
        let mut engine = fresh_engine(cap, EvictionPolicy::Reject);
        let mut used: HashSet<(u64, u32)> = HashSet::new();
        let mut accepted = 0usize;
        let mut overflow_seed: Option<(u64, u32)> = None;

        for (consumer, tag) in seeds {
            if !used.insert((consumer, tag)) {
                continue;
            }
            if engine.subscription_count() == cap {
                overflow_seed = Some((consumer, tag));
                break;
            }
            let sql = predicate_sql(consumer, tag);
            let spec = SubscriptionRequest::new(consumer, sql);
            if engine.register(spec).is_ok() {
                accepted += 1;
            }
        }

        if let Some((consumer, tag)) = overflow_seed {
            prop_assert_eq!(engine.subscription_count(), cap);
            let sql = predicate_sql(consumer, tag);
            let spec = SubscriptionRequest::new(consumer, sql);
            match engine.register(spec) {
                Err(RegisterError::RegistryFull { cap: reported }) => {
                    prop_assert_eq!(reported, cap, "RegistryFull.cap should match the configured cap");
                }
                Ok(result) => {
                    panic!(
                        "Reject policy at cap accepted a fresh registration: subscription_id={} evicted={:?}",
                        result.subscription_id, result.evicted
                    );
                }
                Err(other) => {
                    panic!("expected RegistryFull, got {other:?}");
                }
            }
            prop_assert_eq!(
                engine.subscription_count(),
                cap,
                "rejection must not mutate the registry count"
            );
        }
        prop_assert!(accepted <= cap);
    }

    /// `EvictOldest` is deterministic in its choice of victim: every
    /// time eviction fires, the evicted SubscriptionId is the smallest
    /// id currently live in the registry.
    #[test]
    fn evict_oldest_picks_smallest_subscription_id(
        cap in 1usize..=4,
        seeds in vec((0u64..6, 0u32..32), 1..30),
    ) {
        let mut engine = fresh_engine(cap, EvictionPolicy::EvictOldest);
        let mut live = LiveSet::default();
        let mut used: HashSet<(u64, u32)> = HashSet::new();

        for (consumer, tag) in seeds {
            if !used.insert((consumer, tag)) {
                continue;
            }
            let sql = predicate_sql(consumer, tag);
            let spec = SubscriptionRequest::new(consumer, sql);
            let pre_count = engine.subscription_count();
            let expected_victim = if pre_count == cap { live.min_id() } else { None };

            let Ok(result) = engine.register(spec) else {
                continue;
            };

            if let Some(expected) = expected_victim {
                prop_assert_eq!(
                    result.evicted.as_slice(),
                    &[expected][..],
                    "EvictOldest should evict the smallest live id (cap={}, expected={}, got={:?})",
                    cap,
                    expected,
                    result.evicted,
                );
                live.remove(expected);
            } else {
                prop_assert!(
                    result.evicted.is_empty(),
                    "under-cap registration must not evict (cap={}, pre_count={}, evicted={:?})",
                    cap,
                    pre_count,
                    result.evicted,
                );
            }
            live.insert(result.subscription_id);
        }
    }
}

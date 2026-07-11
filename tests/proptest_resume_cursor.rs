#![cfg(any())] // Phase 11: rewrite against E: CdcEvent shape. SubscriptionEngine took <Dialect,...>, now takes <E: CdcEvent,...>. Tracked in docs/refactor-cdc-event-handoff.md.

//! Property-based tests for the per-`(session, subscription)` resume
//! cursor.
//!
//! Model-based: every action that mutates the engine's cursor state
//! also runs against a reference `HashMap<(SessionId, SubscriptionId),
//! OpaqueCheckpoint>` plus the monotonic-advance rule. After every
//! action we assert the engine and the reference agree on:
//! - every key's stored value (or absence)
//! - the result of `cursor_for`
//! - the iterator returned by `cursors_for_session`
//!
//! `advance_cursor` is the trickiest because its monotonic-rejection
//! contract has to match the engine's rejection. The reference
//! implements the same rule (reject if `attempted < previous`) so
//! divergence shows up as a model-vs-engine disagreement.

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::redundant_clone,
    clippy::explicit_iter_loop
)]

use std::collections::HashMap;

use proptest::collection::vec;
use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{AdvanceCursorError, DefaultIds, OpaqueCheckpoint, SubscriptionEngine, SubscriptionId};

const MAX_SESSIONS: u64 = 4;
const MAX_SUBS: u64 = 4;
const MAX_CHECKPOINT_LEN: usize = 4;

type SessionId = u64;
type Engine = SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>;
type Model = HashMap<(SessionId, SubscriptionId), OpaqueCheckpoint>;

fn fresh_engine() -> Engine {
    SubscriptionEngine::new(
        ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE orders (id INT PRIMARY KEY);").unwrap(),
        PostgreSqlDialect {},
    )
}

#[derive(Debug, Clone)]
enum Action {
    Advance(SessionId, SubscriptionId, OpaqueCheckpoint),
    ForceSet(SessionId, SubscriptionId, OpaqueCheckpoint),
    Drop(SessionId, SubscriptionId),
    UnregSession(SessionId),
    UnregSub(SubscriptionId),
}

fn arb_session_id() -> impl Strategy<Value = SessionId> {
    1u64..=MAX_SESSIONS
}

fn arb_sub_id() -> impl Strategy<Value = SubscriptionId> {
    1u64..=MAX_SUBS
}

fn arb_checkpoint() -> impl Strategy<Value = OpaqueCheckpoint> {
    vec(any::<u8>(), 0..=MAX_CHECKPOINT_LEN).prop_map(OpaqueCheckpoint)
}

fn arb_action() -> impl Strategy<Value = Action> {
    prop_oneof![
        4 => (arb_session_id(), arb_sub_id(), arb_checkpoint())
            .prop_map(|(s, sub, c)| Action::Advance(s, sub, c)),
        2 => (arb_session_id(), arb_sub_id(), arb_checkpoint())
            .prop_map(|(s, sub, c)| Action::ForceSet(s, sub, c)),
        1 => (arb_session_id(), arb_sub_id()).prop_map(|(s, sub)| Action::Drop(s, sub)),
        1 => arb_session_id().prop_map(Action::UnregSession),
        1 => arb_sub_id().prop_map(Action::UnregSub),
    ]
}

/// Apply an action to both the engine and the reference model, and
/// assert that the per-action outputs (return values, error variants)
/// agree.
fn step(engine: &mut Engine, model: &mut Model, action: Action) {
    match action {
        Action::Advance(s, sub, c) => {
            let key = (s, sub);
            let model_previous = model.get(&key).cloned();
            let would_rewind = matches!(&model_previous, Some(prev) if c < *prev);

            let engine_result = engine.advance_cursor(s, sub, c.clone());
            if would_rewind {
                let previous = model_previous.clone().unwrap();
                match engine_result {
                    Err(AdvanceCursorError::NonMonotonic {
                        previous: ep,
                        attempted: ea,
                    }) => {
                        prop_assert_eq_static!(ep, previous);
                        prop_assert_eq_static!(ea, c);
                    }
                    other => panic!(
                        "expected NonMonotonic, got {other:?} (prev={previous:?}, attempt={c:?})"
                    ),
                }
                // Model unchanged on rewind.
            } else {
                let engine_returned = engine_result.unwrap();
                prop_assert_eq_static!(engine_returned, model_previous.clone());
                model.insert(key, c);
            }
        }
        Action::ForceSet(s, sub, c) => {
            let key = (s, sub);
            let model_previous = model.insert(key, c.clone());
            let engine_returned = engine.force_set_cursor(s, sub, c);
            prop_assert_eq_static!(engine_returned, model_previous);
        }
        Action::Drop(s, sub) => {
            let key = (s, sub);
            let model_returned = model.remove(&key);
            let engine_returned = engine.drop_cursor(s, sub);
            prop_assert_eq_static!(engine_returned, model_returned);
        }
        Action::UnregSession(s) => {
            model.retain(|(sess, _), _| *sess != s);
            let _ = engine.unregister_session(s);
        }
        Action::UnregSub(sub) => {
            model.retain(|(_, sub_id), _| *sub_id != sub);
            let _ = engine.unregister_subscription(sub);
        }
    }
}

/// Tiny helper: drop-in `assert_eq!` that panics on mismatch (proptest's
/// `prop_assert_eq!` returns a `TestCaseError`, but we are not inside a
/// `proptest!` body in `step`. The surrounding closure unwraps).
macro_rules! prop_assert_eq_static {
    ($left:expr, $right:expr) => {{
        let l = $left;
        let r = $right;
        assert_eq!(l, r, "model/engine divergence");
    }};
}

use prop_assert_eq_static;

/// After every action sequence the engine and the reference model
/// agree on the full cursor state (per-key cursor and the
/// per-session iterator).
fn assert_invariants(engine: &Engine, model: &Model) {
    // Per-key cursor agreement.
    for ((s, sub), expected) in model.iter() {
        let actual = engine.cursor_for(*s, *sub);
        assert_eq!(
            actual,
            Some(expected),
            "cursor_for({s}, {sub}) divergence (expected {expected:?}, got {actual:?})",
        );
    }
    // Engine must not have extra entries the model lacks. Iterate every
    // (session, sub) pair the universe could touch.
    for s in 1u64..=MAX_SESSIONS {
        for sub in 1u64..=MAX_SUBS {
            if !model.contains_key(&(s, sub)) {
                assert_eq!(
                    engine.cursor_for(s, sub),
                    None,
                    "engine has stale entry for ({s}, {sub})"
                );
            }
        }
    }
    // cursors_for_session agreement: the engine's filtered iterator and
    // the model's filtered keys agree as a multiset.
    for s in 1u64..=MAX_SESSIONS {
        let mut engine_pairs: Vec<(SubscriptionId, OpaqueCheckpoint)> = engine
            .cursors_for_session(s)
            .map(|(sub, c)| (sub, c.clone()))
            .collect();
        engine_pairs.sort_by_key(|(sub, _)| *sub);

        let mut model_pairs: Vec<(SubscriptionId, OpaqueCheckpoint)> = model
            .iter()
            .filter_map(|((sess, sub), c)| {
                if *sess == s {
                    Some((*sub, c.clone()))
                } else {
                    None
                }
            })
            .collect();
        model_pairs.sort_by_key(|(sub, _)| *sub);

        assert_eq!(
            engine_pairs, model_pairs,
            "cursors_for_session({s}) divergence"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    /// Model-based: a sequence of actions on the engine matches a
    /// reference `HashMap` plus the monotonic-rejection rule.
    #[test]
    fn engine_cursor_state_matches_reference_model(
        actions in vec(arb_action(), 0..50),
    ) {
        let mut engine = fresh_engine();
        let mut model: Model = HashMap::new();
        for a in actions {
            step(&mut engine, &mut model, a);
            assert_invariants(&engine, &model);
        }
    }

    /// Lifecycle isolation: `unregister_session(s)` clears exactly the
    /// (s, *) keyspace, never touches another session.
    #[test]
    fn unregister_session_clears_only_target_session(
        seeds in vec((arb_session_id(), arb_sub_id(), arb_checkpoint()), 1..20),
        target in arb_session_id(),
    ) {
        let mut engine = fresh_engine();
        let mut model: Model = HashMap::new();
        for (s, sub, c) in seeds {
            // Use force_set so we always seed (skips the rewind check).
            engine.force_set_cursor(s, sub, c.clone());
            model.insert((s, sub), c);
        }
        let _ = engine.unregister_session(target);
        model.retain(|(s, _), _| *s != target);
        assert_invariants(&engine, &model);
    }

    /// Lifecycle isolation: `unregister_subscription(sub)` clears
    /// exactly the (*, sub) keyspace across every session.
    #[test]
    fn unregister_sub_clears_only_target_sub(
        seeds in vec((arb_session_id(), arb_sub_id(), arb_checkpoint()), 1..20),
        target in arb_sub_id(),
    ) {
        let mut engine = fresh_engine();
        let mut model: Model = HashMap::new();
        for (s, sub, c) in seeds {
            engine.force_set_cursor(s, sub, c.clone());
            model.insert((s, sub), c);
        }
        let _ = engine.unregister_subscription(target);
        model.retain(|(_, sub_id), _| *sub_id != target);
        assert_invariants(&engine, &model);
    }
}

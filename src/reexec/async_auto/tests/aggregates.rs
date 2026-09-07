//! Aggregate folding, seeding and demotion through the wrapper.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

#[test]
fn async_ungrouped_aggregate_folds_through_the_wrapper() {
    let (mut e, tid) = engine_with_values(vec![]);
    let count_id = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::InProcess(_),
            ..
        } => subscription_id,
        other => panic!("expected InProcess, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        count_id,
        crate::AggregateSeedInstall {
            rows: vec![vec![Value::Int(5)]],
            read_at: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    let n = e.apply(&insert_event(tid, 1, 5.0)).unwrap();
    assert_eq!(
        n.aggregate_updates.len(),
        1,
        "one aggregate update through the wrapper"
    );
    assert_eq!(
        n.aggregate_updates[0].folded_value(),
        Some(crate::AggValue::CountStar(6)),
        "the incremented total"
    );
    assert_eq!(
        e.connector().call_count(),
        0,
        "an in-process fold reads nothing"
    );
}

#[test]
fn async_ungrouped_aggregate_folds_across_an_applied_burst() {
    let (mut e, tid) = engine_with_values(vec![]);
    let count_id = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::InProcess(_),
            ..
        } => subscription_id,
        other => panic!("expected InProcess, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        count_id,
        crate::AggregateSeedInstall {
            rows: vec![vec![Value::Int(5)]],
            read_at: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    let aggregate_updates: Vec<_> = [insert_event(tid, 1, 5.0), insert_event(tid, 2, 6.0)]
        .iter()
        .flat_map(|ev| e.apply(ev).unwrap().aggregate_updates)
        .collect();
    assert_eq!(aggregate_updates.len(), 2, "each insert folds");
    assert_eq!(
        aggregate_updates.last().unwrap().folded_value(),
        Some(crate::AggValue::CountStar(7)),
        "the running total after both inserts"
    );
}

#[test]
fn async_ungrouped_aggregate_demotion_resolves_through_the_wrapper() {
    let (mut e, tid) = engine_with_values(vec![]);
    let count_id = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders WHERE status = 'paid'"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::InProcess(_),
            ..
        } => subscription_id,
        other => panic!("expected InProcess, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        count_id,
        crate::AggregateSeedInstall {
            rows: vec![vec![Value::Int(1)]],
            read_at: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    let missing_old = TestEvent::<Postgres>::update(tid, vec![], row(1, 5.0))
        .with_pk_columns([0u16])
        .with_changed_columns([3u16]);
    e.apply(&missing_old).unwrap();
    match block_on(e.resolve_collect()) {
        Err(ReExecError::Cursor { subscription, .. }) => {
            assert_eq!(
                subscription, count_id,
                "the demoted aggregate attempts its whole read"
            );
        }
        Ok(_) => panic!("expected the demotion to attempt a whole read"),
        Err(other) => panic!("expected a Cursor error naming the aggregate, got {other:?}"),
    }
}

#[test]
fn async_snapshot_of_a_folding_aggregate_is_none() {
    let (mut e, _tid) = engine_with_values(vec![]);
    let count_id = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::InProcess(_),
            ..
        } => subscription_id,
        other => panic!("expected InProcess, got {other:?}"),
    };
    crate::Install::install(
        &mut e,
        count_id,
        crate::AggregateSeedInstall {
            rows: vec![vec![Value::Int(5)]],
            read_at: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    assert!(
        block_on(e.snapshot(count_id)).unwrap().is_none(),
        "no bootstrap for a fold"
    );
    assert_eq!(
        e.connector().call_count(),
        0,
        "snapshot reads nothing for a fold"
    );
}

#[test]
fn async_a_seed_that_demotes_at_install_serves_the_whole_read() {
    let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
        catalog(),
        PostgreSqlDialect {},
    )
    .with_max_groups_per_aggregate(1);
    let mut e = AutoResolvingEngine::new(inner, AsyncMode::new(MockAsyncConnector::new(vec![])));
    let grouped = match e
        .register(
            SubscriptionRequest::new(1u64, "SELECT status, COUNT(*) FROM orders GROUP BY status"),
            (),
        )
        .unwrap()
    {
        Registered {
            subscription_id,
            tier: Tier::InProcess(_),
            ..
        } => subscription_id,
        other => panic!("expected InProcess, got {other:?}"),
    };
    let seeded = crate::Install::install(
        &mut e,
        grouped,
        crate::AggregateSeedInstall {
            rows: vec![
                vec![Value::String("open".into()), Value::Int(2), Value::Int(2)],
                vec![Value::String("done".into()), Value::Int(1), Value::Int(1)],
            ],
            read_at: None::<NoCheckpoint>,
        },
    )
    .unwrap();
    assert!(
        !seeded.transitions.is_empty(),
        "the install-time demotion carries a transition"
    );
    match block_on(e.snapshot(grouped)) {
        Err(ReExecError::Cursor { subscription, .. }) => {
            assert_eq!(
                subscription, grouped,
                "the demoted subscription attempts its whole read"
            );
        }
        Ok(answer) => panic!("expected the whole read to be attempted, got {answer:?}"),
        Err(other) => panic!("expected a Cursor error naming the subscription, got {other:?}"),
    }
}

//! Aggregate folding, seeding and demotion through the wrapper.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

#[test]
fn ungrouped_aggregate_folds_through_the_wrapper() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
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
            rows: alloc::vec![alloc::vec![Value::Int(5)]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    // The seeded fold updates through the facade rather than being absorbed.
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
fn ungrouped_aggregate_folds_across_an_applied_burst() {
    let (mut e, tid) = engine_with_values(alloc::vec![]);
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
            rows: alloc::vec![alloc::vec![Value::Int(5)]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    let events = &[insert_event(tid, 1, 5.0), insert_event(tid, 2, 6.0)];
    let folds: alloc::vec::Vec<_> = events
        .iter()
        .flat_map(|ev| e.apply(ev).unwrap().aggregate_updates)
        .collect();
    let last_fold = folds.last();
    assert_eq!(
        last_fold.unwrap().folded_value(),
        Some(crate::AggValue::CountStar(7)),
        "the running total after both inserts"
    );
}

#[test]
fn ungrouped_aggregate_demotion_resolves_through_the_wrapper() {
    // A filtered count needs the old row to know whether it was matching;
    // an UPDATE missing its old image demotes the aggregate to a whole
    // re-read. The wrapper must resolve that with the caller's auth, not
    // panic for want of a stored context.
    let (mut e, tid) = engine_with_values(alloc::vec![]);
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
            rows: alloc::vec![alloc::vec![Value::Int(1)]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    let missing_old = TestEvent::<Postgres>::update(tid, alloc::vec![], row(1, 5.0))
        .with_pk_columns([0u16])
        .with_changed_columns([3u16]);
    // The mock connector holds no cursor, so the demoted whole re-read
    // surfaces as a Cursor error naming the aggregate rather than a panic.
    e.apply(&missing_old).unwrap();
    match e.resolve_collect() {
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
fn snapshot_of_a_folding_aggregate_is_none() {
    // An aggregate seeds through Install, so the wrapper has nothing to
    // bootstrap for it: snapshot returns None rather than mistaking the
    // stored aggregate context for a scalar re-read.
    let (mut e, _tid) = engine_with_values(alloc::vec![]);
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
            rows: alloc::vec![alloc::vec![Value::Int(5)]],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    assert!(
        e.snapshot(count_id).unwrap().is_none(),
        "no bootstrap for a fold"
    );
    assert_eq!(
        e.connector().call_count(),
        0,
        "snapshot reads nothing for a fold"
    );
}

#[test]
fn a_seed_that_demotes_at_install_serves_the_whole_read() {
    // A grouped fold whose seed already exceeds the group budget demotes
    // at install time. The demotion rides the install output as a
    // transition, so the facade must apply it to its own context.
    // Otherwise the context stays a still-folding aggregate and snapshot
    // answers None instead of serving the whole read the demotion asked
    // for.
    let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
        catalog(),
        PostgreSqlDialect {},
    )
    .with_max_groups_per_aggregate(1);
    let mut e = AutoResolvingEngine::new(inner, SyncMode(MockConnector::new(alloc::vec![])));
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
    // Two groups against a budget of one: the install demotes.
    let seeded = crate::Install::install(
        &mut e,
        grouped,
        crate::AggregateSeedInstall {
            rows: alloc::vec![
                alloc::vec![Value::String("open".into()), Value::Int(2), Value::Int(2)],
                alloc::vec![Value::String("done".into()), Value::Int(1), Value::Int(1)],
            ],
            read_at: None::<crate::NoCheckpoint>,
        },
    )
    .unwrap();
    assert!(
        !seeded.transitions.is_empty(),
        "the install-time demotion carries a transition"
    );
    // The mock connector holds no cursor, so a served whole read surfaces
    // as a Cursor error naming the subscription. The bug returns Ok(None)
    // instead, never reaching the connector.
    match e.snapshot(grouped) {
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

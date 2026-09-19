//! What a restored answer needs before it can resolve a read.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

const EXTREME: &str = "SELECT MIN(price) FROM orders";

/// Save one scalar re-read, then reopen the store.
fn saved_store() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().to_path_buf();
    let mut engine = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::with_storage(
        catalog(),
        PostgreSqlDialect {},
        path.clone(),
    )
    .expect("open store")
    .into_parts()
    .0;
    engine
        .register(crate::SubscriptionRequest::new(1u64, EXTREME))
        .expect("the extreme registers");
    drop(engine);
    (dir, path)
}

/// An adopted answer resolves its read like a registered one.
///
/// Registration is what gives a maintained answer the context its reads run
/// under, and a restored answer never passes through it. Adopting is the
/// only other way in, so an answer that came back from disk and was adopted
/// must reach the connector exactly as it did before the restart.
#[test]
fn an_adopted_answer_resolves_through_the_connector() {
    let (_dir, path) = saved_store();

    let restored = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::with_storage(
        catalog(),
        PostgreSqlDialect {},
        path,
    )
    .expect("reopen store");
    assert_eq!(restored.reads().restored.len(), 1, "the answer comes back");
    assert!(restored.reads().dropped.is_empty());

    let orders = crate::catalog_helpers::table_id::<Postgres, _>(&catalog(), "orders")
        .expect("orders table exists");
    let mut engine = AutoResolvingEngine::adopt(
        restored,
        SyncMode(MockConnector::new(alloc::vec![Value::Float(7.0)])),
        |_| (),
    );

    let settled = engine
        .apply(&delete_event(orders, 1, 5.0))
        .unwrap()
        .resolve_collect();
    let notifications = settled.reads.expect("the read resolves");
    assert_eq!(
        engine.connector().call_count(),
        1,
        "the adopted answer reaches the connector"
    );
    assert_eq!(notifications.scalar_updates.len(), 1);
    assert_eq!(
        notifications.scalar_updates[0].value,
        Value::Float(7.0),
        "the answer the connector served is the one delivered"
    );
}

/// An answer taken out without being adopted refuses its read.
///
/// The escape hatch hands back an engine whose answers have no context, and
/// a read for one of them cannot run. It says so rather than ending the
/// process, because a restart that skipped adoption is a caller mistake and
/// not a reason to take the host down.
#[test]
fn an_unadopted_answer_refuses_its_read() {
    let (_dir, path) = saved_store();

    let (inner, reads) =
        SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::with_storage(
            catalog(),
            PostgreSqlDialect {},
            path,
        )
        .expect("reopen store")
        .into_parts();
    let subscription = reads.restored[0].subscription_id;

    let orders = crate::catalog_helpers::table_id::<Postgres, _>(&catalog(), "orders")
        .expect("orders table exists");
    let mut engine = AutoResolvingEngine::new(
        inner,
        SyncMode(MockConnector::new(alloc::vec![Value::Float(7.0)])),
    );

    let settled = engine
        .apply(&delete_event(orders, 1, 5.0))
        .unwrap()
        .resolve_collect();
    let err = settled.reads.expect_err("the read cannot run");
    assert!(
        matches!(
            err,
            crate::reexec::ReExecError::Unadopted { subscription: s } if s == subscription
        ),
        "the refusal names the answer that was never adopted, got {err:?}"
    );
}

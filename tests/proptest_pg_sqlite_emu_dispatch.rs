//! End-to-end dispatch proptest for [`PgSqliteEmuSource`].
//!
//! Replaces the pre-Phase-8.1 `tests/proptest_sqlite_dispatch.rs`.
//! Composes the emulator with a real [`SubscriptionEngine`] and a
//! fixed subscription set, drives arbitrary DML through both the
//! source and an in-memory reference model, dispatches every drained
//! event through the engine, and asserts that
//! [`crate::ConsumerNotifications`] match a Rust-side oracle that
//! evaluates each predicate directly against the pre- and post-event
//! row state.
//!
//! The old test shipped two proptests: (a) direct source-to-engine
//! dispatch, (b) the same events after a `PgOutputBridge` +
//! `PgOutputParser` wire round trip. The two collapse into one now
//! that `PgSqliteEmuSource` always routes every event through the
//! pgoutput wire in its drain loop, so this file only ships one
//! proptest.
//!
//! Generator constraint: `amount` and `id` are bounded i32 ranges.
//! The schema declares them as `INT`, so there is no parser-side
//! float fidelity question here. `status` is restricted to a small
//! alphabet so SQL string-literal escaping is trivial.
//!
//! Gated behind the `pg-sqlite-emu` feature.

#![cfg(feature = "pg-sqlite-emu")]
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;

use proptest::prelude::*;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::CdcEvent;
use subql::{
    ChangeEvent, DefaultIds, EventKind, PgSqliteEmuSource, SubscriptionEngine, SubscriptionRequest,
};

const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

const STATUSES: &[&str] = &["paid", "open", "closed", "pending"];

/// A registered query plus a Rust closure that evaluates the same
/// predicate. `deps` lists the column ordinals the predicate reads;
/// the engine's UPDATE dispatch skips re-evaluation of predicates
/// whose dependency columns did not change, so the oracle mirrors
/// the same rule to stay consistent.
#[derive(Clone, Copy)]
struct Subscription {
    consumer_id: u64,
    sql: &'static str,
    deps: &'static [u16],
    matches: fn(&Row) -> bool,
}

fn subscriptions() -> Vec<Subscription> {
    vec![
        Subscription {
            consumer_id: 1,
            sql: "SELECT * FROM orders WHERE amount > 100",
            deps: &[1],
            matches: |row| row.amount > 100,
        },
        Subscription {
            consumer_id: 2,
            sql: "SELECT * FROM orders WHERE status = 'paid'",
            deps: &[2],
            matches: |row| row.status == "paid",
        },
        Subscription {
            consumer_id: 3,
            sql: "SELECT * FROM orders WHERE amount < 50",
            deps: &[1],
            matches: |row| row.amount < 50,
        },
        Subscription {
            consumer_id: 4,
            sql: "SELECT * FROM orders WHERE id = 5",
            deps: &[0],
            matches: |row| row.id == 5,
        },
    ]
}

fn diff_columns(old: &Row, new: &Row) -> Vec<u16> {
    let mut changed = Vec::new();
    if old.id != new.id {
        changed.push(0);
    }
    if old.amount != new.amount {
        changed.push(1);
    }
    if old.status != new.status {
        changed.push(2);
    }
    changed
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Row {
    id: i64,
    amount: i64,
    status: String,
}

#[derive(Clone, Debug)]
enum Op {
    Insert(Row),
    Update(Row),
    Delete { id: i64 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let id = 1i64..=8;
    let amount = -200i64..=200;
    let status = (0usize..STATUSES.len()).prop_map(|i| STATUSES[i].to_string());
    prop_oneof![
        2 => (id.clone(), amount.clone(), status.clone()).prop_map(|(id, amount, status)| Op::Insert(Row { id, amount, status })),
        2 => (id.clone(), amount, status).prop_map(|(id, amount, status)| Op::Update(Row { id, amount, status })),
        1 => id.prop_map(|id| Op::Delete { id }),
    ]
}

#[derive(Clone, Debug)]
struct Expected {
    inserted: Vec<u64>,
    deleted: Vec<u64>,
    updated: Vec<u64>,
}

/// Evaluate every subscription's predicate against `old_row` and
/// `new_row` and bucket the consumer ids per view-relative delta rule.
///
/// * matched-new-only -> `inserted`
/// * matched-old-only -> `deleted`
/// * matched-both AND at least one dep column changed -> `updated`
///
/// For a no-op UPDATE (no columns actually differ), every matching
/// subscription still lands in `updated`, matching subql's fallback
/// to the index-driven candidate set when `changed_columns` is empty.
fn oracle(subs: &[Subscription], old_row: Option<&Row>, new_row: Option<&Row>) -> Expected {
    let changed = match (old_row, new_row) {
        (Some(old), Some(new)) => diff_columns(old, new),
        _ => Vec::new(),
    };
    let mut inserted = Vec::new();
    let mut deleted = Vec::new();
    let mut updated = Vec::new();
    for sub in subs {
        let matched_old = old_row.is_some_and(sub.matches);
        let matched_new = new_row.is_some_and(sub.matches);
        match (matched_old, matched_new) {
            (false, true) => inserted.push(sub.consumer_id),
            (true, false) => deleted.push(sub.consumer_id),
            (true, true) => {
                let dep_changed = sub.deps.iter().any(|d| changed.contains(d));
                if changed.is_empty() || dep_changed {
                    updated.push(sub.consumer_id);
                }
            }
            (false, false) => {}
        }
    }
    inserted.sort_unstable();
    deleted.sort_unstable();
    updated.sort_unstable();
    Expected {
        inserted,
        deleted,
        updated,
    }
}

fn build_engine(
    source: &PgSqliteEmuSource,
) -> SubscriptionEngine<ChangeEvent, DefaultIds, sql_traits::structs::ParserDB> {
    let mut engine = SubscriptionEngine::<ChangeEvent, DefaultIds, _>::new(
        source.pg_catalog().clone(),
        PostgreSqlDialect {},
    );
    for sub in subscriptions() {
        engine
            .register(SubscriptionRequest::new(sub.consumer_id, sub.sql))
            .expect("subscription registers");
    }
    engine
}

fn quote_status(s: &str) -> String {
    s.replace('\'', "''")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Drive arbitrary DML through the emulator and a reference
    /// model. For each emitted event, the engine's
    /// `ConsumerNotifications` must equal the oracle's bucketed
    /// view-relative delta computed directly against the pre- and
    /// post-event row state.
    #[test]
    fn dispatch_matches_oracle_across_arbitrary_dml(
        ops in prop::collection::vec(op_strategy(), 0..25),
    ) {
        let subs = subscriptions();
        let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL)
            .expect("build source");
        let mut engine = build_engine(&source);
        let mut model: BTreeMap<i64, Row> = BTreeMap::new();

        // Interleave DML with drain so each op surfaces as a discrete
        // event. The SQLite session merges INSERT + UPDATE + DELETE on
        // the same row into a net zero change, so batching drains at
        // the end would drop coverage on any op sequence that touches
        // the same PK more than once.
        for op in &ops {
            let expected = match op {
                Op::Insert(row) => {
                    if model.contains_key(&row.id) {
                        continue;
                    }
                    let sql = format!(
                        "INSERT INTO orders (id, amount, status) VALUES ({id}, {amount}, '{status}')",
                        id = row.id,
                        amount = row.amount,
                        status = quote_status(&row.status),
                    );
                    source.execute(&sql).unwrap();
                    let e = oracle(&subs, None, Some(row));
                    model.insert(row.id, row.clone());
                    e
                }
                Op::Update(row) => {
                    let Some(old) = model.get(&row.id).cloned() else {
                        continue;
                    };
                    // SQLite session extension drops no-op UPDATEs
                    // (row values identical) from the changeset, so
                    // the emulator emits no event. Skip the DML too
                    // to keep the model and the source synchronised.
                    if old == *row {
                        continue;
                    }
                    let sql = format!(
                        "UPDATE orders SET amount = {amount}, status = '{status}' WHERE id = {id}",
                        id = row.id,
                        amount = row.amount,
                        status = quote_status(&row.status),
                    );
                    source.execute(&sql).unwrap();
                    let e = oracle(&subs, Some(&old), Some(row));
                    model.insert(row.id, row.clone());
                    e
                }
                Op::Delete { id } => {
                    let Some(old) = model.remove(id) else {
                        continue;
                    };
                    let sql = format!("DELETE FROM orders WHERE id = {id}");
                    source.execute(&sql).unwrap();
                    oracle(&subs, Some(&old), None)
                }
            };

            let event = source
                .poll_next_event()
                .unwrap()
                .expect("op should produce a drained event");
            let notifs = engine.consumers(&event).unwrap();

            let mut actual_inserted: Vec<u64> = notifs.inserted().to_vec();
            let mut actual_deleted: Vec<u64> = notifs.deleted().to_vec();
            let mut actual_updated: Vec<u64> = notifs.updated().to_vec();
            actual_inserted.sort_unstable();
            actual_deleted.sort_unstable();
            actual_updated.sort_unstable();

            prop_assert_eq!(
                &actual_inserted, &expected.inserted,
                "event kind {:?} inserted mismatch", event.kind()
            );
            prop_assert_eq!(
                &actual_deleted, &expected.deleted,
                "event kind {:?} deleted mismatch", event.kind()
            );
            prop_assert_eq!(
                &actual_updated, &expected.updated,
                "event kind {:?} updated mismatch", event.kind()
            );

            match event.kind() {
                EventKind::Insert => prop_assert!(
                    notifs.deleted().is_empty() && notifs.updated().is_empty(),
                    "INSERT event dispatched with non-empty deleted or updated buckets",
                ),
                EventKind::Delete => prop_assert!(
                    notifs.inserted().is_empty() && notifs.updated().is_empty(),
                    "DELETE event dispatched with non-empty inserted or updated buckets",
                ),
                EventKind::Update | EventKind::Truncate => {}
            }
        }

        // The interleaved drain above should have exhausted the queue
        // exactly, so any trailing event is a bookkeeping bug.
        prop_assert!(
            source.poll_next_event().unwrap().is_none(),
            "emulator queue should be empty after all ops drained",
        );
    }
}

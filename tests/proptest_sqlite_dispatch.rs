//! End-to-end dispatch proptest.
//!
//! Composes [`SqliteCdcSource`] with [`SubscriptionEngine`] and a fixed
//! set of registered queries, drives arbitrary DML through both the
//! source and an in-memory reference model, dispatches every drained
//! event through the engine, and asserts that
//! [`ConsumerNotifications`] match a Rust-side oracle that evaluates
//! each predicate directly against the pre- and post-event row state.
//!
//! What this exercises beyond `proptest_sqlite_cdc.rs`: the WAL event
//! produced by the source is fed into the real engine (compile + VM
//! dispatch + view-relative delta computation), not just inspected for
//! shape. A passing run proves the whole ingest-to-dispatch chain
//! works without Docker.
//!
//! Generator constraint: `amount` and `id` are bounded i32 ranges. The
//! schema declares them as `INT`/`INTEGER`, so there is no parser-side
//! float fidelity question here. `status` is restricted to a small
//! alphabet so SQL string-literal escaping is trivial.
//!
//! Gated via `[[test]] required-features = ["sqlite-cdc"]`.
#![cfg(feature = "sqlite-cdc")]
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use subql::wal::WalParser;
use subql::{
    DefaultIds, EventKind, PgOutputBridge, PgOutputParser, SqliteCdcConfig, SqliteCdcSource,
    SubscriptionEngine, SubscriptionRequest,
};

const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";
const SQLITE_DDL: &str =
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)";

/// Statuses the generator picks from. Kept small so each value collides
/// with the registered `status = 'paid'` predicate often enough to
/// surface dispatch bugs.
const STATUSES: &[&str] = &["paid", "open", "closed", "pending"];

/// A registered query plus a Rust closure that evaluates the same
/// predicate. The closure is the oracle the proptest checks the engine
/// against. `deps` lists the column ordinals the predicate reads.
/// Subql's UPDATE dispatch skips re-evaluation when none of those
/// columns changed, so the oracle needs to know which columns each
/// predicate depends on to mirror that.
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

/// Bit vector of columns that differ between `old` and `new`.
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

#[derive(Clone, Debug)]
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
/// `new_row` and bucket the consumer ids per view-relative delta rule:
/// matched-new-only is `inserted`, matched-old-only is `deleted`,
/// matched-both is `updated`. INSERT events pass `old=None`, DELETE
/// passes `new=None`. For UPDATE, a consumer lands in `updated` only
/// when at least one column the predicate reads actually changed,
/// matching subql's `select_candidates` UPDATE optimization that skips
/// re-evaluation of predicates whose dependency columns are stable.
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
                // Engine semantics: when `changed_columns` is non-empty,
                // `select_candidates` returns only predicates whose deps
                // overlap the changed set, so subscriptions with stable
                // deps never reach dual-eval. When `changed_columns` is
                // empty (no-op UPDATE), the engine falls through to the
                // index-driven candidate set and every matching
                // subscription gets bucketed into `updated`. Mirror both
                // cases.
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

fn build_engine() -> SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> {
    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap());
    let mut engine = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
        catalog,
        PostgreSqlDialect {},
    );
    for sub in subscriptions() {
        engine
            .register(SubscriptionRequest::new(sub.consumer_id, sub.sql))
            .expect("subscription registers");
    }
    engine
}

fn build_source() -> SqliteCdcSource {
    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap());
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    sql_query(SQLITE_DDL).execute(&mut conn).unwrap();
    SqliteCdcSource::new(conn, catalog, SqliteCdcConfig::default()).unwrap()
}

fn quote_status(s: &str) -> String {
    s.replace('\'', "''")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Drive arbitrary DML through both the source and a reference
    /// model. For each emitted event, the engine's
    /// [`ConsumerNotifications`] must equal the oracle's bucketed
    /// view-relative delta computed directly against the pre- and
    /// post-event row state.
    #[test]
    fn dispatch_matches_oracle_across_arbitrary_dml(
        ops in prop::collection::vec(op_strategy(), 0..25),
    ) {
        let subs = subscriptions();
        let mut source = build_source();
        let mut engine = build_engine();
        let mut model: BTreeMap<i64, Row> = BTreeMap::new();
        let mut expected: Vec<Expected> = Vec::new();

        for op in &ops {
            match op {
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
                    expected.push(oracle(&subs, None, Some(row)));
                    model.insert(row.id, row.clone());
                }
                Op::Update(row) => {
                    let Some(old) = model.get(&row.id).cloned() else {
                        continue;
                    };
                    let sql = format!(
                        "UPDATE orders SET amount = {amount}, status = '{status}' WHERE id = {id}",
                        id = row.id,
                        amount = row.amount,
                        status = quote_status(&row.status),
                    );
                    source.execute(&sql).unwrap();
                    expected.push(oracle(&subs, Some(&old), Some(row)));
                    model.insert(row.id, row.clone());
                }
                Op::Delete { id } => {
                    let Some(old) = model.remove(id) else {
                        continue;
                    };
                    let sql = format!("DELETE FROM orders WHERE id = {id}");
                    source.execute(&sql).unwrap();
                    expected.push(oracle(&subs, Some(&old), None));
                }
            }
        }

        for (i, exp) in expected.iter().enumerate() {
            let event = source
                .poll_next_event()
                .unwrap()
                .unwrap_or_else(|| panic!("expected event #{i} but the shadow log was empty"));
            let notifs = engine.consumers(&event).unwrap();

            let mut actual_inserted: Vec<u64> = notifs.inserted().to_vec();
            let mut actual_deleted: Vec<u64> = notifs.deleted().to_vec();
            let mut actual_updated: Vec<u64> = notifs.updated().to_vec();
            actual_inserted.sort_unstable();
            actual_deleted.sort_unstable();
            actual_updated.sort_unstable();

            prop_assert_eq!(
                &actual_inserted, &exp.inserted,
                "event {} ({:?}) inserted mismatch", i, event.kind()
            );
            prop_assert_eq!(
                &actual_deleted, &exp.deleted,
                "event {} ({:?}) deleted mismatch", i, event.kind()
            );
            prop_assert_eq!(
                &actual_updated, &exp.updated,
                "event {} ({:?}) updated mismatch", i, event.kind()
            );

            // Sanity: the event kind matches the model-derived expectation.
            // INSERT/DELETE never produce `updated`; UPDATE never produces
            // both inserted-only and deleted-only for the same consumer.
            match event.kind() {
                EventKind::Insert => prop_assert!(notifs.deleted().is_empty() && notifs.updated().is_empty()),
                EventKind::Delete => prop_assert!(notifs.inserted().is_empty() && notifs.updated().is_empty()),
                EventKind::Update | EventKind::Truncate => {}
            }
        }

        let trailing = source.poll_next_event().unwrap();
        prop_assert!(trailing.is_none(), "shadow log should be empty after drain");
    }

    /// pgoutput round-trip dispatch: the same arbitrary DML, but every `WalEvent` is
    /// routed through [`PgOutputBridge`] and back through
    /// [`PgOutputParser`] before reaching the engine. The resulting
    /// [`subql::ConsumerNotifications`] must equal the same oracle the
    /// end-to-end dispatch test uses, proving the bridge plus the production parser
    /// preserve dispatch semantics across the wire format. A failure
    /// here means either the bridge mis-encodes a shape the engine
    /// dispatches on, or the parser decodes it into an event the engine
    /// treats differently.
    #[test]
    fn pgoutput_roundtrip_dispatch_matches_oracle_across_arbitrary_dml(
        ops in prop::collection::vec(op_strategy(), 0..25),
    ) {
        let subs = subscriptions();
        let mut source = build_source();
        let mut engine = build_engine();
        let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap());
        let mut bridge = PgOutputBridge::new();
        let parser = PgOutputParser::new();
        let mut model: BTreeMap<i64, Row> = BTreeMap::new();
        let mut expected: Vec<Expected> = Vec::new();

        for op in &ops {
            match op {
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
                    expected.push(oracle(&subs, None, Some(row)));
                    model.insert(row.id, row.clone());
                }
                Op::Update(row) => {
                    let Some(old) = model.get(&row.id).cloned() else {
                        continue;
                    };
                    let sql = format!(
                        "UPDATE orders SET amount = {amount}, status = '{status}' WHERE id = {id}",
                        id = row.id,
                        amount = row.amount,
                        status = quote_status(&row.status),
                    );
                    source.execute(&sql).unwrap();
                    expected.push(oracle(&subs, Some(&old), Some(row)));
                    model.insert(row.id, row.clone());
                }
                Op::Delete { id } => {
                    let Some(old) = model.remove(id) else {
                        continue;
                    };
                    let sql = format!("DELETE FROM orders WHERE id = {id}");
                    source.execute(&sql).unwrap();
                    expected.push(oracle(&subs, Some(&old), None));
                }
            }
        }

        for (i, exp) in expected.iter().enumerate() {
            let dispatch_event = source
                .poll_next_event()
                .unwrap()
                .unwrap_or_else(|| panic!("expected event #{i} but the shadow log was empty"));

            // Encode the dispatch event as pgoutput bytes, decode them
            // back through the production parser, dispatch on the
            // result. Bridge state survives across loop iterations so a
            // Relation message is emitted once per table.
            let frames = bridge
                .encode_event(&dispatch_event, &*catalog)
                .expect("bridge encodes catalog-resident event");
            let mut decoded: Vec<_> = Vec::new();
            for frame in frames {
                let events = parser
                    .parse_wal_message(&frame, &*catalog)
                    .expect("parser accepts bridge-encoded frames");
                decoded.extend(events);
            }
            prop_assert_eq!(
                decoded.len(), 1,
                "event {} ({:?}) must decode to exactly one event after the Relation cache warms",
                i, dispatch_event.kind()
            );
            let parsed = decoded.into_iter().next().unwrap();
            let notifs = engine.consumers(&parsed).unwrap();

            let mut actual_inserted: Vec<u64> = notifs.inserted().to_vec();
            let mut actual_deleted: Vec<u64> = notifs.deleted().to_vec();
            let mut actual_updated: Vec<u64> = notifs.updated().to_vec();
            actual_inserted.sort_unstable();
            actual_deleted.sort_unstable();
            actual_updated.sort_unstable();

            prop_assert_eq!(
                &actual_inserted, &exp.inserted,
                "pgoutput round-trip event {} ({:?}) inserted mismatch", i, parsed.kind()
            );
            prop_assert_eq!(
                &actual_deleted, &exp.deleted,
                "pgoutput round-trip event {} ({:?}) deleted mismatch", i, parsed.kind()
            );
            prop_assert_eq!(
                &actual_updated, &exp.updated,
                "pgoutput round-trip event {} ({:?}) updated mismatch", i, parsed.kind()
            );

            match parsed.kind() {
                EventKind::Insert => prop_assert!(notifs.deleted().is_empty() && notifs.updated().is_empty()),
                EventKind::Delete => prop_assert!(notifs.inserted().is_empty() && notifs.updated().is_empty()),
                EventKind::Update | EventKind::Truncate => {}
            }
        }

        let trailing = source.poll_next_event().unwrap();
        prop_assert!(trailing.is_none(), "shadow log should be empty after drain");
    }
}

#![allow(clippy::unwrap_used, clippy::option_if_let_else)]
//! Property-based tests for dispatch correctness
//!
//! Verifies the fundamental invariant: for any subscriptions and events,
//! `engine.consumers()` returns exactly the set of consumers whose SQL WHERE
//! clause matches the dispatched row.

use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use std::collections::{HashMap, HashSet};
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{DefaultIds, SubscriptionEngine, SubscriptionRequest};

mod test_schema {
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;
    use subql::{catalog_helpers, TableId};

    /// Build the proptest fixture: a single 3-column `items` table.
    pub(super) fn proptest_catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE _items_pad (id INT);\n\
         CREATE TABLE items (id INT PRIMARY KEY, amount INT, status TEXT);",
        )
        .expect("proptest items fixture parses")
    }

    pub(super) fn items_id(database: &ParserDB) -> TableId {
        catalog_helpers::table_id(database, "items").expect("items table exists")
    }

    pub(super) fn pad_id(database: &ParserDB) -> TableId {
        catalog_helpers::table_id(database, "_items_pad").expect("_items_pad table exists")
    }
}

mod strategies {
    use proptest::prelude::*;
    use subql::backend::{Postgres, Value};
    use subql::testing::TestEvent;
    use subql::TableId;

    /// A predicate we can generate SQL for and also evaluate directly in Rust.
    #[derive(Debug, Clone)]
    pub(super) enum TestPredicate {
        AmountGt(i64),
        AmountLt(i64),
        AmountEq(i64),
        AmountBetween(i64, i64),
        StatusEq(String),
        IdEq(i64),
        IsNull,
        And(Box<Self>, Box<Self>),
        Or(Box<Self>, Box<Self>),
    }

    impl TestPredicate {
        /// Convert to SQL WHERE clause.
        pub(super) fn to_sql(&self) -> String {
            match self {
                Self::AmountGt(v) => format!("amount > {v}"),
                Self::AmountLt(v) => format!("amount < {v}"),
                Self::AmountEq(v) => format!("amount = {v}"),
                Self::AmountBetween(lo, hi) => format!("amount BETWEEN {lo} AND {hi}"),
                Self::StatusEq(s) => format!("status = '{s}'"),
                Self::IdEq(v) => format!("id = {v}"),
                Self::IsNull => "amount IS NULL".to_string(),
                Self::And(a, b) => format!("({}) AND ({})", a.to_sql(), b.to_sql()),
                Self::Or(a, b) => format!("({}) OR ({})", a.to_sql(), b.to_sql()),
            }
        }

        /// Evaluate predicate against a row (ground truth).
        pub(super) fn eval(
            &self,
            id: &Value<Postgres>,
            amount: &Value<Postgres>,
            status: &Value<Postgres>,
        ) -> Option<bool> {
            match self {
                Self::AmountGt(v) => match amount {
                    Value::Int(a) => Some(*a > *v),
                    _ => None,
                },
                Self::AmountLt(v) => match amount {
                    Value::Int(a) => Some(*a < *v),
                    _ => None,
                },
                Self::AmountEq(v) => match amount {
                    Value::Int(a) => Some(*a == *v),
                    _ => None,
                },
                Self::AmountBetween(lo, hi) => match amount {
                    Value::Int(a) => Some(*a >= *lo && *a <= *hi),
                    _ => None,
                },
                Self::StatusEq(s) => match status {
                    Value::String(st) => Some(st.as_str() == s.as_str()),
                    _ => None,
                },
                Self::IdEq(v) => match id {
                    Value::Int(i) => Some(*i == *v),
                    _ => None,
                },
                Self::IsNull => match amount {
                    Value::Null => Some(true),
                    _ => Some(false),
                },
                Self::And(a, b) => {
                    let ra = a.eval(id, amount, status);
                    let rb = b.eval(id, amount, status);
                    match (ra, rb) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    }
                }
                Self::Or(a, b) => {
                    let ra = a.eval(id, amount, status);
                    let rb = b.eval(id, amount, status);
                    match (ra, rb) {
                        (Some(true), _) | (_, Some(true)) => Some(true),
                        (Some(false), Some(false)) => Some(false),
                        _ => None,
                    }
                }
            }
        }
    }

    /// Strategy for generating test predicates (limited depth).
    pub(super) fn predicate_strategy() -> impl Strategy<Value = TestPredicate> {
        let leaf = prop_oneof![
            (-500i64..500).prop_map(TestPredicate::AmountGt),
            (-500i64..500).prop_map(TestPredicate::AmountLt),
            (-500i64..500).prop_map(TestPredicate::AmountEq),
            (-500i64..500i64)
                .prop_flat_map(|lo| (Just(lo), lo..lo + 1000))
                .prop_map(|(lo, hi)| TestPredicate::AmountBetween(lo, hi)),
            prop_oneof![
                Just("active".to_string()),
                Just("pending".to_string()),
                Just("closed".to_string()),
            ]
            .prop_map(TestPredicate::StatusEq),
            (0i64..100).prop_map(TestPredicate::IdEq),
            Just(TestPredicate::IsNull),
        ];

        leaf.prop_recursive(2, 16, 4, |inner| {
            prop_oneof![
                (inner.clone(), inner.clone())
                    .prop_map(|(a, b)| TestPredicate::And(Box::new(a), Box::new(b))),
                (inner.clone(), inner)
                    .prop_map(|(a, b)| TestPredicate::Or(Box::new(a), Box::new(b))),
            ]
        })
    }

    /// Strategy for generating row cells.
    pub(super) fn row_strategy(
    ) -> impl Strategy<Value = (Value<Postgres>, Value<Postgres>, Value<Postgres>)> {
        let id_cell = (0i64..100).prop_map(Value::Int);
        let amount_cell = prop_oneof![
            9 => (-1000i64..1000).prop_map(Value::Int),
            1 => Just(Value::Null),
        ];
        let status_cell = prop_oneof![
            Just(Value::String("active".into())),
            Just(Value::String("pending".into())),
            Just(Value::String("closed".into())),
            Just(Value::String("unknown".into())),
            Just(Value::Null),
        ];
        (id_cell, amount_cell, status_cell)
    }

    pub(super) fn insert_event(
        tid: TableId,
        id: &Value<Postgres>,
        amount: &Value<Postgres>,
        status: &Value<Postgres>,
    ) -> TestEvent<Postgres> {
        TestEvent::<Postgres>::insert(tid, vec![id.clone(), amount.clone(), status.clone()])
            .with_pk_columns([0u16])
    }

    pub(super) fn delete_event(
        tid: TableId,
        id: &Value<Postgres>,
        amount: &Value<Postgres>,
        status: &Value<Postgres>,
    ) -> TestEvent<Postgres> {
        TestEvent::<Postgres>::delete(tid, vec![id.clone(), amount.clone(), status.clone()])
            .with_pk_columns([0u16])
    }

    pub(super) fn update_event(
        tid: TableId,
        id: &Value<Postgres>,
        old_amount: Value<Postgres>,
        new_amount: &Value<Postgres>,
        status: &Value<Postgres>,
        changed: impl IntoIterator<Item = u16>,
    ) -> TestEvent<Postgres> {
        TestEvent::<Postgres>::update(
            tid,
            vec![id.clone(), old_amount, status.clone()],
            vec![id.clone(), new_amount.clone(), status.clone()],
        )
        .with_pk_columns([0u16])
        .with_changed_columns(changed)
    }
}

use strategies::{
    delete_event, insert_event, predicate_strategy, row_strategy, update_event, TestPredicate,
};
use test_schema::{items_id, pad_id, proptest_catalog};

// Property Tests

proptest! {
    /// The core invariant: dispatch returns exactly the consumers whose predicates match.
    #[test]
    fn dispatch_matches_ground_truth(
        predicates in proptest::collection::vec(predicate_strategy(), 1..20),
        rows in proptest::collection::vec(row_strategy(), 1..10),
    ) {
        let catalog = proptest_catalog();
        let tid = items_id(&catalog);
        let dialect = PostgreSqlDialect {};
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, dialect);

        let mut consumer_predicates: HashMap<u64, TestPredicate> = HashMap::new();

        for (i, pred) in predicates.iter().enumerate() {
            let sql = format!("SELECT * FROM items WHERE {}", pred.to_sql());
            let consumer_id = (i as u64) + 1;
            let spec = SubscriptionRequest::<DefaultIds, Postgres>::new(consumer_id, sql);

            if engine.register(spec).is_ok() {
                consumer_predicates.insert(consumer_id, pred.clone());
            }
        }

        for (id_cell, amount_cell, status_cell) in &rows {
            let event = insert_event(tid, id_cell, amount_cell, status_cell);

            let notifs = engine.consumers(&event).unwrap();
            let matched: HashSet<u64> = notifs.inserted().iter().copied().collect();
            prop_assert!(notifs.deleted().is_empty() && notifs.updated().is_empty(),
                "INSERT should produce no deleted/updated");

            let mut expected: HashSet<u64> = HashSet::new();
            for (&consumer_id, pred) in &consumer_predicates {
                if pred.eval(id_cell, amount_cell, status_cell) == Some(true) {
                    expected.insert(consumer_id);
                }
            }

            let false_positives: Vec<_> = matched.difference(&expected).copied().collect();
            let false_negatives: Vec<_> = expected.difference(&matched).copied().collect();

            prop_assert!(
                false_positives.is_empty() && false_negatives.is_empty(),
                "Dispatch mismatch for row [{:?}, {:?}, {:?}]:\n  false positives: {:?}\n  false negatives: {:?}",
                id_cell, amount_cell, status_cell,
                false_positives, false_negatives,
            );
        }
    }

    /// UPDATE dispatch splits consumers by the verdict on both row images:
    /// matching only afterwards is a view entry, only beforehand an exit, and
    /// matching both an update. A `SELECT *` subscription holds every column,
    /// so it hears about the change whichever column moved, including one its
    /// WHERE clause never reads.
    #[test]
    fn update_matches_both_row_images(
        predicates in proptest::collection::vec(predicate_strategy(), 1..15),
        rows in proptest::collection::vec(row_strategy(), 1..8),
    ) {
        let catalog = proptest_catalog();
        let tid = items_id(&catalog);
        let dialect = PostgreSqlDialect {};
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, dialect);

        let mut consumer_predicates: HashMap<u64, TestPredicate> = HashMap::new();
        for (i, pred) in predicates.iter().enumerate() {
            let sql = format!("SELECT * FROM items WHERE {}", pred.to_sql());
            let consumer_id = (i as u64) + 1;
            let spec = SubscriptionRequest::<DefaultIds, Postgres>::new(consumer_id, sql);
            if engine.register(spec).is_ok() {
                consumer_predicates.insert(consumer_id, pred.clone());
            }
        }

        // Update events where only `amount` (col 1) changed.
        let old_amount = Value::Int(0);
        for (id_cell, amount_cell, status_cell) in &rows {
            let event = update_event(
                tid,
                id_cell,
                old_amount.clone(),
                amount_cell,
                status_cell,
                [1u16],
            );

            let notifs = engine.consumers(&event).unwrap();

            let mut expected_inserted: HashSet<u64> = HashSet::new();
            let mut expected_deleted: HashSet<u64> = HashSet::new();
            let mut expected_updated: HashSet<u64> = HashSet::new();
            for (&consumer_id, pred) in &consumer_predicates {
                let before = pred.eval(id_cell, &old_amount, status_cell) == Some(true);
                let after = pred.eval(id_cell, amount_cell, status_cell) == Some(true);
                let target = match (before, after) {
                    (false, true) => &mut expected_inserted,
                    (true, false) => &mut expected_deleted,
                    (true, true) => &mut expected_updated,
                    (false, false) => continue,
                };
                target.insert(consumer_id);
            }

            let notified = |ids: &[u64]| -> HashSet<u64> { ids.iter().copied().collect() };
            prop_assert_eq!(
                notified(notifs.inserted()),
                expected_inserted,
                "view entry mismatch for {:?}",
                event
            );
            prop_assert_eq!(
                notified(notifs.deleted()),
                expected_deleted,
                "view exit mismatch for {:?}",
                event
            );
            prop_assert_eq!(
                notified(notifs.updated()),
                expected_updated,
                "in-view update mismatch for {:?}",
                event
            );
        }
    }

    /// Delete events use the old_row for predicate evaluation.
    #[test]
    fn delete_matches_old_row(
        predicates in proptest::collection::vec(predicate_strategy(), 1..15),
        rows in proptest::collection::vec(row_strategy(), 1..8),
    ) {
        let catalog = proptest_catalog();
        let tid = items_id(&catalog);
        let dialect = PostgreSqlDialect {};
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, dialect);

        let mut consumer_predicates: HashMap<u64, TestPredicate> = HashMap::new();
        for (i, pred) in predicates.iter().enumerate() {
            let sql = format!("SELECT * FROM items WHERE {}", pred.to_sql());
            let consumer_id = (i as u64) + 1;
            let spec = SubscriptionRequest::<DefaultIds, Postgres>::new(consumer_id, sql);
            if engine.register(spec).is_ok() {
                consumer_predicates.insert(consumer_id, pred.clone());
            }
        }

        for (id_cell, amount_cell, status_cell) in &rows {
            let event = delete_event(tid, id_cell, amount_cell, status_cell);

            let notifs = engine.consumers(&event).unwrap();
            let matched: HashSet<u64> = notifs.deleted().iter().copied().collect();
            prop_assert!(notifs.inserted().is_empty() && notifs.updated().is_empty(),
                "DELETE should produce no inserted/updated");

            let mut expected: HashSet<u64> = HashSet::new();
            for (&consumer_id, pred) in &consumer_predicates {
                if pred.eval(id_cell, amount_cell, status_cell) == Some(true) {
                    expected.insert(consumer_id);
                }
            }

            let false_positives: Vec<_> = matched.difference(&expected).copied().collect();
            let false_negatives: Vec<_> = expected.difference(&matched).copied().collect();

            prop_assert!(
                false_positives.is_empty() && false_negatives.is_empty(),
                "Delete dispatch mismatch for row [{:?}, {:?}, {:?}]:\n  false positives: {:?}\n  false negatives: {:?}",
                id_cell, amount_cell, status_cell,
                false_positives, false_negatives,
            );
        }
    }

    /// Batch registration produces identical dispatch results to individual registration.
    #[test]
    fn batch_register_matches_individual(
        predicates in proptest::collection::vec(predicate_strategy(), 1..15),
        rows in proptest::collection::vec(row_strategy(), 1..5),
    ) {
        let catalog1 = proptest_catalog();
        let tid1 = items_id(&catalog1);
        let mut engine1: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog1, PostgreSqlDialect {});

        let catalog2 = proptest_catalog();
        let tid2 = items_id(&catalog2);
        prop_assert_eq!(tid1, tid2);
        let mut engine2: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog2, PostgreSqlDialect {});

        let mut specs = Vec::new();
        for (i, pred) in predicates.iter().enumerate() {
            let sql = format!("SELECT * FROM items WHERE {}", pred.to_sql());
            let consumer_id = (i as u64) + 1;

            let _ = engine1.register(SubscriptionRequest::<DefaultIds, Postgres>::new(
                consumer_id,
                sql.clone(),
            ));
            specs.push(SubscriptionRequest::<DefaultIds, Postgres>::new(
                consumer_id, sql,
            ));
        }

        engine2.register_batch(specs);

        for (id_cell, amount_cell, status_cell) in &rows {
            let event = insert_event(tid1, id_cell, amount_cell, status_cell);

            let result1: HashSet<u64> = match engine1.consumers(&event) {
                Ok(notifs) => notifs.inserted().iter().copied().collect(),
                Err(_) => HashSet::new(),
            };

            let result2: HashSet<u64> = match engine2.consumers(&event) {
                Ok(notifs) => notifs.inserted().iter().copied().collect(),
                Err(_) => HashSet::new(),
            };

            prop_assert_eq!(
                &result1, &result2,
                "Batch vs individual mismatch for row [{:?}, {:?}, {:?}]",
                id_cell, amount_cell, status_cell,
            );
        }
    }

    /// An event for a table that is in the catalog but has no subscription
    /// must never error and must report nobody affected, no matter how many
    /// subscriptions exist on other tables. This is the multi-table CDC
    /// firehose case: change events arrive for tables nobody is watching yet.
    #[test]
    fn dispatch_to_unsubscribed_cataloged_table_is_empty(
        predicates in proptest::collection::vec(predicate_strategy(), 0..12),
        ids in proptest::collection::vec(any::<i64>(), 1..8),
    ) {
        let catalog = proptest_catalog();
        let pad_tid = pad_id(&catalog);
        let dialect = PostgreSqlDialect {};
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, dialect);

        // Every subscription targets `items`, none target `_items_pad`.
        for (i, pred) in predicates.iter().enumerate() {
            let sql = format!("SELECT * FROM items WHERE {}", pred.to_sql());
            let _ = engine.register(SubscriptionRequest::<DefaultIds, Postgres>::new(
                (i as u64) + 1,
                sql,
            ));
        }

        for id in &ids {
            let event = TestEvent::<Postgres>::insert(pad_tid, vec![Value::Int(*id)])
                .with_pk_columns([0u16]);

            let notifs = engine.consumers(&event).expect("cataloged table never errors");
            prop_assert!(
                notifs.inserted().is_empty()
                    && notifs.updated().is_empty()
                    && notifs.deleted().is_empty(),
                "unsubscribed table should notify nobody",
            );
            prop_assert!(engine.aggregate_updates(&event).expect("no agg path").is_empty());
            prop_assert!(engine.dispatch(&event).expect("dispatch ok").notified().is_empty());
        }
    }
}

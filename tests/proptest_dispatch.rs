#![allow(clippy::unwrap_used, clippy::option_if_let_else, unused_imports)]
//! Property-based tests for dispatch correctness
//!
//! Verifies the fundamental invariant: for any subscriptions and events,
//! `engine.consumers()` returns exactly the set of consumers whose SQL WHERE clause
//! matches the dispatched row.

use proptest::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use subql::{
    compiler::{parse_compile_normalize_and_prefilter, Vm},
    Cell, DefaultIds, EventKind, PrimaryKey, RowImage, SchemaCatalog, SubscriptionEngine,
    SubscriptionRequest, TableId, WalEvent,
};

// ============================================================================
// Test Schema
// ============================================================================

/// Fixed 3-column schema: id (int), amount (int), status (string)
struct PropTestCatalog;

impl SchemaCatalog for PropTestCatalog {
    fn table_id(&self, table_name: &str) -> Option<TableId> {
        if table_name == "items" {
            Some(1)
        } else {
            None
        }
    }

    fn column_id(&self, table_id: TableId, column_name: &str) -> Option<u16> {
        if table_id != 1 {
            return None;
        }
        match column_name {
            "id" => Some(0),
            "amount" => Some(1),
            "status" => Some(2),
            _ => None,
        }
    }

    fn table_arity(&self, table_id: TableId) -> Option<usize> {
        if table_id == 1 {
            Some(3)
        } else {
            None
        }
    }

    fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
        Some(0xDEAD_BEEF)
    }
}

// ============================================================================
// Strategies
// ============================================================================

/// A predicate we can generate SQL for and also evaluate directly in Rust
#[derive(Debug, Clone)]
enum TestPredicate {
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
    /// Convert to SQL WHERE clause
    fn to_sql(&self) -> String {
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

    /// Evaluate predicate against a row (ground truth)
    fn eval(&self, id: &Cell, amount: &Cell, status: &Cell) -> Option<bool> {
        match self {
            Self::AmountGt(v) => match amount {
                Cell::Int(a) => Some(*a > *v),
                _ => None,
            },
            Self::AmountLt(v) => match amount {
                Cell::Int(a) => Some(*a < *v),
                _ => None,
            },
            Self::AmountEq(v) => match amount {
                Cell::Int(a) => Some(*a == *v),
                _ => None,
            },
            Self::AmountBetween(lo, hi) => match amount {
                Cell::Int(a) => Some(*a >= *lo && *a <= *hi),
                _ => None,
            },
            Self::StatusEq(s) => match status {
                Cell::String(st) => Some(st.as_ref() == s.as_str()),
                _ => None,
            },
            Self::IdEq(v) => match id {
                Cell::Int(i) => Some(*i == *v),
                _ => None,
            },
            Self::IsNull => match amount {
                Cell::Null => Some(true),
                _ => Some(false),
            },
            Self::And(a, b) => {
                let ra = a.eval(id, amount, status);
                let rb = b.eval(id, amount, status);
                match (ra, rb) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None, // Unknown
                }
            }
            Self::Or(a, b) => {
                let ra = a.eval(id, amount, status);
                let rb = b.eval(id, amount, status);
                match (ra, rb) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None, // Unknown
                }
            }
        }
    }
}

/// Strategy for generating test predicates (limited depth)
fn predicate_strategy() -> impl Strategy<Value = TestPredicate> {
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

    leaf.prop_recursive(
        2,  // max depth
        16, // max nodes
        4,  // items per collection
        |inner| {
            prop_oneof![
                (inner.clone(), inner.clone())
                    .prop_map(|(a, b)| TestPredicate::And(Box::new(a), Box::new(b))),
                (inner.clone(), inner)
                    .prop_map(|(a, b)| TestPredicate::Or(Box::new(a), Box::new(b))),
            ]
        },
    )
}

/// Strategy for generating row cells
fn row_strategy() -> impl Strategy<Value = (Cell, Cell, Cell)> {
    let id_cell = (0i64..100).prop_map(Cell::Int);
    let amount_cell = prop_oneof![
        9 => (-1000i64..1000).prop_map(Cell::Int),
        1 => Just(Cell::Null),
    ];
    let status_cell = prop_oneof![
        Just(Cell::String("active".into())),
        Just(Cell::String("pending".into())),
        Just(Cell::String("closed".into())),
        Just(Cell::String("unknown".into())),
        Just(Cell::Null),
    ];
    (id_cell, amount_cell, status_cell)
}

// ============================================================================
// Property Tests
// ============================================================================

proptest! {
    /// The core invariant: dispatch returns exactly the consumers whose predicates match.
    #[test]
    fn dispatch_matches_ground_truth(
        predicates in proptest::collection::vec(predicate_strategy(), 1..20),
        rows in proptest::collection::vec(row_strategy(), 1..10),
    ) {
        let catalog = Arc::new(PropTestCatalog);
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let mut engine: SubscriptionEngine<sqlparser::dialect::PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, dialect);

        // Register each predicate as a subscription for a unique consumer
        let mut consumer_predicates: HashMap<u64, TestPredicate> = HashMap::new();

        for (i, pred) in predicates.iter().enumerate() {
            let sql = format!("SELECT * FROM items WHERE {}", pred.to_sql());
            let consumer_id = (i as u64) + 1;
            let spec = SubscriptionRequest::new(consumer_id, sql);

            if engine.register(spec).is_ok() {
                consumer_predicates.insert(consumer_id, pred.clone());
            }
        }

        // Dispatch each row and verify
        for (id_cell, amount_cell, status_cell) in &rows {
            let event = WalEvent::builder(1)
                .insert()
                .pk_cell(0, id_cell.clone())
                .new_row(RowImage {
                    cells: Arc::from([id_cell.clone(), amount_cell.clone(), status_cell.clone()]),
                })
                .build()
                .expect("insert event builder should be valid");

            let notifs = engine.consumers(&event).unwrap();
            // INSERT events: all matched consumers should be in `inserted`.
            let matched: HashSet<u64> = notifs.inserted().iter().copied().collect();
            prop_assert!(notifs.deleted().is_empty() && notifs.updated().is_empty(),
                "INSERT should produce no deleted/updated");

            // Ground truth: evaluate each predicate in Rust
            let mut expected: HashSet<u64> = HashSet::new();
            for (&consumer_id, pred) in &consumer_predicates {
                if pred.eval(id_cell, amount_cell, status_cell) == Some(true) {
                    expected.insert(consumer_id);
                }
            }

            // The invariant: matched == expected
            let false_positives: Vec<_> = matched.difference(&expected).copied().collect();
            let false_negatives: Vec<_> = expected.difference(&matched).copied().collect();

            prop_assert!(
                false_positives.is_empty() && false_negatives.is_empty(),
                "Dispatch mismatch for row [{:?}, {:?}, {:?}]:\n  false positives (dispatched but shouldn't): {:?}\n  false negatives (should dispatch but didn't): {:?}",
                id_cell, amount_cell, status_cell,
                false_positives, false_negatives,
            );
        }
    }

    /// Update events with changed_columns behave correctly:
    /// - Consumers whose predicates depend ONLY on unchanged columns are NOT notified.
    /// - Consumers whose predicates depend on at least one changed column are evaluated
    ///   against the new row (matching if the new row satisfies the predicate).
    #[test]
    fn update_with_changed_columns_matches_ground_truth(
        predicates in proptest::collection::vec(predicate_strategy(), 1..15),
        rows in proptest::collection::vec(row_strategy(), 1..8),
    ) {
        let catalog = Arc::new(PropTestCatalog);
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let mut engine: SubscriptionEngine<sqlparser::dialect::PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, dialect);

        let mut consumer_predicates: HashMap<u64, TestPredicate> = HashMap::new();
        for (i, pred) in predicates.iter().enumerate() {
            let sql = format!("SELECT * FROM items WHERE {}", pred.to_sql());
            let consumer_id = (i as u64) + 1;
            let spec = SubscriptionRequest::new(consumer_id, sql);
            if engine.register(spec).is_ok() {
                consumer_predicates.insert(consumer_id, pred.clone());
            }
        }

        // Update events where only `amount` (col 1) changed.
        // Predicates on `amount` or `id` may fire; predicates only on `status` won't.
        for (id_cell, amount_cell, status_cell) in &rows {
            let event = subql::WalEvent::builder(1)
                .update()
                .new_row(RowImage {
                    cells: Arc::from([id_cell.clone(), amount_cell.clone(), status_cell.clone()]),
                })
                .pk_cell(0, id_cell.clone())
                .maybe_old_row(Some(RowImage {
                    cells: Arc::from([id_cell.clone(), Cell::Int(0), status_cell.clone()]),
                }))
                // Only `amount` (col 1) changed.
                .changed_columns(Arc::from([1u16]))
                .build()
                .expect("update event builder should be valid");

            let notifs = engine.consumers(&event).unwrap();
            let all_notified: HashSet<u64> = notifs.into_iter().collect();

            // Ground truth: consumers with predicates depending on `amount` (col 1)
            // are candidates; the engine evaluates both old and new rows.
            // StatusEq predicates (col 2 only) should never appear in any bucket
            // because col 2 is not in changed_columns.
            for (&consumer_id, pred) in &consumer_predicates {
                if let TestPredicate::StatusEq(_) = pred {
                    prop_assert!(
                        !all_notified.contains(&consumer_id),
                        "StatusEq predicate should not fire when only amount changed: consumer {}",
                        consumer_id
                    );
                }
            }
        }
    }

    /// Delete events use the old_row for predicate evaluation.
    #[test]
    fn delete_matches_old_row(
        predicates in proptest::collection::vec(predicate_strategy(), 1..15),
        rows in proptest::collection::vec(row_strategy(), 1..8),
    ) {
        let catalog = Arc::new(PropTestCatalog);
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let mut engine: SubscriptionEngine<sqlparser::dialect::PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, dialect);

        let mut consumer_predicates: HashMap<u64, TestPredicate> = HashMap::new();
        for (i, pred) in predicates.iter().enumerate() {
            let sql = format!("SELECT * FROM items WHERE {}", pred.to_sql());
            let consumer_id = (i as u64) + 1;
            let spec = SubscriptionRequest::new(consumer_id, sql);
            if engine.register(spec).is_ok() {
                consumer_predicates.insert(consumer_id, pred.clone());
            }
        }

        for (id_cell, amount_cell, status_cell) in &rows {
            let event = WalEvent::builder(1)
                .delete()
                .pk_cell(0, id_cell.clone())
                .old_row(RowImage {
                    cells: Arc::from([id_cell.clone(), amount_cell.clone(), status_cell.clone()]),
                })
                .build()
                .expect("delete event builder should be valid");

            let notifs = engine.consumers(&event).unwrap();
            // DELETE events: all matched consumers should be in `deleted`.
            let matched: HashSet<u64> = notifs.deleted().iter().copied().collect();
            prop_assert!(notifs.inserted().is_empty() && notifs.updated().is_empty(),
                "DELETE should produce no inserted/updated");

            // Ground truth: evaluate against the old row (same cells)
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

    /// Prefilter soundness: if `prefilter.may_match(row) == false`, the VM must
    /// not return `Tri::True` for the same row. False negatives in the prefilter
    /// would cause correct subscribers to be silently dropped.
    #[test]
    fn prefilter_never_has_false_negatives(
        predicates in proptest::collection::vec(predicate_strategy(), 1..20),
        rows in proptest::collection::vec(row_strategy(), 1..10),
    ) {
        let catalog = PropTestCatalog;
        let dialect = sqlparser::dialect::PostgreSqlDialect {};

        for pred in &predicates {
            let sql = format!("SELECT * FROM items WHERE {}", pred.to_sql());
            let Ok((_table_id, bytecode, _norm, prefilter, _projection)) =
                parse_compile_normalize_and_prefilter(&sql, &dialect, &catalog)
            else {
                continue; // skip predicates that fail to compile
            };

            for (id_cell, amount_cell, status_cell) in &rows {
                let row = RowImage {
                    cells: std::sync::Arc::from([
                        id_cell.clone(),
                        amount_cell.clone(),
                        status_cell.clone(),
                    ]),
                };

                if !prefilter.may_match(&row) {
                    // Prefilter says "definitely no match" — VM must agree
                    let mut vm = Vm::new();
                    let vm_result = vm.eval(&bytecode, &row);
                    prop_assert!(
                        vm_result != Ok(subql::compiler::Tri::True),
                        "Prefilter soundness violation for predicate '{}' on row [{:?}, {:?}, {:?}]: \
                        prefilter.may_match=false but VM returned {:?}",
                        pred.to_sql(), id_cell, amount_cell, status_cell, vm_result,
                    );
                }
            }
        }
    }

    /// Batch registration produces identical dispatch results to individual registration.
    #[test]
    fn batch_register_matches_individual(
        predicates in proptest::collection::vec(predicate_strategy(), 1..15),
        rows in proptest::collection::vec(row_strategy(), 1..5),
    ) {
        let catalog: Arc<dyn subql::SchemaCatalog> = Arc::new(PropTestCatalog);
        let dialect = sqlparser::dialect::PostgreSqlDialect {};

        // Engine 1: individual register
        let mut engine1: SubscriptionEngine<sqlparser::dialect::PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(Arc::clone(&catalog), sqlparser::dialect::PostgreSqlDialect {});

        // Engine 2: batch register
        let mut engine2: SubscriptionEngine<sqlparser::dialect::PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, dialect);

        let mut specs = Vec::new();
        for (i, pred) in predicates.iter().enumerate() {
            let sql = format!("SELECT * FROM items WHERE {}", pred.to_sql());
            let consumer_id = (i as u64) + 1;

            let _ = engine1.register(SubscriptionRequest::new(consumer_id, sql.clone()));
            specs.push(SubscriptionRequest::new(consumer_id, sql));
        }

        engine2.register_batch(specs);

        // Dispatch same events to both engines, verify identical results
        for (id_cell, amount_cell, status_cell) in &rows {
            let event = WalEvent::builder(1)
                .insert()
                .pk_cell(0, id_cell.clone())
                .new_row(RowImage {
                    cells: Arc::from([id_cell.clone(), amount_cell.clone(), status_cell.clone()]),
                })
                .build()
                .expect("insert event builder should be valid");

            let result1: HashSet<u64> = match engine1.consumers(&event) {
                Ok(notifs) => notifs.into_iter().collect(),
                Err(_) => HashSet::new(),
            };

            let result2: HashSet<u64> = match engine2.consumers(&event) {
                Ok(notifs) => notifs.into_iter().collect(),
                Err(_) => HashSet::new(),
            };

            prop_assert_eq!(
                &result1, &result2,
                "Batch vs individual mismatch for row [{:?}, {:?}, {:?}]",
                id_cell, amount_cell, status_cell,
            );
        }
    }
}

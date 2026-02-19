#![allow(clippy::unwrap_used, clippy::option_if_let_else)]
//! Property-based tests for dispatch correctness
//!
//! Verifies the fundamental invariant: for any subscriptions and events,
//! `engine.users()` returns exactly the set of users whose SQL WHERE clause
//! matches the dispatched row.

use proptest::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use subql::{
    Cell, DefaultIds, EventKind, PrimaryKey, RowImage, SchemaCatalog, SubscriptionEngine,
    SubscriptionSpec, TableId, WalEvent,
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
    /// The core invariant: dispatch returns exactly the users whose predicates match.
    #[test]
    fn dispatch_matches_ground_truth(
        predicates in proptest::collection::vec(predicate_strategy(), 1..20),
        rows in proptest::collection::vec(row_strategy(), 1..10),
    ) {
        let catalog = Arc::new(PropTestCatalog);
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let mut engine: SubscriptionEngine<sqlparser::dialect::PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, dialect);

        // Register each predicate as a subscription for a unique user
        let mut user_predicates: HashMap<u64, TestPredicate> = HashMap::new();

        for (i, pred) in predicates.iter().enumerate() {
            let sql = format!("SELECT * FROM items WHERE {}", pred.to_sql());
            let user_id = (i as u64) + 1;
            let spec = SubscriptionSpec {
                subscription_id: user_id,
                user_id,
                session_id: None,
                sql,
                updated_at_unix_ms: 0,
            };

            if engine.register(spec).is_ok() {
                user_predicates.insert(user_id, pred.clone());
            }
        }

        // Dispatch each row and verify
        for (id_cell, amount_cell, status_cell) in &rows {
            let event = WalEvent {
                kind: EventKind::Insert,
                table_id: 1,
                pk: PrimaryKey {
                    columns: Arc::from([0u16]),
                    values: Arc::from([id_cell.clone()]),
                },
                old_row: None,
                new_row: Some(RowImage {
                    cells: Arc::from([id_cell.clone(), amount_cell.clone(), status_cell.clone()]),
                }),
                changed_columns: Arc::from([]),
            };

            let matched: HashSet<u64> = engine.users(&event).unwrap().collect();

            // Ground truth: evaluate each predicate in Rust
            let mut expected: HashSet<u64> = HashSet::new();
            for (&user_id, pred) in &user_predicates {
                if pred.eval(id_cell, amount_cell, status_cell) == Some(true) {
                    expected.insert(user_id);
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
            let user_id = (i as u64) + 1;

            let _ = engine1.register(SubscriptionSpec {
                subscription_id: user_id,
                user_id,
                session_id: None,
                sql: sql.clone(),
                updated_at_unix_ms: 0,
            });
            specs.push(SubscriptionSpec {
                subscription_id: user_id,
                user_id,
                session_id: None,
                sql,
                updated_at_unix_ms: 0,
            });
        }

        engine2.register_batch(specs);

        // Dispatch same events to both engines, verify identical results
        for (id_cell, amount_cell, status_cell) in &rows {
            let event = WalEvent {
                kind: EventKind::Insert,
                table_id: 1,
                pk: PrimaryKey {
                    columns: Arc::from([0u16]),
                    values: Arc::from([id_cell.clone()]),
                },
                old_row: None,
                new_row: Some(RowImage {
                    cells: Arc::from([id_cell.clone(), amount_cell.clone(), status_cell.clone()]),
                }),
                changed_columns: Arc::from([]),
            };

            let result1: HashSet<u64> = match engine1.users(&event) {
                Ok(users) => users.collect(),
                Err(_) => HashSet::new(),
            };

            let result2: HashSet<u64> = match engine2.users(&event) {
                Ok(users) => users.collect(),
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

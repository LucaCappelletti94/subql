//! Property-based tests for the re-execution layer's classification of
//! single-table scalar `MIN`/`MAX` queries.
//!
//! The classifier (`src/reexec/plan.rs::build_plan`) is reached
//! through the public `ReExecEngine::register` entry point: queries
//! the core engine rejects (because they aren't delta-composable) fall
//! through to `build_plan`, which returns either a `Partial` plan for
//! single-table scalar MIN/MAX or an `UnsupportedSql` error. Anything
//! the engine accepts directly (rows, COUNT/SUM/AVG, view-relative
//! deltas) is surfaced via `Registered::Engine` and bypasses this code
//! path entirely.
//!
//! Properties tested
//! 1. **Every `SELECT {MIN,MAX}(col) FROM orders [WHERE ...]` over any
//!    typed column yields `Registered::ReExec`**, never the engine
//!    variant and never an error. `MIN`/`MAX` are orderable across
//!    every cell type, so neither the classifier nor the engine has
//!    grounds to reject them.
//! 2. **The reported `column_type` matches the column's declared type.**
//!    `MIN(price)` returns `ScalarKind::Float`. `MIN(quantity)`
//!    returns `ScalarKind::Int`. `MIN(status)` returns
//!    `ScalarKind::String`.
//! 3. **The returned SQL carries the canonical projection alias `v`.**
//!    Materializers load the scalar back by that alias.
//! 4. **Distinct queries get distinct `query_id`s within one engine.**

#![allow(clippy::unwrap_used, clippy::print_stdout)]

use std::collections::HashSet;

use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::reexec::{ReExecEngine, Registered};
use subql::backend::{Postgres, ScalarKind};
use subql::testing::TestEvent;
use subql::{DefaultIds, SubscriptionEngine, SubscriptionRequest};

const CATALOG_DDL: &str = "CREATE TABLE orders (\
    id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);";

type Engine = ReExecEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn fresh_engine() -> Engine {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(CATALOG_DDL).unwrap();
    ReExecEngine::new(SubscriptionEngine::new(catalog, PostgreSqlDialect {}))
}

#[derive(Debug, Clone, Copy)]
enum AggCol {
    Price,
    Quantity,
    Status,
}

impl AggCol {
    const fn name(self) -> &'static str {
        match self {
            Self::Price => "price",
            Self::Quantity => "quantity",
            Self::Status => "status",
        }
    }

    const fn scalar_kind(self) -> ScalarKind {
        match self {
            Self::Price => ScalarKind::Float,
            Self::Quantity => ScalarKind::Int,
            Self::Status => ScalarKind::String,
        }
    }
}

/// WHERE-clause column generator. We restrict WHERE comparisons to
/// numeric columns so a randomly chosen op + integer literal is always
/// a valid SQL predicate. Mixing strings here would require quoting
/// and an op subset, which is out of scope for this test.
#[derive(Debug, Clone, Copy)]
enum WhereCol {
    Price,
    Quantity,
}

impl WhereCol {
    const fn name(self) -> &'static str {
        match self {
            Self::Price => "price",
            Self::Quantity => "quantity",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Agg {
    Min,
    Max,
}

impl Agg {
    const fn keyword(self) -> &'static str {
        match self {
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum CmpOp {
    Gt,
    Lt,
    Eq,
    Neq,
    Ge,
    Le,
}

impl CmpOp {
    const fn sql(self) -> &'static str {
        match self {
            Self::Gt => ">",
            Self::Lt => "<",
            Self::Eq => "=",
            Self::Neq => "!=",
            Self::Ge => ">=",
            Self::Le => "<=",
        }
    }
}

fn arb_agg_col() -> impl Strategy<Value = AggCol> {
    prop_oneof![
        Just(AggCol::Price),
        Just(AggCol::Quantity),
        Just(AggCol::Status),
    ]
}

fn arb_where_col() -> impl Strategy<Value = WhereCol> {
    prop_oneof![Just(WhereCol::Price), Just(WhereCol::Quantity)]
}

fn arb_agg() -> impl Strategy<Value = Agg> {
    prop_oneof![Just(Agg::Min), Just(Agg::Max)]
}

fn arb_op() -> impl Strategy<Value = CmpOp> {
    prop_oneof![
        Just(CmpOp::Gt),
        Just(CmpOp::Lt),
        Just(CmpOp::Eq),
        Just(CmpOp::Neq),
        Just(CmpOp::Ge),
        Just(CmpOp::Le),
    ]
}

/// Optional WHERE clause: either none, or `<filter_col> <op> <literal>`
/// where `<filter_col>` is one of the numeric columns.
fn arb_where_clause() -> impl Strategy<Value = Option<String>> {
    prop_oneof![
        1 => Just(None),
        4 => (arb_where_col(), arb_op(), -100i64..=100i64).prop_map(|(c, o, n)| {
            Some(format!("{} {} {}", c.name(), o.sql(), n))
        }),
    ]
}

fn arb_minmax_query() -> impl Strategy<Value = (AggCol, Agg, String)> {
    (arb_agg_col(), arb_agg(), arb_where_clause()).prop_map(|(col, agg, w)| {
        let sql = w.map_or_else(
            || format!("SELECT {}({}) FROM orders", agg.keyword(), col.name()),
            |w| {
                format!(
                    "SELECT {}({}) FROM orders WHERE {}",
                    agg.keyword(),
                    col.name(),
                    w
                )
            },
        );
        (col, agg, sql)
    })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    /// Every well-formed `MIN`/`MAX` query against a typed numeric
    /// column returns `Registered::ReExec`, carries the column's
    /// declared type, and emits a re-exec SQL string containing the
    /// canonical alias `v`.
    #[test]
    fn minmax_query_is_classified_as_reexec(
        (col, agg, sql) in arb_minmax_query(),
        consumer in 0u64..1000,
    ) {
        let mut engine = fresh_engine();
        let registered = engine
            .register(SubscriptionRequest::<DefaultIds, Postgres>::new(consumer, sql.clone()))
            .unwrap_or_else(|e| panic!("`{sql}` should classify as ReExec, got error: {e:?}"));

        match registered {
            Registered::ReExec {
                column_kind,
                sql: reexec_sql,
                ..
            } => {
                prop_assert_eq!(
                    column_kind,
                    col.scalar_kind(),
                    "agg={:?} col={:?}: column_kind drift",
                    agg,
                    col.name(),
                );
                prop_assert!(
                    reexec_sql.contains("AS v") || reexec_sql.contains("AS \"v\""),
                    "reexec SQL missing canonical `AS v` alias: {reexec_sql:?}",
                );
                prop_assert!(
                    !reexec_sql.is_empty(),
                    "reexec SQL was empty for `{sql}`",
                );
            }
            Registered::Engine(_) => {
                return Err(TestCaseError::Fail(
                    format!("`{sql}` should not be engine-handled but was").into(),
                ));
            }
        }
    }

    /// Distinct re-exec queries in the same engine get distinct
    /// `query_id`s. Reusing the same SQL is allowed to dedup or not.
    /// This test only asserts uniqueness when the SQL strings differ
    /// after the classifier's canonical rendering, so we restrict to
    /// `(agg, col)` combinations that produce distinct rendered SQL.
    #[test]
    fn distinct_minmax_queries_get_distinct_query_ids(
        queries in proptest::collection::vec(arb_minmax_query(), 1..8),
    ) {
        let mut engine = fresh_engine();
        let mut seen_sql: HashSet<String> = HashSet::new();
        let mut seen_qids: HashSet<u64> = HashSet::new();

        for (i, (_, _, sql)) in queries.iter().enumerate() {
            let Some(_) = seen_sql.get(sql) else {
                seen_sql.insert(sql.clone());
                let registered = engine
                    .register(SubscriptionRequest::<DefaultIds, Postgres>::new(i as u64, sql.clone()))
                    .unwrap();
                if let Registered::ReExec { query_id, .. } = registered {
                    prop_assert!(
                        seen_qids.insert(query_id),
                        "duplicate query_id {query_id} for distinct SQL `{sql}`",
                    );
                }
                continue;
            };
            // Skip duplicates: their dedup behaviour is governed by
            // the engine, not by `build_plan`, and is out of scope.
        }
    }
}

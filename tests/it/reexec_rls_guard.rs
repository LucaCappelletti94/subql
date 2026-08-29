//! The reexec wrapper must reject aggregator registrations on
//! RLS-protected tables for BOTH aggregate families: the in-process delta
//! family (`COUNT`/`SUM`/`AVG`/`VAR_*`/`STDDEV_*`) that the core engine
//! accepts directly, and the re-execution family (`MIN`/`MAX`) that the
//! core engine rejects and the wrapper captures. A single in-process IVM
//! state cannot be shared across viewers who each see a different subset
//! of rows, so both families are a cross-viewer disclosure under RLS.
//!
//! Row subscriptions (`SELECT *`) stay accepted on RLS tables: they are
//! filtered per viewer at delivery, so the guard keys on the aggregate
//! projection, not on the table alone.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{
    DefaultIds, RegisterError, Registered, SubscriptionEngine, SubscriptionRequest, TableId, Tier,
};

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

const RLS_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, amount INT, status TEXT); \
     ALTER TABLE t ENABLE ROW LEVEL SECURITY;";
const PLAIN_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, amount INT, status TEXT);";

/// The eight in-process delta aggregates the core engine accepts. Each is
/// a separate case so a single missed variant is visible.
const INPROCESS_AGGREGATES: &[&str] = &[
    "SELECT COUNT(*) FROM t",
    "SELECT COUNT(amount) FROM t",
    "SELECT SUM(amount) FROM t",
    "SELECT AVG(amount) FROM t",
    "SELECT VAR_POP(amount) FROM t",
    "SELECT VAR_SAMP(amount) FROM t",
    "SELECT STDDEV_POP(amount) FROM t",
    "SELECT STDDEV_SAMP(amount) FROM t",
];

fn engine_from(ddl: &str) -> Engine {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(ddl).unwrap();
    SubscriptionEngine::new(catalog, PostgreSqlDialect {})
}

/// Deterministic `table_id` for `t` in a catalog parsed from `ddl`.
fn table_id_of(ddl: &str) -> TableId {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(ddl).unwrap();
    subql::catalog_helpers::table_id(&catalog, "t").unwrap()
}

fn register(engine: &mut Engine, sql: &str) -> Result<Registered, RegisterError> {
    engine.register(SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, sql))
}

#[test]
fn rejects_inprocess_aggregators_on_rls_table() {
    let expected = table_id_of(RLS_DDL);
    for sql in INPROCESS_AGGREGATES {
        // Fresh engine per case so a rejected registration in one case
        // cannot mask another via shared dedup/registry state.
        let mut engine = engine_from(RLS_DDL);
        match register(&mut engine, sql) {
            Err(RegisterError::AggregatorOnRlsTable { table_id }) => {
                assert_eq!(
                    table_id, expected,
                    "`{sql}` rejected for the wrong table id"
                );
            }
            other => panic!("`{sql}` on RLS table should be rejected, got {other:?}"),
        }
    }
}

#[test]
fn allows_inprocess_aggregators_without_rls() {
    for sql in INPROCESS_AGGREGATES {
        let mut engine = engine_from(PLAIN_DDL);
        match register(&mut engine, sql) {
            Ok(Registered {
                tier: Tier::InProcess(result),
                ..
            }) => {
                assert!(
                    result.aggregate_spec().is_some(),
                    "`{sql}` should register as an aggregate projection"
                );
            }
            other => panic!("`{sql}` without RLS should be engine-accepted, got {other:?}"),
        }
    }
}

#[test]
fn allows_row_subscription_on_rls_table() {
    let mut engine = engine_from(RLS_DDL);
    match register(&mut engine, "SELECT * FROM t WHERE amount > 10") {
        Ok(Registered {
            tier: Tier::InProcess(result),
            ..
        }) => {
            assert!(
                result.aggregate_spec().is_none(),
                "row subscription must not carry an aggregate spec"
            );
        }
        other => panic!("row subscription on RLS table should be accepted, got {other:?}"),
    }
}

#[test]
fn min_max_on_rls_still_rejected() {
    let expected = table_id_of(RLS_DDL);
    for sql in ["SELECT MIN(amount) FROM t", "SELECT MAX(amount) FROM t"] {
        let mut engine = engine_from(RLS_DDL);
        match register(&mut engine, sql) {
            Err(RegisterError::AggregatorOnRlsTable { table_id }) => {
                assert_eq!(
                    table_id, expected,
                    "`{sql}` rejected for the wrong table id"
                );
            }
            other => panic!("`{sql}` on RLS table should be rejected, got {other:?}"),
        }
    }
}

#[test]
fn min_max_without_rls_still_captured() {
    for sql in ["SELECT MIN(amount) FROM t", "SELECT MAX(amount) FROM t"] {
        let mut engine = engine_from(PLAIN_DDL);
        match register(&mut engine, sql) {
            Ok(Registered {
                tier: Tier::Scalar { .. },
                ..
            }) => {}
            other => panic!("`{sql}` without RLS should be captured for reexec, got {other:?}"),
        }
    }
}

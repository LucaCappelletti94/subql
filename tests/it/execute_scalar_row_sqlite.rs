//! Docker-free connector round-trip for the multi-column aggregate seed.
//!
//! Runs each aggregate's `aggregate_bootstrap` (`sql` + `kinds`) through
//! `DieselConnector::execute_scalar_row` against an in-memory SQLite
//! database, then feeds the decoded row to `AggAccumulator::seed_from_row`.
//! Ties the registration, the connector decode, and the accumulator to a
//! direct recompute for the whole aggregate family. Real-Postgres coverage
//! rides the existing `#[ignore]` Docker convention elsewhere.

#![allow(
    clippy::unwrap_used,
    clippy::cast_precision_loss,
    clippy::suboptimal_flops,
    clippy::option_if_let_else
)]

use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::reexec::{Connector, DieselConnector};
use subql::testing::TestEvent;
use subql::{
    AggSpec, AggValue, AggregateBootstrap, DefaultIds, SubscriptionEngine, SubscriptionId,
    SubscriptionRequest,
};

diesel::table! {
    json_t (id) {
        id -> Integer,
        payload -> Text,
    }
}

const CATALOG: &str = "CREATE TABLE t (id INT PRIMARY KEY, amount INT);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

/// Register `sql` and return the engine, the subscription holding its running
/// total, and its bootstrap bundle (`sql` + `kinds`).
fn registered(sql: &str) -> (Engine, SubscriptionId, AggregateBootstrap) {
    let db = ParserDB::parse::<PostgreSqlDialect>(CATALOG).unwrap();
    let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {});
    let result = engine
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, sql))
        .unwrap();
    let bootstrap = result
        .served()
        .expect("the engine maintains this one in process")
        .aggregate_bootstrap
        .clone()
        .expect("aggregate registration carries a bootstrap");
    (engine, result.subscription_id, bootstrap)
}

/// Just the bootstrap bundle, for the tests that only read the SQL.
fn bootstrap(sql: &str) -> AggregateBootstrap {
    registered(sql).2
}

fn install_seed(
    engine: &mut Engine,
    subscription: SubscriptionId,
    row: Vec<Value<Postgres>>,
) -> Result<AggValue, subql::AggregateInstallError> {
    let updates = subql::Install::install(
        engine,
        subscription,
        subql::AggregateSeedInstall {
            rows: vec![row],
            read_at: None,
        },
    )?;
    assert_eq!(updates.len(), 1);
    Ok(updates[0]
        .folded_value()
        .expect("ungrouped install sets a value"))
}
/// In-memory SQLite `t` seeded with `amounts` (None = NULL amount).
fn sqlite_with(amounts: &[Option<i64>]) -> DieselConnector<SqliteConnection, Postgres> {
    let mut conn = SqliteConnection::establish(":memory:").unwrap();
    sql_query("CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER)")
        .execute(&mut conn)
        .unwrap();
    for (i, amount) in amounts.iter().enumerate() {
        let id = i + 1;
        let stmt = match amount {
            Some(v) => format!("INSERT INTO t (id, amount) VALUES ({id}, {v})"),
            None => format!("INSERT INTO t (id, amount) VALUES ({id}, NULL)"),
        };
        sql_query(stmt).execute(&mut conn).unwrap();
    }
    DieselConnector::new(conn)
}

/// Textbook aggregate value over `amounts`, using the same formulas as
/// `AggAccumulator::value` so the seed matches it exactly.
fn oracle(spec: &AggSpec, amounts: &[Option<i64>]) -> AggValue {
    let count_star = i64::try_from(amounts.len()).unwrap();
    let nums: Vec<f64> = amounts.iter().flatten().map(|v| *v as f64).collect();
    let numeric = i64::try_from(nums.len()).unwrap();
    let n = nums.len() as f64;
    let sum: f64 = nums.iter().sum();
    let sum_sq: f64 = nums.iter().map(|v| v * v).sum();
    let var_pop = (numeric > 0).then(|| sum_sq / n - (sum / n).powi(2));
    let var_samp = (numeric >= 2).then(|| (sum_sq - sum.powi(2) / n) / (n - 1.0));
    match spec {
        AggSpec::CountStar => AggValue::Count(count_star),
        AggSpec::CountColumn { .. } => AggValue::Count(numeric),
        AggSpec::Sum { .. } => AggValue::Sum(sum),
        AggSpec::Avg { .. } => AggValue::Real((numeric > 0).then(|| sum / n)),
        AggSpec::VarPop { .. } => AggValue::Real(var_pop),
        AggSpec::VarSamp { .. } => AggValue::Real(var_samp),
        AggSpec::StddevPop { .. } => AggValue::Real(var_pop.map(f64::sqrt)),
        AggSpec::StddevSamp { .. } => AggValue::Real(var_samp.map(f64::sqrt)),
        _ => unreachable!("every AggSpec variant handled"),
    }
}

fn all_specs() -> Vec<(&'static str, AggSpec)> {
    vec![
        ("SELECT COUNT(*) FROM t", AggSpec::CountStar),
        (
            "SELECT COUNT(amount) FROM t",
            AggSpec::CountColumn { column: 1 },
        ),
        ("SELECT SUM(amount) FROM t", AggSpec::Sum { column: 1 }),
        ("SELECT AVG(amount) FROM t", AggSpec::Avg { column: 1 }),
        (
            "SELECT VAR_POP(amount) FROM t",
            AggSpec::VarPop { column: 1 },
        ),
        (
            "SELECT VAR_SAMP(amount) FROM t",
            AggSpec::VarSamp { column: 1 },
        ),
        (
            "SELECT STDDEV_POP(amount) FROM t",
            AggSpec::StddevPop { column: 1 },
        ),
        (
            "SELECT STDDEV_SAMP(amount) FROM t",
            AggSpec::StddevSamp { column: 1 },
        ),
    ]
}

#[test]
fn execute_scalar_row_decodes_components() {
    let connector = sqlite_with(&[Some(2), Some(4), Some(6)]);
    // COUNT(*): single Int component.
    let b = bootstrap("SELECT COUNT(*) FROM t");
    let (row, _) = connector
        .execute_scalar_row(
            &subql::reexec::ReadQuery::without_binds(&b.sql),
            &b.kinds,
            &(),
        )
        .unwrap();
    assert_eq!(row, vec![Value::Int(3)]);
    // SUM: single Float component (cast to double).
    let b = bootstrap("SELECT SUM(amount) FROM t");
    let (row, _) = connector
        .execute_scalar_row(
            &subql::reexec::ReadQuery::without_binds(&b.sql),
            &b.kinds,
            &(),
        )
        .unwrap();
    assert_eq!(row, vec![Value::Float(12.0)]);
    // AVG: (sum, count).
    let b = bootstrap("SELECT AVG(amount) FROM t");
    let (row, _) = connector
        .execute_scalar_row(
            &subql::reexec::ReadQuery::without_binds(&b.sql),
            &b.kinds,
            &(),
        )
        .unwrap();
    assert_eq!(row, vec![Value::Float(12.0), Value::Int(3)]);
    // VAR_POP: (sum, sum_sq, count) = (12, 56, 3).
    let b = bootstrap("SELECT VAR_POP(amount) FROM t");
    let (row, _) = connector
        .execute_scalar_row(
            &subql::reexec::ReadQuery::without_binds(&b.sql),
            &b.kinds,
            &(),
        )
        .unwrap();
    assert_eq!(
        row,
        vec![Value::Float(12.0), Value::Float(56.0), Value::Int(3)]
    );
}

#[test]
fn execute_scalar_row_empty_table_is_empty_state() {
    let connector = sqlite_with(&[]);
    // COUNT over empty is 0.
    let b = bootstrap("SELECT COUNT(*) FROM t");
    let (row, _) = connector
        .execute_scalar_row(
            &subql::reexec::ReadQuery::without_binds(&b.sql),
            &b.kinds,
            &(),
        )
        .unwrap();
    assert_eq!(row, vec![Value::Int(0)]);
    // SUM over empty is NULL, COUNT(amount) is 0.
    let (mut engine, subscription, b) = registered("SELECT AVG(amount) FROM t");
    let (row, _) = connector
        .execute_scalar_row(
            &subql::reexec::ReadQuery::without_binds(&b.sql),
            &b.kinds,
            &(),
        )
        .unwrap();
    assert_eq!(row, vec![Value::Null, Value::Int(0)]);
    // The empty row seeds the empty aggregate.
    assert_eq!(
        install_seed(&mut engine, subscription, row),
        Ok(AggValue::Real(None)),
    );
}

#[test]
fn seed_through_connector_matches_recompute() {
    // Includes a NULL amount so COUNT(*) and COUNT(amount) diverge.
    let amounts = [Some(2i64), Some(4), Some(6), None];
    let connector = sqlite_with(&amounts);
    for (sql, spec) in all_specs() {
        let (mut engine, subscription, b) = registered(sql);
        let (row, _) = connector
            .execute_scalar_row(
                &subql::reexec::ReadQuery::without_binds(&b.sql),
                &b.kinds,
                &(),
            )
            .unwrap();
        assert_eq!(
            install_seed(&mut engine, subscription, row),
            Ok(oracle(&spec, &amounts)),
            "seed-through-connector mismatch for `{sql}`"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// For any aggregate and any table, seeding from the row the connector
    /// decodes equals a direct recompute. Pins the SQL, the kinds, the
    /// connector decode, and the fold against one oracle. Amounts are
    /// bounded so the sums stay exact in `f64`.
    #[test]
    fn seed_through_connector_matches_recompute_prop(
        amounts in proptest::collection::vec(
            proptest::option::of(-1000i64..=1000),
            0..=32,
        ),
    ) {
        let connector = sqlite_with(&amounts);
        for (sql, spec) in all_specs() {
            let (mut engine, subscription, b) = registered(sql);
            let (row, _) = connector
                .execute_scalar_row(
                    &subql::reexec::ReadQuery::without_binds(&b.sql),
                    &b.kinds,
                    &(),
                )
                .unwrap();
            prop_assert_eq!(
                install_seed(&mut engine, subscription, row),
                Ok(oracle(&spec, &amounts)),
                "seed-through-connector mismatch for `{}` over {:?}", sql, amounts
            );
        }
    }
}

#[test]
fn grouped_sqlite_json_seed_preserves_original_text() {
    use diesel::connection::SimpleConnection;
    use diesel::prelude::*;
    use sqlparser::dialect::SQLiteDialect;
    use subql::backend::{SQLite, SqliteJsonStorage};

    let ddl = "CREATE TABLE json_t (id INTEGER PRIMARY KEY, payload JSON);";
    let database = ParserDB::parse::<SQLiteDialect>(ddl).unwrap();
    let mut engine: SubscriptionEngine<TestEvent<SQLite>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, SQLiteDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT payload, COUNT(*) FROM json_t GROUP BY payload",
        ))
        .unwrap();
    let bootstrap = registered
        .served()
        .unwrap()
        .aggregate_bootstrap
        .as_ref()
        .unwrap();

    let mut connection = SqliteConnection::establish(":memory:").unwrap();
    connection.batch_execute(ddl).unwrap();
    let raw = String::from("{ \"a\": 1 }");
    diesel::insert_into(json_t::table)
        .values((json_t::id.eq(1), json_t::payload.eq(&raw)))
        .execute(&mut connection)
        .unwrap();
    let connector: DieselConnector<SqliteConnection, SQLite> = DieselConnector::new(connection);
    let (seed_values, _) = connector
        .execute_scalar_row(
            &subql::reexec::ReadQuery::without_binds(&bootstrap.sql),
            &bootstrap.kinds,
            &(),
        )
        .unwrap();
    let Value::Json(value) = &seed_values[0] else {
        panic!("the JSON group keeps its backend value")
    };
    assert_eq!(value.storage(), &SqliteJsonStorage::Text(raw));
}

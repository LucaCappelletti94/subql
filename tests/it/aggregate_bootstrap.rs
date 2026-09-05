//! Bootstrap and reset path for in-process delta aggregators.
//!
//! `Served::aggregate_bootstrap` bundles a runnable component seed
//! query per `AggSpec` with its decode kinds, and
//! `Install<AggregateSeedInstall>` decodes the returned component
//! row into the running total the engine holds. Together they let a caller
//! start an in-process aggregate and start it over after the resets subql
//! mandates (a permission change, through `reset_aggregate_value`), the same
//! courtesy `Tier::Scalar` already gives `MIN`/`MAX`.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};
use subql::backend::{BuiltinKind, MySql, Postgres, Value};
use subql::testing::TestEvent;
use subql::{AggValue, AggregateBootstrap, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, amount INT, status TEXT);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine() -> Engine {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    SubscriptionEngine::new(db, PostgreSqlDialect {})
}

fn bootstrap_of(sql: &str) -> Option<AggregateBootstrap> {
    engine()
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, sql))
        .unwrap()
        .served()
        .expect("the engine maintains this one in process")
        .aggregate_bootstrap
        .clone()
}

/// Register `sql`, hand it `row` as its starting numbers, and answer with the
/// value it then holds. Nothing has been folded against it, so the read cannot
/// have raced a change and needs no stream position.
fn seeded_value(sql: &str, row: &[Value<Postgres>]) -> AggValue {
    let mut engine = engine();
    let registered = engine
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, sql))
        .unwrap();
    let updates = subql::Install::install(
        &mut engine,
        registered.subscription_id,
        subql::AggregateSeedInstall {
            rows: vec![row.to_vec()],
            read_at: None,
        },
    )
    .expect("the starting numbers land");
    assert_eq!(updates.len(), 1);
    updates[0]
        .folded_value()
        .expect("ungrouped install sets a value")
}

#[test]
fn bootstrap_sql_per_aggspec() {
    // (subscription SQL, expected component seed SQL).
    let cases = [
        ("SELECT COUNT(*) FROM t", "SELECT COUNT(*) AS c0 FROM t"),
        (
            "SELECT COUNT(amount) FROM t",
            "SELECT COUNT(amount) AS c0 FROM t",
        ),
        (
            "SELECT SUM(amount) FROM t",
            // `SUM` reads its contribution count too, since a sum over no
            // contributing row is NULL on every engine rather than zero.
            "SELECT SUM(amount) AS c0, COUNT(amount) AS c1 FROM t",
        ),
        (
            "SELECT AVG(amount) FROM t",
            "SELECT SUM(amount) AS c0, COUNT(amount) AS c1 FROM t",
        ),
        (
            "SELECT VAR_POP(amount) FROM t",
            "SELECT SUM(amount) AS c0, SUM(amount * 1.0 * amount) AS c1, COUNT(amount) AS c2 FROM t",
        ),
        (
            "SELECT VAR_SAMP(amount) FROM t",
            "SELECT SUM(amount) AS c0, SUM(amount * 1.0 * amount) AS c1, COUNT(amount) AS c2 FROM t",
        ),
        (
            "SELECT STDDEV_POP(amount) FROM t",
            "SELECT SUM(amount) AS c0, SUM(amount * 1.0 * amount) AS c1, COUNT(amount) AS c2 FROM t",
        ),
        (
            "SELECT STDDEV_SAMP(amount) FROM t",
            "SELECT SUM(amount) AS c0, SUM(amount * 1.0 * amount) AS c1, COUNT(amount) AS c2 FROM t",
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(
            bootstrap_of(sql)
                .map(|b| b.query.sql().to_string())
                .as_deref(),
            Some(expected),
            "bootstrap SQL mismatch for `{sql}`"
        );
    }
}

#[test]
fn bootstrap_sql_preserves_where() {
    assert_eq!(
        bootstrap_of("SELECT SUM(amount) FROM t WHERE amount > 10")
            .map(|b| b.query.sql().to_string())
            .as_deref(),
        Some("SELECT SUM(amount) AS c0, COUNT(amount) AS c1 FROM t WHERE amount > 10"),
    );
    assert_eq!(
        bootstrap_of("SELECT COUNT(*) FROM t WHERE status = 'open'")
            .map(|b| b.query.sql().to_string())
            .as_deref(),
        Some("SELECT COUNT(*) AS c0 FROM t WHERE status = 'open'"),
    );
}

#[test]
fn aggregate_bootstrap_carries_registration_binds() {
    let bootstrap = engine()
        .register(
            SubscriptionRequest::new(1u64, "SELECT SUM(amount) FROM t WHERE amount > $1")
                .binds(vec![Value::Int(10)]),
        )
        .expect("aggregate registers")
        .served()
        .expect("aggregate is maintained in process")
        .aggregate_bootstrap
        .clone()
        .expect("aggregate has a bootstrap");
    assert_eq!(
        bootstrap.query.sql(),
        "SELECT SUM(amount) AS c0, COUNT(amount) AS c1 FROM t WHERE amount > $1"
    );
    assert_eq!(bootstrap.query.binds(), &[Value::Int(10)]);
}

/// The per-column decode kinds are a pure function of the AggSpec and line
/// up one-to-one with the seed SQL columns.
#[test]
fn bootstrap_kinds_per_aggspec() {
    let int = BuiltinKind::Int;
    let float = BuiltinKind::Float;
    let cases: [(&str, Vec<BuiltinKind>); 8] = [
        ("SELECT COUNT(*) FROM t", vec![int]),
        ("SELECT COUNT(amount) FROM t", vec![int]),
        ("SELECT SUM(amount) FROM t", vec![float, int]),
        ("SELECT AVG(amount) FROM t", vec![float, int]),
        ("SELECT VAR_POP(amount) FROM t", vec![float, float, int]),
        ("SELECT VAR_SAMP(amount) FROM t", vec![float, float, int]),
        ("SELECT STDDEV_POP(amount) FROM t", vec![float, float, int]),
        ("SELECT STDDEV_SAMP(amount) FROM t", vec![float, float, int]),
    ];
    for (sql, expected) in cases {
        let bundle = bootstrap_of(sql).expect("aggregate registration has a bootstrap");
        assert_eq!(bundle.kinds, expected, "kinds mismatch for `{sql}`");
        let column_count = bundle.query.sql().matches(" AS c").count();
        assert_eq!(
            bundle.kinds.len(),
            column_count,
            "kinds length must match seed column count for `{sql}`"
        );
    }
}

#[test]
fn bootstrap_sql_is_row_subscription_safe() {
    assert_eq!(bootstrap_of("SELECT * FROM t WHERE amount > 1"), None);
    assert_eq!(bootstrap_of("SELECT * FROM t"), None);
}

#[test]
fn a_seed_row_decodes_into_the_value_it_describes() {
    // COUNT family: single `c` component.
    assert_eq!(
        seeded_value("SELECT COUNT(*) FROM t", &[Value::Int(5)]),
        AggValue::Count(5),
    );
    assert_eq!(
        seeded_value("SELECT COUNT(amount) FROM t", &[Value::Int(3)]),
        AggValue::Count(3),
    );
    // SUM: `(s, c)` components, from an integer or float column.
    assert_eq!(
        seeded_value(
            "SELECT SUM(amount) FROM t",
            &[Value::Int(10), Value::Int(1)]
        ),
        AggValue::Sum(Some(10.0)),
    );
    assert_eq!(
        seeded_value(
            "SELECT SUM(amount) FROM t",
            &[Value::Float(2.5), Value::Int(2)]
        ),
        AggValue::Sum(Some(2.5)),
    );
    // AVG: `(s, c)` components.
    assert_eq!(
        seeded_value(
            "SELECT AVG(amount) FROM t",
            &[Value::Float(10.0), Value::Int(4)],
        ),
        AggValue::Real(Some(2.5)),
    );
    // VAR_POP: `(s, sq, c)`. amounts [2, 4, 6] -> sum=12, sum_sq=56, n=3.
    // var_pop = 56/3 - (12/3)^2 = 2.6666666666666665.
    assert_eq!(
        seeded_value(
            "SELECT VAR_POP(amount) FROM t",
            &[Value::Float(12.0), Value::Float(56.0), Value::Int(3)],
        ),
        AggValue::Real(Some(56.0 / 3.0 - 16.0)),
    );
    // STDDEV_POP over the same components is sqrt(var_pop).
    assert_eq!(
        seeded_value(
            "SELECT STDDEV_POP(amount) FROM t",
            &[Value::Float(12.0), Value::Float(56.0), Value::Int(3)],
        ),
        AggValue::Real(Some((56.0f64 / 3.0 - 16.0).sqrt())),
    );
}

#[test]
fn a_seed_over_an_empty_table_is_the_empty_value() {
    // Zero matching rows: COUNT returns 0, SUM/variance components are NULL.
    assert_eq!(
        seeded_value("SELECT COUNT(*) FROM t", &[Value::Int(0)]),
        AggValue::Count(0),
    );
    assert_eq!(
        seeded_value("SELECT SUM(amount) FROM t", &[Value::Null, Value::Int(0)]),
        AggValue::Sum(None),
    );
    assert_eq!(
        seeded_value("SELECT AVG(amount) FROM t", &[Value::Null, Value::Int(0)],),
        AggValue::Real(None),
    );
    assert_eq!(
        seeded_value(
            "SELECT VAR_POP(amount) FROM t",
            &[Value::Null, Value::Null, Value::Int(0)],
        ),
        AggValue::Real(None),
    );
}

/// The reset contract is actually runnable: seeding again from the bootstrap
/// components computed over the current table equals a direct recompute.
#[test]
fn reseed_matches_recompute() {
    // A registration must expose runnable bootstrap SQL to seed again after a
    // reset for a permission change.
    assert!(bootstrap_of("SELECT AVG(amount) FROM t").is_some());

    let mut engine = engine();
    let registered = engine
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(
            1u64,
            "SELECT AVG(amount) FROM t",
        ))
        .unwrap();
    let subscription = registered.subscription_id;
    subql::Install::install(
        &mut engine,
        subscription,
        subql::AggregateSeedInstall {
            rows: vec![vec![Value::Float(3.0), Value::Int(1)]],
            read_at: None,
        },
    )
    .expect("the first numbers land");

    // A permission change moved the answer without any event saying so, so the
    // caller resets and reads again. Current table amounts: [2, 4, 6].
    assert!(engine.reset_aggregate_value(subscription));
    let updates = subql::Install::install(
        &mut engine,
        subscription,
        subql::AggregateSeedInstall {
            rows: vec![vec![Value::Float(12.0), Value::Int(3)]],
            read_at: None,
        },
    )
    .expect("the new starting numbers land");
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].folded_value(), Some(AggValue::Real(Some(4.0))));
}

/// A table whose group column name carries the dialect's own identifier
/// delimiter. Legal in Postgres, where a delimited identifier escapes its
/// delimiter by doubling it, so the column below is named `a"b`.
const QUOTED_DDL: &str =
    "CREATE TABLE t (\"a\"\"b\" INT NOT NULL, amount INT, PRIMARY KEY (\"a\"\"b\"));";

/// The MySQL peer of [`QUOTED_DDL`], where the delimiter is a backtick, so the
/// column below is named ``a`b``.
const QUOTED_DDL_MYSQL: &str =
    "CREATE TABLE t (`a``b` INT NOT NULL, amount INT, PRIMARY KEY (`a``b`));";

/// The grouped registration both quoted tables are asked about. A grouped
/// aggregate must project its group columns, so the group column appears twice.
const QUOTED_GROUPED_SQL: &str = "SELECT \"a\"\"b\", SUM(amount) FROM t GROUP BY \"a\"\"b\"";

const QUOTED_GROUPED_SQL_MYSQL: &str = "SELECT `a``b`, SUM(amount) FROM t GROUP BY `a``b`";

fn quoted_bootstrap(ddl: &str, sql: &str) -> Option<AggregateBootstrap> {
    let db = ParserDB::parse::<PostgreSqlDialect>(ddl).unwrap();
    let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, sql))
        .unwrap()
        .served()
        .expect("the engine maintains this one in process")
        .aggregate_bootstrap
        .clone()
}

/// The value of the first projected column of `sql`, which must be a bare
/// identifier. Reads the delimiter back off the parse rather than off the text,
/// so a wrongly escaped name shows up as a parse failure or a different value
/// rather than as a string that merely looks right.
fn first_projected_ident(dialect: &dyn sqlparser::dialect::Dialect, sql: &str) -> String {
    use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};

    let mut parsed = sqlparser::parser::Parser::parse_sql(dialect, sql)
        .unwrap_or_else(|e| panic!("the seed query must re-parse, got {e} for `{sql}`"));
    assert_eq!(parsed.len(), 1, "one statement");
    let Statement::Query(query) = parsed.remove(0) else {
        panic!("the seed query is a SELECT");
    };
    let SetExpr::Select(select) = *query.body else {
        panic!("the seed query is a plain SELECT");
    };
    match select.projection.first() {
        Some(
            SelectItem::UnnamedExpr(Expr::Identifier(ident))
            | SelectItem::ExprWithAlias {
                expr: Expr::Identifier(ident),
                ..
            },
        ) => ident.value.clone(),
        other => panic!("expected an identifier first, got {other:?}"),
    }
}

/// A group column whose name carries the delimiter must still produce a seed
/// query.
///
/// It did not. The seed projection quoted a catalog-supplied name by wrapping
/// it in the delimiter without doubling an embedded one, the result failed to
/// re-parse, and the failure was swallowed into the same `None` that means "this
/// is not an aggregate". The registration then succeeded reporting
/// `Tier::InProcess` while the consumer had no way to seed the accumulator.
#[test]
fn a_group_column_carrying_a_quote_still_seeds() {
    assert!(
        quoted_bootstrap(QUOTED_DDL, QUOTED_GROUPED_SQL).is_some(),
        "a grouped aggregate over a delimiter-carrying column must seed"
    );
}

/// The seeded group column names the column, delimiter intact.
///
/// The peer of the test above, and the one that pins the escaping rather than
/// merely the absence of `None`: dropping the group column from the projection
/// would also make a seed that re-parses.
#[test]
fn a_seeded_group_column_reparses() {
    let bootstrap =
        quoted_bootstrap(QUOTED_DDL, QUOTED_GROUPED_SQL).expect("a grouped aggregate seeds");
    assert_eq!(
        first_projected_ident(&PostgreSqlDialect {}, bootstrap.query.sql()),
        "a\"b",
        "seed SQL was `{}`",
        bootstrap.query.sql()
    );
}

/// The same, on a dialect whose delimiter is not a double quote, so the fix
/// cannot be hardcoded to `"`.
#[test]
fn a_group_column_carrying_a_backtick_still_seeds_on_mysql() {
    type MysqlEngine = SubscriptionEngine<TestEvent<MySql>, DefaultIds, ParserDB>;

    let db = ParserDB::parse::<MySqlDialect>(QUOTED_DDL_MYSQL).unwrap();
    let mut engine: MysqlEngine = SubscriptionEngine::new(db, MySqlDialect {});
    let bootstrap = engine
        .register(SubscriptionRequest::<DefaultIds, MySql>::new(
            1u64,
            QUOTED_GROUPED_SQL_MYSQL,
        ))
        .unwrap()
        .served()
        .expect("the engine maintains this one in process")
        .aggregate_bootstrap
        .clone()
        .expect("a grouped aggregate seeds");
    assert_eq!(
        first_projected_ident(&MySqlDialect {}, bootstrap.query.sql()),
        "a`b",
        "seed SQL was `{}`",
        bootstrap.query.sql()
    );
}

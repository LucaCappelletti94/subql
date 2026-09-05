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

use bigdecimal::BigDecimal;
use core::str::FromStr as _;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};
use subql::backend::{BuiltinKind, MySql, Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    AggValue, AggregateBootstrap, DefaultIds, NumericValue, SubscriptionEngine, SubscriptionRequest,
};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, amount INT, status TEXT, \
                   big BIGINT);";

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
            "SELECT SUM(amount) AS c0, VAR_POP(amount) * COUNT(amount) AS c1, \
             COUNT(amount) AS c2 FROM t",
        ),
        (
            "SELECT VAR_SAMP(amount) FROM t",
            "SELECT SUM(amount) AS c0, VAR_POP(amount) * COUNT(amount) AS c1, \
             COUNT(amount) AS c2 FROM t",
        ),
        (
            "SELECT STDDEV_POP(amount) FROM t",
            "SELECT SUM(amount) AS c0, VAR_POP(amount) * COUNT(amount) AS c1, \
             COUNT(amount) AS c2 FROM t",
        ),
        (
            "SELECT STDDEV_SAMP(amount) FROM t",
            "SELECT SUM(amount) AS c0, VAR_POP(amount) * COUNT(amount) AS c1, \
             COUNT(amount) AS c2 FROM t",
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
    let decimal = BuiltinKind::Decimal;
    let cases: [(&str, Vec<BuiltinKind>); 10] = [
        ("SELECT COUNT(*) FROM t", vec![int]),
        ("SELECT COUNT(amount) FROM t", vec![int]),
        // `amount` is an `INT`, whose sum is a `bigint` on Postgres, so the
        // total component decodes exactly rather than as a double.
        ("SELECT SUM(amount) FROM t", vec![int, int]),
        // `AVG` holds the same exact total `SUM` does, so its total
        // component decodes exactly too.
        ("SELECT AVG(amount) FROM t", vec![int, int]),
        // The variance family's first component is `SUM(amount)` too, so
        // it decodes in the type the engine sums into, exactly as `SUM`'s
        // own does. Only the middle component, the sum of squared
        // deviations, is nobody's exact answer and stays a double.
        ("SELECT VAR_POP(amount) FROM t", vec![int, float, int]),
        ("SELECT VAR_SAMP(amount) FROM t", vec![int, float, int]),
        ("SELECT STDDEV_POP(amount) FROM t", vec![int, float, int]),
        ("SELECT STDDEV_SAMP(amount) FROM t", vec![int, float, int]),
        // `big` is a `BIGINT`, whose sum PostgreSQL answers as `numeric`,
        // so the total decodes as a decimal rather than an integer.
        ("SELECT SUM(big) FROM t", vec![decimal, int]),
        ("SELECT VAR_POP(big) FROM t", vec![decimal, float, int]),
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
        AggValue::CountStar(5),
    );
    assert_eq!(
        seeded_value("SELECT COUNT(amount) FROM t", &[Value::Int(3)]),
        AggValue::CountColumn(3),
    );
    // SUM: `(s, c)` components. The total decodes in the type the engine
    // sums into, which for this `INT` column is a `bigint`.
    assert_eq!(
        seeded_value(
            "SELECT SUM(amount) FROM t",
            &[Value::Int(10), Value::Int(1)]
        ),
        AggValue::Sum(Some(NumericValue::Integer(10))),
    );
    assert_eq!(
        seeded_value(
            "SELECT SUM(amount) FROM t",
            &[Value::Int(-4), Value::Int(2)]
        ),
        AggValue::Sum(Some(NumericValue::Integer(-4))),
    );
    // AVG: `(s, c)` components.
    assert_eq!(
        seeded_value(
            "SELECT AVG(amount) FROM t",
            &[Value::Int(10), Value::Int(4)],
        ),
        // A mean is PostgreSQL's own numeric division of the total by the
        // count: sixteen significant digits.
        AggValue::Avg(Some(NumericValue::Decimal(
            BigDecimal::from_str("2.5000000000000000").unwrap()
        ))),
    );
    // VAR_POP: `(sum, squared_deviations, count)`. Amounts [2, 4, 6] give
    // sum 12, deviations 8 (4 + 0 + 4) and n 3, which is what the server
    // hands back as `var_pop(x) * count(x)`.
    assert_eq!(
        seeded_value(
            "SELECT VAR_POP(amount) FROM t",
            &[Value::Float(12.0), Value::Float(8.0), Value::Int(3)],
        ),
        AggValue::VarPop(Some(8.0 / 3.0)),
    );
    // STDDEV_POP over the same components is its square root.
    assert_eq!(
        seeded_value(
            "SELECT STDDEV_POP(amount) FROM t",
            &[Value::Float(12.0), Value::Float(8.0), Value::Int(3)],
        ),
        AggValue::StddevPop(Some((8.0f64 / 3.0).sqrt())),
    );
}

#[test]
fn a_seed_over_an_empty_table_is_the_empty_value() {
    // Zero matching rows: COUNT returns 0, SUM/variance components are NULL.
    assert_eq!(
        seeded_value("SELECT COUNT(*) FROM t", &[Value::Int(0)]),
        AggValue::CountStar(0),
    );
    assert_eq!(
        seeded_value("SELECT SUM(amount) FROM t", &[Value::Null, Value::Int(0)]),
        AggValue::Sum(None),
    );
    assert_eq!(
        seeded_value("SELECT AVG(amount) FROM t", &[Value::Null, Value::Int(0)],),
        AggValue::Avg(None),
    );
    assert_eq!(
        seeded_value(
            "SELECT VAR_POP(amount) FROM t",
            &[Value::Null, Value::Null, Value::Int(0)],
        ),
        AggValue::VarPop(None),
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
            rows: vec![vec![Value::Int(3), Value::Int(1)]],
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
            rows: vec![vec![Value::Int(12), Value::Int(3)]],
            read_at: None,
        },
    )
    .expect("the new starting numbers land");
    assert_eq!(updates.len(), 1);
    assert_eq!(
        updates[0].folded_value(),
        Some(AggValue::Avg(Some(NumericValue::Decimal(
            BigDecimal::from_str("4.0000000000000000").unwrap()
        ))))
    );
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

/// A widened seed's first component is `SUM(arg)` whatever the projected
/// function is, so its declared kind is the total's and not the spec's.
///
/// A sibling `HAVING` that reads what the projected function does not
/// maintain widens the seed to `[SUM(arg), squared deviations,
/// COUNT(arg)]` for every spec. The declared kinds were taken from the
/// first entry of the *spec's* own list, which is `Int` for a `COUNT` and
/// `Float` for the variance family, so a widened seed asked the database
/// for the wrong type and then handed the cell to an accumulator that
/// ignores it.
///
/// `big` is a `BIGINT`, whose sum PostgreSQL answers as `numeric`, which
/// is what separates the total's kind from the count's own `Int`.
#[test]
fn a_widened_seed_declares_the_kind_its_total_decodes_in() {
    let string = BuiltinKind::String;
    let int = BuiltinKind::Int;
    let float = BuiltinKind::Float;
    let decimal = BuiltinKind::Decimal;

    for (sql, expected) in [
        (
            "SELECT status, COUNT(big) FROM t GROUP BY status HAVING SUM(big) > 10",
            vec![string, decimal, float, int, int],
        ),
        (
            "SELECT status, VAR_POP(amount) FROM t GROUP BY status HAVING SUM(amount) > 10",
            vec![string, int, float, int, int],
        ),
        (
            "SELECT status, COUNT(amount) FROM t GROUP BY status HAVING SUM(amount) > 10",
            vec![string, int, float, int, int],
        ),
    ] {
        let bundle = bootstrap_of(sql).expect("the widened aggregate has a bootstrap");
        assert!(
            bundle.query.sql().contains("SUM("),
            "a widened seed reads a total: {}",
            bundle.query.sql()
        );
        assert_eq!(bundle.kinds, expected, "kinds mismatch for `{sql}`");
        assert_eq!(
            bundle.kinds.len(),
            bundle.query.sql().matches(" AS c").count(),
            "one kind per seed column for `{sql}`"
        );
    }
}

/// And the kinds are load-bearing: a total seeded through the cell its
/// declared kind describes holds the number the seed carried.
///
/// This is the silent zero. `Total::Integer::seed` matches only
/// `Value::Int`, so a total declared as a float arrives as `Value::Float`,
/// is ignored, and stays at zero. The `HAVING` then reads zero and the
/// group never crosses into the result, with no error anywhere.
#[test]
fn a_widened_group_passes_its_having_on_the_seeded_total() {
    let sql = "SELECT status, VAR_POP(amount) FROM t GROUP BY status HAVING SUM(amount) > 10";
    let mut engine = engine();
    let registered = engine
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, sql))
        .expect("the widened aggregate registers");
    let bundle = registered
        .served()
        .expect("it is maintained in process")
        .aggregate_bootstrap
        .clone()
        .expect("it has a bootstrap");

    // The row a connector honouring those declared kinds would return for
    // one group of two rows summing to 100.
    let row: Vec<Value<Postgres>> = bundle
        .kinds
        .iter()
        .enumerate()
        .map(|(slot, kind)| match (slot, kind) {
            (0, _) => Value::String("open".to_string()),
            (1, BuiltinKind::Int) => Value::Int(100),
            (1, BuiltinKind::Decimal) => {
                Value::Decimal(BigDecimal::from_str("100").expect("100 parses"))
            }
            (1, _) => Value::Float(100.0),
            (2, _) => Value::Float(50.0),
            _ => Value::Int(2),
        })
        .collect();

    let updates = subql::Install::install(
        &mut engine,
        registered.subscription_id,
        subql::AggregateSeedInstall {
            rows: vec![row],
            read_at: None,
        },
    )
    .expect("the starting numbers land");

    assert_eq!(
        updates.len(),
        1,
        "the group sums to 100, so it passes `SUM(amount) > 10` and is reported; \
         a total left at zero fails the condition and reports nothing"
    );
}

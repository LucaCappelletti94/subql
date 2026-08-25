//! A subscription is the rows of one table that satisfy one WHERE clause,
//! delivered as they change. Every other part of a `SELECT` asks a question
//! about rows the change event does not carry, and subql used to accept those
//! statements and then answer the reduced query instead, twice over: delta
//! maintenance ignored the clause while the generated seed SQL kept it, so the
//! seed and the live stream answered different questions.
//!
//! These tests pin the refusal, at the surface a caller registers through.
//! Each refused clause is a future capture: the re-execution wrapper triggers
//! on `RegisterError::UnsupportedSql`, so a clause the core engine accepts is
//! a clause no tier above it can ever serve.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};
use subql::backend::{MySql, Postgres};
use subql::testing::TestEvent;
use subql::{DefaultIds, RegisterError, Registered, SubscriptionEngine, SubscriptionRequest, Tier};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, status TEXT, amount INT); \
     CREATE TABLE m (id INT PRIMARY KEY, t_id INT, owner TEXT); \
     CREATE TABLE g (id INT PRIMARY KEY, status TEXT, amount INT, price DOUBLE PRECISION, \
     paid NUMERIC, doc JSON, at TIMESTAMP);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;
type Wrapper = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine() -> Engine {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    SubscriptionEngine::new(db, PostgreSqlDialect {})
}

/// Register `sql` and return why it is not served in process, failing loudly
/// on anything else. A clause outside the served shape is not turned away any
/// more: it lands on a tier that re-reads the caller's own statement, which
/// honours the clause the in-process evaluator would have dropped.
fn refusal(sql: &str) -> String {
    match engine().register(SubscriptionRequest::new(1u64, sql)) {
        Ok(Registered {
            tier: Tier::InProcess(served),
            ..
        }) => panic!("{sql} should not be served in process, got {served:?}"),
        Ok(registered) => registered
            .not_served_because
            .expect("a tier that needs a read says why"),
        Err(RegisterError::UnsupportedSql(message)) => message,
        Err(other) => panic!("{sql} should land on a read tier, got {other:?}"),
    }
}

/// The refusal names the clause the caller wrote, so the message is actionable
/// without reading subql's source.
fn refused_naming(sql: &str, clause: &str) {
    let message = refusal(sql);
    assert!(
        message.contains(clause),
        "the refusal of {sql} should name {clause}, got {message:?}"
    );
}

/// A grouped aggregate over columns that identify a group is served as a
/// grouped fold rather than refused.
///
/// The clause used to be refused outright because delta maintenance ignored it
/// while the seed SQL kept it, so the two answered different questions. It is
/// served now, and the shape is exactly the one the fold can maintain: the
/// group columns projected alongside one aggregate, grouped by those same
/// columns.
#[test]
fn a_grouped_aggregate_over_groupable_columns_is_served() {
    for sql in [
        "SELECT status, COUNT(*) FROM g GROUP BY status",
        "SELECT status, SUM(amount) FROM g GROUP BY status",
        "SELECT status, id, COUNT(*) FROM g GROUP BY status, id",
        "SELECT at, AVG(amount) FROM g GROUP BY at",
        "SELECT status, COUNT(*) FROM g WHERE amount > 3 GROUP BY status",
    ] {
        let result = engine().register(SubscriptionRequest::new(1u64, sql));
        assert!(
            result.is_ok(),
            "{sql} should register as a grouped fold, got {result:?}"
        );
    }
}

/// `HAVING` off the fast path stays refused: without a grouped fold to check
/// it against, the seed and the live stream would answer different questions.
/// The served fast-path shape is pinned in `tests/grouped_having.rs`.
#[test]
fn having_outside_the_fast_path_is_refused() {
    refused_naming("SELECT COUNT(*) FROM t HAVING COUNT(*) > 3", "HAVING");
    refused_naming(
        "SELECT status, COUNT(*) FROM g GROUP BY status HAVING SUM(price) > 3",
        "HAVING",
    );
}

/// A group column whose values the database groups together while their
/// encoding separates them cannot identify a group.
///
/// Measured: Postgres puts `0.0` with `-0.0` and `1.0::numeric` with
/// `1.00::numeric` in one group, and json documents differing only in
/// whitespace or key order are one value. The fold decides a group by encoding
/// the value, so it would seed one group and then open a second from zero,
/// leaving both totals wrong with nothing failing. Refused, so the query is
/// served by re-reading it instead.
#[test]
fn a_group_column_that_cannot_identify_a_group_is_refused() {
    for sql in [
        "SELECT price, COUNT(*) FROM g GROUP BY price",
        "SELECT paid, COUNT(*) FROM g GROUP BY paid",
        "SELECT doc, COUNT(*) FROM g GROUP BY doc",
    ] {
        let message = refusal(sql);
        assert!(
            message.contains("group"),
            "the refusal of {sql} should say the column cannot identify a group, got {message:?}"
        );
    }
}

/// Only the shape the fold can maintain is served. Everything else is a
/// question about rows the fold does not hold, so it is refused here and
/// re-read by the tier above.
#[test]
fn a_grouped_shape_the_fold_cannot_maintain_is_refused() {
    // Grouped by an expression rather than a bare column: the changed row
    // cannot name its own group without evaluating it.
    refusal("SELECT lower(status), COUNT(*) FROM g GROUP BY lower(status)");
    // `GROUP BY ALL` names its groups by position in the projection.
    refusal("SELECT status, COUNT(*) FROM g GROUP BY ALL");
    // Projects a column that is not grouped by.
    refusal("SELECT amount, COUNT(*) FROM g GROUP BY status");
    // Groups by a column the projection does not carry, so a delivered value
    // could not be attributed to anything the caller asked for.
    refusal("SELECT COUNT(*) FROM g GROUP BY status");
    // Two aggregates: the fold maintains one accumulator per group.
    refusal("SELECT status, COUNT(*), SUM(amount) FROM g GROUP BY status");
    // An aggregate outside the accumulable family.
    refusal("SELECT status, MIN(amount) FROM g GROUP BY status");
}

/// The same clauses on a row subscription. Nothing about the projection makes
/// the clause servable, so the refusal is the statement's, not the aggregate's.
#[test]
fn a_grouped_or_filtered_row_subscription_is_refused() {
    refused_naming("SELECT * FROM t GROUP BY status", "GROUP BY");
    refused_naming("SELECT * FROM t GROUP BY id", "GROUP BY");
    refused_naming("SELECT * FROM t HAVING COUNT(*) > 3", "HAVING");
}

/// `DISTINCT` collapses rows the subscription delivers one by one, so a
/// duplicate row was delivered where the query asked for one. `COUNT(DISTINCT
/// x)` was already refused inside the projection; the statement-level spelling
/// was not.
#[test]
fn distinct_is_refused() {
    refused_naming("SELECT DISTINCT * FROM t", "DISTINCT");
    refused_naming("SELECT DISTINCT id, status, amount FROM t", "DISTINCT");
}

/// A bound on how many rows come back is a question about the other rows, and
/// a change event carries one row.
#[test]
fn a_row_bound_is_refused() {
    refused_naming("SELECT * FROM t LIMIT 5", "LIMIT");
    refused_naming("SELECT * FROM t OFFSET 2", "OFFSET");
    refused_naming("SELECT * FROM t LIMIT 5 OFFSET 2", "LIMIT");
    refused_naming("SELECT * FROM t FETCH FIRST 5 ROWS ONLY", "FETCH");
    refused_naming("SELECT COUNT(*) FROM t LIMIT 1", "LIMIT");
}

/// Ordering is the one refused clause that does not change which rows the
/// answer contains. It is refused anyway: a subscription that accepted it
/// would drop it, and the re-execution tier that will serve it needs the core
/// engine to decline it first.
#[test]
fn ordering_is_refused() {
    refused_naming("SELECT * FROM t ORDER BY id", "ORDER BY");
    refused_naming("SELECT * FROM t ORDER BY status DESC, id", "ORDER BY");
}

/// The worst of the family: the CTE shadows the catalog table, so the table
/// name resolved to the real `t` and the stream delivered rows from a relation
/// the statement does not read, while the generated seed SQL kept the `WITH`
/// and read the CTE. Two different answers from one registration.
#[test]
fn a_common_table_expression_is_refused() {
    refused_naming("WITH x AS (SELECT 1) SELECT * FROM t", "WITH");
    refused_naming(
        "WITH t AS (SELECT 1 AS id, 'x' AS status, 2 AS amount) SELECT * FROM t",
        "WITH",
    );
    refused_naming("WITH t AS (SELECT 1 AS id) SELECT COUNT(*) FROM t", "WITH");
}

/// Clauses that ask the database for something a standing subscription has no
/// transaction, cursor, or output encoding to give.
#[test]
fn a_clause_the_subscription_cannot_honour_is_refused() {
    refused_naming("SELECT * FROM t FOR UPDATE", "FOR UPDATE");
    refused_naming(
        "SELECT * FROM t WINDOW w AS (PARTITION BY status)",
        "WINDOW",
    );
    refused_naming("SELECT * INTO other FROM t", "INTO");
}

/// The served shape itself, so the refusals above are not a blanket one. The
/// last two spell out a default rather than asking for anything: `ALL` asks for
/// every row with duplicates kept, and `LIMIT ALL` asks for no bound, which is
/// exactly what a subscription delivers.
#[test]
fn the_served_shape_still_registers() {
    for sql in [
        "SELECT * FROM t",
        "SELECT * FROM t WHERE amount > 3",
        "SELECT id, status, amount FROM t",
        "SELECT COUNT(*) FROM t",
        "SELECT COUNT(*) FROM t WHERE status = 'a'",
        "SELECT SUM(amount) FROM t WHERE amount > 3",
        "SELECT COUNT(*) AS n FROM t",
        "SELECT ALL * FROM t",
        "SELECT * FROM t LIMIT ALL",
    ] {
        let outcome = engine().register(SubscriptionRequest::new(1u64, sql));
        assert!(outcome.is_ok(), "{sql} should register, got {outcome:?}");
    }
}

/// The seed SQL a caller runs to bootstrap an aggregate is rendered from the
/// statement verbatim, so it can only stay faithful to the maintained state
/// while the statement carries nothing the maintenance drops.
#[test]
fn an_accepted_aggregate_seeds_from_the_statement_it_maintains() {
    let report = engine()
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT COUNT(*) FROM t WHERE status = 'a'",
        ))
        .unwrap();
    let bootstrap = report
        .served()
        .expect("the engine maintains this one in process")
        .aggregate_bootstrap
        .clone()
        .expect("an aggregate seeds");
    assert_eq!(
        bootstrap.sql,
        "SELECT COUNT(*) AS c0 FROM t WHERE status = 'a'"
    );
}

/// A grouped aggregate's seed keeps the grouping and projects the group
/// columns ahead of the components, one row per group.
///
/// This is the half that made the clause a defect before it was served: the
/// seed SQL kept the `GROUP BY` while the maintenance ignored it, so the two
/// answered different questions. They agree now only because the seed says
/// which group each row is, and `group_columns` is how a caller knows where
/// the group values stop and the components begin.
#[test]
fn a_grouped_aggregate_seeds_one_row_per_group() {
    let report = engine()
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT status, COUNT(*) FROM g WHERE amount > 3 GROUP BY status",
        ))
        .unwrap();
    let bootstrap = report
        .served()
        .expect("the engine maintains this one in process")
        .aggregate_bootstrap
        .clone()
        .expect("a grouped aggregate seeds");
    assert_eq!(
        bootstrap.sql,
        "SELECT \"status\", COUNT(*) AS c0, COUNT(*) AS c1 FROM g WHERE amount > 3 GROUP BY status"
    );
    assert_eq!(bootstrap.group_columns, 1);
    assert_eq!(
        bootstrap.kinds.len(),
        3,
        "one group column, one component, one source-row count"
    );

    // Two group columns lead in GROUP BY order, which is the order a group's
    // key encodes in, so a seeded group lines up with a later change's.
    let report = engine()
        .register(SubscriptionRequest::new(
            2u64,
            "SELECT id, status, SUM(amount) FROM g GROUP BY status, id",
        ))
        .unwrap();
    let bootstrap = report
        .served()
        .expect("the engine maintains this one in process")
        .aggregate_bootstrap
        .clone()
        .expect("a grouped aggregate seeds");
    assert_eq!(
        bootstrap.sql,
        "SELECT \"status\", \"id\", SUM(amount) AS c0, COUNT(*) AS c1 FROM g GROUP BY status, id"
    );
    assert_eq!(bootstrap.group_columns, 2);
}

/// A dropped clause must never ride a *scalar* re-execution. That would make
/// the re-run answer a different query than the one being read: `GROUP BY`
/// returns one row per group where a scalar read takes one value.
///
/// These statements are captured rather than refused now, because the
/// whole-re-read tier re-runs them verbatim and delivers every row they ask
/// for, which is correct rather than silently wrong. The invariant this pins is
/// therefore about which tier they land in, not about being rejected: the core
/// engine still refuses them, and the wrapper never treats them as scalars.
#[test]
fn a_dropped_clause_never_rides_a_scalar_reexecution() {
    for sql in [
        "SELECT MIN(amount) FROM t GROUP BY status",
        "SELECT MAX(amount) FROM t HAVING COUNT(*) > 1",
        "SELECT MIN(amount) FROM t ORDER BY amount",
        "SELECT MIN(amount) FROM t LIMIT 1",
    ] {
        // The core engine's refusal is what routes it upward at all.
        refused_naming(sql, "not supported");

        let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
        let mut wrapper: Wrapper = SubscriptionEngine::new(db, PostgreSqlDialect {});
        match wrapper.register(SubscriptionRequest::new(1u64, sql)) {
            Ok(Registered {
                tier: Tier::WholeRows { sql: captured, .. },
                ..
            }) => {
                assert_eq!(captured, sql, "the tier re-reads the statement as written");
            }
            other => panic!("{sql} should be captured for a whole re-read, got {other:?}"),
        }
    }
}

/// The scalar the wrapper does serve stays captured, so the test above pins a
/// clause refusal rather than a broken wrapper.
#[test]
fn the_reexecution_wrapper_still_captures_a_bare_scalar() {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let mut wrapper: Wrapper = SubscriptionEngine::new(db, PostgreSqlDialect {});
    let outcome = wrapper.register(SubscriptionRequest::new(1u64, "SELECT MIN(amount) FROM t"));
    assert!(
        matches!(
            outcome,
            Ok(Registered {
                tier: Tier::Scalar { .. },
                ..
            })
        ),
        "a bare scalar MIN should still be captured, got {outcome:?}"
    );
}

/// The nested query of a membership subquery is bounded by the same rule as
/// the outer statement, stated in one place so the two cannot drift. A `LIMIT`
/// there really does change which rows the subscription delivers, and the
/// refusal is the statement's, not the term planner's, so it reports as
/// unsupported SQL in every build.
#[test]
fn a_clause_inside_a_membership_subquery_is_refused() {
    refused_naming(
        "SELECT * FROM t WHERE id IN (SELECT t_id FROM m LIMIT 1)",
        "LIMIT",
    );
    refused_naming(
        "SELECT * FROM t WHERE id IN (SELECT t_id FROM m GROUP BY t_id)",
        "GROUP BY",
    );
    refused_naming(
        "SELECT * FROM t WHERE id IN (SELECT t_id FROM m ORDER BY t_id)",
        "ORDER BY",
    );
    refused_naming(
        "SELECT * FROM t WHERE id IN (SELECT DISTINCT t_id FROM m)",
        "DISTINCT",
    );
    refused_naming(
        "SELECT * FROM t WHERE id IN (SELECT t_id FROM m HAVING COUNT(*) > 1)",
        "HAVING",
    );
}

/// Three clauses only MySQL spells, reached through the MySQL backend's own
/// dialect. A query hint and a `SQL_NO_CACHE` change nothing about the answer,
/// which is exactly why accepting them would be a promise subql never keeps:
/// they ask the database to plan or cache the read a certain way, and there is
/// no read.
#[test]
fn a_mysql_only_clause_is_refused() {
    type MySqlEngine = SubscriptionEngine<TestEvent<MySql>, DefaultIds, ParserDB>;

    for (sql, clause) in [
        ("SELECT * FROM t LIMIT 2, 5", "LIMIT"),
        ("SELECT SQL_NO_CACHE * FROM t", "a SELECT modifier"),
        ("SELECT HIGH_PRIORITY * FROM t", "a SELECT modifier"),
        (
            "SELECT /*+ MAX_EXECUTION_TIME(1) */ * FROM t",
            "an optimizer hint",
        ),
    ] {
        let db = ParserDB::parse::<MySqlDialect>(DDL).unwrap();
        let mut engine: MySqlEngine = SubscriptionEngine::new(db, MySqlDialect {});
        match engine.register(SubscriptionRequest::new(1u64, sql)) {
            Ok(Registered {
                tier: Tier::InProcess(served),
                ..
            }) => panic!("{sql} should not be served in process, got {served:?}"),
            Ok(registered) => {
                let reason = registered
                    .not_served_because
                    .expect("a tier that needs a read says why");
                assert!(
                    reason.contains(clause),
                    "the reason for {sql} should name {clause}, got {reason:?}"
                );
            }
            Err(other) => panic!("{sql} should land on a read tier, got {other:?}"),
        }
    }
}

/// The MySQL served shape still registers, so the test above is not refusing
/// the dialect itself.
#[test]
fn the_served_shape_still_registers_on_mysql() {
    let db = ParserDB::parse::<MySqlDialect>(DDL).unwrap();
    let mut engine: SubscriptionEngine<TestEvent<MySql>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, MySqlDialect {});
    let outcome = engine.register(SubscriptionRequest::new(
        1u64,
        "SELECT * FROM t WHERE amount > 3",
    ));
    assert!(
        outcome.is_ok(),
        "the served shape should register, got {outcome:?}"
    );
}

/// Grouping by text is served on Postgres and refused on MySQL, because the
/// two databases disagree about when two text values are one group.
///
/// Measured on `mysql:8.0` as this repo's own container starts it: the server
/// collation is `utf8mb4_0900_ai_ci`, so `'a'` and `'A'` are one group, while
/// Postgres and SQLite give two. A fold decides a group by encoding the value,
/// so on MySQL it would seed one group and then open a second from zero and
/// both totals would be wrong. Worse than a per-column setting could fix,
/// since that collation comes from server and table defaults and is absent
/// from the schema text subql parses. Grouping by an integer stays served on
/// both, so this pins the text difference rather than a broken dialect.
#[test]
fn text_grouping_is_served_on_postgres_and_refused_on_mysql() {
    let outcome = engine().register(SubscriptionRequest::new(
        1u64,
        "SELECT status, COUNT(*) FROM g GROUP BY status",
    ));
    assert!(
        outcome.is_ok(),
        "Postgres groups text by bytes, so this is served, got {outcome:?}"
    );

    let db = ParserDB::parse::<MySqlDialect>(DDL).unwrap();
    let mut mysql: SubscriptionEngine<TestEvent<MySql>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, MySqlDialect {});
    match mysql.register(SubscriptionRequest::new(
        1u64,
        "SELECT status, COUNT(*) FROM g GROUP BY status",
    )) {
        Ok(Registered {
            tier: Tier::InProcess(served),
            ..
        }) => panic!("MySQL must not fold a text group in process, got {served:?}"),
        Ok(registered) => {
            let reason = registered
                .not_served_because
                .expect("a tier that needs a read says why");
            assert!(
                reason.contains("group"),
                "the reason should name the group column, got {reason:?}"
            );
        }
        Err(other) => panic!("MySQL should re-read it instead, got {other:?}"),
    }

    let db = ParserDB::parse::<MySqlDialect>(DDL).unwrap();
    let mut mysql: SubscriptionEngine<TestEvent<MySql>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, MySqlDialect {});
    let outcome = mysql.register(SubscriptionRequest::new(
        1u64,
        "SELECT amount, COUNT(*) FROM g GROUP BY amount",
    ));
    assert!(
        outcome.is_ok(),
        "an integer group column is served on MySQL too, got {outcome:?}"
    );
}

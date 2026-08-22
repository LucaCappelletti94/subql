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
use subql::reexec::{ReExecEngine, Registered};
use subql::testing::TestEvent;
use subql::{DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, status TEXT, amount INT); \
     CREATE TABLE m (id INT PRIMARY KEY, t_id INT, owner TEXT);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;
type Wrapper = ReExecEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine() -> Engine {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    SubscriptionEngine::new(db, PostgreSqlDialect {})
}

/// Register `sql` and return the refusal message, failing loudly on anything
/// else. Every clause outside the served shape is `UnsupportedSql`, which is
/// also the variant the re-execution wrapper listens for.
fn refusal(sql: &str) -> String {
    match engine().register(SubscriptionRequest::new(1u64, sql)) {
        Err(RegisterError::UnsupportedSql(message)) => message,
        other => panic!("{sql} should be refused as unsupported SQL, got {other:?}"),
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

/// The two assertions connetto's finding document states as the expected
/// behaviour. `GROUP BY` asks for one count per group while the maintained
/// state is a single accumulator, and the seed SQL kept the clause, so the
/// caller started from one arbitrary group's count and folded global deltas
/// onto it.
#[test]
fn a_grouped_or_filtered_aggregate_is_refused() {
    refused_naming("SELECT COUNT(*) FROM t GROUP BY status", "GROUP BY");
    refused_naming("SELECT COUNT(*) FROM t HAVING COUNT(*) > 3", "HAVING");
    refused_naming(
        "SELECT COUNT(*) FROM t GROUP BY status HAVING COUNT(*) > 3",
        "GROUP BY",
    );
    refused_naming("SELECT SUM(amount) FROM t GROUP BY status", "GROUP BY");
    refused_naming("SELECT COUNT(*) FROM t GROUP BY ALL", "GROUP BY");
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
    let bootstrap = report.aggregate_bootstrap.expect("an aggregate seeds");
    assert_eq!(
        bootstrap.sql,
        "SELECT COUNT(*) AS c0 FROM t WHERE status = 'a'"
    );
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
        let mut wrapper: Wrapper =
            ReExecEngine::new(SubscriptionEngine::new(db, PostgreSqlDialect {}));
        match wrapper.register(SubscriptionRequest::new(1u64, sql)) {
            Ok(Registered::Captured { sql: captured, .. }) => {
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
    let mut wrapper: Wrapper = ReExecEngine::new(SubscriptionEngine::new(db, PostgreSqlDialect {}));
    let outcome = wrapper.register(SubscriptionRequest::new(1u64, "SELECT MIN(amount) FROM t"));
    assert!(
        matches!(outcome, Ok(Registered::ReExec { .. })),
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
            Err(RegisterError::UnsupportedSql(message)) => assert!(
                message.contains(clause),
                "the refusal of {sql} should name {clause}, got {message:?}"
            ),
            other => panic!("{sql} should be refused, got {other:?}"),
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

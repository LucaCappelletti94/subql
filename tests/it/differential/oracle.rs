//! One oracle per engine: what the server itself answers for a predicate
//! over a row.
//!
//! # Why these statements are raw SQL
//!
//! The typed diesel DSL is the rule everywhere else in this tree, and it
//! cannot express this harness at all: the schema, the row and the
//! predicate are the case, generated per run, so there is no `table!` to
//! write and no column list known at compile time. A generated predicate
//! such as `amount / quantity > 3` or `label ILIKE 'a\%b'` is text by
//! construction, and asking the server what it answers for that text is
//! the entire purpose. Every statement here is therefore
//! [`diesel::sql_query`], and nothing else in the harness is.
//!
//! # What counts as an answer
//!
//! A successful row is a nullable boolean, which is exactly subql's
//! tri-state: `TRUE`, `FALSE` and `NULL` are [`Tri::True`], [`Tri::False`]
//! and [`Tri::Unknown`]. A raise is not a fourth answer, it is the absence
//! of one, and it is kept apart from `NULL` because the engines disagree
//! about which cases raise: measured in Phase C9, PostgreSQL raises for a
//! zero divisor where MySQL and SQLite answer `NULL`.
//!
//! # Why the pairing is part of the oracle
//!
//! An oracle is only an oracle for the backend that targets its engine.
//! Comparing MySQL's answer against the PostgreSQL backend would call
//! `7 / 2 > 3` a divergence when both are right about their own engine, so
//! [`Oracle::Backend`] names the pairing and
//! [`OracleCase::registered_and_served`] proves the predicate is served in
//! process under that backend before any comparison is worth making.
#![allow(clippy::unwrap_used)]

use diesel::{sql_query, RunQueryDsl};
use sql_traits::structs::ParserDB;
use subql::backend::Backend;
use subql::compiler::{SqlLiteralParse, Tri};
use subql::testing::TestEvent;
use subql::{DefaultIds, SubscriptionEngine, SubscriptionRequest};

/// What an engine did with one predicate over one row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OracleVerdict {
    /// It answered a nullable boolean, which is the tri-state.
    Answered(Tri),
    /// It raised. Not an answer, and never equal to one: a harness that
    /// read this as [`Tri::Unknown`] would have called every measured
    /// raise agreement.
    Refused(String),
}

impl OracleVerdict {
    /// The tri-state this verdict carries, or `None` when the engine
    /// refused to answer at all.
    #[must_use]
    pub const fn answered(&self) -> Option<Tri> {
        match self {
            Self::Answered(tri) => Some(*tri),
            Self::Refused(_) => None,
        }
    }
}

/// Which engine an oracle speaks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
    Postgres,
    MySql,
    Sqlite,
}

/// One engine, asked directly.
pub trait Oracle {
    /// The subql backend that targets this engine, and the only one whose
    /// answers this oracle judges.
    type Backend: Backend + SqlLiteralParse;

    /// Which engine this is, checked against the paired backend so a
    /// mismatched pairing fails rather than reporting a divergence that
    /// is only a mismatch.
    const ENGINE: Engine;

    /// The dialect that backend compiles under.
    fn dialect() -> <Self::Backend as Backend>::Dialect;

    /// Create the table, insert the row, and answer what the predicate
    /// evaluates to over it.
    fn answer(&mut self, case: &OracleCase<'_>) -> OracleVerdict;

    /// The same, for a case spelled inline.
    fn answer_case(&mut self, ddl: &[&str], insert: &str, predicate: &str) -> OracleVerdict {
        self.answer(&OracleCase {
            ddl,
            insert,
            predicate,
        })
    }
}

/// Which engine a subql backend targets, so a pairing can be checked
/// rather than assumed.
fn engine_of<B: Backend + 'static>() -> Engine {
    use core::any::TypeId;

    let id = TypeId::of::<B>();
    if id == TypeId::of::<subql::backend::Postgres>() {
        Engine::Postgres
    } else if id == TypeId::of::<subql::backend::MySql>() {
        Engine::MySql
    } else if id == TypeId::of::<subql::backend::SQLite>() {
        Engine::Sqlite
    } else {
        panic!("a differential case needs a backend with an engine to compare against")
    }
}

/// One case: a schema, a row, and a predicate over it.
pub struct OracleCase<'a> {
    /// The statements that build the schema, in order.
    ///
    /// A list rather than one string because PostgreSQL refuses two
    /// commands in one prepared statement, measured while building the
    /// generated schema, whose collation has to be declared first.
    pub ddl: &'a [&'a str],
    pub insert: &'a str,
    pub predicate: &'a str,
}

impl OracleCase<'_> {
    /// The statements that put the row in place, in order. Dropping
    /// first is what lets one connection serve every case in a run.
    fn setup(&self) -> Vec<&str> {
        let mut statements = vec!["DROP TABLE IF EXISTS t"];
        statements.extend(self.ddl.iter().copied());
        statements.push(self.insert);
        statements
    }

    /// The schema as one text, for `ParserDB`, which reads several
    /// statements at once.
    fn catalog_ddl(&self) -> String {
        self.ddl.join(";\n")
    }

    /// The statement the oracle evaluates, which is the predicate read as
    /// a value rather than as a filter, so that `NULL` comes back as
    /// `NULL` instead of as a row that did not match.
    fn verdict_sql(&self) -> String {
        format!("SELECT ({}) AS verdict FROM t", self.predicate)
    }

    /// Register this case's predicate under the paired backend and prove
    /// the engine serves it in process.
    ///
    /// A refused registration is not a divergence and must never be read
    /// as one: it means subql routed the statement to a database read, so
    /// there is no in-process answer to compare. A case that cannot be
    /// served is skipped by the caller, and `false` says so.
    pub fn registered_and_served<O>(&self) -> bool
    where
        O: Oracle,
        O::Backend: 'static,
        <O::Backend as Backend>::Dialect: sqlparser::dialect::Dialect + Default,
    {
        assert_eq!(
            engine_of::<O::Backend>(),
            O::ENGINE,
            "an oracle judges only the backend that targets its engine"
        );
        let database = ParserDB::parse::<<O::Backend as Backend>::Dialect>(&self.catalog_ddl())
            .expect("DDL parses");
        let mut engine: SubscriptionEngine<TestEvent<O::Backend>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(database, O::dialect());
        engine
            .register(SubscriptionRequest::new(
                1u64,
                format!("SELECT * FROM t WHERE {}", self.predicate),
            ))
            .is_ok_and(|registered| registered.not_served_because.is_none())
    }
}

/// A nullable boolean, as PostgreSQL returns one.
#[derive(diesel::QueryableByName)]
struct BoolVerdict {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Bool>)]
    verdict: Option<bool>,
}

/// A nullable integer, which is what MySQL and SQLite return for a
/// boolean expression: measured, both answer `1` and `0` rather than a
/// boolean type.
#[derive(diesel::QueryableByName)]
struct IntVerdict {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    verdict: Option<i64>,
}

/// Map a nullable boolean to the tri-state, which is the whole mapping:
/// `NULL` is unknown, and nothing else is.
const fn tri_of(verdict: Option<bool>) -> Tri {
    match verdict {
        Some(true) => Tri::True,
        Some(false) => Tri::False,
        None => Tri::Unknown,
    }
}

/// PostgreSQL, asked over a live connection.
pub struct PgOracle {
    pub connection: diesel::PgConnection,
}

impl Oracle for PgOracle {
    type Backend = subql::backend::Postgres;
    const ENGINE: Engine = Engine::Postgres;

    fn dialect() -> sqlparser::dialect::PostgreSqlDialect {
        sqlparser::dialect::PostgreSqlDialect {}
    }

    fn answer(&mut self, case: &OracleCase<'_>) -> OracleVerdict {
        for statement in case.setup() {
            if let Err(error) = sql_query(statement).execute(&mut self.connection) {
                return OracleVerdict::Refused(error.to_string());
            }
        }
        match sql_query(case.verdict_sql()).get_result::<BoolVerdict>(&mut self.connection) {
            Ok(row) => OracleVerdict::Answered(tri_of(row.verdict)),
            Err(error) => OracleVerdict::Refused(error.to_string()),
        }
    }
}

/// MySQL, asked over a live connection.
pub struct MySqlOracle {
    pub connection: diesel::MysqlConnection,
}

impl Oracle for MySqlOracle {
    type Backend = subql::backend::MySql;
    const ENGINE: Engine = Engine::MySql;

    fn dialect() -> sqlparser::dialect::MySqlDialect {
        sqlparser::dialect::MySqlDialect {}
    }

    fn answer(&mut self, case: &OracleCase<'_>) -> OracleVerdict {
        for statement in case.setup() {
            if let Err(error) = sql_query(statement).execute(&mut self.connection) {
                return OracleVerdict::Refused(error.to_string());
            }
        }
        match sql_query(case.verdict_sql()).get_result::<IntVerdict>(&mut self.connection) {
            Ok(row) => OracleVerdict::Answered(tri_of(row.verdict.map(|flag| flag != 0))),
            Err(error) => OracleVerdict::Refused(error.to_string()),
        }
    }
}

/// SQLite, asked in memory, which needs no container.
pub struct SqliteOracle {
    pub connection: diesel::SqliteConnection,
}

impl SqliteOracle {
    /// A fresh in-memory database.
    #[must_use]
    pub fn open() -> Self {
        use diesel::Connection as _;

        Self {
            connection: diesel::SqliteConnection::establish(":memory:")
                .expect("an in-memory SQLite database opens"),
        }
    }
}

impl Oracle for SqliteOracle {
    type Backend = subql::backend::SQLite;
    const ENGINE: Engine = Engine::Sqlite;

    fn dialect() -> sqlparser::dialect::SQLiteDialect {
        sqlparser::dialect::SQLiteDialect {}
    }

    fn answer(&mut self, case: &OracleCase<'_>) -> OracleVerdict {
        for statement in case.setup() {
            if let Err(error) = sql_query(statement).execute(&mut self.connection) {
                return OracleVerdict::Refused(error.to_string());
            }
        }
        match sql_query(case.verdict_sql()).get_result::<IntVerdict>(&mut self.connection) {
            Ok(row) => OracleVerdict::Answered(tri_of(row.verdict.map(|flag| flag != 0))),
            Err(error) => OracleVerdict::Refused(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Engine, MySqlOracle, Oracle, OracleCase, OracleVerdict, PgOracle, SqliteOracle};
    use subql::compiler::Tri;

    const PG_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, amount INT, label TEXT)";
    // Only the MySQL case reads it, and that case needs a MySQL client.
    #[cfg(any(
        feature = "executor-diesel-postgres",
        feature = "executor-diesel-async-postgres",
        feature = "executor-diesel-postgres-r2d2",
        feature = "executor-diesel-mysql",
        feature = "executor-diesel-async-mysql",
        feature = "diesel-typed-mysql",
        feature = "apply-patchset-mysql",
        feature = "apply-patchset-mysql-async",
    ))]
    const MYSQL_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, amount INT, \
                             label VARCHAR(64) COLLATE utf8mb4_bin)";
    const SQLITE_DDL: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER, label TEXT)";
    const INSERT: &str = "INSERT INTO t (id, amount, label) VALUES (1, 7, 'abc')";
    /// A row whose `amount` is NULL, so a comparison over it is unknown.
    const NULL_INSERT: &str = "INSERT INTO t (id, amount, label) VALUES (1, NULL, 'abc')";

    /// The three answers an engine has, and the mapping onto the
    /// tri-state, asked of each engine in turn.
    fn assert_tri_state<O: Oracle>(oracle: &mut O, ddl: &str) {
        assert_eq!(
            oracle.answer_case(&[ddl], INSERT, "amount = 7"),
            OracleVerdict::Answered(Tri::True),
            "a true predicate answers TRUE"
        );
        assert_eq!(
            oracle.answer_case(&[ddl], INSERT, "amount = 8"),
            OracleVerdict::Answered(Tri::False),
            "a false predicate answers FALSE"
        );
        assert_eq!(
            oracle.answer_case(&[ddl], NULL_INSERT, "amount = 7"),
            OracleVerdict::Answered(Tri::Unknown),
            "a comparison against NULL answers NULL, which is unknown"
        );
    }

    #[test]
    fn sqlite_oracle_answers_the_tri_state() {
        let mut oracle = SqliteOracle::open();
        assert_tri_state(&mut oracle, SQLITE_DDL);
    }

    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn pg_oracle_answers_the_tri_state() {
        let container = crate::common::pg_with_wal2json();
        let port = crate::common::pg_port(&container);
        let mut oracle = PgOracle {
            connection: crate::common::pg_connect(port),
        };
        assert_tri_state(&mut oracle, PG_DDL);
    }

    #[cfg(any(
        feature = "executor-diesel-postgres",
        feature = "executor-diesel-async-postgres",
        feature = "executor-diesel-postgres-r2d2",
        feature = "executor-diesel-mysql",
        feature = "executor-diesel-async-mysql",
        feature = "diesel-typed-mysql",
        feature = "apply-patchset-mysql",
        feature = "apply-patchset-mysql-async",
    ))]
    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn mysql_oracle_answers_the_tri_state() {
        let container = crate::common::mysql_8();
        let port = crate::common::mysql_port(&container);
        let mut oracle = MySqlOracle {
            connection: crate::common::mysql_connect(port),
        };
        assert_tri_state(&mut oracle, MYSQL_DDL);
    }

    /// An engine that raises has not answered `NULL`, and the harness must
    /// not read it as unknown.
    ///
    /// Measured in Phase C9: PostgreSQL raises `division by zero` where
    /// MySQL and SQLite answer `NULL`. Both are recorded here, because the
    /// point is that the verdict tells them apart: SQLite's `NULL` is a
    /// real answer and PostgreSQL's raise is not.
    #[test]
    fn an_oracle_error_is_not_unknown() {
        let mut sqlite = SqliteOracle::open();
        let divide_by_zero = sqlite.answer_case(
            &[SQLITE_DDL],
            "INSERT INTO t VALUES (1, 0, 'abc')",
            "1 / amount > 0",
        );
        assert_eq!(
            divide_by_zero,
            OracleVerdict::Answered(Tri::Unknown),
            "measured: SQLite answers NULL for a zero divisor, which is an answer"
        );

        let refused = OracleVerdict::Refused("division by zero".to_string());
        assert_eq!(
            refused.answered(),
            None,
            "a refusal carries no tri-state at all, so it cannot compare equal to one"
        );
        assert_ne!(
            refused,
            OracleVerdict::Answered(Tri::Unknown),
            "and it is not unknown, which is the confusion this type exists to prevent"
        );
    }

    /// PostgreSQL's own raise, asked of the server rather than asserted
    /// from memory.
    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn pg_oracle_reports_a_raise_rather_than_unknown() {
        let container = crate::common::pg_with_wal2json();
        let port = crate::common::pg_port(&container);
        let mut oracle = PgOracle {
            connection: crate::common::pg_connect(port),
        };
        let verdict = oracle.answer_case(
            &[PG_DDL],
            "INSERT INTO t (id, amount, label) VALUES (1, 0, 'abc')",
            "1 / amount > 0",
        );
        assert!(
            matches!(&verdict, OracleVerdict::Refused(message) if message.contains("division by zero")),
            "measured: PostgreSQL raises `division by zero`, got {verdict:?}"
        );
        assert_eq!(
            verdict.answered(),
            None,
            "so there is no tri-state to compare against subql's"
        );
    }

    /// An oracle whose engine and backend disagree, which is the mistake
    /// the pairing check exists to catch: comparing MySQL's answers
    /// against the PostgreSQL backend would report `7 / 2 > 3` as a
    /// divergence when each engine is right about itself.
    ///
    /// Spelled here rather than reached by mutating a real oracle,
    /// because swapping a real one's backend does not compile: its
    /// `dialect` would return the wrong dialect type. Only a wrong
    /// `ENGINE` is expressible, and this is it.
    struct MispairedOracle;

    impl Oracle for MispairedOracle {
        type Backend = subql::backend::Postgres;
        const ENGINE: Engine = Engine::MySql;

        fn dialect() -> sqlparser::dialect::PostgreSqlDialect {
            sqlparser::dialect::PostgreSqlDialect {}
        }

        fn answer(&mut self, _case: &OracleCase<'_>) -> OracleVerdict {
            unreachable!("the pairing is refused before anything is asked")
        }
    }

    #[test]
    #[should_panic(expected = "an oracle judges only the backend that targets its engine")]
    fn a_mispaired_oracle_is_refused_before_it_answers() {
        let case = OracleCase {
            ddl: &[PG_DDL],
            insert: INSERT,
            predicate: "amount = 7",
        };
        let _served = case.registered_and_served::<MispairedOracle>();
    }

    /// An oracle judges only the backend that targets its engine, which
    /// the pairing check enforces before any comparison.
    #[test]
    fn an_oracle_is_paired_with_one_backend() {
        assert_eq!(SqliteOracle::ENGINE, Engine::Sqlite);
        assert_eq!(PgOracle::ENGINE, Engine::Postgres);
        assert_eq!(MySqlOracle::ENGINE, Engine::MySql);
        let case = OracleCase {
            ddl: &[SQLITE_DDL],
            insert: INSERT,
            predicate: "amount = 7",
        };
        assert!(
            case.registered_and_served::<SqliteOracle>(),
            "this predicate is served in process, so its answers are comparable"
        );
    }
}

//! The sweep: generate a case, ask the engine, ask subql, compare.
//!
//! E1 built the oracles, E2 the generators and E3 the two layers, each
//! with cases written by hand. This is the loop that joins them and is
//! the only part that can find a divergence nobody thought of.
//!
//! # What a case is, and why it is one value
//!
//! A [`Case`] is the triple the engines are asked about: the statements
//! that build the schema, the row, and the predicate. It is one value
//! rather than three arguments so that proptest can shrink it as a
//! whole, and so a failure reports the whole triple rather than the part
//! the assertion happened to mention. A divergence you cannot reproduce
//! is a divergence you cannot fix.
//!
//! # What agreement means
//!
//! The engine answers `TRUE`, `FALSE`, `NULL`, or refuses. subql answers
//! a tri-state, or classifies the predicate as needing a database read,
//! or refuses to register it. Only one pairing is a divergence: both
//! sides answered, and the answers differ. Every other pairing is a
//! legitimate outcome and is counted rather than asserted:
//!
//! ```text
//! engine       subql                    verdict
//! answered     answered, same           agreement
//! answered     answered, different      DIVERGENCE
//! answered     routed to a read         skipped: no in-process answer to compare
//! answered     refused by the engine    DIVERGENCE: subql says the engine will not
//!                                       run a statement the engine just ran
//! setup failed  anything                 HARNESS DEFECT: the case was never asked
//! refused      anything                 skipped: a raise has no answer to compare
//! ```
//!
//! The fourth row matters because `RegisterError::RefusedByEngine` is a
//! claim about the engine, and the oracle is standing right there to
//! check it.
#![allow(clippy::unwrap_used)]

use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, TestRunner};
use sql_traits::structs::ParserDB;
use subql::backend::{Backend, Value};
use subql::compiler::Tri;
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest};

use super::generators::{predicate_forms, row_strategy, schema_statements, Row};
use super::oracle::{Engine, Oracle, OracleCase, OracleVerdict};

/// One generated case: the schema, the row, and the predicate.
#[derive(Clone, Debug)]
pub struct Case {
    pub schema: Vec<String>,
    pub row: Row,
    pub predicate: String,
}

impl Case {
    /// The case as a reproduction, which is what a failure has to carry.
    #[must_use]
    pub fn reproduction(&self) -> String {
        let mut out = String::new();
        for statement in &self.schema {
            out.push_str(statement);
            out.push_str(";\n");
        }
        out.push_str(&self.row.insert_sql());
        out.push_str(";\n-- predicate: ");
        out.push_str(&self.predicate);
        out
    }

    /// What the engine answers for it.
    fn engine_says<O: Oracle>(&self, oracle: &mut O) -> OracleVerdict {
        let borrowed: Vec<&str> = self.schema.iter().map(String::as_str).collect();
        oracle.answer(&OracleCase {
            ddl: &borrowed,
            insert: &self.row.insert_sql(),
            predicate: &self.predicate,
        })
    }

    /// What subql answers for it, in process.
    fn subql_says<O>(&self) -> SubqlAnswer
    where
        O: Oracle,
        O::Backend: Backend<Int = i64, Float = f64, Decimal = bigdecimal::BigDecimal, String = String>
            + 'static,
        <O::Backend as Backend>::Bool: From<bool>,
        <O::Backend as Backend>::Bytes: From<Vec<u8>>,
        <O::Backend as Backend>::Dialect: sqlparser::dialect::Dialect + Default,
    {
        let catalog_ddl = self.schema.join(";\n");
        let Ok(database) = ParserDB::parse::<<O::Backend as Backend>::Dialect>(&catalog_ddl) else {
            return SubqlAnswer::Unparsed;
        };
        let Some(table) = catalog_helpers::table_id(&database, "t") else {
            return SubqlAnswer::Unparsed;
        };
        let arity = catalog_helpers::table_arity(&database, table).unwrap_or(0);
        let mut engine: SubscriptionEngine<TestEvent<O::Backend>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(database, O::dialect());
        let registered = match engine.register(SubscriptionRequest::new(
            1u64,
            format!("SELECT * FROM t WHERE {}", self.predicate),
        )) {
            Ok(registered) => registered,
            Err(RegisterError::NotServedInProcess(_)) => return SubqlAnswer::NeedsRead,
            Err(RegisterError::RefusedByEngine { reason, .. }) => {
                return SubqlAnswer::RefusedByEngine(reason);
            }
            Err(other) => return SubqlAnswer::Rejected(format!("{other}")),
        };
        if registered.not_served_because.is_some() {
            return SubqlAnswer::NeedsRead;
        }
        let cells = self.cells_in_schema_order::<O::Backend>(arity, O::ENGINE);
        let notifications = match engine.consumers(&TestEvent::insert(table, cells)) {
            Ok(notifications) => notifications,
            Err(error) => return SubqlAnswer::Rejected(format!("{error}")),
        };
        if !notifications.evaluation_failures().is_empty() {
            return SubqlAnswer::Refused;
        }
        if !notifications.unanswered().is_empty() {
            return SubqlAnswer::NeedsRead;
        }
        SubqlAnswer::Answered(if notifications.inserted().is_empty() {
            // A row the predicate did not select is `FALSE` or `NULL`,
            // and dispatch does not distinguish them: only `TRUE`
            // selects. The comparison below reads the engine the same
            // way, so the two are compared on the one thing dispatch
            // reports.
            Tri::False
        } else {
            Tri::True
        })
    }

    /// The row as the event carries it: every column of the schema in
    /// order, with the generated cells placed and the rest `NULL`.
    fn cells_in_schema_order<B>(&self, arity: usize, engine: Engine) -> Vec<Value<B>>
    where
        B: Backend<Int = i64, Float = f64, Decimal = bigdecimal::BigDecimal, String = String>,
        B::Bool: From<bool>,
        B::Bytes: From<Vec<u8>>,
    {
        let catalog_ddl = self.schema.join(";\n");
        let mut cells: Vec<Value<B>> = (0..arity).map(|_| Value::Null).collect();
        if let Some(first) = cells.first_mut() {
            *first = Value::Int(1);
        }
        // The generated row names its columns, and the schema text is
        // where their order lives, so the placement is by name rather
        // than by position.
        for (name, cell) in &self.row.cells {
            if let Some(slot) = column_slot(&catalog_ddl, name) {
                if let Some(place) = cells.get_mut(slot) {
                    // As the engine stores it, not as it was written: a
                    // `char(n)` is padded on write by PostgreSQL, and the
                    // wire carries the padded value.
                    *place = cell
                        .as_stored(engine, &declared_type(&catalog_ddl, name))
                        .value::<B>();
                }
            }
        }
        cells
    }
}

/// Every column declaration of this schema's `CREATE TABLE`, in order.
///
/// Split at top-level commas only. Splitting on every comma put
/// `DECIMAL(20,4)` in two and shifted every later column by one, so the
/// row subql was handed had its cells one slot out. The sweep found it,
/// and only against MySQL, because that is the one schema here whose
/// declared types carry a comma: PostgreSQL's `NUMERIC` and both
/// `VARCHAR(16)` and `CHAR(5)` do not.
fn declarations(catalog_ddl: &str) -> Vec<&str> {
    let Some(open) = catalog_ddl.find("CREATE TABLE t (") else {
        return Vec::new();
    };
    let body = &catalog_ddl[open + "CREATE TABLE t (".len()..];
    let Some(close) = body.rfind(')') else {
        return Vec::new();
    };
    let body = &body[..close];
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, character) in body.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                out.push(body[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    out.push(body[start..].trim());
    out
}

/// What `column` is declared as in this schema, or the empty string.
fn declared_type(catalog_ddl: &str, column: &str) -> String {
    declarations(catalog_ddl)
        .into_iter()
        .find_map(|declaration| {
            declaration
                .strip_prefix(column)
                .filter(|rest| rest.starts_with(' '))
                .map(|rest| rest.trim().to_string())
        })
        .unwrap_or_default()
}

/// Where `column` sits in the `CREATE TABLE` this schema declares.
///
/// Read out of the statement text rather than the catalog, because the
/// catalog is what the comparison is testing and reading the answer from
/// the thing under test proves nothing.
fn column_slot(catalog_ddl: &str, column: &str) -> Option<usize> {
    declarations(catalog_ddl)
        .into_iter()
        .position(|declaration| {
            declaration
                .strip_prefix(column)
                .is_some_and(|rest| rest.starts_with(' '))
        })
}

/// What subql did with a case.
#[derive(Clone, Debug, PartialEq, Eq)]
enum SubqlAnswer {
    /// Answered in process.
    Answered(Tri),
    /// Classified as needing a database read, so there is no in-process
    /// answer to compare.
    NeedsRead,
    /// Refused the row's arithmetic, which pairs with an engine that
    /// raises.
    Refused,
    /// Claimed the engine will not execute the statement.
    RefusedByEngine(String),
    /// Rejected the statement for a reason that is not about semantics,
    /// which the generator should not be producing.
    Rejected(String),
    /// The generated DDL did not parse, likewise.
    Unparsed,
}

/// A case generated for `engine`, over one fixed predicate.
///
/// The predicate is a parameter rather than another generated component
/// so the sweep can cover every form instead of sampling them. Sampling
/// is what let the acceptance check pass while a reverted fix went
/// unnoticed: one form in twenty-six, times the one cell value that
/// exercises it, is a lottery a few dozen cases do not win.
pub fn case_strategy(engine: Engine, predicate: String) -> impl Strategy<Value = Case> {
    let schema = schema_statements(engine);
    row_strategy(engine).prop_map(move |row| Case {
        schema: schema.clone(),
        row,
        predicate: predicate.clone(),
    })
}

/// Sweep every predicate form past `oracle`, `rows_per_form` generated
/// rows each, and stop at the first divergence with the triple that
/// produced it.
///
/// `regressions` is where a failing seed is recorded, so the case that
/// failed is the first case the next run tries. proptest's default
/// resolves a path from the source file of the macro that built the
/// runner, which a runner built in a function does not have, and it says
/// so on every failure; naming the file is what that default wants.
pub fn sweep<O>(
    oracle: &mut O,
    engine: Engine,
    rows_per_form: u32,
    regressions: &'static str,
) -> Sweep
where
    O: Oracle,
    O::Backend: Backend<Int = i64, Float = f64, Decimal = bigdecimal::BigDecimal, String = String>
        + subql::compiler::SqlLiteralParse
        + 'static,
    <O::Backend as Backend>::Bool: From<bool>,
    <O::Backend as Backend>::Bytes: From<Vec<u8>>,
    <O::Backend as Backend>::Dialect: sqlparser::dialect::Dialect + Default,
{
    let counts = core::cell::RefCell::new(Sweep::default());
    // proptest hands the case closure out as `Fn`, so both the
    // connection and the counters are shared through cells.
    let shared = core::cell::RefCell::new(oracle);
    let mut outcome = Ok(());
    for predicate in predicate_forms(engine) {
        counts.borrow_mut().forms += 1;
        let mut runner = TestRunner::new(ProptestConfig {
            cases: rows_per_form,
            failure_persistence: Some(Box::new(
                proptest::test_runner::FileFailurePersistence::Direct(regressions),
            )),
            ..ProptestConfig::default()
        });
        outcome = runner.run(&case_strategy(engine, predicate), |case| {
            let engine_verdict = case.engine_says(*shared.borrow_mut());
            let subql_answer = case.subql_says::<O>();
            let mut counts = counts.borrow_mut();
            match (&engine_verdict, &subql_answer) {
                (OracleVerdict::SetupFailed(reason), _) => {
                    return Err(TestCaseError::fail(format!(
                        "{engine:?} would not build this case, so nothing was \
                         compared:\n{}\n-- {engine:?}: {reason}",
                        case.reproduction()
                    )));
                }
                (OracleVerdict::Refused(_), _) => counts.engine_refused += 1,
                (_, SubqlAnswer::NeedsRead) => counts.needs_read += 1,
                (OracleVerdict::Answered(_), SubqlAnswer::Refused) => counts.subql_refused += 1,
                (OracleVerdict::Answered(_), SubqlAnswer::RefusedByEngine(reason)) => {
                    return Err(TestCaseError::fail(format!(
                        "subql says {engine:?} will not execute this statement, and it just \
                         did:\n{}\n-- subql: {reason}",
                        case.reproduction()
                    )));
                }
                (OracleVerdict::Answered(_), SubqlAnswer::Rejected(reason)) => {
                    return Err(TestCaseError::fail(format!(
                        "the generator produced a statement subql rejects for a reason that is \
                         not about semantics:\n{}\n-- subql: {reason}",
                        case.reproduction()
                    )));
                }
                (OracleVerdict::Answered(_), SubqlAnswer::Unparsed) => {
                    return Err(TestCaseError::fail(format!(
                        "the generated DDL did not parse:\n{}",
                        case.reproduction()
                    )));
                }
                (OracleVerdict::Answered(engine_tri), SubqlAnswer::Answered(subql_tri)) => {
                    // Dispatch reports only whether the row was selected, so
                    // the engine is read the same way: `TRUE` selects and
                    // nothing else does.
                    let engine_selects = *engine_tri == Tri::True;
                    let subql_selects = *subql_tri == Tri::True;
                    if engine_selects == subql_selects {
                        counts.agreed += 1;
                    } else {
                        return Err(TestCaseError::fail(format!(
                            "{engine:?} answered {engine_tri:?} and subql answered \
                             {subql_tri:?}:\n{}",
                            case.reproduction()
                        )));
                    }
                }
            }
            Ok(())
        });
        if outcome.is_err() {
            break;
        }
    }
    let mut counts = counts.into_inner();
    counts.failure = match outcome {
        Ok(()) => None,
        Err(error) => Some(error.to_string()),
    };
    counts
}

/// What one sweep found, so a run that compares nothing cannot pass for a
/// run that found nothing.
#[derive(Default, Debug)]
pub struct Sweep {
    /// Predicate forms swept, so a run that covered one form cannot pass
    /// for a run that covered them all.
    pub forms: u32,
    /// Cases where both sides answered and agreed.
    pub agreed: u32,
    /// Cases the engine refused, which have no answer to compare.
    pub engine_refused: u32,
    /// Cases subql routed to a database read.
    pub needs_read: u32,
    /// Cases subql refused the arithmetic for.
    pub subql_refused: u32,
    /// The first divergence, with its reproduction.
    pub failure: Option<String>,
}

/// Where a failing seed is recorded for the shipped sweeps.
///
/// Beside the test, and git-ignored by the rule at `.gitignore:33`, so
/// the replay is local: a divergence found here is tried first on the
/// next run on this machine. Carrying it to another machine is the
/// reproduction in the failure message, not the seed.
pub const REGRESSIONS: &str = "tests/it/differential/sweep.proptest-regressions";

#[cfg(test)]
mod tests {
    use super::{case_strategy, sweep, Case, REGRESSIONS};

    /// Rows per predicate form.
    ///
    /// Read from the environment so the depth is a deployment decision
    /// rather than a recompile: CI runs the shallow default on every pull
    /// request and a deep one on its weekly schedule, which is where a
    /// rare collation or version divergence has time to show up.
    fn rows_per_form() -> u32 {
        rows_from(std::env::var("SUBQL_SWEEP_ROWS").ok().as_deref())
    }

    /// The depth a declared value asks for.
    ///
    /// Split from the read because the crate forbids `unsafe` and so a
    /// test cannot set an environment variable, and because the fallback
    /// is the part worth pinning: a misread that swept zero cases would
    /// pass every assertion here except `agreed > 0`.
    fn rows_from(declared: Option<&str>) -> u32 {
        declared
            .and_then(|rows| rows.parse().ok())
            .filter(|rows| *rows > 0)
            .unwrap_or(48)
    }
    use crate::differential::generators::schema_statements;
    use crate::differential::oracle::{Engine, SqliteOracle};
    use proptest::strategy::{Strategy as _, ValueTree as _};
    use proptest::test_runner::TestRunner;

    /// The sweep compares, and says how much it compared.
    ///
    /// A harness that skipped every case would pass every assertion
    /// about divergences, so the count of agreements is asserted too.
    /// SQLite needs no container, which is why the sweep's own test runs
    /// here rather than behind Docker.
    #[test]
    fn the_sqlite_sweep_agrees_and_says_how_often() {
        let mut oracle = SqliteOracle::open();
        let found = sweep(&mut oracle, Engine::Sqlite, rows_per_form(), REGRESSIONS);
        assert!(
            found.failure.is_none(),
            "the sweep found a divergence: {}",
            found.failure.unwrap_or_default()
        );
        assert!(
            found.agreed > 0,
            "a sweep that compared nothing is not a sweep that found nothing: {found:?}"
        );
        assert_eq!(
            usize::try_from(found.forms).expect("a form count fits a usize"),
            crate::differential::generators::predicate_forms(Engine::Sqlite).len(),
            "every predicate form is swept, not sampled: {found:?}"
        );
    }

    /// The depth CI sets is the depth the sweep runs.
    ///
    /// Pinned because the weekly deep run is worth nothing if the
    /// variable is read wrong or not read at all, and nothing else in the
    /// suite would notice: a shallow sweep passes exactly like a deep one.
    #[test]
    fn the_sweep_depth_comes_from_the_environment() {
        assert_eq!(rows_from(Some("7")), 7, "the declared depth is honoured");
        assert_eq!(
            rows_from(Some("not a number")),
            48,
            "an unreadable depth falls back rather than sweeping zero cases"
        );
        assert_eq!(rows_from(Some("0")), 48, "as does a zero-case sweep");
        assert_eq!(rows_from(None), 48, "as does an absent one");
    }

    /// The same sweep against PostgreSQL, which is where the rules only
    /// that engine has are reachable: its NaN total order, its `jsonb`
    /// ordering, its `numeric` scale.
    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn the_postgres_sweep_agrees_and_says_how_often() {
        let container = crate::common::pg_with_wal2json();
        let port = crate::common::pg_port(&container);
        let mut oracle = crate::differential::oracle::PgOracle {
            connection: crate::common::pg_connect(port),
        };
        let found = sweep(&mut oracle, Engine::Postgres, rows_per_form(), REGRESSIONS);
        assert!(
            found.failure.is_none(),
            "the sweep found a divergence: {}",
            found.failure.unwrap_or_default()
        );
        assert!(found.agreed > 0, "the sweep compared nothing: {found:?}");
        assert_eq!(
            usize::try_from(found.forms).expect("a form count fits a usize"),
            crate::differential::generators::predicate_forms(Engine::Postgres).len()
        );
    }

    /// And against MySQL, whose division scale and padding collations no
    /// other engine has.
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
    fn the_mysql_sweep_agrees_and_says_how_often() {
        let container = crate::common::mysql_8();
        let port = crate::common::mysql_port(&container);
        let mut oracle = crate::differential::oracle::MySqlOracle {
            connection: crate::common::mysql_connect(port),
        };
        let found = sweep(&mut oracle, Engine::MySql, rows_per_form(), REGRESSIONS);
        assert!(
            found.failure.is_none(),
            "the sweep found a divergence: {}",
            found.failure.unwrap_or_default()
        );
        assert!(found.agreed > 0, "the sweep compared nothing: {found:?}");
        assert_eq!(
            usize::try_from(found.forms).expect("a form count fits a usize"),
            crate::differential::generators::predicate_forms(Engine::MySql).len()
        );
    }

    /// A case renders as the triple that reproduces it.
    #[test]
    fn a_case_reproduces_itself() {
        let mut runner = TestRunner::deterministic();
        let case: Case = case_strategy(Engine::Sqlite, "narrow = wide".to_string())
            .new_tree(&mut runner)
            .expect("a case generates")
            .current();
        let text = case.reproduction();
        assert!(
            text.contains("CREATE TABLE t ("),
            "the schema is in the reproduction: {text}"
        );
        assert!(text.contains("INSERT INTO t ("), "and the row: {text}");
        assert!(text.contains("-- predicate: "), "and the predicate: {text}");
        for statement in schema_statements(Engine::Sqlite) {
            assert!(
                text.contains(&statement),
                "every schema statement, so the case runs as written: {text}"
            );
        }
    }
}

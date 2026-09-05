//! What the sweep hands back when it finds something.
//!
//! A divergence is worth finding only if it can be acted on, and the
//! sweep's product is not a boolean: it is the triple that reproduces the
//! disagreement, shrunk to the smallest row proptest can still fail with,
//! and a seed on disk so the next run tries that case first. This module
//! pins all three, because each has a way of quietly not happening.
//!
//! The engine here is a test double rather than a database. A real
//! divergence cannot be conjured on demand without reintroducing one, so
//! the oracle contradicts subql by construction: it answers `TRUE` for
//! every case it is asked about. That exercises the reporting path
//! against a guaranteed disagreement, which is the part under test.

use super::oracle::{Engine, Oracle, OracleCase, OracleVerdict};
use super::sweep::sweep;
use subql::backend::SQLite;
use subql::compiler::Tri;

/// Where this module records its own seeds.
///
/// Not the shipped sweeps' file. These failures are manufactured, and a
/// seed for a manufactured failure is noise in a real sweep's replay set.
const TRIAGE_REGRESSIONS: &str = "target/triage.proptest-regressions";

/// An engine that answers `TRUE` to everything.
///
/// Paired with the SQLite backend because the pairing is checked, and
/// SQLite is the engine whose oracle needs no container, so this test
/// runs everywhere.
struct ContraryOracle;

impl Oracle for ContraryOracle {
    type Backend = SQLite;

    const ENGINE: Engine = Engine::Sqlite;

    fn dialect() -> sqlparser::dialect::SQLiteDialect {
        sqlparser::dialect::SQLiteDialect {}
    }

    fn answer(&mut self, _case: &OracleCase<'_>) -> OracleVerdict {
        OracleVerdict::Answered(Tri::True)
    }
}

/// A divergence is reported as the triple that reproduces it.
///
/// The three parts are asserted separately because a report that names
/// only the predicate is the failure mode this test exists for: it was
/// the actual reading of a reverted-fix failure during the acceptance
/// check, where the predicate was read, the row was not, and the case was
/// mistaken for a whole new class of defect. A reproduction that has to
/// be pieced together from a previous failure is not a reproduction.
#[test]
fn a_failing_case_reports_a_reproducible_triple() {
    let _ = std::fs::remove_file(TRIAGE_REGRESSIONS);
    let mut oracle = ContraryOracle;
    let found = sweep(&mut oracle, Engine::Sqlite, 8, TRIAGE_REGRESSIONS);

    let failure = found
        .failure
        .expect("an engine that contradicts subql on every case diverges on the first one");
    assert!(
        failure.contains("CREATE TABLE t ("),
        "the report carries the schema: {failure}"
    );
    assert!(
        failure.contains("INSERT INTO t ("),
        "the report carries the row, which is the part that went unread: {failure}"
    );
    assert!(
        failure.contains("-- predicate:"),
        "the report carries the predicate: {failure}"
    );
    assert!(
        failure.contains("answered"),
        "and it says what each side answered: {failure}"
    );
}

/// The failing seed is written where the next run will read it.
///
/// Without this the sweep is a lottery: it generates a fresh case set
/// per run, so a divergence found once can vanish and reappear on this
/// machine. proptest only persists when it is told a file it can
/// resolve, and its default cannot resolve one from a runner built
/// inside a function, which is what the sweep does; it said so on every
/// failure. The file is git-ignored, so the replay is local and the
/// reproduction in the failure message is what travels.
#[test]
fn a_failure_records_its_seed() {
    let _ = std::fs::remove_file(TRIAGE_REGRESSIONS);
    let mut oracle = ContraryOracle;
    let found = sweep(&mut oracle, Engine::Sqlite, 8, TRIAGE_REGRESSIONS);
    assert!(
        found.failure.is_some(),
        "the double diverges by construction"
    );

    let persisted = std::fs::read_to_string(TRIAGE_REGRESSIONS)
        .expect("the failing seed is recorded beside the run");
    assert!(
        persisted.contains("cc "),
        "proptest records a seed line: {persisted}"
    );
    let _ = std::fs::remove_file(TRIAGE_REGRESSIONS);
}

/// The sweep stops at the first divergence rather than sweeping on.
///
/// A sweep that carried on would report a count of failures and a last
/// case, which is the wrong product: the first divergence is the one with
/// the smallest reproduction, and every later one is likely the same
/// defect seen again.
#[test]
fn a_divergence_stops_the_sweep() {
    let _ = std::fs::remove_file(TRIAGE_REGRESSIONS);
    let mut oracle = ContraryOracle;
    let found = sweep(&mut oracle, Engine::Sqlite, 8, TRIAGE_REGRESSIONS);
    assert!(found.failure.is_some());
    assert_eq!(
        found.forms, 1,
        "the first form diverged, so no later form was swept: {found:?}"
    );
    // Agreement is not asserted to be zero. An engine that answers
    // `TRUE` to everything agrees with subql on any row subql also
    // selects, so the divergence lands on a later row of the same form
    // and the cases before it are genuine agreements.
    let _ = std::fs::remove_file(TRIAGE_REGRESSIONS);
}

/// A refused setup and a refused predicate are told apart.
///
/// Asserted directly, because the sweep only observes the distinction
/// when the generators are wrong, and they are not: with correct
/// generators no setup fails, so collapsing the two outcomes back into
/// one changes nothing the sweep can see. What it changes is whether the
/// next generator regression is loud or silent, and that is exactly the
/// failure this distinction was added for.
#[test]
fn a_refused_setup_is_not_a_refused_predicate() {
    let mut oracle = super::oracle::SqliteOracle::open();
    let refused_setup = oracle.answer_case(&["CREATE TABLE t (id NOT A TYPE)"], "SELECT 1", "1");
    assert!(
        matches!(refused_setup, OracleVerdict::SetupFailed(_)),
        "DDL SQLite will not run is a setup failure: {refused_setup:?}"
    );

    let refused_predicate = oracle.answer_case(
        &["CREATE TABLE t (id INTEGER PRIMARY KEY)"],
        "INSERT INTO t (id) VALUES (1)",
        "abs(-9223372036854775808) > 0",
    );
    assert!(
        matches!(refused_predicate, OracleVerdict::Refused(_)),
        "a predicate SQLite raises on is a refusal, not a setup failure: {refused_predicate:?}"
    );
}

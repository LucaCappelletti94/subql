//! Generated sequences through the whole engine, checked against SQLite.
//!
//! `subql::test_harnesses::harness_engine_model_sqlite` registers, ends and
//! restarts subscriptions of every kind between inserts, updates, deletes and
//! truncates, and compares each answer a subscriber holds with SQLite's own.
//! The libFuzzer target `fuzz_engine_model_sqlite` drives the same harness at
//! full speed, and a failing byte string here replays there unchanged.

use core::cell::Cell;
use proptest::collection::vec;
use proptest::prelude::any;
use proptest::test_runner::{Config, FileFailurePersistence, TestCaseError, TestRunner};

use subql::test_harnesses::{engine_model_sqlite, EngineModelCoverage};

/// Where a failing byte string is recorded.
const REGRESSIONS: &str = "tests/it/engine_model.proptest-regressions";

/// `SUBQL_SWEEP_ROWS` times two sequences per run, so the pull-request depth
/// is 96 and the weekly one 1200. A sequence opens a store and restarts, so
/// it costs about as much as fifty generated filters.
fn cases() -> u32 {
    std::env::var("SUBQL_SWEEP_ROWS")
        .ok()
        .and_then(|rows| rows.parse::<u32>().ok())
        .filter(|rows| *rows > 0)
        .unwrap_or(48)
        .saturating_mul(2)
}

#[test]
fn generated_sequences_agree_with_sqlite() {
    let mut runner = TestRunner::new(Config {
        cases: cases(),
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(REGRESSIONS))),
        ..Config::default()
    });
    let coverage = Cell::new(EngineModelCoverage::default());
    let outcome = runner.run(&vec(any::<u8>(), 256..2048), |bytes| {
        let reached =
            std::panic::catch_unwind(|| engine_model_sqlite(&bytes)).map_err(|panic| {
                let message = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("the harness panicked");
                TestCaseError::fail(message.to_string())
            })?;
        let mut total = coverage.get();
        total += reached;
        coverage.set(total);
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("{failure}");
    }
    // Every kind of answer, a tier change and a restart have to be reached,
    // or the run passed by comparing nothing of that kind.
    let reached = coverage.get();
    assert!(
        reached.rows > 0
            && reached.aggregates > 0
            && reached.reads > 0
            && reached.transitions > 0
            && reached.restarts > 0,
        "the sequences left a kind uncompared: {reached:?}"
    );
}

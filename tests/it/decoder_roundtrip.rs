//! Generated rows through every CDC decoder, checked against their values.
//!
//! `subql::test_harnesses::harness_decoder_roundtrip` writes a row of typed
//! cells as pgoutput, wal2json v1 and v2 or Maxwell writes it, decodes it
//! through subql, and requires every cell back. The libFuzzer target
//! `fuzz_decoder_roundtrip` drives the same harness at full speed, and a
//! failing byte string here replays there unchanged.

use proptest::collection::vec;
use proptest::prelude::any;
use proptest::test_runner::{Config, FileFailurePersistence, TestCaseError, TestRunner};
use subql::test_harnesses::harness_decoder_roundtrip;

/// Where a failing byte string is recorded.
const REGRESSIONS: &str = "tests/it/decoder_roundtrip.proptest-regressions";

/// Rows per run: `SUBQL_SWEEP_ROWS` times fifty, so the pull-request depth
/// is 2400 and the weekly one 30000.
fn cases() -> u32 {
    std::env::var("SUBQL_SWEEP_ROWS")
        .ok()
        .and_then(|rows| rows.parse::<u32>().ok())
        .filter(|rows| *rows > 0)
        .unwrap_or(48)
        .saturating_mul(50)
}

#[test]
fn generated_rows_decode_to_their_values() {
    let mut runner = TestRunner::new(Config {
        cases: cases(),
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(REGRESSIONS))),
        ..Config::default()
    });
    let outcome = runner.run(&vec(any::<u8>(), 64..512), |bytes| {
        std::panic::catch_unwind(|| harness_decoder_roundtrip(&bytes)).map_err(|panic| {
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("the harness panicked");
            TestCaseError::fail(message.to_string())
        })
    });
    if let Err(failure) = outcome {
        panic!("{failure}");
    }
}

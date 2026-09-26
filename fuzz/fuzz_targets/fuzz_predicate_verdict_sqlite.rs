#![no_main]
use libfuzzer_sys::fuzz_target;
use subql::test_harnesses::harness_predicate_verdict_sqlite;

fuzz_target!(|data: &[u8]| {
    harness_predicate_verdict_sqlite(data);
});

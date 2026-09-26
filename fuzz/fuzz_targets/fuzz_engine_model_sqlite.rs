#![no_main]
use libfuzzer_sys::fuzz_target;
use subql::test_harnesses::harness_engine_model_sqlite;

fuzz_target!(|data: &[u8]| {
    harness_engine_model_sqlite(data);
});

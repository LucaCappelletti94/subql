#![no_main]
use libfuzzer_sys::fuzz_target;
use subql::test_harnesses::harness_sqlite_pgoutput_e2e;

fuzz_target!(|data: &[u8]| {
    harness_sqlite_pgoutput_e2e(data);
});

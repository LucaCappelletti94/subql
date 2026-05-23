#![no_main]
use libfuzzer_sys::fuzz_target;
use subql::test_harnesses::harness_aggregate_consistency;

fuzz_target!(|data: &[u8]| {
    harness_aggregate_consistency(data);
});

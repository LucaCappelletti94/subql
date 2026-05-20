#![no_main]

use libfuzzer_sys::fuzz_target;
use subql::test_harnesses::harness_parse_sql;

fuzz_target!(|data: &[u8]| {
    harness_parse_sql(data);
});

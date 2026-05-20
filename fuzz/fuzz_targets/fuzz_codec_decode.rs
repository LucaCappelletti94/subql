#![no_main]

use libfuzzer_sys::fuzz_target;
use subql::test_harnesses::harness_codec_decode;

fuzz_target!(|data: &[u8]| {
    harness_codec_decode(data);
});

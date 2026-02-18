use honggfuzz::fuzz;
use subql::test_harnesses::harness_codec_decode;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            harness_codec_decode(data);
        });
    }
}

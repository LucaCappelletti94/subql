use honggfuzz::fuzz;
use subql::test_harnesses::harness_canonicalize;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            harness_canonicalize(data);
        });
    }
}

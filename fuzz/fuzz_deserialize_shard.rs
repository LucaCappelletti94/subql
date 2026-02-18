use honggfuzz::fuzz;
use subql::test_harnesses::harness_deserialize_shard;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            harness_deserialize_shard(data);
        });
    }
}

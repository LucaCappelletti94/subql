use honggfuzz::fuzz;
use subql::fuzz_helpers::FuzzCatalog;
use subql::persistence::shard::deserialize_shard;

fn main() {
    let catalog = FuzzCatalog;

    loop {
        fuzz!(|data: &[u8]| {
            // Any Err is fine; panics are bugs.
            let _ = deserialize_shard(data, &catalog);
        });
    }
}

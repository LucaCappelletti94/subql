use honggfuzz::fuzz;
use subql::persistence::codec;
use subql::persistence::shard::ShardPayload;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            // Try decoding as different types.
            // Any Err is fine; panics are bugs.
            let _ = codec::decode::<ShardPayload>(data);
            let _ = codec::decode::<Vec<u8>>(data);
            let _ = codec::decode::<String>(data);
        });
    }
}

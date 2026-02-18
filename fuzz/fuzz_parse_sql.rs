use honggfuzz::fuzz;
use subql::test_harnesses::harness_parse_sql;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            harness_parse_sql(data);
        });
    }
}

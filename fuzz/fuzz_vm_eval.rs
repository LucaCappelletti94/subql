use honggfuzz::fuzz;
use subql::test_harnesses::harness_vm_eval;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            harness_vm_eval(data);
        });
    }
}

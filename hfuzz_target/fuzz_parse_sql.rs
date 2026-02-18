use honggfuzz::fuzz;
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use subql::compiler::parse_and_compile;
use subql::fuzz_helpers::FuzzCatalog;

fn main() {
    let catalog = FuzzCatalog;
    let pg = PostgreSqlDialect {};
    let generic = GenericDialect {};

    loop {
        fuzz!(|data: &[u8]| {
            let sql = String::from_utf8_lossy(data);

            // Any Err is fine; panics are bugs.
            let _ = parse_and_compile(&sql, &pg, &catalog);
            let _ = parse_and_compile(&sql, &generic, &catalog);
        });
    }
}

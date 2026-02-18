use honggfuzz::fuzz;
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use subql::compiler::canonicalize::{hash_sql, normalize_sql};

fn main() {
    let pg = PostgreSqlDialect {};
    let generic = GenericDialect {};

    loop {
        fuzz!(|data: &[u8]| {
            let sql = String::from_utf8_lossy(data);

            for dialect in [&pg as &dyn sqlparser::dialect::Dialect, &generic] {
                if let Ok(normalized) = normalize_sql(&sql, dialect) {
                    // Determinism check: hashing the same string twice must match.
                    let h1 = hash_sql(&normalized);
                    let h2 = hash_sql(&normalized);
                    assert_eq!(h1, h2, "hash_sql is not deterministic");
                }
            }
        });
    }
}

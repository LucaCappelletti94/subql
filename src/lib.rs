#![doc = include_str!("../README.md")]

// Lint configuration is in [lints] section of Cargo.toml

// Re-export public API
pub use errors::*;
pub use runtime::{MatchedUsers, SubscriptionEngine};
pub use types::*;
pub use wal::{
    DebeziumParser, MaxwellParser, PgOutputParser, Wal2JsonV1Parser, Wal2JsonV2Parser,
    WalParseError, WalParser,
};

// Internal modules
mod errors;
mod types;

pub mod compiler;
pub mod config;
pub mod persistence;
pub mod runtime;
pub mod wal;

#[cfg(any(feature = "testing", test))]
pub mod test_harnesses;

// Version and metadata
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
pub(crate) mod testing {
    use crate::{SchemaCatalog, TableId};
    use std::collections::HashMap;

    pub struct MockCatalog {
        pub tables: HashMap<String, (TableId, usize)>,
        pub columns: HashMap<(TableId, String), u16>,
    }

    impl SchemaCatalog for MockCatalog {
        fn table_id(&self, table_name: &str) -> Option<TableId> {
            self.tables.get(table_name).map(|(id, _)| *id)
        }

        fn column_id(&self, table_id: TableId, column_name: &str) -> Option<u16> {
            self.columns
                .get(&(table_id, column_name.to_string()))
                .copied()
        }

        fn table_arity(&self, table_id: TableId) -> Option<usize> {
            self.tables
                .values()
                .find(|(id, _)| *id == table_id)
                .map(|(_, arity)| *arity)
        }

        fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
            Some(0xABCD_1234_5678_9ABC)
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_compiles() {
        assert_eq!(2 + 2, 4);
    }
}

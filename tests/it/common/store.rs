//! A temporary store directory, and the engines opened on it.
//!
//! Every persistence test opens an engine on a fresh directory, and the
//! directory has to outlive every engine that reads it, which is what
//! makes the pair worth holding together rather than repeating.

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{DefaultIds, RestoredReads, SubscriptionEngine};

/// The engine shape every store-backed test in this suite uses.
pub type StoredEngine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

/// A store directory that lives as long as the test holding it.
pub struct TempStore {
    dir: tempfile::TempDir,
}

impl TempStore {
    /// A directory with nothing in it yet.
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().expect("a temporary directory"),
        }
    }

    /// Where the shards and the reads file live.
    pub fn path(&self) -> std::path::PathBuf {
        self.dir.path().to_path_buf()
    }

    /// Open an engine on it, dropping the report of what came back.
    ///
    /// For the first open, where nothing can come back, and for a reopen
    /// whose report the test does not read.
    pub fn open(&self, database: ParserDB) -> StoredEngine {
        self.open_reporting(database).0
    }

    /// Open an engine on it, keeping the report of what came back.
    pub fn open_reporting(&self, database: ParserDB) -> (StoredEngine, RestoredReads<Postgres>) {
        StoredEngine::with_storage(database, PostgreSqlDialect {}, self.path())
            .expect("the store opens")
            .into_parts()
    }
}

impl Default for TempStore {
    fn default() -> Self {
        Self::new()
    }
}

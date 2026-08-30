use alloc::string::String;
use alloc::vec::Vec;

use rls2fga_types::{BoundQuery, RecordDescription};

use crate::ColumnId;

/// A condition parameter every question's context has to carry.
///
/// rls2fga reports these because a model carrying a request-gated grant is
/// unusable without them: a question that omits one is refused by the service
/// outright rather than answered no.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredParameter {
    /// The name a context uses for it.
    pub parameter: String,
    /// The session setting it mirrors, so a watcher can be asked for it.
    /// [`None`] means no watcher can answer it, which today is the clock.
    pub setting_key: Option<String>,
    /// Whether it holds a set rather than one value.
    ///
    /// Read off the reported separator and nothing else. The separator itself
    /// is how the values reach Postgres joined into one string, and a watcher
    /// holds them apart, so splitting one here would duplicate a rule the
    /// deployment already owns.
    pub list: bool,
}

impl RequiredParameter {
    /// Whether a watcher can be asked for this one.
    #[must_use]
    pub const fn watcher_supplied(&self) -> bool {
        self.setting_key.is_some()
    }
}

// TableShapes

/// Shapes reaching one table's rows, resolved to subql ids once.
#[derive(Debug, Default)]
pub struct TableShapes {
    /// Shapes whose records a row of this table settles on its own.
    pub settled: Vec<RecordDescription>,
    /// Queries to replay when a row of this table changes, with the columns
    /// each one binds, in the order its placeholders take them.
    pub requeries: Vec<(Vec<ColumnId>, BoundQuery)>,
}

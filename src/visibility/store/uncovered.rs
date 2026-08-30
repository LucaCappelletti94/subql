use alloc::string::String;
use alloc::vec::Vec;

use rls2fga_types::BoundQuery;

use crate::backend::{Backend, Value};

/// Why a shape's records cannot be kept current from the change stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UncoveredReason {
    /// The shape reads a column a row image cannot answer, a list column or
    /// one whose declared kind has no row-side spelling, and it carries no
    /// query to fall back on.
    UnreadableColumn,
    /// The shape reads this table but carries no query bound to it, so a
    /// change arriving here has nothing to replay.
    NoBoundQuery,
    /// The slice a replay of this shape determines is also stated by another
    /// shape, so reconciling it would delete that shape's facts.
    SharedSlice,
}

/// A shape whose records this cannot keep current, named so a caller does
/// not believe its store is complete when it is not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Uncovered {
    /// `OpenFGA` type the relation is defined on.
    pub type_name: String,
    /// Relation whose records go unmaintained.
    pub relation: String,
    /// Table whose changes cannot be turned into record movement.
    pub table: String,
    /// Which of the two gaps this is.
    pub reason: UncoveredReason,
}

/// A query to replay for one changed row, because its records span more
/// than that row.
///
/// Replaying it and handing the rows to the terminal policy's
/// `reconcile_records` is the caller's, and it is
/// due before the event is delivered. See the module's own section on when a
/// replayed query has to have finished, which says what a caller accepts by
/// deferring it. The result is the whole truth for the slice
/// [`BoundQuery::scope`](rls2fga_types::BoundQuery::scope)
/// declares, which is what makes the reconciliation able to remove.
///
/// The key is a typed [`Value`] rather than text, so the caller binds it
/// through its own type system and no cast is needed anywhere.
#[derive(Clone, Debug, PartialEq)]
pub struct Requery<'a, B: Backend> {
    /// The query `rls2fga` bound to one row of this table. Its SQL takes
    /// the key as `$1` through `$n`.
    pub query: &'a BoundQuery,
    /// The values to bind, one per column of
    /// [`BoundQuery::key_columns`](rls2fga_types::BoundQuery::key_columns)
    /// and in that order. Several of them where the key spans several columns,
    /// which is one key rather than several.
    pub key: Vec<Value<B>>,
}

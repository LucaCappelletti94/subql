use alloc::string::String;
use alloc::vec::Vec;

use rls2fga_types::BoundQuery;

use super::Materialisation;
use crate::backend::{Backend, Value};

/// Why a shape's records cannot be kept current from the change stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UncoveredReason {
    /// The shape reads a column a row image cannot answer, a list column or
    /// one whose declared kind has no row-side spelling.
    UnreadableColumn,
    /// The shape reads this table but carries no query bound to it, so a
    /// change arriving here has nothing to replay.
    NoBoundQuery,
    /// The region this shape states facts in needs one authoritative
    /// reconcile, and a producer stating facts there enumerates none of them,
    /// so the group cannot be formed.
    MissingEnumeration,
    /// The shape follows from a derivation this version does not understand,
    /// so nothing here can say what would keep it current.
    UnknownDerivation,
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

/// A query bound to one changed row, because its records span more than that
/// row.
///
/// Replaying it and handing the rows to the terminal policy's
/// `reconcile_records` is the caller's, and it is due before the event is
/// delivered. See the module's own section on when a replayed query has to
/// have finished, which says what a caller accepts by deferring it. The result
/// is the whole truth for the slice
/// [`BoundQuery::scope`](rls2fga_types::BoundQuery::scope) declares narrowed
/// to the key, which is what makes the reconciliation able to remove.
///
/// The key is a typed [`Value`] rather than text, so the caller binds it
/// through its own type system and no cast is needed anywhere.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyedRequery<'a, B: Backend> {
    /// The query `rls2fga` bound to one row of this table. Its SQL takes
    /// the key as `$1` through `$n`.
    pub query: &'a BoundQuery,
    /// The values to bind, one per column of
    /// [`BoundQuery::key_columns`](rls2fga_types::BoundQuery::key_columns)
    /// and in that order. Several of them where the key spans several columns,
    /// which is one key rather than several.
    pub key: Vec<Value<B>>,
}

/// What a changed row obliges the caller to re-run.
///
/// An enum rather than one struct with an optional key, so a consumer cannot
/// keep compiling while silently handling only the kind it knew about. A
/// keyless replay handled as though it were keyed would reconcile the wrong
/// region, and one ignored entirely would leave `uncovered()` quiet about
/// facts nothing maintains.
#[derive(Clone, Debug, PartialEq)]
pub enum Requery<'a, B: Backend> {
    /// One query narrowed to the changed row, reconciled against the slice its
    /// key names.
    Keyed(KeyedRequery<'a, B>),
    /// Every producer stating facts in one region, reconciled as a unit.
    ///
    /// Carries the whole group rather than the member the change arrived on,
    /// because reconciling one member's rows over the region deletes what its
    /// siblings state there. The group is handed to the terminal policy's
    /// `materialise`, which runs every member itself.
    Whole(&'a Materialisation),
}

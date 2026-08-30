//! What one changed row moves in the authorization store.
//!
//! A per-row answer is worthless if the facts behind it are stale, so the
//! change stream also drives the store. For each changed row this reports
//! the facts it added, the facts it removed, and the queries the caller has
//! to replay for the facts no single row settles.
//!
//! # A wider index than the answering one
//!
//! [`RowPolicy`](crate::visibility::policy::RowPolicy) keys recipes by the
//! table whose rows the object is keyed on, because that is what makes a
//! relation answerable from one row. This cannot, because a shape may
//! describe one type from a different table: a membership row describes a
//! team, so a change to the membership table moves records on the team.
//! Every shape whose row table matches the changed row is therefore in
//! scope, across every relation and every type, and whether one row decides
//! the relation is irrelevant here.
//!
//! # Three kinds of shape, three outcomes
//!
//! A shape the row settles yields records directly, and the difference
//! between the two images is what moved. A shape whose records span two
//! tables cannot be differenced from one of them, so the query
//! `rls2fga` already bound to one row is handed over with the key read
//! off the row, and the caller runs it: the result is the whole truth for
//! the slice the query declares, so reconciling it both writes what is new
//! and takes out what the result stopped returning. A shape that can be
//! neither differenced nor replayed is named in
//! [`Shapes::uncovered`](crate::visibility::shapes::Shapes::uncovered) and nothing else, because a caller that
//! believes its store is complete when it is not is the failure this whole
//! path exists to remove. That covers a shape in a list column, and one
//! whose slice another shape also states, which reconciling would clobber.
//!
//! # When a replayed query has to have finished
//!
//! Applying the difference is not the caller's job, and replaying the
//! queries is. The records this reports reach the store through the terminal
//! policy's own `apply`, which orders the write against the questions that
//! read it. A replayed
//! query is the one part that cannot work that way, because its rows come
//! from a database only the caller can reach, and it carries an obligation
//! worth stating rather than leaving to be discovered.
//!
//! **Replay the query and hand what it returned to the terminal policy's
//! `reconcile_records` before the event is delivered.** Until then the store
//! still holds the facts from before the
//! change, so a question about any row those facts reach is answered from a
//! world that has moved. In the deny direction that costs a row delivered
//! late. In the allow direction it is a row handed to somebody whose access
//! has already gone, which is the one failure this path exists to remove,
//! and no later correction takes the row back.
//!
//! The cost is real and it is the reason this sentence exists rather than a
//! default: the change path then waits on one database read per affected
//! shape. A caller that would rather not wait can defer the replay, and is
//! thereby choosing a window whose width is its own replay lag, during which
//! a withdrawn permission still reads. That is a decision to take
//! deliberately, so it is written here rather than inferred from silence.
//!
//! # Refusing rather than emitting part of it
//!
//! A difference that is right about some shapes and silently wrong about
//! others is worse than no difference, so anything unreadable refuses the
//! whole event. That includes a previous image carrying only its key, which
//! cannot say what the row granted, and which would otherwise report every
//! fact as added and leave the old ones in the store forever.
//!
//! What `REPLICA IDENTITY FULL` settles, and what it does not. It gives an
//! update and a delete a complete previous image. It does not give an
//! insert one, which is structural rather than configured, and an insert is
//! simply all additions. And it does not reach the SQLite changeset source,
//! where an update carries the key plus the changed columns and marks the
//! rest absent on both sides. That case is correct here for a reason worth
//! stating rather than inheriting: a column that did not change reads
//! absent on both sides, yields no record on either, and so moves nothing.

pub(crate) mod diff;
pub(crate) mod store_diff;
pub(crate) mod uncovered;

pub(crate) use diff::name_gap;
pub use store_diff::{StoreDiff, StoreDiffError};
pub use uncovered::{Requery, Uncovered, UncoveredReason};

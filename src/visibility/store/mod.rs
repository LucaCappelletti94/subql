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
//! # Four kinds of shape, and who maintains each
//!
//! A shape a row settles, alone in the region it states facts in, yields
//! records directly and the difference between the two images is what moved. A
//! shape whose records span two tables, alone in its region, hands over the
//! query `rls2fga` bound to one row with the key read off the row, and the
//! caller runs it: the result is the whole truth for the slice that key names.
//!
//! A shape whose records depend on rows the changed row does not name carries
//! no key at all, because a whole-table aggregate moves records keyed on rows
//! that never changed. And a region more than one shape states facts in cannot
//! be reconciled by any one of them, since deleting what one replay stopped
//! returning would delete what its siblings still state. Both are answered the
//! same way: the region becomes a [`Materialisation`], every member's
//! unnarrowed query runs, the rows are unioned, and the region is reconciled
//! once. That is one authoritative operation per region, so a shape under a
//! group's authority is neither differenced nor keyed-replayed beside it.
//!
//! A shape none of that reaches is named in
//! [`Shapes::uncovered`](crate::visibility::shapes::Shapes::uncovered) and
//! nothing else, because a caller that believes its store is complete when it
//! is not is the failure this whole path exists to remove. That covers a shape
//! in a list column, and a region holding a shape nothing enumerates.
//!
//! # The load runs the same operation
//!
//! [`Shapes::materialisations`](crate::visibility::shapes::Shapes::materialisations)
//! is every group. The load runs all of them and an event runs the ones it
//! obliged, which is the same call, so the load heals whatever drifted and the
//! replay path is exercised at startup rather than only by a change that
//! happens to arrive.
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
pub(crate) mod group;
pub(crate) mod store_diff;
pub(crate) mod uncovered;

pub(crate) use diff::name_gap;
pub use group::{Enumeration, Materialisation, Region, RegionPart, Replay, Replayer};
pub use store_diff::{StoreDiff, StoreDiffError};
pub use uncovered::{KeyedRequery, Requery, Uncovered, UncoveredReason};

//! Layer 2: maintenance.
//!
//! Per-query state machines that consume CDC events and decide, in-process,
//! whether the query's result is unchanged, has a newly-computed value, or
//! cannot be maintained without a database re-query. This layer NEVER
//! touches the database: it only reads cells through the event's `value_at`
//! accessor and evaluates the query's WHERE clause via the engine VM.

use crate::backend::{Backend, CdcEvent, RowKind, Value};
use crate::compiler::sql_shape::ScalarAggKind;
use crate::compiler::value_cmp::{compare_ordered_values, values_equal};
use crate::compiler::{BytecodeProgram, Tri, Vm};
use crate::{ColumnId, EventKind};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cmp::Ordering;
use sql_traits::prelude::DatabaseLike;

/// Outcome of feeding one CDC event to a maintained query.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Maintenance<B: Backend> {
    /// The event does not change the query's result.
    Unchanged,
    /// The event produced a new result value in-process.
    Updated(Value<B>),
    /// The maintenance state machine cannot decide in-process; the caller
    /// must re-execute against the authoritative store and call
    /// [`MaintainedQuery::install`] with the new value.
    NeedsReexecution,
}

/// A query maintained by the re-execution layer.
///
/// Implementors never touch the database. When they cannot decide
/// in-process they return [`Maintenance::NeedsReexecution`]. The engine
/// then surfaces a [`ReExecutionTrigger`](super::ReExecutionTrigger) for
/// the Subscription Materializer, which re-runs the SQL and calls
/// [`install`](Self::install) with the recomputed value.
pub(super) trait MaintainedQuery<B: Backend> {
    /// Feed a CDC event. `vm` is lent for WHERE-membership evaluation.
    fn on_event<E, DB>(&mut self, event: &E, vm: &mut Vm<B>, db: &DB) -> Maintenance<B>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike;

    /// Adopt a value produced by the materializer's re-execution.
    fn install(&mut self, value: Value<B>);

    /// Columns whose change can affect the result.
    fn dependency_columns(&self) -> &[ColumnId];
}

/// Incrementally-maintained single-table scalar `MIN` / `MAX`.
///
/// Inserts and most updates / deletes are handled in memory. A database
/// re-query is required only when the current extreme value is removed
/// or displaced (then we cannot know the next extreme without scanning),
/// or when an event's row image is too incomplete to decide.
pub(super) struct MinMaxQuery<B: Backend> {
    kind: ScalarAggKind,
    agg_column: ColumnId,
    where_program: Arc<BytecodeProgram<B>>,
    dependency_columns: Vec<ColumnId>,
    /// Current extreme. `Value::Null` means the (filtered) set is empty.
    current: Value<B>,
}

impl<B: Backend> MinMaxQuery<B> {
    pub(super) const fn new(
        kind: ScalarAggKind,
        agg_column: ColumnId,
        where_program: Arc<BytecodeProgram<B>>,
        dependency_columns: Vec<ColumnId>,
        initial: Value<B>,
    ) -> Self {
        Self {
            kind,
            agg_column,
            where_program,
            dependency_columns,
            current: initial,
        }
    }

    /// Whether the `row` view of `event` satisfies the query's WHERE
    /// clause (only `Tri::True` counts. NULL / Unknown excludes the row,
    /// per SQL).
    fn matches<E, DB>(&self, event: &E, row: RowKind, vm: &mut Vm<B>, db: &DB) -> bool
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        matches!(vm.eval(&self.where_program, event, row, db), Ok(Tri::True))
    }

    /// The aggregated column's value from the `row` view of `event`.
    fn agg_value<E, DB>(&self, event: &E, row: RowKind, db: &DB) -> Value<B>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        event
            .value_at(db, row, self.agg_column)
            .unwrap_or(Value::Missing)
    }

    /// Whether any column the query depends on is absent (`Missing`) in
    /// the `row` view of `event` (a sparse image we cannot reason about).
    fn any_dependency_missing<E, DB>(&self, event: &E, row: RowKind, db: &DB) -> bool
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        self.dependency_columns.iter().any(|&col| {
            event
                .value_at(db, row, col)
                .map_or(true, |v| v.is_missing())
        })
    }

    /// Whether `candidate` would become the new extreme versus `current`.
    /// A non-present candidate (NULL / Missing) never participates. Into
    /// an empty set (current Null) any present value wins.
    fn is_more_extreme(&self, candidate: &Value<B>) -> bool {
        if candidate.is_absent() {
            return false;
        }
        if self.current.is_null() {
            return true;
        }
        let wins: fn(Ordering) -> bool = match self.kind {
            ScalarAggKind::Min => |o| o == Ordering::Less,
            ScalarAggKind::Max => |o| o == Ordering::Greater,
        };
        matches!(
            compare_ordered_values(candidate, &self.current, wins),
            Tri::True
        )
    }

    /// Insert half: fold a (matching) new row into the extreme. Never
    /// forces a re-query.
    fn on_insert_row<E, DB>(
        &mut self,
        event: &E,
        row: RowKind,
        vm: &mut Vm<B>,
        db: &DB,
    ) -> Maintenance<B>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        if !self.matches(event, row, vm, db) {
            return Maintenance::Unchanged;
        }
        let candidate = self.agg_value(event, row, db);
        if self.is_more_extreme(&candidate) {
            self.current = candidate;
            Maintenance::Updated(self.current.clone())
        } else {
            Maintenance::Unchanged
        }
    }

    /// Delete half: decide whether removing `row` displaces the extreme.
    /// Does not mutate state. Returns only `Unchanged` or
    /// `NeedsReexecution`.
    fn on_delete_row<E, DB>(
        &self,
        event: &E,
        row: RowKind,
        vm: &mut Vm<B>,
        db: &DB,
    ) -> Maintenance<B>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        if self.any_dependency_missing(event, row, db) {
            return Maintenance::NeedsReexecution;
        }
        if !self.matches(event, row, vm, db) {
            return Maintenance::Unchanged;
        }
        let value = self.agg_value(event, row, db);
        if !value.is_absent() && values_equal(&value, &self.current) {
            // The current extreme (or a tie of it) was removed, the next
            // extreme is unknown without a scan.
            Maintenance::NeedsReexecution
        } else {
            Maintenance::Unchanged
        }
    }
}

impl<B: Backend> MaintainedQuery<B> for MinMaxQuery<B> {
    fn on_event<E, DB>(&mut self, event: &E, vm: &mut Vm<B>, db: &DB) -> Maintenance<B>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        match event.kind() {
            EventKind::Insert => self.on_insert_row(event, RowKind::New, vm, db),
            EventKind::Delete => self.on_delete_row(event, RowKind::Old, vm, db),
            EventKind::Update => match self.on_delete_row(event, RowKind::Old, vm, db) {
                // The extreme was displaced: a fresh scan also reflects
                // the insert half, so re-query covers both.
                Maintenance::NeedsReexecution => Maintenance::NeedsReexecution,
                _ => self.on_insert_row(event, RowKind::New, vm, db),
            },
            EventKind::Truncate => {
                if self.current.is_null() {
                    Maintenance::Unchanged
                } else {
                    self.current = Value::Null;
                    Maintenance::Updated(Value::Null)
                }
            }
        }
    }

    fn install(&mut self, value: Value<B>) {
        self.current = value;
    }

    fn dependency_columns(&self) -> &[ColumnId] {
        &self.dependency_columns
    }
}

/// Enum-dispatch wrapper holding any maintained query.
///
/// A future `Total` variant (single-table row re-execution) and an
/// aggregate-re-execution variant plug in here without disturbing the
/// engine. Each services the same
/// [`ReExecutionTrigger`](super::ReExecutionTrigger) seam and is
/// `install`ed identically by the Subscription Materializer.
pub(super) enum QueryRuntime<B: Backend> {
    Partial(MinMaxQuery<B>),
    /// Re-read in full on any relevant change, holding nothing.
    Total(TotalQuery),
}

impl<B: Backend> QueryRuntime<B> {
    pub(super) fn on_event<E, DB>(&mut self, event: &E, vm: &mut Vm<B>, db: &DB) -> Maintenance<B>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        match self {
            Self::Partial(q) => q.on_event(event, vm, db),
            Self::Total(q) => q.on_event(event, vm, db),
        }
    }

    pub(super) fn install(&mut self, value: Value<B>) {
        match self {
            Self::Partial(q) => q.install(value),
            Self::Total(q) => MaintainedQuery::<B>::install(q, value),
        }
    }

    pub(super) fn dependency_columns(&self) -> &[ColumnId] {
        match self {
            Self::Partial(q) => q.dependency_columns(),
            Self::Total(q) => MaintainedQuery::<B>::dependency_columns(q),
        }
    }
}

// Test body deferred to Phase 10 per docs/refactor-cdc-event-handoff.md.

/// A query whose answer subql cannot maintain at all, only re-read.
///
/// The catch-all tier: a filter the in-process predicate language cannot
/// evaluate, a `DISTINCT`, a set operation, a computed projection. Every event
/// touching a table it reads means the answer may have moved, and since nothing
/// about the answer is held there is nothing to compare against, so the honest
/// response to every event is [`Maintenance::NeedsReexecution`].
///
/// Holding no state is the point rather than a shortcut: the alternative is a
/// copy of every captured result set in memory, which is the cost that was
/// declined when whole-result delivery was chosen over difference delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TotalQuery {
    /// Every column of every table the query reads.
    ///
    /// A computed projection can depend on any column, so narrowing this would
    /// mean guessing which UPDATEs matter. The UPDATE filter in the engine
    /// treats an empty list as "nothing depends on anything" and skips the
    /// event, so this must be the full set rather than empty.
    dependency_columns: Vec<crate::ColumnId>,
}

impl TotalQuery {
    pub(super) const fn new(dependency_columns: Vec<crate::ColumnId>) -> Self {
        Self { dependency_columns }
    }
}

impl<B: Backend> MaintainedQuery<B> for TotalQuery {
    fn on_event<E: crate::backend::CdcEvent<Backend = B>, DB: DatabaseLike>(
        &mut self,
        _event: &E,
        _vm: &mut crate::compiler::Vm<B>,
        _database: &DB,
    ) -> Maintenance<B> {
        Maintenance::NeedsReexecution
    }

    fn install(&mut self, _value: Value<B>) {
        // Nothing to install: the tier holds no answer, so a re-read is
        // delivered to the consumer rather than stored here.
    }

    fn dependency_columns(&self) -> &[crate::ColumnId] {
        &self.dependency_columns
    }
}

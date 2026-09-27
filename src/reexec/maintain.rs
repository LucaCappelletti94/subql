//! Layer 2: maintenance.
//!
//! Per-query state machines that consume CDC events and decide, in-process,
//! whether the query's result is unchanged, has a newly-computed value, or
//! cannot be maintained without a database re-query. This layer NEVER
//! touches the database: it only reads cells through the event's `value_at`
//! accessor and evaluates the query's WHERE clause via the engine VM.

use crate::backend::ComparisonContext;
use crate::backend::{Backend, CdcEvent, RowKind, ScalarText, Value};
use crate::checkpoint::{ReadFence, Seen, UnseenLog};
use crate::compiler::literals::SqlLiteralParse;
use crate::compiler::sql_shape::ScalarAggKind;
use crate::compiler::value_cmp::{compare_ordered_values, values_equal};
use crate::compiler::{BytecodeProgram, Tri, Vm};
use crate::{Checkpoint, ColumnId, EventKind};
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cmp::Ordering;
use hashbrown::HashMap;
use sql_traits::prelude::DatabaseLike;

/// Outcome of feeding one CDC event to a maintained query.
#[derive(Debug, Clone, PartialEq)]
pub enum Maintenance<B: Backend> {
    /// The event does not change the query's result.
    Unchanged,
    /// The event produced a new result value in-process.
    Updated(Value<B>),
    /// The maintenance state machine cannot decide in-process, so the caller
    /// must re-execute against the authoritative store and install the new
    /// value.
    NeedsReexecution,
}

/// A read tier that holds no answer, only which reads a change calls for.
///
/// Implementors never touch the database. When they cannot decide
/// in-process they return [`Maintenance::NeedsReexecution`]. The engine
/// then surfaces a [`ReExecutionTrigger`](super::ReExecutionTrigger) for
/// the Subscription Materializer, which re-runs the SQL.
pub trait MaintainedQuery<B: Backend> {
    /// Feed a CDC event. `vm` is lent for WHERE-membership evaluation.
    fn on_event<E, DB>(&mut self, event: &E, vm: &mut Vm<B>, db: &DB) -> Maintenance<B>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike;

    /// Columns whose change can affect the result.
    fn dependency_columns(&self) -> &[ColumnId];
}

/// What one change does to a scalar extreme, decided from the event once so
/// it can be applied live and again on top of a later read.
struct ExtremeChange<B: Backend> {
    /// A matching row left, or `None` when no matching row did.
    removed: Option<Removed<B>>,
    /// The aggregated value of a matching row that arrived.
    added: Option<Value<B>>,
    emptied: bool,
}

enum Removed<B: Backend> {
    /// A column the query depends on is missing from the old row.
    Unknown,
    Row(Value<B>),
}

/// The fence of the engine's latest fenced install, shared by every value.
///
/// A value may forget the entries it holds unless a read of the value is
/// outstanding that was asked before that install, since such a read may take
/// an older snapshot. Every later read takes a newer one.
pub struct LatestFence<C: Checkpoint> {
    /// Fenced installs so far, which also stamps when a read is asked.
    installs: u64,
    latest: Option<C::Fence>,
    /// A fence-only read was asked and no fence has been installed since.
    /// Any fenced install ends the wait, so a probe a caller dropped is asked
    /// again once a log still wants one.
    pub(crate) probing: bool,
}

impl<C: Checkpoint> LatestFence<C> {
    pub const fn new() -> Self {
        Self {
            installs: 0,
            latest: None,
            probing: false,
        }
    }

    /// Remember `fence` as the newest, before the install that brought it.
    pub fn record(&mut self, fence: &C::Fence) -> u64 {
        self.installs += 1;
        self.latest = Some(fence.clone());
        self.probing = false;
        self.installs
    }

    /// The stamp of a read asked now.
    pub const fn now(&self) -> u64 {
        self.installs
    }

    /// The latest fence, if a value whose read was asked at `asked` may trim
    /// by it.
    pub(crate) fn trims(&self, asked: Option<u64>) -> Option<&C::Fence> {
        let fence = self.latest.as_ref()?;
        asked
            .is_none_or(|asked| asked >= self.installs)
            .then_some(fence)
    }
}

impl<C: Checkpoint> Default for LatestFence<C> {
    fn default() -> Self {
        Self::new()
    }
}

/// What installing a read into a scalar extreme produced.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarInstallOutcome<B: Backend> {
    /// The value after the read and every change it missed.
    Value(Value<B>),
    /// Only another read can say what the value is.
    ReadAgain,
}

/// Incrementally-maintained single-table scalar `MIN` / `MAX`.
///
/// Inserts and most updates / deletes are handled in memory. A database
/// re-query is required only when the current extreme value is removed
/// or displaced (then we cannot know the next extreme without scanning),
/// or when an event's row image is too incomplete to decide.
pub struct MinMaxQuery<B: Backend, C: Checkpoint> {
    kind: ScalarAggKind,
    agg_column: ColumnId,
    where_program: Arc<BytecodeProgram<B>>,
    dependency_columns: Vec<ColumnId>,
    database_reads_per_consumer: bool,
    /// The extreme, once known. `Some(Value::Null)` means the filtered set is
    /// empty, `None` means nobody has said yet, which no change can decide.
    current: Option<Value<B>>,
    unseen: UnseenLog<C, ExtremeChange<B>>,
    /// When the outstanding read was asked, per [`LatestFence::now`].
    asked: Option<u64>,
    fence: ReadFence<C>,
}

impl<B: Backend, C: Checkpoint> MinMaxQuery<B, C> {
    pub const fn new(
        kind: ScalarAggKind,
        agg_column: ColumnId,
        where_program: Arc<BytecodeProgram<B>>,
        dependency_columns: Vec<ColumnId>,
        database_reads_per_consumer: bool,
    ) -> Self {
        Self {
            kind,
            agg_column,
            where_program,
            dependency_columns,
            database_reads_per_consumer,
            current: None,
            unseen: UnseenLog::new(),
            // The first read is asked at registration, before any install.
            asked: Some(0),
            fence: ReadFence::none(),
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

    fn change<E, DB>(&self, event: &E, vm: &mut Vm<B>, db: &DB) -> ExtremeChange<B>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        let kind = event.kind();
        let removed = matches!(kind, EventKind::Delete | EventKind::Update)
            .then(|| {
                if self.any_dependency_missing(event, RowKind::Old, db) {
                    Some(Removed::Unknown)
                } else {
                    self.matches(event, RowKind::Old, vm, db)
                        .then(|| Removed::Row(self.agg_value(event, RowKind::Old, db)))
                }
            })
            .flatten();
        let added = matches!(kind, EventKind::Insert | EventKind::Update)
            .then(|| {
                self.matches(event, RowKind::New, vm, db)
                    .then(|| self.agg_value(event, RowKind::New, db))
            })
            .flatten();
        ExtremeChange {
            removed,
            added,
            emptied: kind == EventKind::Truncate,
        }
    }

    /// Whether `candidate` would become the new extreme, or `None` when the
    /// current one is unknown and no comparison can be made.
    /// A non-present candidate (NULL / Missing) never participates. Into an
    /// empty set (`Some(Null)`) any present value wins.
    fn is_more_extreme(&self, candidate: &Value<B>) -> Option<bool> {
        let current = self.current.as_ref()?;
        if candidate.is_absent() {
            return Some(false);
        }
        if current.is_null() {
            return Some(true);
        }
        let wins: fn(Ordering) -> bool = match self.kind {
            ScalarAggKind::Min => |o| o == Ordering::Less,
            ScalarAggKind::Max => |o| o == Ordering::Greater,
        };
        // A refusal is undecidable, which is exactly what `None` already
        // means here: the caller reads the database instead. Cross-kind
        // cannot arise on one column's own values, so this is a guard
        // rather than a live path, and a guard is the point: assuming it
        // unreachable is how the comparison came to drop rows in silence.
        Some(matches!(
            compare_ordered_values(ComparisonContext::none(), candidate, current, wins).ok()?,
            Tri::True
        ))
    }

    fn apply(&mut self, change: &ExtremeChange<B>) -> Maintenance<B> {
        if change.emptied {
            // The table is empty afterwards, so this resolves an unknown
            // extreme as well as replacing a known one.
            if self.current.as_ref().is_some_and(Value::is_null) {
                return Maintenance::Unchanged;
            }
            self.current = Some(Value::Null);
            return Maintenance::Updated(Value::Null);
        }
        match &change.removed {
            Some(Removed::Unknown) => return Maintenance::NeedsReexecution,
            Some(Removed::Row(value)) => {
                let Some(current) = self.current.as_ref() else {
                    return Maintenance::NeedsReexecution;
                };
                // A refusal cannot say whether the extreme left, so the safe
                // answer is the one that asks the database. A fresh scan also
                // reflects the insert half of an update.
                if !value.is_absent()
                    && values_equal(ComparisonContext::none(), value, current).unwrap_or(true)
                {
                    return Maintenance::NeedsReexecution;
                }
            }
            None => {}
        }
        let Some(candidate) = &change.added else {
            return Maintenance::Unchanged;
        };
        match self.is_more_extreme(candidate) {
            // Nobody has said what the extreme is, so this row cannot be it:
            // the table may hold a more extreme one this engine never saw.
            None => Maintenance::NeedsReexecution,
            Some(true) if self.database_reads_per_consumer => Maintenance::NeedsReexecution,
            Some(true) => {
                self.current = Some(candidate.clone());
                Maintenance::Updated(candidate.clone())
            }
            Some(false) => Maintenance::Unchanged,
        }
    }

    /// Feed one change. A change the last read already holds is dropped, and
    /// every other positioned one is kept until a fence shows it held.
    pub fn on_event<E, DB>(
        &mut self,
        event: &E,
        vm: &mut Vm<B>,
        db: &DB,
        cap: usize,
        latest: &LatestFence<C>,
    ) -> Maintenance<B>
    where
        E: CdcEvent<Backend = B, Checkpoint = C>,
        DB: DatabaseLike,
    {
        let at = event.checkpoint();
        if !self.fence.admits(at.as_ref()) {
            return Maintenance::Unchanged;
        }
        let change = self.change(event, vm, db);
        let outcome = self.apply(&change);
        if matches!(outcome, Maintenance::NeedsReexecution) && self.asked.is_none() {
            self.asked = Some(latest.now());
        }
        // An unpositioned change can never be judged against a fence.
        if let Some(at) = at {
            self.unseen.push(at, change, cap);
            if self.unseen.wants_fence(cap) {
                self.forget_seen(latest);
            }
        }
        outcome
    }

    /// Drop the kept changes the engine's latest fence holds, when that is
    /// safe for the read outstanding here.
    pub fn forget_seen(&mut self, latest: &LatestFence<C>) {
        if let Some(fence) = latest.trims(self.asked) {
            self.unseen.forget_held(fence);
        }
    }

    /// Whether the kept changes are near their cap and a fence-only read
    /// should trim them.
    pub const fn wants_fence(&self, cap: usize) -> bool {
        self.unseen.wants_fence(cap)
    }

    /// Adopt a read's answer, then apply every kept change its snapshot did
    /// not hold. `asked` stamps a read asked again.
    ///
    /// Without a fence nothing kept can be judged, so the answer is taken as
    /// it is. A change the read held leaves the log. One it missed stays,
    /// since the next read may miss it too. A missed change that removes the
    /// answer asks for another read.
    pub fn install(
        &mut self,
        value: Value<B>,
        fence: Option<C::Fence>,
        asked: u64,
    ) -> ScalarInstallOutcome<B> {
        self.current = Some(value);
        let Some(fence) = fence else {
            self.unseen = UnseenLog::new();
            self.asked = None;
            self.fence = ReadFence::none();
            return ScalarInstallOutcome::Value(self.current.clone().expect("just set"));
        };
        if self.unseen.overflowed {
            self.current = None;
            self.unseen = UnseenLog::new();
            self.asked = Some(asked);
            return ScalarInstallOutcome::ReadAgain;
        }
        let mut unseen = core::mem::take(&mut self.unseen);
        let mut removed = false;
        unseen.entries.retain(|(at, change)| {
            if at.seen_by(&fence) == Seen::Held {
                return false;
            }
            removed = removed || matches!(self.apply(change), Maintenance::NeedsReexecution);
            true
        });
        self.unseen = unseen;
        self.fence = ReadFence::new(Some(fence));
        if removed {
            self.asked = Some(asked);
            return ScalarInstallOutcome::ReadAgain;
        }
        self.asked = None;
        ScalarInstallOutcome::Value(self.current.clone().expect("the install set the extreme"))
    }

    pub fn dependency_columns(&self) -> &[ColumnId] {
        &self.dependency_columns
    }
}

struct GroupedExtreme<B: Backend, C: Checkpoint> {
    values: Vec<Value<B>>,
    current: Value<B>,
    rows: i64,
    /// The extreme the consumer last saw for this group, `None` while the
    /// group is outside the announced result. Emissions are diffs against
    /// this, so a value that moved under a pending read is announced by the
    /// read's install rather than twice or never.
    announced: Option<Value<B>>,
    /// The fence of the scoped read that last set the group.
    fence: ReadFence<C>,
}

impl<B: Backend, C: Checkpoint> GroupedExtreme<B, C> {
    fn identity(&self, key: &[u8]) -> crate::GroupIdentity<B> {
        crate::GroupIdentity {
            key: key.to_vec(),
            values: self.values.clone(),
        }
    }

    fn into_identity(self, key: Vec<u8>) -> crate::GroupIdentity<B> {
        crate::GroupIdentity {
            key,
            values: self.values,
        }
    }
}

struct ExtremeRow<B: Backend> {
    key: Vec<u8>,
    values: Vec<Value<B>>,
    value: Value<B>,
}

impl<B: Backend> Clone for ExtremeRow<B> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            values: self.values.clone(),
            value: self.value.clone(),
        }
    }
}

enum GroupedRowChange<B: Backend> {
    Insert(ExtremeRow<B>),
    Delete(ExtremeRow<B>),
    Refresh { key: Vec<u8>, values: Vec<Value<B>> },
    MissingGroup,
}

impl<B: Backend> Clone for GroupedRowChange<B> {
    fn clone(&self) -> Self {
        match self {
            Self::Insert(row) => Self::Insert(row.clone()),
            Self::Delete(row) => Self::Delete(row.clone()),
            Self::Refresh { key, values } => Self::Refresh {
                key: key.clone(),
                values: values.clone(),
            },
            Self::MissingGroup => Self::MissingGroup,
        }
    }
}

impl<B: Backend> GroupedRowChange<B> {
    fn key(&self) -> Option<&[u8]> {
        match self {
            Self::Insert(row) | Self::Delete(row) => Some(&row.key),
            Self::Refresh { key, .. } => Some(key),
            Self::MissingGroup => None,
        }
    }
}

/// One change a scoped read's answer may lack, in stream order.
enum GroupEntry<'a, B: Backend> {
    Row(&'a GroupedRowChange<B>),
    Emptied,
}

/// A scoped read asked for one group.
struct GroupRead<B: Backend> {
    values: Vec<Value<B>>,
    /// When it was asked, per [`LatestFence::now`].
    asked: u64,
}

#[derive(Clone)]
enum PendingGroupedEvent<B: Backend> {
    Rows(Vec<GroupedRowChange<B>>),
    Truncate,
}

struct PendingGrouped<B: Backend, C: Checkpoint> {
    events: Vec<(Option<C>, PendingGroupedEvent<B>)>,
    overflowed: bool,
}

impl<B: Backend, C: Checkpoint> PendingGrouped<B, C> {
    const fn new() -> Self {
        Self {
            events: Vec::new(),
            overflowed: false,
        }
    }

    fn push(&mut self, checkpoint: Option<&C>, event: PendingGroupedEvent<B>, cap: usize) {
        if self.events.len() >= cap {
            self.overflowed = true;
            return;
        }
        self.events.push((checkpoint.cloned(), event));
    }
}

enum ObservedRow<B: Backend> {
    Excluded,
    Included(ExtremeRow<B>),
    Refresh { key: Vec<u8>, values: Vec<Value<B>> },
    MissingGroup,
}

pub struct GroupedRead<B: Backend, C: Checkpoint> {
    pub group: Vec<u8>,
    pub query: crate::reexec::BoundQuery<B>,
    pub column_kinds: [crate::backend::ScalarFamily; 2],
    pub checkpoint: Option<C>,
}

type GroupedValueChange<B> = (crate::GroupIdentity<B>, crate::AggregateValueChange<B>);

pub struct GroupedMaintenance<B: Backend, C: Checkpoint> {
    pub changes: Vec<GroupedValueChange<B>>,
    pub reads: Vec<GroupedRead<B, C>>,
    pub group_limit: bool,
    pub missing_group: bool,
}

impl<B: Backend, C: Checkpoint> GroupedMaintenance<B, C> {
    const fn empty() -> Self {
        Self {
            changes: Vec::new(),
            reads: Vec::new(),
            group_limit: false,
            missing_group: false,
        }
    }
}

pub struct GroupedMinMaxQuery<B: Backend, C: Checkpoint> {
    plan: crate::reexec::plan::GroupedMinMaxPlan<B>,
    groups: HashMap<Vec<u8>, GroupedExtreme<B, C>>,
    pending: Option<PendingGrouped<B, C>>,
    /// The seed read's fence, judging every group no scoped read has set.
    fence: ReadFence<C>,
    pending_reads: HashMap<Vec<u8>, GroupRead<B>>,
    /// Each group's changes no read of it has shown held, kept apart from the
    /// group so they outlive its removal.
    unseen: HashMap<Vec<u8>, UnseenLog<C, GroupedRowChange<B>>>,
    /// Truncates no fence has shown held, which every group's read replays.
    unseen_truncates: UnseenLog<C, ()>,
    /// A log crossed half its cap since the last fence-only read.
    wants_fence: bool,
    database_reads_per_consumer: bool,
}

impl<B: Backend + SqlLiteralParse, C: Checkpoint> GroupedMinMaxQuery<B, C> {
    pub fn new(
        plan: crate::reexec::plan::GroupedMinMaxPlan<B>,
        database_reads_per_consumer: bool,
    ) -> Self {
        Self {
            plan,
            groups: HashMap::new(),
            pending: Some(PendingGrouped::new()),
            fence: ReadFence::none(),
            pending_reads: HashMap::new(),
            unseen: HashMap::new(),
            unseen_truncates: UnseenLog::new(),
            wants_fence: false,
            database_reads_per_consumer,
        }
    }

    pub fn dependency_columns(&self) -> &[ColumnId] {
        &self.plan.dependency_columns
    }

    fn observe<E, DB>(
        &self,
        event: &E,
        row: RowKind,
        vm: &mut Vm<B>,
        db: &DB,
    ) -> Result<ObservedRow<B>, crate::ValueError>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        let values = self
            .plan
            .group_columns
            .iter()
            .map(|column| event.value_at(db, row, *column))
            .collect::<Result<Vec<_>, _>>()?;
        if values.iter().any(Value::is_missing) {
            return Ok(ObservedRow::MissingGroup);
        }
        let Some(key) = self.plan.group_key_encoder.encode(&values) else {
            return Ok(ObservedRow::MissingGroup);
        };
        if self.plan.where_dependency_columns.iter().any(|column| {
            event
                .value_at(db, row, *column)
                .map_or(true, |value| value.is_missing())
        }) {
            return Ok(ObservedRow::Refresh { key, values });
        }
        if !matches!(
            vm.eval(&self.plan.where_program, event, row, db),
            Ok(Tri::True)
        ) {
            return Ok(ObservedRow::Excluded);
        }
        let value = event.value_at(db, row, self.plan.agg_column)?;
        if value.is_missing() {
            return Ok(ObservedRow::Refresh { key, values });
        }
        Ok(ObservedRow::Included(ExtremeRow { key, values, value }))
    }

    fn event_changes<E, DB>(
        &self,
        event: &E,
        vm: &mut Vm<B>,
        db: &DB,
    ) -> Result<PendingGroupedEvent<B>, crate::ValueError>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        if event.kind() == EventKind::Truncate {
            return Ok(PendingGroupedEvent::Truncate);
        }
        let mut changes = Vec::with_capacity(2);
        if matches!(event.kind(), EventKind::Delete | EventKind::Update) {
            match self.observe(event, RowKind::Old, vm, db)? {
                ObservedRow::Excluded => {}
                ObservedRow::Included(row) => changes.push(GroupedRowChange::Delete(row)),
                ObservedRow::Refresh { key, values } => {
                    changes.push(GroupedRowChange::Refresh { key, values });
                }
                ObservedRow::MissingGroup => changes.push(GroupedRowChange::MissingGroup),
            }
        }
        if matches!(event.kind(), EventKind::Insert | EventKind::Update) {
            match self.observe(event, RowKind::New, vm, db)? {
                ObservedRow::Excluded => {}
                ObservedRow::Included(row) => changes.push(GroupedRowChange::Insert(row)),
                ObservedRow::Refresh { key, values } => {
                    changes.push(GroupedRowChange::Refresh { key, values });
                }
                ObservedRow::MissingGroup => changes.push(GroupedRowChange::MissingGroup),
            }
        }
        Ok(PendingGroupedEvent::Rows(changes))
    }

    fn candidate_wins(kind: ScalarAggKind, candidate: &Value<B>, current: &Value<B>) -> bool {
        if candidate.is_absent() {
            return false;
        }
        if current.is_null() {
            return true;
        }
        let wins: fn(Ordering) -> bool = match kind {
            ScalarAggKind::Min => |ordering| ordering == Ordering::Less,
            ScalarAggKind::Max => |ordering| ordering == Ordering::Greater,
        };
        matches!(
            compare_ordered_values(ComparisonContext::none(), candidate, current, wins),
            Ok(Tri::True)
        )
    }

    /// Whether a group with this extreme and row count belongs to the
    /// announced result. `None` without a `HAVING`. A NULL extreme compares
    /// UNKNOWN and never passes, matching SQL.
    fn passes(
        having: Option<&crate::reexec::plan::GroupedHavingCheck<B>>,
        current: &Value<B>,
        rows: i64,
    ) -> bool {
        match having {
            None => true,
            Some(crate::reexec::plan::GroupedHavingCheck::Extreme { op, threshold }) => {
                let op = *op;
                matches!(
                    compare_ordered_values(
                        ComparisonContext::none(),
                        current,
                        threshold,
                        move |ordering| op.admits(ordering),
                    ),
                    Ok(Tri::True)
                )
            }
            Some(crate::reexec::plan::GroupedHavingCheck::RowCount { op, threshold }) => {
                op.admits(rows.cmp(threshold))
            }
        }
    }

    /// The change to announce after a group's state settled, diffed against
    /// the value the consumer last saw, which it also updates. A group whose
    /// state moved under a pending read is never diffed here: its announced
    /// value must keep saying what the consumer holds until the read's
    /// install speaks.
    fn crossing(
        having: Option<&crate::reexec::plan::GroupedHavingCheck<B>>,
        group: &mut GroupedExtreme<B, C>,
    ) -> Option<crate::AggregateValueChange<B>> {
        if Self::passes(having, &group.current, group.rows) {
            let repeat = group.announced.as_ref().is_some_and(|seen| {
                values_equal(ComparisonContext::none(), seen, &group.current).unwrap_or(false)
            });
            group.announced = Some(group.current.clone());
            return (!repeat).then(|| {
                crate::AggregateValueChange::Set(crate::AggregateResultValue::Scalar(
                    group.current.clone(),
                ))
            });
        }
        group
            .announced
            .take()
            .map(|_| crate::AggregateValueChange::Remove)
    }

    /// Ask for a scoped read of `key`, keeping the group's changes from here
    /// until it lands.
    fn request_read(
        &mut self,
        key: Vec<u8>,
        values: Vec<Value<B>>,
        checkpoint: Option<&C>,
        asked: u64,
        output: &mut GroupedMaintenance<B, C>,
    ) -> Result<(), crate::RegisterError> {
        let query = crate::reexec::plan::render_grouped_scalar_read(&self.plan, &values)?;
        self.pending_reads
            .entry(key.clone())
            .or_insert(GroupRead { values, asked });
        output.reads.push(GroupedRead {
            group: key,
            query,
            column_kinds: [self.plan.agg_kind, crate::backend::ScalarFamily::Int],
            checkpoint: checkpoint.cloned(),
        });
        Ok(())
    }

    /// When the oldest outstanding group read was asked, which bounds what a
    /// shared fence may trim from the truncates every group read replays.
    fn oldest_ask(&self) -> Option<u64> {
        self.pending_reads.values().map(|read| read.asked).min()
    }

    /// Keep `change` to the group `key` until a fence shows it held.
    fn remember(
        &mut self,
        key: &[u8],
        at: &C,
        change: &GroupedRowChange<B>,
        cap: usize,
        latest: &LatestFence<C>,
    ) {
        let asked = self.pending_reads.get(key).map(|read| read.asked);
        let log = self.unseen.entry(key.to_vec()).or_default();
        log.push(at.clone(), change.clone(), cap);
        if log.wants_fence(cap) {
            if let Some(fence) = latest.trims(asked) {
                log.forget_held(fence);
            }
            self.wants_fence |= log.wants_fence(cap);
        }
    }

    fn remember_truncate(&mut self, at: &C, cap: usize, latest: &LatestFence<C>) {
        // Truncates are rare and cost a position each, and forgetting one
        // would lose it for every group, so this log has no ceiling.
        self.unseen_truncates.push(at.clone(), (), usize::MAX);
        if self.unseen_truncates.wants_fence(cap) {
            if let Some(fence) = latest.trims(self.oldest_ask()) {
                self.unseen_truncates.forget_held(fence);
            }
            self.wants_fence |= self.unseen_truncates.wants_fence(cap);
        }
    }

    /// Drop every kept change the engine's latest fence holds, where that is
    /// safe for the reads outstanding.
    pub fn forget_seen(&mut self, latest: &LatestFence<C>, cap: usize) {
        let pending_reads = &self.pending_reads;
        self.unseen.retain(|key, log| {
            if let Some(fence) = latest.trims(pending_reads.get(key).map(|read| read.asked)) {
                log.forget_held(fence);
            }
            !log.is_empty()
        });
        if let Some(fence) = latest.trims(self.oldest_ask()) {
            self.unseen_truncates.forget_held(fence);
        }
        self.wants_fence = self.unseen_truncates.wants_fence(cap)
            || self.unseen.values().any(|log| log.wants_fence(cap));
    }

    /// Whether a kept log crossed half its cap and a fence-only read should
    /// trim it.
    pub const fn wants_fence(&self) -> bool {
        self.wants_fence
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one pass judges, records and applies each group's changes in event order"
    )]
    fn apply_event(
        &mut self,
        event: &PendingGroupedEvent<B>,
        group_limit: usize,
        checkpoint: Option<&C>,
        pending_cap: usize,
        latest: &LatestFence<C>,
    ) -> Result<GroupedMaintenance<B, C>, crate::RegisterError> {
        let mut output = GroupedMaintenance::empty();
        let PendingGroupedEvent::Rows(changes) = event else {
            if let Some(at) = checkpoint {
                self.remember_truncate(at, pending_cap, latest);
            }
            let mut removed = Vec::new();
            self.groups.retain(|key, group| {
                // A group a later read set keeps what that read saw after the truncate.
                let kept = !group.fence.admits(checkpoint);
                if !kept && group.announced.is_some() {
                    removed.push((group.identity(key), crate::AggregateValueChange::Remove));
                }
                kept
            });
            output.changes.extend(removed);
            return Ok(output);
        };
        // Phase one applies the row changes and remembers which groups
        // moved. Nothing is announced yet: a group that turns out to need a
        // re-read must reach the consumer through that read alone.
        let mut refresh = HashMap::<Vec<u8>, Vec<Value<B>>>::new();
        let mut touched: Vec<Vec<u8>> = Vec::new();
        let touch = |touched: &mut Vec<Vec<u8>>, key: &[u8]| {
            if !touched.iter().any(|held| held == key) {
                touched.push(key.to_vec());
            }
        };
        for change in changes {
            if let Some(key) = change.key() {
                let held = self
                    .groups
                    .get_mut(key)
                    .is_some_and(|group| !group.fence.admits(checkpoint));
                if held {
                    continue;
                }
                if let Some(at) = checkpoint {
                    self.remember(key, at, change, pending_cap, latest);
                }
            }
            match change {
                GroupedRowChange::MissingGroup => output.missing_group = true,
                GroupedRowChange::Refresh { key, values } => {
                    refresh.insert(key.clone(), values.clone());
                }
                GroupedRowChange::Delete(row) => {
                    let Some(group) = self.groups.get_mut(&row.key) else {
                        continue;
                    };
                    match Self::delete_row(group, row) {
                        RowEffect::Emptied => {
                            // The removal is final, so it does not wait for
                            // phase two: an announced group says goodbye now.
                            if let Some(group) = self.groups.remove(&row.key) {
                                if group.announced.is_some() {
                                    output.changes.push((
                                        group.into_identity(row.key.clone()),
                                        crate::AggregateValueChange::Remove,
                                    ));
                                }
                            }
                        }
                        RowEffect::Reread => {
                            refresh.insert(row.key.clone(), row.values.clone());
                        }
                        RowEffect::Moved => touch(&mut touched, &row.key),
                    }
                }
                GroupedRowChange::Insert(row) => {
                    if self.apply_insert(row, group_limit, &mut output, &mut refresh) {
                        touch(&mut touched, &row.key);
                    }
                }
            }
        }
        for (key, values) in refresh {
            touched.retain(|held| held != &key);
            if !self.groups.contains_key(&key) {
                if self.groups.len() >= group_limit {
                    output.group_limit = true;
                    continue;
                }
                self.groups.insert(
                    key.clone(),
                    GroupedExtreme {
                        values: values.clone(),
                        current: Value::Null,
                        rows: 0,
                        announced: None,
                        fence: ReadFence::none(),
                    },
                );
            }
            let values = self
                .groups
                .get(&key)
                .map_or(values, |group| group.values.clone());
            self.request_read(key, values, checkpoint, latest.now(), &mut output)?;
        }
        // Phase two announces each settled group's difference from what the
        // consumer last saw.
        let having = self.plan.having.as_ref();
        for key in touched {
            let Some(group) = self.groups.get_mut(&key) else {
                continue;
            };
            if let Some(change) = Self::crossing(having, group) {
                output.changes.push((group.identity(&key), change));
            }
        }
        Ok(output)
    }

    /// Take one matching row out of `group`.
    fn delete_row(group: &mut GroupedExtreme<B, C>, row: &ExtremeRow<B>) -> RowEffect {
        group.rows -= 1;
        if group.rows <= 0 {
            RowEffect::Emptied
        } else if !row.value.is_absent()
            && values_equal(ComparisonContext::none(), &row.value, &group.current).unwrap_or(true)
        {
            RowEffect::Reread
        } else {
            RowEffect::Moved
        }
    }

    /// Fold one observed insert or force a scoped read when event data cannot
    /// be trusted for this consumer.
    fn apply_insert(
        &mut self,
        row: &ExtremeRow<B>,
        group_limit: usize,
        output: &mut GroupedMaintenance<B, C>,
        refresh: &mut HashMap<Vec<u8>, Vec<Value<B>>>,
    ) -> bool {
        if self.database_reads_per_consumer {
            refresh.insert(row.key.clone(), row.values.clone());
            return false;
        }
        if let Some(group) = self.groups.get_mut(&row.key) {
            group.rows += 1;
            if Self::candidate_wins(self.plan.kind, &row.value, &group.current) {
                group.current.clone_from(&row.value);
            }
        } else {
            if self.groups.len() >= group_limit {
                output.group_limit = true;
                return false;
            }
            self.groups.insert(
                row.key.clone(),
                GroupedExtreme {
                    values: row.values.clone(),
                    current: row.value.clone(),
                    rows: 1,
                    announced: None,
                    fence: ReadFence::none(),
                },
            );
        }
        true
    }

    pub fn on_event<E, DB>(
        &mut self,
        event: &E,
        vm: &mut Vm<B>,
        db: &DB,
        pending_cap: usize,
        group_limit: usize,
        latest: &LatestFence<C>,
    ) -> Result<GroupedMaintenance<B, C>, crate::DispatchError>
    where
        E: CdcEvent<Backend = B, Checkpoint = C>,
        DB: DatabaseLike,
    {
        let at = event.checkpoint();
        if self.pending.is_some() {
            let change = self.event_changes(event, vm, db)?;
            let missing_group = matches!(
                &change,
                PendingGroupedEvent::Rows(changes)
                    if changes.iter().any(|change| matches!(change, GroupedRowChange::MissingGroup))
            );
            if let Some(pending) = &mut self.pending {
                pending.push(at.as_ref(), change, pending_cap);
            }
            let mut output = GroupedMaintenance::empty();
            output.missing_group = missing_group;
            return Ok(output);
        }
        // What the seed read holds, every later read holds too.
        if !self.fence.admits(at.as_ref()) {
            return Ok(GroupedMaintenance::empty());
        }
        let change = self.event_changes(event, vm, db)?;
        self.apply_event(&change, group_limit, at.as_ref(), pending_cap, latest)
            .map_err(|error| crate::DispatchError::TierTransition {
                subscription: 0,
                message: error.to_string(),
            })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "seed reconciliation validates, replays and atomically commits one state map"
    )]
    pub fn install_seed(
        &mut self,
        subscription: crate::SubscriptionId,
        rows: &[Vec<Value<B>>],
        fence: Option<C::Fence>,
        pending_cap: usize,
        group_limit: usize,
        latest: &LatestFence<C>,
    ) -> Result<GroupedMaintenance<B, C>, crate::AggregateInstallError> {
        let Some(pending) = self.pending.take() else {
            return Err(crate::AggregateInstallError::AlreadySeeded(subscription));
        };
        if pending.overflowed {
            self.pending = Some(pending);
            return Err(crate::AggregateInstallError::TooManyChangesDuringRead {
                subscription,
                cap: pending_cap,
            });
        }
        if !pending.events.is_empty()
            && (fence.is_none() || pending.events.iter().any(|(at, _)| at.is_none()))
        {
            self.pending = Some(pending);
            return Err(crate::AggregateInstallError::PositionUnknown(subscription));
        }
        let group_columns = self.plan.group_columns.len();
        let mut groups = HashMap::with_capacity(rows.len());
        for row in rows {
            if row.len() != group_columns + 2 {
                self.pending = Some(pending);
                return Err(crate::AggregateInstallError::GroupedRowArity {
                    subscription,
                    expected: group_columns + 2,
                    got: row.len(),
                });
            }
            let values = row[..group_columns].to_vec();
            let Some(key) = self.plan.group_key_encoder.encode(&values) else {
                self.pending = Some(pending);
                return Err(crate::AggregateInstallError::GroupKeyUnencodable(
                    subscription,
                ));
            };
            let Value::Int(count) = &row[group_columns + 1] else {
                self.pending = Some(pending);
                return Err(crate::AggregateInstallError::GroupedRowCount(subscription));
            };
            let count = sql_scalar_text::parse_i64(&count.scalar_text())
                .filter(|count| *count > 0)
                .ok_or(crate::AggregateInstallError::GroupedRowCount(subscription))?;
            if !groups.contains_key(&key) && groups.len() >= group_limit {
                self.pending = Some(pending);
                return Err(crate::AggregateInstallError::GroupLimit {
                    subscription,
                    limit: group_limit,
                });
            }
            if groups
                .insert(
                    key,
                    GroupedExtreme {
                        values,
                        current: row[group_columns].clone(),
                        rows: count,
                        announced: None,
                        fence: ReadFence::none(),
                    },
                )
                .is_some()
            {
                self.pending = Some(pending);
                return Err(crate::AggregateInstallError::DuplicateGroup(subscription));
            }
        }
        self.groups = groups;
        self.fence = ReadFence::new(fence);
        let mut output = GroupedMaintenance::empty();
        for (at, event) in &pending.events {
            if !self.fence.admits(at.as_ref()) {
                continue;
            }
            let replayed = self
                .apply_event(event, group_limit, at.as_ref(), pending_cap, latest)
                .map_err(|error| crate::AggregateInstallError::TierTransition {
                    subscription,
                    message: error.to_string(),
                })?;
            // Replay emissions are discarded: nothing was announced before this
            // install, so the opening pass below speaks for the final state once,
            // matching the fold twin.
            output.reads.extend(replayed.reads);
            output.group_limit |= replayed.group_limit;
            output.missing_group |= replayed.missing_group;
        }
        let pending_reads: hashbrown::HashSet<Vec<u8>> =
            output.reads.iter().map(|read| read.group.clone()).collect();
        // Announce only the groups that pass the condition. The rest install
        // silently and are already current the moment they cross in.
        let having = self.plan.having.as_ref();
        output.changes.extend(
            self.groups
                .iter_mut()
                .filter(|(key, _)| !pending_reads.contains(*key))
                .filter_map(|(key, group)| {
                    group.announced = Self::passes(having, &group.current, group.rows)
                        .then(|| group.current.clone());
                    group.announced.is_some().then(|| {
                        (
                            group.identity(key),
                            crate::AggregateValueChange::Set(crate::AggregateResultValue::Scalar(
                                group.current.clone(),
                            )),
                        )
                    })
                }),
        );
        output
            .changes
            .sort_unstable_by(|left, right| left.0.key.cmp(&right.0.key));
        Ok(output)
    }

    /// Install one scoped read's result, then apply every kept change to the
    /// group, and every kept truncate, its snapshot did not hold. `asked`
    /// stamps a read asked again.
    ///
    /// A change the read held leaves the group's log. One it missed stays,
    /// since the next read may miss it too. The read's fence then judges the
    /// group's later changes until one passes it.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "the read's answer, its fence, its trigger and the engine's two ceilings"
    )]
    pub fn install_group(
        &mut self,
        subscription: crate::SubscriptionId,
        key: &[u8],
        row: &[Value<B>],
        fence: Option<C::Fence>,
        checkpoint: Option<&C>,
        group_limit: usize,
        asked: u64,
    ) -> Result<GroupedMaintenance<B, C>, crate::AggregateInstallError> {
        if row.len() != 2 {
            return Err(crate::AggregateInstallError::GroupedRowArity {
                subscription,
                expected: 2,
                got: row.len(),
            });
        }
        let Value::Int(count) = &row[1] else {
            return Err(crate::AggregateInstallError::GroupedRowCount(subscription));
        };
        let count = sql_scalar_text::parse_i64(&count.scalar_text())
            .ok_or(crate::AggregateInstallError::GroupedRowCount(subscription))?;
        let mut output = GroupedMaintenance::empty();
        let read = self.pending_reads.remove(key);
        let existing = self.groups.remove(key);
        let was_present = existing.is_some();
        let (values, announced) = match (existing, read) {
            (Some(group), _) => (group.values, group.announced),
            (None, Some(read)) => (read.values, None),
            (None, None) if count <= 0 => return Ok(output),
            (None, None) => {
                return Err(crate::AggregateInstallError::UnexpectedGroupRead(
                    subscription,
                ))
            }
        };
        let mut group = GroupedExtreme {
            values,
            current: row[0].clone(),
            rows: count.max(0),
            announced,
            fence: ReadFence::none(),
        };
        let mut log = self.unseen.remove(key).unwrap_or_default();
        let reread = match fence {
            None => {
                log = UnseenLog::new();
                false
            }
            Some(_) if log.overflowed => {
                log = UnseenLog::new();
                true
            }
            Some(fence) => {
                let reread = self.replay_unseen(&mut group, &mut log, &fence);
                group.fence = ReadFence::new(Some(fence));
                reread
            }
        };
        if !log.is_empty() {
            self.unseen.insert(key.to_vec(), log);
        }
        if !was_present && (reread || group.rows > 0) && self.groups.len() >= group_limit {
            return Err(crate::AggregateInstallError::GroupLimit {
                subscription,
                limit: group_limit,
            });
        }
        if reread {
            let values = group.values.clone();
            self.groups.insert(key.to_vec(), group);
            self.request_read(key.to_vec(), values, checkpoint, asked, &mut output)
                .map_err(|error| crate::AggregateInstallError::TierTransition {
                    subscription,
                    message: error.to_string(),
                })?;
            return Ok(output);
        }
        if group.rows <= 0 {
            if group.announced.is_some() {
                output.changes.push((
                    group.into_identity(key.to_vec()),
                    crate::AggregateValueChange::Remove,
                ));
            }
            return Ok(output);
        }
        if let Some(change) = Self::crossing(self.plan.having.as_ref(), &mut group) {
            output.changes.push((group.identity(key), change));
        }
        self.groups.insert(key.to_vec(), group);
        Ok(output)
    }

    /// Apply to `group`, in stream order, every entry of its log and every
    /// kept truncate that `fence` does not hold, dropping the log entries it
    /// holds. `true` when only another read can say what the group holds, from
    /// which point entries are kept without being applied.
    fn replay_unseen(
        &self,
        group: &mut GroupedExtreme<B, C>,
        log: &mut UnseenLog<C, GroupedRowChange<B>>,
        fence: &C::Fence,
    ) -> bool {
        let mut reread = false;
        let mut truncates = self
            .unseen_truncates
            .entries
            .iter()
            .map(|(at, ())| at)
            .filter(|at| at.seen_by(fence) != Seen::Held)
            .peekable();
        let mut replay = |group: &mut GroupedExtreme<B, C>, entry: GroupEntry<'_, B>| {
            reread = reread || self.replay(group, &entry);
        };
        log.entries.retain(|(at, change)| {
            while truncates.next_if(|truncate| *truncate < at).is_some() {
                replay(group, GroupEntry::Emptied);
            }
            if at.seen_by(fence) == Seen::Held {
                return false;
            }
            replay(group, GroupEntry::Row(change));
            true
        });
        for _ in truncates {
            replay(group, GroupEntry::Emptied);
        }
        reread
    }

    /// Apply one kept change on top of a scoped read's answer. `true` when
    /// only another read can say what the group holds.
    fn replay(&self, group: &mut GroupedExtreme<B, C>, entry: &GroupEntry<'_, B>) -> bool {
        match entry {
            GroupEntry::Emptied => {
                group.rows = 0;
                group.current = Value::Null;
                false
            }
            GroupEntry::Row(GroupedRowChange::Insert(row)) => {
                if self.database_reads_per_consumer {
                    return true;
                }
                group.rows += 1;
                if Self::candidate_wins(self.plan.kind, &row.value, &group.current) {
                    group.current.clone_from(&row.value);
                }
                false
            }
            GroupEntry::Row(GroupedRowChange::Delete(row)) => match Self::delete_row(group, row) {
                RowEffect::Emptied => {
                    group.rows = 0;
                    group.current = Value::Null;
                    false
                }
                RowEffect::Reread => true,
                RowEffect::Moved => false,
            },
            GroupEntry::Row(GroupedRowChange::Refresh { .. }) => true,
            GroupEntry::Row(GroupedRowChange::MissingGroup) => false,
        }
    }
}

/// What taking one row out of a group did to it.
enum RowEffect {
    /// No row is left.
    Emptied,
    /// The extreme left and only a read can name the next one.
    Reread,
    Moved,
}

/// Enum-dispatch wrapper holding any maintained query.
pub enum QueryRuntime<B: Backend, C: Checkpoint = crate::NoCheckpoint> {
    Partial(MinMaxQuery<B, C>),
    /// Grouped extrema with a checkpoint-aware seed window.
    Grouped(alloc::boxed::Box<GroupedMinMaxQuery<B, C>>),
    /// Re-read in full on any relevant change, holding nothing.
    Total(TotalQuery),
    /// Ask only about the rows that changed.
    Keyed(KeyedQuery<B>),
}

impl<B: Backend + SqlLiteralParse, C: Checkpoint> QueryRuntime<B, C> {
    pub fn on_event<E, DB>(
        &mut self,
        event: &E,
        vm: &mut Vm<B>,
        db: &DB,
        cap: usize,
        latest: &LatestFence<C>,
    ) -> Maintenance<B>
    where
        E: CdcEvent<Backend = B, Checkpoint = C>,
        DB: DatabaseLike,
    {
        match self {
            Self::Partial(query) => query.on_event(event, vm, db, cap, latest),
            Self::Grouped(_) => {
                unreachable!("grouped maintenance uses its multi-group output")
            }
            Self::Total(query) => query.on_event(event, vm, db),
            Self::Keyed(query) => query.on_event(event, vm, db),
        }
    }

    pub fn install(
        &mut self,
        value: Value<B>,
        fence: Option<C::Fence>,
        asked: u64,
    ) -> ScalarInstallOutcome<B> {
        match self {
            Self::Partial(query) => query.install(value, fence, asked),
            Self::Grouped(_) => {
                unreachable!("a grouped result uses its concrete install input")
            }
            // Neither tier holds an answer, so the read goes to the consumer.
            Self::Total(_) | Self::Keyed(_) => ScalarInstallOutcome::Value(value),
        }
    }

    /// Whether this value keeps enough unseen changes that a fence-only read
    /// should trim them.
    pub fn wants_fence(&self, cap: usize) -> bool {
        match self {
            Self::Partial(query) => query.wants_fence(cap),
            Self::Grouped(query) => query.wants_fence(),
            Self::Total(_) | Self::Keyed(_) => false,
        }
    }

    /// Drop the unseen changes `latest` shows held, where that is safe.
    pub fn forget_seen(&mut self, latest: &LatestFence<C>, cap: usize) {
        match self {
            Self::Partial(query) => query.forget_seen(latest),
            Self::Grouped(query) => query.forget_seen(latest, cap),
            Self::Total(_) | Self::Keyed(_) => {}
        }
    }

    pub fn dependency_columns(&self) -> &[ColumnId] {
        match self {
            Self::Partial(query) => query.dependency_columns(),
            Self::Grouped(query) => query.dependency_columns(),
            Self::Total(query) => MaintainedQuery::<B>::dependency_columns(query),
            Self::Keyed(query) => MaintainedQuery::<B>::dependency_columns(query),
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
pub struct TotalQuery {
    /// Every column of every table the query reads.
    ///
    /// A computed projection can depend on any column, so narrowing this would
    /// mean guessing which UPDATEs matter. The UPDATE filter in the engine
    /// treats an empty list as "nothing depends on anything" and skips the
    /// event, so this must be the full set rather than empty.
    dependency_columns: Vec<crate::ColumnId>,
}

impl TotalQuery {
    pub const fn new(dependency_columns: Vec<crate::ColumnId>) -> Self {
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

    fn dependency_columns(&self) -> &[crate::ColumnId] {
        &self.dependency_columns
    }
}

/// A query maintained by asking the database only about the rows that changed.
///
/// Holds no answer and no copy of the result: only the keys changed since the
/// last resolve, which the resolver drains. That is bounded by the change
/// volume of one batch rather than by the size of the answer, which is what
/// makes this tier cost proportional to the change.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyedQuery<B: Backend> {
    /// Every column of the table: the filter is one the engine could not
    /// compile, so it may read any of them.
    dependency_columns: Vec<crate::ColumnId>,
    /// Keys whose membership has to be re-asked, accumulated across a batch so
    /// several changes to one query cost one read.
    pending: Vec<Vec<Value<B>>>,
    /// The same keys encoded, so recording one is a hash lookup rather than a
    /// scan of every key already held. `Value` carries floats, so it has
    /// neither `Hash` nor `Ord`, and its serialized form is what can be
    /// compared cheaply. Without this, a batch of n changes costs n squared
    /// key comparisons.
    seen: hashbrown::HashSet<Vec<u8>>,
    /// A table that sent a change with no readable key, held until the resolve
    /// can surface it. Swallowing it would leave the subscription silently
    /// stale, which is the one outcome this tier must not have.
    keyless_change: Option<crate::TableId>,
}

impl<B: Backend> KeyedQuery<B> {
    pub fn new(dependency_columns: Vec<crate::ColumnId>) -> Self {
        Self {
            dependency_columns,
            pending: Vec::new(),
            seen: hashbrown::HashSet::new(),
            keyless_change: None,
        }
    }

    /// Record a key to ask about, ignoring one already held.
    ///
    /// A key that cannot be encoded is recorded without the duplicate check,
    /// which costs a longer `IN` list and never a missed row.
    fn record(&mut self, key: Vec<Value<B>>) {
        match crate::backend::encode_value_key(&key) {
            Some(encoded) => {
                if self.seen.insert(encoded) {
                    self.pending.push(key);
                }
            }
            None => self.pending.push(key),
        }
    }

    /// The keys accumulated so far, leaving them queued.
    ///
    /// A read works from this copy so that a failed or abandoned read loses
    /// nothing: the keys stay recorded until [`remove_pending`](Self::remove_pending)
    /// says they were delivered.
    pub fn pending_snapshot(&self) -> Vec<Vec<Value<B>>> {
        self.pending.clone()
    }

    /// Drop exactly the delivered keys, keeping any recorded since the
    /// snapshot they were read from.
    ///
    /// This tier is the only one that cannot heal itself: the others re-read
    /// everything, so a later change repairs an earlier lost answer, while
    /// this one asks only about the keys named in it, and a dropped key
    /// leaves that row wrong until it happens to change again. Removal after
    /// delivery is what makes a failed read cost a retry, never a row.
    pub fn remove_pending(&mut self, delivered: &[Vec<Value<B>>]) {
        let removed: hashbrown::HashSet<Vec<u8>> = delivered
            .iter()
            .filter_map(|key| crate::backend::encode_value_key(key))
            .collect();
        self.pending.retain(|key| {
            crate::backend::encode_value_key(key).map_or_else(
                // An unencodable key falls back to the scan, which is correct
                // and merely slower, rather than being kept forever.
                || !delivered.contains(key),
                |encoded| !removed.contains(&encoded),
            )
        });
        for encoded in &removed {
            self.seen.remove(encoded);
        }
    }

    /// Take the table that sent an unkeyed change, if one did.
    pub const fn take_keyless_change(&mut self) -> Option<crate::TableId> {
        self.keyless_change.take()
    }
}

impl<B: Backend> MaintainedQuery<B> for KeyedQuery<B> {
    fn on_event<E: crate::backend::CdcEvent<Backend = B>, DB: DatabaseLike>(
        &mut self,
        event: &E,
        _vm: &mut crate::compiler::Vm<B>,
        database: &DB,
    ) -> Maintenance<B> {
        // The primary-key projection is always populated for a row-level event,
        // which is what lets this tier ask about the changed row by name
        // whatever the change was, including a delete whose row is gone.
        let key = event.with_pk_columns(database, |columns| {
            let mut key = Vec::with_capacity(columns.len());
            for &column in columns {
                match event.value_at_known_pk(database, column) {
                    Ok(value) if !value.is_missing() => key.push(value),
                    _ => return None,
                }
            }
            (!key.is_empty()).then_some(key)
        });
        let Some(key) = key else {
            self.keyless_change = Some(event.table_id(database));
            return Maintenance::NeedsReexecution;
        };
        self.record(key);
        Maintenance::NeedsReexecution
    }

    fn dependency_columns(&self) -> &[crate::ColumnId] {
        &self.dependency_columns
    }
}

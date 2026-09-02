//! Layer 2: maintenance.
//!
//! Per-query state machines that consume CDC events and decide, in-process,
//! whether the query's result is unchanged, has a newly-computed value, or
//! cannot be maintained without a database re-query. This layer NEVER
//! touches the database: it only reads cells through the event's `value_at`
//! accessor and evaluates the query's WHERE clause via the engine VM.

use crate::backend::{Backend, CdcEvent, RowKind, ScalarText, Value};
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
pub trait MaintainedQuery<B: Backend> {
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
pub struct MinMaxQuery<B: Backend> {
    kind: ScalarAggKind,
    agg_column: ColumnId,
    where_program: Arc<BytecodeProgram<B>>,
    dependency_columns: Vec<ColumnId>,
    database_reads_per_consumer: bool,
    /// The extreme, once known. `Some(Value::Null)` means the filtered set is
    /// empty, `None` means nobody has said yet, which no change can decide.
    current: Option<Value<B>>,
}

impl<B: Backend> MinMaxQuery<B> {
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
        Some(matches!(
            compare_ordered_values(candidate, current, wins),
            Tri::True
        ))
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
        match self.is_more_extreme(&candidate) {
            // Nobody has said what the extreme is, so this row cannot be it:
            // the table may hold a more extreme one this engine never saw.
            None => Maintenance::NeedsReexecution,
            Some(true) => {
                if self.database_reads_per_consumer {
                    return Maintenance::NeedsReexecution;
                }
                self.current = Some(candidate.clone());
                Maintenance::Updated(candidate)
            }
            Some(false) => Maintenance::Unchanged,
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
        let Some(current) = self.current.as_ref() else {
            return Maintenance::NeedsReexecution;
        };
        let value = self.agg_value(event, row, db);
        if !value.is_absent() && values_equal(&value, current) {
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
                // The table is empty afterwards, so this resolves an unknown
                // extreme as well as replacing a known one.
                if self.current.as_ref().is_some_and(Value::is_null) {
                    Maintenance::Unchanged
                } else {
                    self.current = Some(Value::Null);
                    Maintenance::Updated(Value::Null)
                }
            }
        }
    }

    fn install(&mut self, value: Value<B>) {
        self.current = Some(value);
    }

    fn dependency_columns(&self) -> &[ColumnId] {
        &self.dependency_columns
    }
}

struct GroupedExtreme<B: Backend> {
    values: Vec<Value<B>>,
    current: Value<B>,
    rows: i64,
    /// The extreme the consumer last saw for this group, `None` while the
    /// group is outside the announced result. Emissions are diffs against
    /// this, so a value that moved under a pending read is announced by the
    /// read's install rather than twice or never.
    announced: Option<Value<B>>,
}

impl<B: Backend> GroupedExtreme<B> {
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

#[derive(Clone)]
struct ExtremeRow<B: Backend> {
    key: Vec<u8>,
    values: Vec<Value<B>>,
    value: Value<B>,
}

#[derive(Clone)]
enum GroupedRowChange<B: Backend> {
    Insert(ExtremeRow<B>),
    Delete(ExtremeRow<B>),
    Refresh { key: Vec<u8>, values: Vec<Value<B>> },
    MissingGroup,
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
    pub column_kinds: [crate::backend::BuiltinKind; 2],
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
    groups: HashMap<Vec<u8>, GroupedExtreme<B>>,
    pending: Option<PendingGrouped<B, C>>,
    pending_reads: HashMap<Vec<u8>, Vec<Value<B>>>,
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
            pending_reads: HashMap::new(),
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
        matches!(compare_ordered_values(candidate, current, wins), Tri::True)
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
                    compare_ordered_values(current, threshold, move |ordering| op.admits(ordering)),
                    Tri::True
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
        group: &mut GroupedExtreme<B>,
    ) -> Option<crate::AggregateValueChange<B>> {
        if Self::passes(having, &group.current, group.rows) {
            let repeat = group
                .announced
                .as_ref()
                .is_some_and(|seen| values_equal(seen, &group.current));
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

    fn apply_event(
        &mut self,
        event: &PendingGroupedEvent<B>,
        group_limit: usize,
        checkpoint: Option<&C>,
    ) -> Result<GroupedMaintenance<B, C>, crate::RegisterError> {
        let mut output = GroupedMaintenance::empty();
        let PendingGroupedEvent::Rows(changes) = event else {
            output.changes.extend(
                self.groups
                    .iter()
                    .filter(|(_, group)| group.announced.is_some())
                    .map(|(key, group)| (group.identity(key), crate::AggregateValueChange::Remove)),
            );
            self.groups.clear();
            self.pending_reads.clear();
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
            match change {
                GroupedRowChange::MissingGroup => output.missing_group = true,
                GroupedRowChange::Refresh { key, values } => {
                    refresh.insert(key.clone(), values.clone());
                }
                GroupedRowChange::Delete(row) => {
                    let Some(group) = self.groups.get_mut(&row.key) else {
                        continue;
                    };
                    group.rows -= 1;
                    if group.rows <= 0 {
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
                    } else if !row.value.is_absent() && values_equal(&row.value, &group.current) {
                        refresh.insert(row.key.clone(), row.values.clone());
                    } else {
                        touch(&mut touched, &row.key);
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
                    },
                );
            }
            let values = self
                .groups
                .get(&key)
                .map_or(values, |group| group.values.clone());
            self.pending_reads.insert(key.clone(), values.clone());
            let query = crate::reexec::plan::render_grouped_scalar_read(&self.plan, &values)?;
            output.reads.push(GroupedRead {
                group: key,
                query,
                column_kinds: [self.plan.agg_kind, crate::backend::BuiltinKind::Int],
                checkpoint: checkpoint.cloned(),
            });
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
    ) -> Result<GroupedMaintenance<B, C>, crate::DispatchError>
    where
        E: CdcEvent<Backend = B, Checkpoint = C>,
        DB: DatabaseLike,
    {
        let change = self.event_changes(event, vm, db)?;
        if let Some(pending) = &mut self.pending {
            let missing_group = matches!(
                &change,
                PendingGroupedEvent::Rows(changes)
                    if changes.iter().any(|change| matches!(change, GroupedRowChange::MissingGroup))
            );
            pending.push(event.checkpoint().as_ref(), change, pending_cap);
            let mut output = GroupedMaintenance::empty();
            output.missing_group = missing_group;
            return Ok(output);
        }
        self.apply_event(&change, group_limit, event.checkpoint().as_ref())
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
        read_at: Option<&C>,
        pending_cap: usize,
        group_limit: usize,
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
            && (read_at.is_none() || pending.events.iter().any(|(at, _)| at.is_none()))
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
                    },
                )
                .is_some()
            {
                self.pending = Some(pending);
                return Err(crate::AggregateInstallError::DuplicateGroup(subscription));
            }
        }
        self.groups = groups;
        let mut output = GroupedMaintenance::empty();
        for (at, event) in &pending.events {
            if at.as_ref() <= read_at {
                continue;
            }
            let replayed = self
                .apply_event(event, group_limit, at.as_ref())
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

    /// Install one scoped read's result.
    pub fn install_group(
        &mut self,
        subscription: crate::SubscriptionId,
        key: &[u8],
        row: &[Value<B>],
        group_limit: usize,
    ) -> Result<Option<GroupedValueChange<B>>, crate::AggregateInstallError> {
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
        if count <= 0 {
            self.pending_reads.remove(key);
            let change = self.groups.remove(key).and_then(|group| {
                group.announced.is_some().then(|| {
                    (
                        group.into_identity(key.to_vec()),
                        crate::AggregateValueChange::Remove,
                    )
                })
            });
            return Ok(change);
        }
        if !self.groups.contains_key(key) && self.groups.len() >= group_limit {
            return Err(crate::AggregateInstallError::GroupLimit {
                subscription,
                limit: group_limit,
            });
        }
        let having = self.plan.having.as_ref();
        let (values, change) = if let Some(group) = self.groups.get_mut(key) {
            group.current.clone_from(&row[0]);
            group.rows = count;
            let change = Self::crossing(having, group);
            (group.values.clone(), change)
        } else {
            let values = self.pending_reads.get(key).cloned().ok_or(
                crate::AggregateInstallError::UnexpectedGroupRead(subscription),
            )?;
            let mut group = GroupedExtreme {
                values: values.clone(),
                current: row[0].clone(),
                rows: count,
                announced: None,
            };
            let change = Self::crossing(having, &mut group);
            self.groups.insert(key.to_vec(), group);
            (values, change)
        };
        self.pending_reads.remove(key);
        Ok(change.map(|change| {
            (
                crate::GroupIdentity {
                    key: key.to_vec(),
                    values,
                },
                change,
            )
        }))
    }
}

/// Enum-dispatch wrapper holding any maintained query.
pub enum QueryRuntime<B: Backend, C: Checkpoint = crate::NoCheckpoint> {
    Partial(MinMaxQuery<B>),
    /// Grouped extrema with a checkpoint-aware seed window.
    Grouped(alloc::boxed::Box<GroupedMinMaxQuery<B, C>>),
    /// Re-read in full on any relevant change, holding nothing.
    Total(TotalQuery),
    /// Ask only about the rows that changed.
    Keyed(KeyedQuery<B>),
}

impl<B: Backend + SqlLiteralParse, C: Checkpoint> QueryRuntime<B, C> {
    pub fn on_event<E, DB>(&mut self, event: &E, vm: &mut Vm<B>, db: &DB) -> Maintenance<B>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        match self {
            Self::Partial(query) => query.on_event(event, vm, db),
            Self::Grouped(_) => {
                unreachable!("grouped maintenance uses its multi-group output")
            }
            Self::Total(query) => query.on_event(event, vm, db),
            Self::Keyed(query) => query.on_event(event, vm, db),
        }
    }

    pub fn install(&mut self, value: Value<B>) {
        match self {
            Self::Partial(query) => query.install(value),
            Self::Grouped(_) => {
                unreachable!("a grouped result uses its concrete install input")
            }
            Self::Total(query) => MaintainedQuery::<B>::install(query, value),
            Self::Keyed(query) => MaintainedQuery::<B>::install(query, value),
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

    fn install(&mut self, _value: Value<B>) {
        // Nothing to install: the tier holds no answer, so a re-read is
        // delivered to the consumer rather than stored here.
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
        let mut key = Vec::new();
        for column in event.pk_columns(database) {
            match event.value_at(database, crate::backend::RowKind::Pk, column) {
                Ok(value) if !value.is_missing() => key.push(value),
                _ => {
                    self.keyless_change = Some(event.table_id(database));
                    return Maintenance::NeedsReexecution;
                }
            }
        }
        if key.is_empty() {
            self.keyless_change = Some(event.table_id(database));
            return Maintenance::NeedsReexecution;
        }
        self.record(key);
        Maintenance::NeedsReexecution
    }

    fn install(&mut self, _value: Value<B>) {
        // Nothing to install: this tier holds no answer.
    }

    fn dependency_columns(&self) -> &[crate::ColumnId] {
        &self.dependency_columns
    }
}

//! Layer 2 — maintenance.
//!
//! Per-query state machines that consume CDC events and decide, in-process,
//! whether the query's result is unchanged, has a newly-computed value, or
//! cannot be maintained without a database re-query. This layer NEVER touches
//! the database: it only reads the event's row images and evaluates the
//! query's WHERE clause via the engine VM.

use crate::compiler::cell_cmp::{cells_equal, compare_ordered_cells};
use crate::compiler::sql_shape::ScalarAggKind;
use crate::compiler::{BytecodeProgram, Tri, Vm};
use crate::{Cell, ColumnId, EventKind, RowImage, WalEvent};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cmp::Ordering;

/// Sentinel for a column absent from a (sparse) row image.
const MISSING: Cell = Cell::Missing;

/// Outcome of feeding one CDC event to a maintained query.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Maintenance {
    /// The result provably did not change; emit nothing, no database access.
    Unchanged,
    /// The result changed; the new value was computed in-process.
    Updated(Cell),
    /// The result cannot be maintained in-process; the engine surfaces a
    /// [`ReExecutionTrigger`](super::ReExecutionTrigger) so the Subscription
    /// Materializer re-queries the database and calls [`install`] with the
    /// recomputed value.
    ///
    /// [`install`]: super::ReExecEngine::install
    NeedsReexecution,
}

/// A query maintained by the re-execution layer.
///
/// Implementors never touch the database; when they cannot decide in-process
/// they return [`Maintenance::NeedsReexecution`]. The engine then surfaces a
/// [`ReExecutionTrigger`](super::ReExecutionTrigger) for the Subscription
/// Materializer, which re-runs the SQL and calls
/// [`install`](Self::install) with the recomputed value. The SQL to re-run
/// and the scalar's [`ColumnType`](crate::ColumnType) are returned to the
/// materializer at registration time via
/// [`Registered::ReExec`](crate::reexec::Registered::ReExec) so they live in
/// the plan, not on the runtime. Kept object-safe even though storage uses
/// enum dispatch ([`QueryRuntime`]).
pub(super) trait MaintainedQuery {
    /// Feed a CDC event. `vm` is lent for WHERE-membership evaluation.
    fn on_event(&mut self, event: &WalEvent, vm: &mut Vm) -> Maintenance;
    /// Adopt a value produced by the materializer's re-execution.
    fn install(&mut self, value: Cell);
    /// Columns whose change can affect the result.
    fn dependency_columns(&self) -> &[ColumnId];
}

/// Incrementally-maintained single-table scalar `MIN`/`MAX`.
///
/// Inserts and most updates/deletes are handled in memory; a database re-query
/// is required only when the current extreme value is removed or displaced
/// (then we cannot know the next extreme without scanning), or when an event's
/// row image is too incomplete to decide.
pub(super) struct MinMaxQuery {
    kind: ScalarAggKind,
    agg_column: ColumnId,
    where_program: Arc<BytecodeProgram>,
    dependency_columns: Vec<ColumnId>,
    /// Current extreme; `Cell::Null` means the (filtered) set is empty.
    current: Cell,
}

impl MinMaxQuery {
    pub(super) const fn new(
        kind: ScalarAggKind,
        agg_column: ColumnId,
        where_program: Arc<BytecodeProgram>,
        dependency_columns: Vec<ColumnId>,
        initial: Cell,
    ) -> Self {
        Self {
            kind,
            agg_column,
            where_program,
            dependency_columns,
            current: initial,
        }
    }

    /// The current maintained value (`Cell::Null` when the result set is
    /// empty). Used by the state-machine unit tests; production code reads
    /// values through the engine's emitted [`ScalarUpdate`]s instead.
    #[cfg(test)]
    pub(super) const fn current(&self) -> &Cell {
        &self.current
    }

    /// Whether `row` satisfies the query's WHERE clause (only `Tri::True`
    /// counts; NULL/Unknown excludes the row, per SQL).
    fn matches(&self, row: &RowImage, vm: &mut Vm) -> bool {
        matches!(vm.eval(&self.where_program, row), Ok(Tri::True))
    }

    /// The aggregated column's cell, or `Missing` if absent from the image.
    fn agg_cell<'a>(&self, row: &'a RowImage) -> &'a Cell {
        row.get(self.agg_column).unwrap_or(&MISSING)
    }

    /// Whether any column the query depends on is absent/missing in `row`
    /// (a sparse image we cannot reason about).
    fn any_dependency_missing(&self, row: &RowImage) -> bool {
        self.dependency_columns
            .iter()
            .any(|&c| row.get(c).is_none_or(Cell::is_missing))
    }

    /// Whether `candidate` would become the new extreme versus `current`.
    /// A non-present candidate (NULL/Missing) never participates; into an
    /// empty set (current Null) any present value wins.
    fn is_more_extreme(&self, candidate: &Cell) -> bool {
        if !candidate.is_present() {
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
            compare_ordered_cells(candidate, &self.current, wins),
            Tri::True
        )
    }

    /// Insert half: fold a (matching) new row into the extreme. Never forces a
    /// re-query.
    fn on_insert_row(&mut self, row: &RowImage, vm: &mut Vm) -> Maintenance {
        if !self.matches(row, vm) {
            return Maintenance::Unchanged;
        }
        let candidate = self.agg_cell(row);
        if self.is_more_extreme(candidate) {
            self.current = candidate.clone();
            Maintenance::Updated(self.current.clone())
        } else {
            Maintenance::Unchanged
        }
    }

    /// Delete half: decide whether removing `row` displaces the extreme. Does
    /// not mutate state; returns only `Unchanged` or `NeedsReexecution`.
    fn on_delete_row(&self, row: &RowImage, vm: &mut Vm) -> Maintenance {
        // A sparse image cannot be reasoned about: re-query.
        if self.any_dependency_missing(row) {
            return Maintenance::NeedsReexecution;
        }
        if !self.matches(row, vm) {
            return Maintenance::Unchanged;
        }
        let cell = self.agg_cell(row);
        if cell.is_present() && cells_equal(cell, &self.current) {
            // The current extreme (or a tie of it) was removed; the next
            // extreme is unknown without a scan.
            Maintenance::NeedsReexecution
        } else {
            Maintenance::Unchanged
        }
    }
}

impl MaintainedQuery for MinMaxQuery {
    fn on_event(&mut self, event: &WalEvent, vm: &mut Vm) -> Maintenance {
        match event.kind() {
            EventKind::Insert => event
                .new_row()
                .map_or(Maintenance::NeedsReexecution, |row| {
                    self.on_insert_row(row, vm)
                }),
            EventKind::Delete => event
                .old_row()
                .map_or(Maintenance::NeedsReexecution, |row| {
                    self.on_delete_row(row, vm)
                }),
            EventKind::Update => {
                // delete(old) then insert(new). Without a complete old image we
                // cannot model the delete half.
                let (Some(old), Some(new)) = (event.old_row(), event.new_row()) else {
                    return Maintenance::NeedsReexecution;
                };
                match self.on_delete_row(old, vm) {
                    // The extreme was displaced: a fresh scan also reflects the
                    // insert half, so re-query covers both.
                    Maintenance::NeedsReexecution => Maintenance::NeedsReexecution,
                    _ => self.on_insert_row(new, vm),
                }
            }
            EventKind::Truncate => {
                if self.current.is_null() {
                    Maintenance::Unchanged
                } else {
                    self.current = Cell::Null;
                    Maintenance::Updated(Cell::Null)
                }
            }
        }
    }

    fn install(&mut self, value: Cell) {
        self.current = value;
    }

    fn dependency_columns(&self) -> &[ColumnId] {
        &self.dependency_columns
    }
}

/// Enum-dispatch wrapper holding any maintained query.
///
/// A future `Total` variant (single-table row re-execution) and an
/// aggregate-re-execution variant plug in here without disturbing the engine;
/// each services the same [`ReExecutionTrigger`](super::ReExecutionTrigger)
/// seam and is `install`ed identically by the Subscription Materializer.
pub(super) enum QueryRuntime {
    Partial(MinMaxQuery),
}

impl QueryRuntime {
    pub(super) fn on_event(&mut self, event: &WalEvent, vm: &mut Vm) -> Maintenance {
        match self {
            Self::Partial(q) => q.on_event(event, vm),
        }
    }

    pub(super) fn install(&mut self, value: Cell) {
        match self {
            Self::Partial(q) => q.install(value),
        }
    }

    pub(super) fn dependency_columns(&self) -> &[ColumnId] {
        match self {
            Self::Partial(q) => q.dependency_columns(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::option_if_let_else)]
mod tests {
    use super::*;
    use crate::compiler::parser::parse_and_compile;
    use crate::compiler::Instruction;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    // orders(id=0, price=1, quantity=2, status=3); agg column is price (1).
    const PRICE: ColumnId = 1;

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
        )
        .unwrap()
    }

    /// Build a `MinMaxQuery` over `price`, with an optional WHERE filter and an
    /// initial extreme value.
    fn query(kind: ScalarAggKind, where_sql: Option<&str>, initial: Cell) -> MinMaxQuery {
        let (program, mut deps) = match where_sql {
            None => (
                BytecodeProgram::new(vec![Instruction::PushLiteral(Cell::Bool(true))]),
                Vec::new(),
            ),
            Some(w) => {
                let (_t, prog) = parse_and_compile(
                    &alloc::format!("SELECT * FROM orders WHERE {w}"),
                    &PostgreSqlDialect {},
                    &catalog(),
                )
                .unwrap();
                let deps = prog.dependency_columns.clone();
                (prog, deps)
            }
        };
        if !deps.contains(&PRICE) {
            deps.push(PRICE);
        }
        MinMaxQuery::new(kind, PRICE, Arc::new(program), deps, initial)
    }

    fn row(price: Cell, status: &str) -> RowImage {
        RowImage {
            cells: Arc::from([
                Cell::Int(0),
                price,
                Cell::Int(0),
                Cell::String(Arc::from(status)),
            ]),
        }
    }

    fn insert(price: Cell, status: &str) -> WalEvent {
        WalEvent::builder(0)
            .insert()
            .pk_cell(0, Cell::Int(0))
            .new_row(row(price, status))
            .build()
            .unwrap()
    }

    fn delete(price: Cell, status: &str) -> WalEvent {
        WalEvent::builder(0)
            .delete()
            .pk_cell(0, Cell::Int(0))
            .old_row(row(price, status))
            .build()
            .unwrap()
    }

    fn update(old: RowImage, new: RowImage) -> WalEvent {
        WalEvent::builder(0)
            .update()
            .new_row(new)
            .pk_cell(0, Cell::Int(0))
            .maybe_old_row(Some(old))
            .build()
            .unwrap()
    }

    #[test]
    fn insert_below_min_updates() {
        let mut q = query(ScalarAggKind::Min, None, Cell::Float(5.0));
        let mut vm = Vm::new();
        assert_eq!(
            q.on_event(&insert(Cell::Float(2.0), "x"), &mut vm),
            Maintenance::Updated(Cell::Float(2.0))
        );
        assert_eq!(q.current(), &Cell::Float(2.0));
    }

    #[test]
    fn insert_above_min_unchanged() {
        let mut q = query(ScalarAggKind::Min, None, Cell::Float(5.0));
        let mut vm = Vm::new();
        assert_eq!(
            q.on_event(&insert(Cell::Float(9.0), "x"), &mut vm),
            Maintenance::Unchanged
        );
        assert_eq!(q.current(), &Cell::Float(5.0));
    }

    #[test]
    fn insert_into_empty_updates() {
        let mut q = query(ScalarAggKind::Min, None, Cell::Null);
        let mut vm = Vm::new();
        assert_eq!(
            q.on_event(&insert(Cell::Float(3.0), "x"), &mut vm),
            Maintenance::Updated(Cell::Float(3.0))
        );
    }

    #[test]
    fn insert_null_agg_unchanged() {
        let mut q = query(ScalarAggKind::Min, None, Cell::Float(5.0));
        let mut vm = Vm::new();
        assert_eq!(
            q.on_event(&insert(Cell::Null, "x"), &mut vm),
            Maintenance::Unchanged
        );
    }

    #[test]
    fn insert_not_matching_where_unchanged() {
        let mut q = query(
            ScalarAggKind::Min,
            Some("status = 'paid'"),
            Cell::Float(5.0),
        );
        let mut vm = Vm::new();
        // price 1.0 would lower the min, but status doesn't match the filter.
        assert_eq!(
            q.on_event(&insert(Cell::Float(1.0), "open"), &mut vm),
            Maintenance::Unchanged
        );
    }

    #[test]
    fn delete_non_extreme_unchanged() {
        let q = query(ScalarAggKind::Min, None, Cell::Float(5.0));
        let mut vm = Vm::new();
        assert_eq!(
            q.on_delete_row(&row(Cell::Float(9.0), "x"), &mut vm),
            Maintenance::Unchanged
        );
    }

    #[test]
    fn delete_extreme_needs_reexec() {
        let mut q = query(ScalarAggKind::Min, None, Cell::Float(5.0));
        let mut vm = Vm::new();
        assert_eq!(
            q.on_event(&delete(Cell::Float(5.0), "x"), &mut vm),
            Maintenance::NeedsReexecution
        );
    }

    #[test]
    fn delete_missing_agg_needs_reexec() {
        let mut q = query(ScalarAggKind::Min, None, Cell::Float(5.0));
        let mut vm = Vm::new();
        assert_eq!(
            q.on_event(&delete(Cell::Missing, "x"), &mut vm),
            Maintenance::NeedsReexecution
        );
    }

    #[test]
    fn delete_out_of_filter_unchanged() {
        let mut q = query(
            ScalarAggKind::Min,
            Some("status = 'paid'"),
            Cell::Float(5.0),
        );
        let mut vm = Vm::new();
        // Deleting a non-matching row (even if its value equals current) can't
        // affect the filtered aggregate.
        assert_eq!(
            q.on_event(&delete(Cell::Float(5.0), "open"), &mut vm),
            Maintenance::Unchanged
        );
    }

    #[test]
    fn update_extreme_to_higher_needs_reexec() {
        let mut q = query(ScalarAggKind::Min, None, Cell::Float(5.0));
        let mut vm = Vm::new();
        let ev = update(row(Cell::Float(5.0), "x"), row(Cell::Float(10.0), "x"));
        assert_eq!(q.on_event(&ev, &mut vm), Maintenance::NeedsReexecution);
    }

    #[test]
    fn update_lowering_non_extreme_updates() {
        let mut q = query(ScalarAggKind::Min, None, Cell::Float(5.0));
        let mut vm = Vm::new();
        // old 9.0 is non-extreme (no re-query); new 1.0 becomes the new min.
        let ev = update(row(Cell::Float(9.0), "x"), row(Cell::Float(1.0), "x"));
        assert_eq!(
            q.on_event(&ev, &mut vm),
            Maintenance::Updated(Cell::Float(1.0))
        );
    }

    #[test]
    fn update_unrelated_unchanged() {
        let mut q = query(ScalarAggKind::Min, None, Cell::Float(5.0));
        let mut vm = Vm::new();
        // Both values above the min and equal: no displacement, no new extreme.
        let ev = update(row(Cell::Float(9.0), "x"), row(Cell::Float(9.0), "y"));
        assert_eq!(q.on_event(&ev, &mut vm), Maintenance::Unchanged);
    }

    #[test]
    fn update_without_old_row_needs_reexec() {
        let mut q = query(ScalarAggKind::Min, None, Cell::Float(5.0));
        let mut vm = Vm::new();
        let ev = WalEvent::builder(0)
            .update()
            .new_row(row(Cell::Float(1.0), "x"))
            .pk_cell(0, Cell::Int(0))
            .build()
            .unwrap();
        assert_eq!(q.on_event(&ev, &mut vm), Maintenance::NeedsReexecution);
    }

    #[test]
    fn truncate_empties_then_noops() {
        let mut q = query(ScalarAggKind::Min, None, Cell::Float(5.0));
        let mut vm = Vm::new();
        let ev = WalEvent::builder(0).truncate().build().unwrap();
        assert_eq!(q.on_event(&ev, &mut vm), Maintenance::Updated(Cell::Null));
        assert_eq!(q.current(), &Cell::Null);
        // Truncate on an already-empty set emits nothing.
        assert_eq!(q.on_event(&ev, &mut vm), Maintenance::Unchanged);
    }

    #[test]
    fn max_mirror() {
        let mut q = query(ScalarAggKind::Max, None, Cell::Float(5.0));
        let mut vm = Vm::new();
        assert_eq!(
            q.on_event(&insert(Cell::Float(9.0), "x"), &mut vm),
            Maintenance::Updated(Cell::Float(9.0))
        );
        assert_eq!(
            q.on_event(&insert(Cell::Float(1.0), "x"), &mut vm),
            Maintenance::Unchanged
        );
        // Deleting the current max forces a re-query.
        assert_eq!(
            q.on_event(&delete(Cell::Float(9.0), "x"), &mut vm),
            Maintenance::NeedsReexecution
        );
    }

    #[test]
    fn install_sets_current() {
        let mut q = query(ScalarAggKind::Min, None, Cell::Null);
        q.install(Cell::Float(7.0));
        assert_eq!(q.current(), &Cell::Float(7.0));
    }
}

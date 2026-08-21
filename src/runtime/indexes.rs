//! Hybrid indexing for fast candidate selection
//!
//! Five index types:
//! 1. Equality: (col, val) -> predicates with col=val
//! 2. Range: col -> predicates with col IN range
//! 3. NULL: (col, kind) -> predicates checking IS NULL / IS NOT NULL
//! 4. Fallback: unindexable predicates (LIKE, complex expressions)
//! 5. Dependency: col -> predicates referencing col
//!
//! Dependency answers whether a predicate's verdict can change, which prunes
//! aggregate UPDATE candidates and rescues predicates reading a cell that
//! failed to decode. A full-row subscription asks a second question the WHERE
//! clause cannot answer, whether the row image it delivers changed, so its
//! UPDATE candidates come from [`HybridIndexes::full_row`] instead.

use super::ids::PredicateId;
use crate::backend::{Backend, Value};
use crate::compiler::sql_shape::QueryProjection;
use crate::compiler::{BytecodeProgram, Instruction, PlannerAtom, PlannerValue};
use crate::ColumnId;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cmp::Ordering;
use hashbrown::HashMap;
use roaring::RoaringBitmap;

/// Indexable cell value (excludes NULL/Missing)
///
/// Float is stored as u64 (via f64::to_bits()) for hashing.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum IndexableCell {
    Bool(bool),
    Int(i64),
    Float(u64), // f64::to_bits()
    String(Arc<str>),
}

impl IndexableCell {
    /// Convert a `Value<B>` into an `IndexableCell` when its payload has an
    /// index key.
    ///
    /// Delegates to [`PlannerValue::from_value`], which is also what the
    /// planner files an equality atom with, so a row cell probes with the
    /// key its predicate was indexed under. Deriving the two separately is
    /// what left an atom unprobeable and its predicate silently missed.
    #[must_use]
    pub fn from_value<B: Backend>(v: &Value<B>) -> Option<Self> {
        PlannerValue::from_value(v).map(|value| Self::from_planner(&value))
    }

    /// Convert planner value to runtime indexable cell.
    #[must_use]
    pub fn from_planner(value: &PlannerValue) -> Self {
        match value {
            PlannerValue::Bool(b) => Self::Bool(*b),
            PlannerValue::Int(i) => Self::Int(*i),
            PlannerValue::Float(bits) => Self::Float(*bits),
            PlannerValue::String(s) => Self::String(Arc::clone(s)),
        }
    }
}

/// Convert a `Value<B>` back to an `i64` when the underlying payload
/// downcasts to `i64` (Postgres / MySql / SQLite `Backend::Int`). Returns
/// `None` for exotic backends whose integer type is not `i64`.
#[must_use]
fn value_as_i64<B: Backend>(v: &Value<B>) -> Option<i64> {
    use core::any::Any;
    match v {
        Value::Int(x) => (x as &dyn Any).downcast_ref::<i64>().copied(),
        _ => None,
    }
}

/// Range index entry (sorted by lower bound)
#[derive(Clone, Debug)]
pub struct RangeEntry {
    pub predicate_id: PredicateId,
    pub lower: Option<i64>, // None = unbounded
    pub upper: Option<i64>, // None = unbounded
}

/// NULL check kind
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum NullKind {
    IsNull,
    IsNotNull,
}

/// Indexable atom extracted from bytecode
///
/// Represents simple conditions that can be indexed.
#[derive(Clone, Debug)]
pub enum IndexableAtom {
    /// col = val
    Equality {
        column_id: ColumnId,
        value: IndexableCell,
    },
    /// col >= lower AND col <= upper
    Range {
        column_id: ColumnId,
        lower: Option<i64>,
        upper: Option<i64>,
    },
    /// col IS NULL / IS NOT NULL
    Null { column_id: ColumnId, kind: NullKind },
    /// Unindexable (LIKE, complex expressions)
    Fallback,
}

impl IndexableAtom {
    /// Convert planner atom to runtime indexable atom.
    #[must_use]
    pub fn from_planner(atom: &PlannerAtom) -> Self {
        match atom {
            PlannerAtom::Equality { column_id, value } => Self::Equality {
                column_id: *column_id,
                value: IndexableCell::from_planner(value),
            },
            PlannerAtom::Range {
                column_id,
                lower,
                upper,
            } => Self::Range {
                column_id: *column_id,
                lower: *lower,
                upper: *upper,
            },
            PlannerAtom::Null { column_id, is_null } => Self::Null {
                column_id: *column_id,
                kind: if *is_null {
                    NullKind::IsNull
                } else {
                    NullKind::IsNotNull
                },
            },
        }
    }
}

/// Hybrid indexes for candidate selection
#[derive(Clone)]
pub struct HybridIndexes {
    /// Equality: col -> (val -> `RoaringBitmap<PredicateId>`)
    pub equality: HashMap<ColumnId, HashMap<IndexableCell, RoaringBitmap>>,

    /// Range: col -> `Vec<RangeEntry>` (sorted by lower bound)
    pub range: HashMap<ColumnId, Vec<RangeEntry>>,

    /// NULL: (col, kind) -> `RoaringBitmap<PredicateId>`
    pub null_checks: HashMap<(ColumnId, NullKind), RoaringBitmap>,

    /// Fallback: unindexable predicates
    pub fallback: RoaringBitmap,

    /// Dependency: col -> `RoaringBitmap<PredicateId>`. Consulted for a cell
    /// that failed to decode, and for aggregate UPDATE pruning through the
    /// parallel [`agg_dependency`](Self::agg_dependency).
    pub dependency: HashMap<ColumnId, RoaringBitmap>,

    /// Row-projection predicates. Their subscriptions receive the whole row
    /// image, so a change to any column is observable even when the WHERE
    /// clause reads none of the changed columns. This is the whole UPDATE
    /// candidate set, see
    /// [`select_update_candidates`](Self::select_update_candidates).
    pub full_row: RoaringBitmap,

    // -----------------------------------------------------------------------
    // Aggregate (COUNT/SUM/...) predicate indexes, parallel to row indexes.
    // Kept separate so `select_candidates` never returns agg predicates and
    // `select_agg_candidates` never returns row predicates.
    // -----------------------------------------------------------------------
    /// Aggregate predicates (all of them, for INSERT/DELETE dispatch)
    pub agg_fallback: RoaringBitmap,

    /// col -> aggregate predicates referencing that column (UPDATE optimization)
    pub agg_dependency: HashMap<ColumnId, RoaringBitmap>,

    /// Aggregate predicates with no dependency columns (always re-evaluated)
    pub agg_dependency_free: RoaringBitmap,
}

impl HybridIndexes {
    /// Create new empty indexes
    #[must_use]
    pub fn new() -> Self {
        Self {
            equality: HashMap::new(),
            range: HashMap::new(),
            null_checks: HashMap::new(),
            fallback: RoaringBitmap::new(),
            dependency: HashMap::new(),
            full_row: RoaringBitmap::new(),
            agg_fallback: RoaringBitmap::new(),
            agg_dependency: HashMap::new(),
            agg_dependency_free: RoaringBitmap::new(),
        }
    }

    /// Return the set of predicate IDs that depend on at least one of the
    /// `changed_cols`, plus those that read no column at all.
    ///
    /// Prunes the UPDATE candidates of
    /// [`select_agg_candidates`](Self::select_agg_candidates). Row predicates
    /// are not pruned this way, see
    /// [`select_update_candidates`](Self::select_update_candidates).
    #[must_use]
    pub fn select_update_deps(
        free: &RoaringBitmap,
        dep_map: &HashMap<ColumnId, RoaringBitmap>,
        changed_cols: &[ColumnId],
    ) -> RoaringBitmap {
        let mut candidates = free.clone();
        for &col in changed_cols {
            if let Some(deps) = dep_map.get(&col) {
                candidates |= deps;
            }
        }
        candidates
    }

    /// Candidate row predicates for an UPDATE.
    ///
    /// Every full-row predicate qualifies and no row image enters the choice.
    /// Narrowing by changed columns strands a subscription filtering on none of
    /// them with a stale row image, and probing the new image alone hides a row
    /// that left the view. Neither narrowing is sound while `SELECT *` is the
    /// only row projection subql delivers, since such a subscription observes
    /// every column. A narrower row projection would earn a
    /// [`select_update_deps`](Self::select_update_deps) term here.
    #[must_use]
    pub fn select_update_candidates(&self) -> RoaringBitmap {
        self.full_row.clone()
    }

    /// Add a predicate to the indexes.
    ///
    /// `projection` decides the routing: an aggregate lands in the parallel
    /// agg bitmaps and never appears in a row-dispatch bitmap, while any other
    /// projection delivers full row images and joins
    /// [`full_row`](Self::full_row).
    pub fn add_predicate(
        &mut self,
        pred_id: PredicateId,
        atoms: &[IndexableAtom],
        deps: &[ColumnId],
        projection: &QueryProjection,
    ) {
        let pred_id_u32 = pred_id.as_u32();

        if matches!(projection, QueryProjection::Aggregate(_)) {
            // An aggregate's value moves only when a column it reads moves, so
            // per-column dependencies prune its UPDATE candidates.
            if deps.is_empty() {
                self.agg_dependency_free.insert(pred_id_u32);
            } else {
                for &col_id in deps {
                    self.agg_dependency
                        .entry(col_id)
                        .or_default()
                        .insert(pred_id_u32);
                }
            }
            // All agg predicates go in agg_fallback so INSERT/DELETE picks them up.
            self.agg_fallback.insert(pred_id_u32);
            return;
        }

        // Per-column entries serve the undecodable-cell path in
        // `select_candidates`. Row UPDATE candidates come from `full_row`,
        // since a full row image exposes every column.
        for &col_id in deps {
            self.dependency
                .entry(col_id)
                .or_default()
                .insert(pred_id_u32);
        }
        self.full_row.insert(pred_id_u32);

        // If no atoms were provided by the planner, this predicate has no
        // trigger path and no unconditional scan requirement.
        if atoms.is_empty() {
            return;
        }

        // Add to appropriate indexes
        for atom in atoms {
            match atom {
                IndexableAtom::Equality { column_id, value } => {
                    self.equality
                        .entry(*column_id)
                        .or_default()
                        .entry(value.clone())
                        .or_default()
                        .insert(pred_id_u32);
                }

                IndexableAtom::Range {
                    column_id,
                    lower,
                    upper,
                } => {
                    let entries = self.range.entry(*column_id).or_default();
                    let insert_at = entries.partition_point(|existing| {
                        lower_bound_cmp(existing.lower, *lower) != Ordering::Greater
                    });
                    entries.insert(
                        insert_at,
                        RangeEntry {
                            predicate_id: pred_id,
                            lower: *lower,
                            upper: *upper,
                        },
                    );
                }

                IndexableAtom::Null { column_id, kind } => {
                    self.null_checks
                        .entry((*column_id, *kind))
                        .or_default()
                        .insert(pred_id_u32);
                }

                IndexableAtom::Fallback => {
                    self.fallback.insert(pred_id_u32);
                }
            }
        }
    }

    /// Select candidate agg predicates for a row/event.
    ///
    /// For UPDATE events with non-empty `changed_cols`: returns the union of
    /// `agg_dependency_free` and agg predicates depending on changed columns.
    ///
    /// For INSERT/DELETE (or UPDATE with empty changed_cols): returns all agg
    /// predicates (`agg_fallback`).
    #[must_use]
    pub fn select_agg_candidates(
        &self,
        kind: crate::EventKind,
        changed_cols: &[ColumnId],
    ) -> RoaringBitmap {
        if kind == crate::EventKind::Update && !changed_cols.is_empty() {
            return Self::select_update_deps(
                &self.agg_dependency_free,
                &self.agg_dependency,
                changed_cols,
            );
        }
        self.agg_fallback.clone()
    }

    /// Query equality index
    #[must_use]
    pub fn query_equality(
        &self,
        col_id: ColumnId,
        value: &IndexableCell,
    ) -> Option<&RoaringBitmap> {
        self.equality
            .get(&col_id)
            .and_then(|per_col| per_col.get(value))
    }

    /// Query range index (return predicates whose ranges contain value)
    pub fn query_range_into(
        &self,
        col_id: ColumnId,
        value: &IndexableCell,
        out: &mut RoaringBitmap,
    ) {
        // Only works for numeric values; NaN never matches ordered ranges.
        let Some(numeric) = NumericValue::from_indexable(value) else {
            return;
        };

        if let Some(entries) = self.range.get(&col_id) {
            for entry in entries {
                // Entries are sorted by lower bound; once lower exceeds the
                // searched value, no later entries can match.
                if let Some(lower) = entry.lower {
                    if !numeric.gte_lower(lower) {
                        break;
                    }
                }

                let in_upper = entry.upper.is_none_or(|u| numeric.lte_upper(u));

                if in_upper {
                    out.insert(entry.predicate_id.as_u32());
                }
            }
        }
    }

    /// Query range index (return predicates whose ranges contain value)
    #[must_use]
    pub fn query_range(&self, col_id: ColumnId, value: &IndexableCell) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        self.query_range_into(col_id, value, &mut result);
        result
    }

    /// Sort range entries by lower bound (for efficient querying)
    pub fn finalize_ranges(&mut self) {
        for entries in self.range.values_mut() {
            entries.sort_by(|a, b| lower_bound_cmp(a.lower, b.lower));
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum NumericValue {
    Int(i64),
    Float(f64),
}

impl NumericValue {
    const fn from_indexable(value: &IndexableCell) -> Option<Self> {
        match value {
            IndexableCell::Int(i) => Some(Self::Int(*i)),
            IndexableCell::Float(bits) => {
                let f = f64::from_bits(*bits);
                if f.is_nan() {
                    None
                } else {
                    Some(Self::Float(f))
                }
            }
            _ => None,
        }
    }

    fn gte_lower(self, lower: i64) -> bool {
        match self {
            Self::Int(v) => v >= lower,
            #[allow(clippy::cast_precision_loss)]
            Self::Float(v) => v >= lower as f64,
        }
    }

    fn lte_upper(self, upper: i64) -> bool {
        match self {
            Self::Int(v) => v <= upper,
            #[allow(clippy::cast_precision_loss)]
            Self::Float(v) => v <= upper as f64,
        }
    }
}

fn lower_bound_cmp(lhs: Option<i64>, rhs: Option<i64>) -> Ordering {
    match (lhs, rhs) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less, // Unbounded comes first
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) => x.cmp(&y),
    }
}

impl Default for HybridIndexes {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract indexable atoms from bytecode
///
/// Analyzes bytecode to identify simple conditions that can be indexed.
/// Returns empty vec if predicate is too complex to index efficiently.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn extract_indexable_atoms<B: Backend>(
    bytecode: &BytecodeProgram<B>,
    _deps: &[ColumnId],
) -> Vec<IndexableAtom> {
    let mut atoms = Vec::new();
    let instructions = &bytecode.instructions;

    // Safety: OR predicates require ALL branches to be indexed for
    // correctness. If any branch isn't indexed, the predicate could be
    // missed during candidate selection. Add Fallback to ensure OR
    // predicates are always VM-evaluated.
    let has_or = instructions
        .iter()
        .any(|instr| matches!(instr, Instruction::Or | Instruction::JumpIfTrue(_)));
    if has_or {
        atoms.push(IndexableAtom::Fallback);
    }

    let mut i = 0;
    while i < instructions.len() {
        // 5-instruction pattern: LoadColumn, PushLiteral, Negate, PushLiteral, Between.
        if let Some(
            [Instruction::LoadColumn(col), Instruction::PushLiteral(lower), Instruction::Negate, Instruction::PushLiteral(upper), Instruction::Between],
        ) = instructions.get(i..i.saturating_add(5))
        {
            if let (Some(lower_val), Some(upper_val)) = (value_as_i64(lower), value_as_i64(upper)) {
                atoms.push(IndexableAtom::Range {
                    column_id: *col,
                    lower: Some(-lower_val),
                    upper: Some(upper_val),
                });
            }
            i += 5;
            continue;
        }

        // 4-instruction pattern: LoadColumn, PushLiteral, PushLiteral, Between.
        if let Some(
            [Instruction::LoadColumn(col), Instruction::PushLiteral(lower), Instruction::PushLiteral(upper), Instruction::Between],
        ) = instructions.get(i..i.saturating_add(4))
        {
            if let (Some(lower_val), Some(upper_val)) = (value_as_i64(lower), value_as_i64(upper)) {
                atoms.push(IndexableAtom::Range {
                    column_id: *col,
                    lower: Some(lower_val),
                    upper: Some(upper_val),
                });
            }
            i += 4;
            continue;
        }

        // 3-instruction patterns.
        match instructions.get(i..i.saturating_add(3)) {
            // LoadColumn, PushLiteral, Equal -> Equality index.
            Some(
                [Instruction::LoadColumn(col), Instruction::PushLiteral(val), Instruction::Equal],
            ) => {
                if let Some(indexable) = IndexableCell::from_value(val) {
                    atoms.push(IndexableAtom::Equality {
                        column_id: *col,
                        value: indexable,
                    });
                }
                i += 3;
            }

            // LoadColumn, PushLiteral, NotEqual -> not easily indexable, skip.
            Some(
                [Instruction::LoadColumn(_), Instruction::PushLiteral(_), Instruction::NotEqual],
            ) => {
                i += 3;
            }

            // LoadColumn, PushLiteral, GreaterThan -> Range [val+1, inf).
            Some(
                [Instruction::LoadColumn(col), Instruction::PushLiteral(val), Instruction::GreaterThan],
            ) => {
                if let Some(int_val) = value_as_i64(val) {
                    atoms.push(IndexableAtom::Range {
                        column_id: *col,
                        lower: Some(int_val.saturating_add(1)),
                        upper: None,
                    });
                }
                i += 3;
            }

            // LoadColumn, PushLiteral, GreaterThanOrEqual -> Range [val, inf).
            Some(
                [Instruction::LoadColumn(col), Instruction::PushLiteral(val), Instruction::GreaterThanOrEqual],
            ) => {
                if let Some(int_val) = value_as_i64(val) {
                    atoms.push(IndexableAtom::Range {
                        column_id: *col,
                        lower: Some(int_val),
                        upper: None,
                    });
                }
                i += 3;
            }

            // LoadColumn, PushLiteral, LessThan -> Range (-inf, val-1].
            Some(
                [Instruction::LoadColumn(col), Instruction::PushLiteral(val), Instruction::LessThan],
            ) => {
                if let Some(int_val) = value_as_i64(val) {
                    atoms.push(IndexableAtom::Range {
                        column_id: *col,
                        lower: None,
                        upper: Some(int_val.saturating_sub(1)),
                    });
                }
                i += 3;
            }

            // LoadColumn, PushLiteral, LessThanOrEqual -> Range (-inf, val].
            Some(
                [Instruction::LoadColumn(col), Instruction::PushLiteral(val), Instruction::LessThanOrEqual],
            ) => {
                if let Some(int_val) = value_as_i64(val) {
                    atoms.push(IndexableAtom::Range {
                        column_id: *col,
                        lower: None,
                        upper: Some(int_val),
                    });
                }
                i += 3;
            }

            _ => match instructions.get(i) {
                // LoadColumn, IsNull / IsNotNull / In -> NULL / multiple equality atoms.
                Some(Instruction::LoadColumn(col)) => {
                    if matches!(instructions.get(i + 1), Some(Instruction::IsNull)) {
                        atoms.push(IndexableAtom::Null {
                            column_id: *col,
                            kind: NullKind::IsNull,
                        });
                        i += 2;
                    } else if matches!(instructions.get(i + 1), Some(Instruction::IsNotNull)) {
                        atoms.push(IndexableAtom::Null {
                            column_id: *col,
                            kind: NullKind::IsNotNull,
                        });
                        i += 2;
                    } else if let Some(Instruction::In(values)) = instructions.get(i + 1) {
                        for val in values {
                            if let Some(indexable) = IndexableCell::from_value(val) {
                                atoms.push(IndexableAtom::Equality {
                                    column_id: *col,
                                    value: indexable,
                                });
                            }
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }

                Some(_) => i += 1,
                None => break,
            },
        }
    }

    if atoms.is_empty() {
        vec![IndexableAtom::Fallback]
    } else {
        atoms
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::approx_constant)]
mod tests {
    use super::*;
    use crate::backend::{Postgres, Value};

    /// Every predicate under test here is a row subscription. Aggregate
    /// routing is covered in `partition`, where the candidate sets differ.
    const ROWS: &QueryProjection = &QueryProjection::Rows;

    #[test]
    fn test_indexable_cell_from_value() {
        assert_eq!(
            IndexableCell::from_value(&Value::<Postgres>::Int(42)),
            Some(IndexableCell::Int(42))
        );

        assert_eq!(
            IndexableCell::from_value(&Value::<Postgres>::Bool(true)),
            Some(IndexableCell::Bool(true))
        );

        assert_eq!(IndexableCell::from_value(&Value::<Postgres>::Null), None);
        assert_eq!(IndexableCell::from_value(&Value::<Postgres>::Missing), None);

        let float = IndexableCell::from_value(&Value::<Postgres>::Float(3.14));
        assert!(matches!(float, Some(IndexableCell::Float(_))));

        let string = IndexableCell::from_value(&Value::<Postgres>::String("test".into()));
        assert_eq!(string, Some(IndexableCell::String("test".into())));
    }

    #[test]
    fn test_equality_index() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Equality {
            column_id: 5,
            value: IndexableCell::Int(42),
        }];

        indexes.add_predicate(pred_id, &atoms, &[5], ROWS);

        let result = indexes.query_equality(5, &IndexableCell::Int(42));
        assert!(result.is_some());
        assert!(result.unwrap().contains(pred_id.as_u32()));

        let result = indexes.query_equality(5, &IndexableCell::Int(99));
        assert!(result.is_none());
    }

    #[test]
    fn test_equality_index_column_scoping() {
        let mut indexes = HybridIndexes::new();

        let pred_a = PredicateId::from_slab_index(0);
        indexes.add_predicate(
            pred_a,
            &[IndexableAtom::Equality {
                column_id: 5,
                value: IndexableCell::Int(42),
            }],
            &[5],
            ROWS,
        );

        let pred_b = PredicateId::from_slab_index(1);
        indexes.add_predicate(
            pred_b,
            &[IndexableAtom::Equality {
                column_id: 6,
                value: IndexableCell::Int(42),
            }],
            &[6],
            ROWS,
        );

        let hit_col_5 = indexes.query_equality(5, &IndexableCell::Int(42)).unwrap();
        assert!(hit_col_5.contains(pred_a.as_u32()));
        assert!(!hit_col_5.contains(pred_b.as_u32()));

        let hit_col_6 = indexes.query_equality(6, &IndexableCell::Int(42)).unwrap();
        assert!(hit_col_6.contains(pred_b.as_u32()));
        assert!(!hit_col_6.contains(pred_a.as_u32()));

        assert!(indexes.query_equality(7, &IndexableCell::Int(42)).is_none());
        assert!(indexes
            .query_equality(5, &IndexableCell::Int(999))
            .is_none());
    }

    #[test]
    fn test_range_index() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Range {
            column_id: 3,
            lower: Some(10),
            upper: Some(20),
        }];

        indexes.add_predicate(pred_id, &atoms, &[3], ROWS);

        // Value in range
        let result = indexes.query_range(3, &IndexableCell::Int(15));
        assert!(result.contains(pred_id.as_u32()));

        // Value outside range
        let result = indexes.query_range(3, &IndexableCell::Int(25));
        assert!(!result.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_null_index() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Null {
            column_id: 7,
            kind: NullKind::IsNull,
        }];

        indexes.add_predicate(pred_id, &atoms, &[7], ROWS);

        let bitmap = indexes.null_checks.get(&(7, NullKind::IsNull));
        assert!(bitmap.is_some());
        assert!(bitmap.unwrap().contains(pred_id.as_u32()));
    }

    #[test]
    fn test_fallback_index() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Fallback];

        indexes.add_predicate(pred_id, &atoms, &[], ROWS);

        assert!(indexes.fallback.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_dependency_tracking() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Fallback];

        indexes.add_predicate(pred_id, &atoms, &[1, 2, 5], ROWS);

        assert!(indexes
            .dependency
            .get(&1)
            .unwrap()
            .contains(pred_id.as_u32()));
        assert!(indexes
            .dependency
            .get(&2)
            .unwrap()
            .contains(pred_id.as_u32()));
        assert!(indexes
            .dependency
            .get(&5)
            .unwrap()
            .contains(pred_id.as_u32()));
    }

    #[test]
    fn test_predicate_reading_no_column_gets_no_dependency_entry() {
        let mut indexes = HybridIndexes::new();
        let pred_id = PredicateId::from_slab_index(0);
        indexes.add_predicate(pred_id, &[IndexableAtom::Fallback], &[], ROWS);

        assert!(indexes.dependency.is_empty());
        assert!(
            indexes.full_row.contains(pred_id.as_u32()),
            "a row predicate is an UPDATE candidate whether or not it reads a column"
        );
    }

    #[test]
    fn test_no_atoms_goes_to_fallback() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);

        indexes.add_predicate(pred_id, &[], &[], ROWS);

        assert!(!indexes.fallback.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_extract_equality_pattern() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: age = 42
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Value::<Postgres>::Int(42)),
            Instruction::Equal,
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[1]);

        assert_eq!(atoms.len(), 1);
        assert!(matches!(
            atoms[0],
            IndexableAtom::Equality {
                column_id: 1,
                value: IndexableCell::Int(42)
            }
        ));
    }

    #[test]
    fn test_extract_range_patterns() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: age > 18
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Value::<Postgres>::Int(18)),
            Instruction::GreaterThan,
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[1]);

        assert_eq!(atoms.len(), 1);
        assert!(matches!(
            atoms[0],
            IndexableAtom::Range {
                column_id: 1,
                lower: Some(19),
                upper: None
            }
        ));
    }

    #[test]
    fn test_extract_between_pattern() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: age BETWEEN 18 AND 65
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Value::<Postgres>::Int(18)),
            Instruction::PushLiteral(Value::<Postgres>::Int(65)),
            Instruction::Between,
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[1]);

        assert_eq!(atoms.len(), 1);
        assert!(matches!(
            atoms[0],
            IndexableAtom::Range {
                column_id: 1,
                lower: Some(18),
                upper: Some(65)
            }
        ));
    }

    #[test]
    fn test_extract_null_patterns() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: status IS NULL
        let bytecode =
            BytecodeProgram::<Postgres>::new(vec![Instruction::LoadColumn(2), Instruction::IsNull]);

        let atoms = extract_indexable_atoms(&bytecode, &[2]);

        assert_eq!(atoms.len(), 1);
        assert!(matches!(
            atoms[0],
            IndexableAtom::Null {
                column_id: 2,
                kind: NullKind::IsNull
            }
        ));
    }

    #[test]
    fn test_extract_in_pattern() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: status IN ('active', 'pending')
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(2),
            Instruction::In(vec![
                Value::<Postgres>::String("active".into()),
                Value::<Postgres>::String("pending".into()),
            ]),
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[2]);

        assert_eq!(atoms.len(), 2);
        assert!(atoms.iter().any(|a| matches!(
            a,
            IndexableAtom::Equality {
                column_id: 2,
                value: IndexableCell::String(s)
            } if s.as_ref() == "active"
        )));
        assert!(atoms.iter().any(|a| matches!(
            a,
            IndexableAtom::Equality {
                column_id: 2,
                value: IndexableCell::String(s)
            } if s.as_ref() == "pending"
        )));
    }

    #[test]
    fn test_extract_complex_returns_fallback() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: name LIKE '%test%' (complex, not easily indexable)
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::<Postgres>::String("%test%".into())),
            Instruction::Like {
                case_sensitive: true,
            },
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[0]);

        assert_eq!(atoms.len(), 1);
        assert!(matches!(atoms[0], IndexableAtom::Fallback));
    }

    #[test]
    fn test_hybrid_indexes_default() {
        let indexes = HybridIndexes::default();
        assert!(indexes.equality.is_empty());
        assert!(indexes.fallback.is_empty());
    }

    #[test]
    fn test_range_query_with_float() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Range {
            column_id: 3,
            lower: Some(10),
            upper: Some(20),
        }];

        indexes.add_predicate(pred_id, &atoms, &[3], ROWS);

        // Query with float value (gets converted to int for range)
        let result = indexes.query_range(3, &IndexableCell::Float(15.5f64.to_bits()));
        assert!(result.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_range_query_with_string() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Range {
            column_id: 3,
            lower: Some(10),
            upper: Some(20),
        }];

        indexes.add_predicate(pred_id, &atoms, &[3], ROWS);

        // Query with string value (doesn't match range, returns empty)
        let result = indexes.query_range(3, &IndexableCell::String("test".into()));
        assert!(!result.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_range_unbounded_lower() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Range {
            column_id: 3,
            lower: None, // Unbounded lower
            upper: Some(20),
        }];

        indexes.add_predicate(pred_id, &atoms, &[3], ROWS);

        // Value below upper bound
        let result = indexes.query_range(3, &IndexableCell::Int(10));
        assert!(result.contains(pred_id.as_u32()));

        // Value above upper bound
        let result = indexes.query_range(3, &IndexableCell::Int(25));
        assert!(!result.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_range_unbounded_upper() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Range {
            column_id: 3,
            lower: Some(10),
            upper: None, // Unbounded upper
        }];

        indexes.add_predicate(pred_id, &atoms, &[3], ROWS);

        // Value above lower bound
        let result = indexes.query_range(3, &IndexableCell::Int(15));
        assert!(result.contains(pred_id.as_u32()));

        // Value below lower bound
        let result = indexes.query_range(3, &IndexableCell::Int(5));
        assert!(!result.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_range_fully_unbounded() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Range {
            column_id: 3,
            lower: None,
            upper: None, // Fully unbounded
        }];

        indexes.add_predicate(pred_id, &atoms, &[3], ROWS);

        // Any value should match
        let result = indexes.query_range(3, &IndexableCell::Int(100));
        assert!(result.contains(pred_id.as_u32()));

        let result = indexes.query_range(3, &IndexableCell::Int(-100));
        assert!(result.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_equality_query_different_types() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);

        // Add equality for Bool
        indexes.add_predicate(
            pred_id,
            &[IndexableAtom::Equality {
                column_id: 1,
                value: IndexableCell::Bool(true),
            }],
            &[1],
            ROWS,
        );

        // Add equality for String
        let pred_id2 = PredicateId::from_slab_index(1);
        indexes.add_predicate(
            pred_id2,
            &[IndexableAtom::Equality {
                column_id: 2,
                value: IndexableCell::String("test".into()),
            }],
            &[2],
            ROWS,
        );

        // Query bool
        let result = indexes.query_equality(1, &IndexableCell::Bool(true));
        assert!(result.is_some());

        // Query string
        let result = indexes.query_equality(2, &IndexableCell::String("test".into()));
        assert!(result.is_some());
    }

    #[test]
    fn test_null_check_is_not_null() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Null {
            column_id: 5,
            kind: NullKind::IsNotNull,
        }];

        indexes.add_predicate(pred_id, &atoms, &[5], ROWS);

        let bitmap = indexes.null_checks.get(&(5, NullKind::IsNotNull));
        assert!(bitmap.is_some());
        assert!(bitmap.unwrap().contains(pred_id.as_u32()));
    }

    // ========================================================================
    // Push Coverage: Extract Indexable Atoms - All Patterns
    // ========================================================================

    #[test]
    fn test_extract_not_equal_pattern() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: age != 42 (not indexable, skipped)
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Value::<Postgres>::Int(42)),
            Instruction::NotEqual,
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[1]);

        // NotEqual is not indexable, should fall back
        assert_eq!(atoms.len(), 1);
        assert!(matches!(atoms[0], IndexableAtom::Fallback));
    }

    #[test]
    fn test_extract_greater_than_or_equal_pattern() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: age >= 18
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Value::<Postgres>::Int(18)),
            Instruction::GreaterThanOrEqual,
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[1]);

        assert_eq!(atoms.len(), 1);
        assert!(matches!(
            atoms[0],
            IndexableAtom::Range {
                column_id: 1,
                lower: Some(18),
                upper: None
            }
        ));
    }

    #[test]
    fn test_extract_less_than_pattern() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: age < 30
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Value::<Postgres>::Int(30)),
            Instruction::LessThan,
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[1]);

        assert_eq!(atoms.len(), 1);
        assert!(matches!(
            atoms[0],
            IndexableAtom::Range {
                column_id: 1,
                lower: None,
                upper: Some(29)
            }
        ));
    }

    #[test]
    fn test_extract_less_than_or_equal_pattern() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: age <= 65
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Value::<Postgres>::Int(65)),
            Instruction::LessThanOrEqual,
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[1]);

        assert_eq!(atoms.len(), 1);
        assert!(matches!(
            atoms[0],
            IndexableAtom::Range {
                column_id: 1,
                lower: None,
                upper: Some(65)
            }
        ));
    }

    #[test]
    fn test_extract_is_not_null_pattern() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: status IS NOT NULL
        let bytecode = BytecodeProgram::<Postgres>::new(vec![
            Instruction::LoadColumn(2),
            Instruction::IsNotNull,
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[2]);

        assert_eq!(atoms.len(), 1);
        assert!(matches!(
            atoms[0],
            IndexableAtom::Null {
                column_id: 2,
                kind: NullKind::IsNotNull
            }
        ));
    }

    #[test]
    fn test_extract_gte_with_float_not_indexed() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: price >= 9.99 (Float - range only works with Int)
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Value::<Postgres>::Float(9.99)),
            Instruction::GreaterThanOrEqual,
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[1]);

        // Float can't be range-indexed, falls back
        assert_eq!(atoms.len(), 1);
        assert!(matches!(atoms[0], IndexableAtom::Fallback));
    }

    #[test]
    fn test_extract_lt_with_float_not_indexed() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: price < 100.0 (Float - range only works with Int)
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Value::<Postgres>::Float(100.0)),
            Instruction::LessThan,
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[1]);

        assert_eq!(atoms.len(), 1);
        assert!(matches!(atoms[0], IndexableAtom::Fallback));
    }

    #[test]
    fn test_extract_lte_with_float_not_indexed() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: price <= 50.0 (Float - range only works with Int)
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Value::<Postgres>::Float(50.0)),
            Instruction::LessThanOrEqual,
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[1]);

        assert_eq!(atoms.len(), 1);
        assert!(matches!(atoms[0], IndexableAtom::Fallback));
    }

    #[test]
    fn test_finalize_ranges_sort_order() {
        let mut indexes = HybridIndexes::new();

        // Add multiple range entries with different lower bounds
        let pred1 = PredicateId::from_slab_index(0);
        let pred2 = PredicateId::from_slab_index(1);
        let pred3 = PredicateId::from_slab_index(2);

        // Unbounded lower (should come first after sort)
        indexes.add_predicate(
            pred1,
            &[IndexableAtom::Range {
                column_id: 1,
                lower: None,
                upper: Some(100),
            }],
            &[1],
            ROWS,
        );

        // Bounded lower (should come last)
        indexes.add_predicate(
            pred2,
            &[IndexableAtom::Range {
                column_id: 1,
                lower: Some(50),
                upper: Some(200),
            }],
            &[1],
            ROWS,
        );

        // Another unbounded lower (None,None case)
        indexes.add_predicate(
            pred3,
            &[IndexableAtom::Range {
                column_id: 1,
                lower: None,
                upper: None,
            }],
            &[1],
            ROWS,
        );

        indexes.finalize_ranges();

        // After sorting: unbounded (None) comes before bounded (Some)
        let entries = indexes.range.get(&1).unwrap();
        assert_eq!(entries.len(), 3);

        // First two entries should have None lower bounds
        assert!(entries[0].lower.is_none());
        assert!(entries[1].lower.is_none());
        // Last should have Some lower bound
        assert_eq!(entries[2].lower, Some(50));
    }

    #[test]
    fn test_select_update_deps() {
        let mut free = RoaringBitmap::new();
        free.insert(0); // pred 0 is dependency-free

        let mut dep_map = HashMap::new();
        let mut col1_deps = RoaringBitmap::new();
        col1_deps.insert(1); // pred 1 depends on col 1
        dep_map.insert(1_u16, col1_deps);

        // No changed columns: only free
        let result = HybridIndexes::select_update_deps(&free, &dep_map, &[]);
        assert!(result.contains(0));
        assert!(!result.contains(1));

        // Changed col 1: free + col1 deps
        let result = HybridIndexes::select_update_deps(&free, &dep_map, &[1]);
        assert!(result.contains(0));
        assert!(result.contains(1));

        // Changed col 99: only free (no deps for col 99)
        let result = HybridIndexes::select_update_deps(&free, &dep_map, &[99]);
        assert!(result.contains(0));
        assert!(!result.contains(1));
    }
}

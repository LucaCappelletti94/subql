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
use crate::compiler::{PlannerAtom, PlannerValue};
use crate::ColumnId;
use alloc::sync::Arc;
use hashbrown::HashMap;
use roaring::RoaringBitmap;
use rpds::RedBlackTreeMapSync;

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

/// Range index key, ordered by lower bound.
///
/// `Ord` puts an unbounded lower bound first and orders the rest along the
/// numeric line, so a probe walks the column in bound order and stops at the
/// first key it cannot reach. The predicate id only breaks ties between equal
/// bounds.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RangeKey {
    pub lower: Option<i64>,
    pub predicate_id: PredicateId,
}

/// One column's range entries, keyed by lower bound, valued by upper bound.
///
/// Persistent because registration clones the whole index to patch it, and a
/// `Vec` made that clone cost the length of the column.
pub type RangeColumn = RedBlackTreeMapSync<RangeKey, Option<i64>>;

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

    /// Range: col -> entries ordered by lower bound
    pub range: HashMap<ColumnId, RangeColumn>,

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

    // Aggregate (COUNT/SUM/...) predicate indexes, parallel to row indexes.
    // Kept separate so `select_candidates` never returns agg predicates and
    // `select_agg_candidates` never returns row predicates.
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

        if matches!(
            projection,
            QueryProjection::Aggregate(_) | QueryProjection::GroupedAggregate { .. }
        ) {
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
                    let key = RangeKey {
                        lower: *lower,
                        predicate_id: pred_id,
                    };
                    let column = self.range.entry(*column_id).or_default();
                    // One predicate can file two ranges on a column sharing a
                    // lower bound, as `a BETWEEN 5 AND 10 OR a BETWEEN 5 AND
                    // 20` does. The key holds one of them, so it holds the
                    // wider upper bound, which keeps the index a superset and
                    // leaves the verdict to the comparator.
                    let upper = column
                        .get(&key)
                        .map_or(*upper, |held| widest_upper(*held, *upper));
                    column.insert_mut(key, upper);
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

    /// Take a predicate out of the indexes, by the same routing that put it
    /// in.
    ///
    /// The caller states the atoms, dependencies and projection the predicate
    /// was added with, because those are what decided where its bit went, and
    /// reading them back off a store that no longer holds it is not possible.
    pub fn remove_predicate(
        &mut self,
        pred_id: PredicateId,
        atoms: &[IndexableAtom],
        deps: &[ColumnId],
        projection: &QueryProjection,
    ) {
        let pred_id_u32 = pred_id.as_u32();

        if matches!(
            projection,
            QueryProjection::Aggregate(_) | QueryProjection::GroupedAggregate { .. }
        ) {
            if deps.is_empty() {
                self.agg_dependency_free.remove(pred_id_u32);
            } else {
                for &col_id in deps {
                    if let Some(bitmap) = self.agg_dependency.get_mut(&col_id) {
                        bitmap.remove(pred_id_u32);
                        if bitmap.is_empty() {
                            self.agg_dependency.remove(&col_id);
                        }
                    }
                }
            }
            self.agg_fallback.remove(pred_id_u32);
            return;
        }

        for &col_id in deps {
            if let Some(bitmap) = self.dependency.get_mut(&col_id) {
                bitmap.remove(pred_id_u32);
                if bitmap.is_empty() {
                    self.dependency.remove(&col_id);
                }
            }
        }
        self.full_row.remove(pred_id_u32);

        for atom in atoms {
            match atom {
                IndexableAtom::Equality { column_id, value } => {
                    if let Some(values) = self.equality.get_mut(column_id) {
                        if let Some(bitmap) = values.get_mut(value) {
                            bitmap.remove(pred_id_u32);
                            if bitmap.is_empty() {
                                values.remove(value);
                            }
                        }
                        if values.is_empty() {
                            self.equality.remove(column_id);
                        }
                    }
                }

                IndexableAtom::Range {
                    column_id, lower, ..
                } => {
                    if let Some(entries) = self.range.get_mut(column_id) {
                        entries.remove_mut(&RangeKey {
                            lower: *lower,
                            predicate_id: pred_id,
                        });
                        if entries.is_empty() {
                            self.range.remove(column_id);
                        }
                    }
                }

                IndexableAtom::Null { column_id, kind } => {
                    if let Some(bitmap) = self.null_checks.get_mut(&(*column_id, *kind)) {
                        bitmap.remove(pred_id_u32);
                        if bitmap.is_empty() {
                            self.null_checks.remove(&(*column_id, *kind));
                        }
                    }
                }

                IndexableAtom::Fallback => {
                    self.fallback.remove(pred_id_u32);
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
        // Ranges are numeric, so a cell of any other scalar is not a
        // candidate for one.
        let Some(numeric) = NumericValue::from_indexable(value) else {
            return;
        };

        let Some(entries) = self.range.get(&col_id) else {
            return;
        };

        // A NaN has no place on the numeric line, and this index is not told
        // which backend it serves: PostgreSQL orders NaN above every number,
        // IEEE leaves it unordered. Bounding it here would put the prefilter
        // ahead of the comparator and drop rows the database returns, so
        // every entry on the column stays a candidate and the comparator
        // alone decides.
        if numeric.is_unordered() {
            out.extend(entries.keys().map(|key| key.predicate_id.as_u32()));
            return;
        }

        for (key, upper) in entries {
            // Entries walk in lower-bound order, so once a lower bound
            // exceeds the searched value no later entry can match.
            if let Some(lower) = key.lower {
                if !numeric.gte_lower(lower) {
                    break;
                }
            }

            if upper.is_none_or(|u| numeric.lte_upper(u)) {
                out.insert(key.predicate_id.as_u32());
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
            // NaN is carried, not dropped: the caller decides what an
            // unordered value means for pruning.
            IndexableCell::Float(bits) => Some(Self::Float(f64::from_bits(*bits))),
            _ => None,
        }
    }

    /// Whether this value has no position on the numeric line, which only a
    /// NaN has. The bound tests below are meaningless for it.
    const fn is_unordered(self) -> bool {
        match self {
            Self::Int(_) => false,
            Self::Float(v) => v.is_nan(),
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

/// The upper bound that admits every row either bound admits. Unbounded wins,
/// otherwise the larger one does.
const fn widest_upper(lhs: Option<i64>, rhs: Option<i64>) -> Option<i64> {
    match (lhs, rhs) {
        (Some(left), Some(right)) => Some(if left >= right { left } else { right }),
        _ => None,
    }
}

impl Default for HybridIndexes {
    fn default() -> Self {
        Self::new()
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

    /// `a BETWEEN 5 AND 10 OR a BETWEEN 5 AND 20` files two ranges for one
    /// predicate under one lower bound. The column keeps one entry per bound,
    /// so it must keep the wider of the two, or a row inside the wider range
    /// never becomes a candidate and its subscription never sees it.
    #[test]
    fn two_ranges_sharing_a_lower_bound_keep_the_wider_one() {
        let mut indexes = HybridIndexes::new();
        let pred_id = PredicateId::from_slab_index(0);
        // The wider range arrives first, so an index that lets the last
        // atom win keeps the narrow one and loses the rows between.
        let atoms = vec![
            IndexableAtom::Range {
                column_id: 3,
                lower: Some(5),
                upper: Some(20),
            },
            IndexableAtom::Range {
                column_id: 3,
                lower: Some(5),
                upper: Some(10),
            },
        ];

        indexes.add_predicate(pred_id, &atoms, &[3], ROWS);

        let result = indexes.query_range(3, &IndexableCell::Int(15));
        assert!(
            result.contains(pred_id.as_u32()),
            "a row the second range admits is a candidate"
        );
        let result = indexes.query_range(3, &IndexableCell::Int(25));
        assert!(
            !result.contains(pred_id.as_u32()),
            "and one past both is not"
        );
    }

    /// An unbounded upper admits everything above the lower bound, so it is
    /// the wider one whichever order the two arrive in.
    #[test]
    fn an_unbounded_range_survives_a_bounded_twin() {
        let mut indexes = HybridIndexes::new();
        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![
            IndexableAtom::Range {
                column_id: 3,
                lower: Some(5),
                upper: None,
            },
            IndexableAtom::Range {
                column_id: 3,
                lower: Some(5),
                upper: Some(10),
            },
        ];

        indexes.add_predicate(pred_id, &atoms, &[3], ROWS);

        assert!(
            indexes
                .query_range(3, &IndexableCell::Int(1_000))
                .contains(pred_id.as_u32()),
            "the unbounded range still answers"
        );
    }

    /// Two predicates may hold the same bounds on the same column, and the
    /// column is keyed by bound, so removing one must take only its own key
    /// with it.
    #[test]
    fn removing_one_range_leaves_a_twin_on_the_same_bounds() {
        let mut indexes = HybridIndexes::new();
        let removed = PredicateId::from_slab_index(0);
        let twin = PredicateId::from_slab_index(1);
        let atoms = vec![IndexableAtom::Range {
            column_id: 3,
            lower: Some(10),
            upper: Some(20),
        }];

        indexes.add_predicate(removed, &atoms, &[3], ROWS);
        indexes.add_predicate(twin, &atoms, &[3], ROWS);
        indexes.remove_predicate(removed, &atoms, &[3], ROWS);

        let result = indexes.query_range(3, &IndexableCell::Int(15));
        assert!(
            !result.contains(removed.as_u32()),
            "the removed predicate is gone from the column"
        );
        assert!(
            result.contains(twin.as_u32()),
            "and the one that shared its bounds is not"
        );
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

    // Push Coverage: Extract Indexable Atoms - All Patterns

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

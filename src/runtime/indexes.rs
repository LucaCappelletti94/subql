//! Hybrid indexing for fast candidate selection
//!
//! Five index types:
//! 1. Equality: (col, val) → predicates with col=val
//! 2. Range: col → predicates with col IN range
//! 3. NULL: (col, kind) → predicates checking IS NULL / IS NOT NULL
//! 4. Fallback: unindexable predicates (LIKE, complex expressions)
//! 5. Dependency: col → predicates referencing col (for UPDATE optimization)

use std::sync::Arc;
use std::cmp::Ordering;
use ahash::AHashMap;
use roaring::RoaringBitmap;
use crate::{Cell, ColumnId};
use super::ids::PredicateId;

/// Indexable cell value (excludes NULL/Missing)
///
/// Float is stored as u64 (via f64::to_bits()) for hashing.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum IndexableCell {
    Bool(bool),
    Int(i64),
    Float(u64),  // f64::to_bits()
    String(Arc<str>),
}

impl IndexableCell {
    /// Convert Cell to IndexableCell (None if NULL/Missing)
    #[must_use]
    pub fn from_cell(cell: &Cell) -> Option<Self> {
        match cell {
            Cell::Bool(b) => Some(Self::Bool(*b)),
            Cell::Int(i) => Some(Self::Int(*i)),
            Cell::Float(f) => Some(Self::Float(f.to_bits())),
            Cell::String(s) => Some(Self::String(s.clone())),
            Cell::Null | Cell::Missing => None,
        }
    }
}

/// Equality index key
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EqualityKey {
    pub column_id: ColumnId,
    pub value: IndexableCell,
}

/// Range index entry (sorted by lower bound)
#[derive(Clone, Debug)]
pub struct RangeEntry {
    pub predicate_id: PredicateId,
    pub lower: Option<i64>,  // None = unbounded
    pub upper: Option<i64>,  // None = unbounded
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
    Null {
        column_id: ColumnId,
        kind: NullKind,
    },
    /// Unindexable (LIKE, complex expressions)
    Fallback,
}

/// Hybrid indexes for candidate selection
#[derive(Clone)]
pub struct HybridIndexes {
    /// Equality: (col, val) → RoaringBitmap<PredicateId>
    pub equality: AHashMap<EqualityKey, RoaringBitmap>,

    /// Range: col → Vec<RangeEntry> (sorted by lower bound)
    pub range: AHashMap<ColumnId, Vec<RangeEntry>>,

    /// NULL: (col, kind) → RoaringBitmap<PredicateId>
    pub null_checks: AHashMap<(ColumnId, NullKind), RoaringBitmap>,

    /// Fallback: unindexable predicates
    pub fallback: RoaringBitmap,

    /// Dependency: col → RoaringBitmap<PredicateId> (for UPDATE optimization)
    pub dependency: AHashMap<ColumnId, RoaringBitmap>,
}

impl HybridIndexes {
    /// Create new empty indexes
    #[must_use]
    pub fn new() -> Self {
        Self {
            equality: AHashMap::new(),
            range: AHashMap::new(),
            null_checks: AHashMap::new(),
            fallback: RoaringBitmap::new(),
            dependency: AHashMap::new(),
        }
    }

    /// Add predicate to indexes based on indexable atoms
    pub fn add_predicate(&mut self, pred_id: PredicateId, atoms: &[IndexableAtom], deps: &[ColumnId]) {
        let pred_id_u32 = pred_id.as_u32();

        // Track dependencies
        for &col_id in deps {
            self.dependency
                .entry(col_id)
                .or_default()
                .insert(pred_id_u32);
        }

        // If no indexable atoms, add to fallback
        if atoms.is_empty() {
            self.fallback.insert(pred_id_u32);
            return;
        }

        // Add to appropriate indexes
        let mut indexed = false;

        for atom in atoms {
            match atom {
                IndexableAtom::Equality { column_id, value } => {
                    let key = EqualityKey {
                        column_id: *column_id,
                        value: value.clone(),
                    };
                    self.equality
                        .entry(key)
                        .or_default()
                        .insert(pred_id_u32);
                    indexed = true;
                }

                IndexableAtom::Range { column_id, lower, upper } => {
                    self.range
                        .entry(*column_id)
                        .or_default()
                        .push(RangeEntry {
                            predicate_id: pred_id,
                            lower: *lower,
                            upper: *upper,
                        });
                    indexed = true;
                }

                IndexableAtom::Null { column_id, kind } => {
                    self.null_checks
                        .entry((*column_id, *kind))
                        .or_default()
                        .insert(pred_id_u32);
                    indexed = true;
                }

                IndexableAtom::Fallback => {
                    self.fallback.insert(pred_id_u32);
                    indexed = true;
                }
            }
        }

        // If nothing indexed, add to fallback
        if !indexed {
            self.fallback.insert(pred_id_u32);
        }
    }

    /// Query equality index
    #[must_use]
    pub fn query_equality(&self, col_id: ColumnId, value: &IndexableCell) -> Option<&RoaringBitmap> {
        let key = EqualityKey {
            column_id: col_id,
            value: value.clone(),
        };
        self.equality.get(&key)
    }

    /// Query range index (return predicates whose ranges contain value)
    #[must_use]
    pub fn query_range(&self, col_id: ColumnId, value: &IndexableCell) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();

        // Only works for numeric values
        let int_val = match value {
            IndexableCell::Int(i) => *i,
            IndexableCell::Float(bits) => f64::from_bits(*bits) as i64,
            _ => return result,
        };

        if let Some(entries) = self.range.get(&col_id) {
            for entry in entries {
                // Check if int_val is in [lower, upper]
                let in_lower = entry.lower.map_or(true, |l| int_val >= l);
                let in_upper = entry.upper.map_or(true, |u| int_val <= u);

                if in_lower && in_upper {
                    result.insert(entry.predicate_id.as_u32());
                }
            }
        }

        result
    }

    /// Sort range entries by lower bound (for efficient querying)
    pub fn finalize_ranges(&mut self) {
        for entries in self.range.values_mut() {
            entries.sort_by(|a, b| {
                match (a.lower, b.lower) {
                    (None, None) => Ordering::Equal,
                    (None, Some(_)) => Ordering::Less,  // Unbounded comes first
                    (Some(_), None) => Ordering::Greater,
                    (Some(x), Some(y)) => x.cmp(&y),
                }
            });
        }
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
pub fn extract_indexable_atoms(
    _bytecode: &crate::compiler::BytecodeProgram,
    _deps: &[ColumnId],
) -> Vec<IndexableAtom> {
    // TODO: Full atom extraction in Phase 2
    // For now, return Fallback for all predicates
    vec![IndexableAtom::Fallback]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indexable_cell_from_cell() {
        assert_eq!(
            IndexableCell::from_cell(&Cell::Int(42)),
            Some(IndexableCell::Int(42))
        );

        assert_eq!(
            IndexableCell::from_cell(&Cell::Bool(true)),
            Some(IndexableCell::Bool(true))
        );

        assert_eq!(IndexableCell::from_cell(&Cell::Null), None);
        assert_eq!(IndexableCell::from_cell(&Cell::Missing), None);
    }

    #[test]
    fn test_equality_index() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Equality {
            column_id: 5,
            value: IndexableCell::Int(42),
        }];

        indexes.add_predicate(pred_id, &atoms, &[5]);

        let result = indexes.query_equality(5, &IndexableCell::Int(42));
        assert!(result.is_some());
        assert!(result.unwrap().contains(pred_id.as_u32()));

        let result = indexes.query_equality(5, &IndexableCell::Int(99));
        assert!(result.is_none());
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

        indexes.add_predicate(pred_id, &atoms, &[3]);

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

        indexes.add_predicate(pred_id, &atoms, &[7]);

        let bitmap = indexes.null_checks.get(&(7, NullKind::IsNull));
        assert!(bitmap.is_some());
        assert!(bitmap.unwrap().contains(pred_id.as_u32()));
    }

    #[test]
    fn test_fallback_index() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Fallback];

        indexes.add_predicate(pred_id, &atoms, &[]);

        assert!(indexes.fallback.contains(pred_id.as_u32()));
    }

    #[test]
    fn test_dependency_tracking() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);
        let atoms = vec![IndexableAtom::Fallback];

        indexes.add_predicate(pred_id, &atoms, &[1, 2, 5]);

        assert!(indexes.dependency.get(&1).unwrap().contains(pred_id.as_u32()));
        assert!(indexes.dependency.get(&2).unwrap().contains(pred_id.as_u32()));
        assert!(indexes.dependency.get(&5).unwrap().contains(pred_id.as_u32()));
    }

    #[test]
    fn test_no_atoms_goes_to_fallback() {
        let mut indexes = HybridIndexes::new();

        let pred_id = PredicateId::from_slab_index(0);

        indexes.add_predicate(pred_id, &[], &[]);

        assert!(indexes.fallback.contains(pred_id.as_u32()));
    }
}

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
            Cell::String(s) => Some(Self::String(Arc::clone(s))),
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
    #[allow(clippy::cast_possible_truncation)]
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
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn extract_indexable_atoms(
    bytecode: &crate::compiler::BytecodeProgram,
    _deps: &[ColumnId],
) -> Vec<IndexableAtom> {
    use crate::compiler::Instruction;

    let mut atoms = Vec::new();
    let instructions = &bytecode.instructions;

    // Pattern matching on instruction sequences
    let mut i = 0;
    while i < instructions.len() {
        // Check for 4-instruction patterns first (BETWEEN)
        if let Some([Instruction::LoadColumn(col), Instruction::PushLiteral(lower), Instruction::PushLiteral(upper), Instruction::Between])
            = instructions.get(i..i.saturating_add(4))
        {
            if let (Cell::Int(lower_val), Cell::Int(upper_val)) = (lower, upper) {
                atoms.push(IndexableAtom::Range {
                    column_id: *col,
                    lower: Some(*lower_val),
                    upper: Some(*upper_val),
                });
            }
            i += 4;
            continue;
        }

        // Check for 3-instruction patterns
        match instructions.get(i..i.saturating_add(3)) {
            // Pattern 1: LoadColumn, PushLiteral, Equal → Equality index
            Some([Instruction::LoadColumn(col), Instruction::PushLiteral(val), Instruction::Equal]) => {
                if let Some(indexable) = IndexableCell::from_cell(val) {
                    atoms.push(IndexableAtom::Equality {
                        column_id: *col,
                        value: indexable,
                    });
                }
                i += 3;
            }

            // Pattern 2: LoadColumn, PushLiteral, NotEqual → Skip (not easily indexable)
            Some([Instruction::LoadColumn(_), Instruction::PushLiteral(_), Instruction::NotEqual]) => {
                i += 3;
            }

            // Pattern 3: LoadColumn, PushLiteral, GreaterThan → Range [val+1, ∞)
            Some([Instruction::LoadColumn(col), Instruction::PushLiteral(val), Instruction::GreaterThan]) => {
                if let Cell::Int(int_val) = val {
                    atoms.push(IndexableAtom::Range {
                        column_id: *col,
                        lower: Some(int_val.saturating_add(1)),
                        upper: None,
                    });
                }
                i += 3;
            }

            // Pattern 4: LoadColumn, PushLiteral, GreaterThanOrEqual → Range [val, ∞)
            Some([Instruction::LoadColumn(col), Instruction::PushLiteral(val), Instruction::GreaterThanOrEqual]) => {
                if let Cell::Int(int_val) = val {
                    atoms.push(IndexableAtom::Range {
                        column_id: *col,
                        lower: Some(*int_val),
                        upper: None,
                    });
                }
                i += 3;
            }

            // Pattern 5: LoadColumn, PushLiteral, LessThan → Range (-∞, val-1]
            Some([Instruction::LoadColumn(col), Instruction::PushLiteral(val), Instruction::LessThan]) => {
                if let Cell::Int(int_val) = val {
                    atoms.push(IndexableAtom::Range {
                        column_id: *col,
                        lower: None,
                        upper: Some(int_val.saturating_sub(1)),
                    });
                }
                i += 3;
            }

            // Pattern 6: LoadColumn, PushLiteral, LessThanOrEqual → Range (-∞, val]
            Some([Instruction::LoadColumn(col), Instruction::PushLiteral(val), Instruction::LessThanOrEqual]) => {
                if let Cell::Int(int_val) = val {
                    atoms.push(IndexableAtom::Range {
                        column_id: *col,
                        lower: None,
                        upper: Some(*int_val),
                    });
                }
                i += 3;
            }

            _ => {
                // Check for other patterns
                match instructions.get(i) {
                    // Pattern 7: LoadColumn, IsNull → NULL check
                    Some(Instruction::LoadColumn(col)) => {
                        if matches!(instructions.get(i + 1), Some(Instruction::IsNull)) {
                            atoms.push(IndexableAtom::Null {
                                column_id: *col,
                                kind: NullKind::IsNull,
                            });
                            i += 2;
                        } else if matches!(instructions.get(i + 1), Some(Instruction::IsNotNull)) {
                            // Pattern 8: LoadColumn, IsNotNull → NULL check
                            atoms.push(IndexableAtom::Null {
                                column_id: *col,
                                kind: NullKind::IsNotNull,
                            });
                            i += 2;
                        } else if matches!(instructions.get(i + 1), Some(Instruction::In(_))) {
                            // Pattern 9: LoadColumn, In([...]) → Multiple equality atoms
                            if let Some(Instruction::In(values)) = instructions.get(i + 1) {
                                for val in values {
                                    if let Some(indexable) = IndexableCell::from_cell(val) {
                                        atoms.push(IndexableAtom::Equality {
                                            column_id: *col,
                                            value: indexable,
                                        });
                                    }
                                }
                            }
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }

                    // Pattern 10: LoadColumn, PushLiteral, PushLiteral, Between → Range [lower, upper]
                    Some(_) => i += 1,

                    None => break,
                }
            }
        }
    }

    // If no indexable atoms found, return Fallback
    if atoms.is_empty() {
        vec![IndexableAtom::Fallback]
    } else {
        atoms
    }
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

    #[test]
    fn test_extract_equality_pattern() {
        use crate::compiler::{BytecodeProgram, Instruction};

        // Bytecode for: age = 42
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Cell::Int(42)),
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
            Instruction::PushLiteral(Cell::Int(18)),
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
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::PushLiteral(Cell::Int(65)),
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
        let bytecode = BytecodeProgram::new(vec![
            Instruction::LoadColumn(2),
            Instruction::IsNull,
        ]);

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
                Cell::String("active".into()),
                Cell::String("pending".into()),
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
            Instruction::PushLiteral(Cell::String("%test%".into())),
            Instruction::Like { case_sensitive: true },
        ]);

        let atoms = extract_indexable_atoms(&bytecode, &[0]);

        assert_eq!(atoms.len(), 1);
        assert!(matches!(atoms[0], IndexableAtom::Fallback));
    }

    // ========================================================================
    // Phase 3: Push to 95% Coverage - Indexes Completion
    // ========================================================================

    #[test]
    fn test_indexable_cell_from_float() {
        let float_cell = Cell::Float(3.14);
        let indexable = IndexableCell::from_cell(&float_cell);
        assert!(indexable.is_some());
        assert!(matches!(indexable, Some(IndexableCell::Float(_))));
    }

    #[test]
    fn test_indexable_cell_from_string() {
        let string_cell = Cell::String("test".into());
        let indexable = IndexableCell::from_cell(&string_cell);
        assert_eq!(indexable, Some(IndexableCell::String("test".into())));
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

        indexes.add_predicate(pred_id, &atoms, &[3]);

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

        indexes.add_predicate(pred_id, &atoms, &[3]);

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
            lower: None,  // Unbounded lower
            upper: Some(20),
        }];

        indexes.add_predicate(pred_id, &atoms, &[3]);

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
            upper: None,  // Unbounded upper
        }];

        indexes.add_predicate(pred_id, &atoms, &[3]);

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
            upper: None,  // Fully unbounded
        }];

        indexes.add_predicate(pred_id, &atoms, &[3]);

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
        indexes.add_predicate(pred_id, &[IndexableAtom::Equality {
            column_id: 1,
            value: IndexableCell::Bool(true),
        }], &[1]);

        // Add equality for String
        let pred_id2 = PredicateId::from_slab_index(1);
        indexes.add_predicate(pred_id2, &[IndexableAtom::Equality {
            column_id: 2,
            value: IndexableCell::String("test".into()),
        }], &[2]);

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

        indexes.add_predicate(pred_id, &atoms, &[5]);

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
            Instruction::PushLiteral(Cell::Int(42)),
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
            Instruction::PushLiteral(Cell::Int(18)),
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
            Instruction::PushLiteral(Cell::Int(30)),
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
            Instruction::PushLiteral(Cell::Int(65)),
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
        let bytecode = BytecodeProgram::new(vec![
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
            Instruction::PushLiteral(Cell::Float(9.99)),
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
            Instruction::PushLiteral(Cell::Float(100.0)),
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
            Instruction::PushLiteral(Cell::Float(50.0)),
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
        indexes.add_predicate(pred1, &[IndexableAtom::Range {
            column_id: 1, lower: None, upper: Some(100),
        }], &[1]);

        // Bounded lower (should come last)
        indexes.add_predicate(pred2, &[IndexableAtom::Range {
            column_id: 1, lower: Some(50), upper: Some(200),
        }], &[1]);

        // Another unbounded lower (None,None case)
        indexes.add_predicate(pred3, &[IndexableAtom::Range {
            column_id: 1, lower: None, upper: None,
        }], &[1]);

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
}

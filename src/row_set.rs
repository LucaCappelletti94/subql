//! Per-PK delta between two row sets.
//!
//! Pure function: given a previous snapshot and a current snapshot of the
//! same query keyed by primary key, produce the inserted / deleted /
//! updated triples that bring `prev` to `next`. Used by total-row-reexec
//! consumers, by snapshot refresh code, and by any caller that needs to
//! turn a re-execution result into CDC-like deltas.

use crate::{Cell, ColumnId, RowImage};
use alloc::vec::Vec;
use hashbrown::HashMap;

/// Per-PK delta between two row sets.
///
/// `inserted`: rows present in `next` but not `prev` (PK didn't exist before).
/// `deleted`:  rows present in `prev` but not `next` (PK no longer exists).
/// `updated`:  rows whose PK exists in both but whose cell contents
///             differ. The tuple is `(prev_row, next_row)`.
///
/// Order within each bucket is unspecified; callers that care about
/// stable ordering should sort by PK after the call.
///
/// # Examples
///
/// ```
/// use subql::RowSetDelta;
///
/// let delta = RowSetDelta::default();
/// assert!(delta.inserted.is_empty());
/// assert!(delta.deleted.is_empty());
/// assert!(delta.updated.is_empty());
/// ```
#[derive(Debug, Clone, Default)]
pub struct RowSetDelta {
    /// Rows that appeared in `next` but were not in `prev`.
    pub inserted: Vec<RowImage>,
    /// Rows that disappeared from `next` but were in `prev`.
    pub deleted: Vec<RowImage>,
    /// `(prev_row, next_row)` for rows whose PK survived but whose cells
    /// changed.
    pub updated: Vec<(RowImage, RowImage)>,
}

/// Diff two row sets keyed by `pk_cols`.
///
/// Inputs must be deduplicated by PK (the caller's snapshot results
/// normally are). Every row in `prev` and `next` is expected to contain
/// the columns in `pk_cols`; rows missing a PK column hash to a partial
/// key and may produce spurious matches.
///
/// Complexity: O(prev.len() + next.len()) hashed comparisons.
///
/// # Examples
///
/// One PK appears only in `next` (insert), one only in `prev`
/// (delete), one in both with differing cells (update).
///
/// ```
/// use std::sync::Arc;
/// use subql::{row_set_delta, Cell, ColumnId, RowImage};
///
/// let prev = vec![
///     RowImage { cells: Arc::from([Cell::Int(1), Cell::String("paid".into())]) },
///     RowImage { cells: Arc::from([Cell::Int(2), Cell::String("open".into())]) },
/// ];
/// let next = vec![
///     RowImage { cells: Arc::from([Cell::Int(1), Cell::String("shipped".into())]) },
///     RowImage { cells: Arc::from([Cell::Int(3), Cell::String("paid".into())]) },
/// ];
/// let pk_cols: &[ColumnId] = &[0];
///
/// let delta = row_set_delta(&prev, &next, pk_cols);
/// assert_eq!(delta.inserted.len(), 1);
/// assert_eq!(delta.deleted.len(), 1);
/// assert_eq!(delta.updated.len(), 1);
/// ```
#[must_use]
pub fn row_set_delta(prev: &[RowImage], next: &[RowImage], pk_cols: &[ColumnId]) -> RowSetDelta {
    let mut prev_by_pk: HashMap<Vec<u8>, &RowImage> = HashMap::with_capacity(prev.len());
    for row in prev {
        prev_by_pk.insert(pk_key(row, pk_cols), row);
    }

    let mut delta = RowSetDelta::default();
    for next_row in next {
        let key = pk_key(next_row, pk_cols);
        if let Some(prev_row) = prev_by_pk.remove(&key) {
            if !rows_equal(prev_row, next_row) {
                delta.updated.push((prev_row.clone(), next_row.clone()));
            }
            // PK match + cells equal: row unchanged, drop silently.
        } else {
            delta.inserted.push(next_row.clone());
        }
    }
    // Everything left in prev_by_pk was not seen in next.
    for prev_row in prev_by_pk.into_values() {
        delta.deleted.push(prev_row.clone());
    }

    delta
}

/// Serialize the PK cells of `row` into a canonical byte key suitable for
/// use as a `HashMap` key. Uses postcard so the encoding is deterministic
/// for the same `(cell, ordering)` and stable across the binary.
fn pk_key(row: &RowImage, pk_cols: &[ColumnId]) -> Vec<u8> {
    // Collect references; postcard serializes through Serialize impls.
    let pk_cells: Vec<&Cell> = pk_cols.iter().filter_map(|&c| row.get(c)).collect();
    postcard::to_allocvec(&pk_cells).unwrap_or_default()
}

/// Cell-by-cell equality between two row images.
fn rows_equal(a: &RowImage, b: &RowImage) -> bool {
    a.cells.len() == b.cells.len() && a.cells.iter().zip(b.cells.iter()).all(|(x, y)| x == y)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use alloc::sync::Arc;

    fn row(id: i64, p: i64) -> RowImage {
        RowImage {
            cells: Arc::from([Cell::Int(id), Cell::Int(p)]),
        }
    }

    /// T7.1: mixed insert + delete keyed by `id`.
    #[test]
    fn t7_1_insert_and_delete() {
        let prev = [row(1, 5), row(2, 9)];
        let next = [row(2, 9), row(3, 11)];
        let delta = row_set_delta(&prev, &next, &[0]);
        assert_eq!(delta.inserted.len(), 1);
        assert!(rows_equal(&delta.inserted[0], &row(3, 11)));
        assert_eq!(delta.deleted.len(), 1);
        assert!(rows_equal(&delta.deleted[0], &row(1, 5)));
        assert!(delta.updated.is_empty());
    }

    /// T7.2: same PK, different cells = update.
    #[test]
    fn t7_2_update_in_place() {
        let prev = [row(1, 5)];
        let next = [row(1, 7)];
        let delta = row_set_delta(&prev, &next, &[0]);
        assert!(delta.inserted.is_empty());
        assert!(delta.deleted.is_empty());
        assert_eq!(delta.updated.len(), 1);
        let (old, new) = &delta.updated[0];
        assert!(rows_equal(old, &row(1, 5)));
        assert!(rows_equal(new, &row(1, 7)));
    }

    /// T7.3: empty inputs yield all-empty delta.
    #[test]
    fn t7_3_empty_inputs() {
        let delta = row_set_delta(&[], &[], &[0]);
        assert!(delta.inserted.is_empty());
        assert!(delta.deleted.is_empty());
        assert!(delta.updated.is_empty());
    }

    /// Identical sets produce no delta entries.
    #[test]
    fn identical_sets_are_empty() {
        let set = [row(1, 5), row(2, 9), row(3, 11)];
        let delta = row_set_delta(&set, &set, &[0]);
        assert!(delta.inserted.is_empty());
        assert!(delta.deleted.is_empty());
        assert!(delta.updated.is_empty());
    }

    /// Composite PK: two columns must both match for "same PK".
    #[test]
    fn composite_pk_disambiguates() {
        // Same id=1, different p (a "composite key" using both cols).
        let prev = [row(1, 5)];
        let next = [row(1, 7)];
        // PK = (id, p): the next row has a different PK → insert + delete.
        let delta = row_set_delta(&prev, &next, &[0, 1]);
        assert_eq!(delta.inserted.len(), 1);
        assert_eq!(delta.deleted.len(), 1);
        assert!(delta.updated.is_empty());
    }
}

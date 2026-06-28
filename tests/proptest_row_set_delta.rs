//! Property-based tests for [`row_set_delta`].
//!
//! `row_set_delta` is the pure math behind the view-relative UPDATE
//! deltas surfaced to consumers (see `src/row_set.rs`). Whoever calls
//! it (snapshot refresh, total-row re-exec) relies on a small set of
//! algebraic guarantees that are easy to spell out and miss-prone to
//! hand-test, so they get proptest coverage here.
//!
//! Properties tested
//! 1. **Self-diff is empty.** `delta(s, s)` produces no entries for any
//!    PK-unique set `s`.
//! 2. **Empty prev = pure inserts.** `delta(empty, s)` returns every
//!    row of `s` as an insert and emits no deletes or updates.
//! 3. **Empty next = pure deletes.** `delta(s, empty)` returns every
//!    row of `s` as a delete and emits no inserts or updates.
//! 4. **Bucket disjointness.** The PK sets of `inserted`, `deleted`,
//!    and `updated` are pairwise disjoint for any inputs.
//! 5. **Bucket completeness.** Every PK in `next` lands in exactly one
//!    of `inserted` / `updated` / (untouched-but-present). Every PK in
//!    `prev` lands in exactly one of `deleted` / `updated` /
//!    (untouched-but-present).
//! 6. **Reconstruction.** Starting from `prev` and applying the delta
//!    (drop `deleted` PKs, add `inserted` rows, replace each
//!    `updated.prev` with `updated.next`) yields a set PK-equal and
//!    cell-equal to `next`.

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::cast_possible_truncation
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use proptest::collection::vec;
use proptest::prelude::*;
use subql::{row_set_delta, Cell, ColumnId, RowImage};

/// Two-column rows: `(id INT PK, value INT)`. Enough surface to exercise
/// the "same PK, different cells = update" path without dragging in
/// composite-key concerns (those get their own property).
const PK_COLS_SINGLE: &[ColumnId] = &[0];

/// Three-column rows: `(a INT, b INT, value INT)` with composite PK
/// `(a, b)`. Used to verify the function honours multi-column PKs.
const PK_COLS_COMPOSITE: &[ColumnId] = &[0, 1];

fn single_pk_row(id: i64, value: i64) -> RowImage {
    RowImage {
        cells: Arc::from([Cell::Int(id), Cell::Int(value)]),
    }
}

fn composite_pk_row(a: i64, b: i64, value: i64) -> RowImage {
    RowImage {
        cells: Arc::from([Cell::Int(a), Cell::Int(b), Cell::Int(value)]),
    }
}

/// A PK-unique row set generator. `BTreeMap` keys on the PK ints so the
/// final `Vec<RowImage>` is guaranteed to have no PK collisions, which
/// is the documented input contract.
fn arb_single_pk_set() -> impl Strategy<Value = Vec<RowImage>> {
    vec((0i64..16, 0i64..32), 0..16).prop_map(|pairs| {
        let by_pk: BTreeMap<i64, i64> = pairs.into_iter().collect();
        by_pk
            .into_iter()
            .map(|(id, value)| single_pk_row(id, value))
            .collect()
    })
}

fn arb_composite_pk_set() -> impl Strategy<Value = Vec<RowImage>> {
    vec((0i64..4, 0i64..4, 0i64..16), 0..16).prop_map(|tuples| {
        let by_pk: BTreeMap<(i64, i64), i64> =
            tuples.into_iter().map(|(a, b, v)| ((a, b), v)).collect();
        by_pk
            .into_iter()
            .map(|((a, b), v)| composite_pk_row(a, b, v))
            .collect()
    })
}

/// Project the PK cells out of `row` as a sortable / hashable tuple. We
/// know every row in these tests stores `Cell::Int` in the PK slot, so
/// extracting via `Cell::Int` is total. Anything else is a test bug.
fn pk_of(row: &RowImage, pk_cols: &[ColumnId]) -> Vec<i64> {
    pk_cols
        .iter()
        .map(|&c| match row.get(c).unwrap() {
            Cell::Int(n) => *n,
            other => panic!("non-Int PK cell in test input: {other:?}"),
        })
        .collect()
}

fn cells_eq(a: &RowImage, b: &RowImage) -> bool {
    a.cells.len() == b.cells.len() && a.cells.iter().zip(b.cells.iter()).all(|(x, y)| x == y)
}

/// Apply a [`RowSetDelta`] to `prev` and return the rebuilt set, keyed
/// by PK so callers can compare to `next` regardless of bucket order.
fn apply_delta(
    prev: &[RowImage],
    delta: &subql::RowSetDelta,
    pk_cols: &[ColumnId],
) -> BTreeMap<Vec<i64>, RowImage> {
    let mut out: BTreeMap<Vec<i64>, RowImage> = prev
        .iter()
        .map(|r| (pk_of(r, pk_cols), r.clone()))
        .collect();
    for d in &delta.deleted {
        out.remove(&pk_of(d, pk_cols));
    }
    for (old, new) in &delta.updated {
        let pk_old = pk_of(old, pk_cols);
        let pk_new = pk_of(new, pk_cols);
        assert_eq!(pk_old, pk_new, "updated entry changed its PK");
        out.insert(pk_new, new.clone());
    }
    for i in &delta.inserted {
        out.insert(pk_of(i, pk_cols), i.clone());
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    /// Self-diff: `delta(s, s)` is empty for every PK-unique set `s`.
    #[test]
    fn self_diff_is_empty(set in arb_single_pk_set()) {
        let delta = row_set_delta(&set, &set, PK_COLS_SINGLE);
        prop_assert!(delta.inserted.is_empty(), "self-diff produced inserts: {:?}", delta.inserted);
        prop_assert!(delta.deleted.is_empty(), "self-diff produced deletes: {:?}", delta.deleted);
        prop_assert!(delta.updated.is_empty(), "self-diff produced updates: {:?}", delta.updated);
    }

    /// Empty prev yields a pure-insert delta.
    #[test]
    fn empty_prev_is_pure_inserts(set in arb_single_pk_set()) {
        let delta = row_set_delta(&[], &set, PK_COLS_SINGLE);
        prop_assert_eq!(delta.inserted.len(), set.len(), "insert count must equal next set size");
        prop_assert!(delta.deleted.is_empty(), "empty-prev delta produced deletes: {:?}", delta.deleted);
        prop_assert!(delta.updated.is_empty(), "empty-prev delta produced updates: {:?}", delta.updated);
        // Every row in `set` is in `inserted`, by PK.
        let inserted_pks: BTreeSet<Vec<i64>> =
            delta.inserted.iter().map(|r| pk_of(r, PK_COLS_SINGLE)).collect();
        let expected_pks: BTreeSet<Vec<i64>> =
            set.iter().map(|r| pk_of(r, PK_COLS_SINGLE)).collect();
        prop_assert_eq!(inserted_pks, expected_pks);
    }

    /// Empty next yields a pure-delete delta.
    #[test]
    fn empty_next_is_pure_deletes(set in arb_single_pk_set()) {
        let delta = row_set_delta(&set, &[], PK_COLS_SINGLE);
        prop_assert_eq!(delta.deleted.len(), set.len(), "delete count must equal prev set size");
        prop_assert!(delta.inserted.is_empty(), "empty-next delta produced inserts: {:?}", delta.inserted);
        prop_assert!(delta.updated.is_empty(), "empty-next delta produced updates: {:?}", delta.updated);
        let deleted_pks: BTreeSet<Vec<i64>> =
            delta.deleted.iter().map(|r| pk_of(r, PK_COLS_SINGLE)).collect();
        let expected_pks: BTreeSet<Vec<i64>> =
            set.iter().map(|r| pk_of(r, PK_COLS_SINGLE)).collect();
        prop_assert_eq!(deleted_pks, expected_pks);
    }

    /// Bucket disjointness + completeness: every PK in `prev` or `next`
    /// lands in exactly one of {inserted, deleted, updated, unchanged}.
    #[test]
    fn buckets_disjoint_and_complete(
        prev in arb_single_pk_set(),
        next in arb_single_pk_set(),
    ) {
        let delta = row_set_delta(&prev, &next, PK_COLS_SINGLE);

        let inserted_pks: BTreeSet<Vec<i64>> =
            delta.inserted.iter().map(|r| pk_of(r, PK_COLS_SINGLE)).collect();
        let deleted_pks: BTreeSet<Vec<i64>> =
            delta.deleted.iter().map(|r| pk_of(r, PK_COLS_SINGLE)).collect();
        let updated_pks: BTreeSet<Vec<i64>> =
            delta.updated.iter().map(|(p, _)| pk_of(p, PK_COLS_SINGLE)).collect();

        // Pairwise disjoint.
        prop_assert!(
            inserted_pks.is_disjoint(&deleted_pks),
            "inserted and deleted not disjoint: {:?}",
            inserted_pks.intersection(&deleted_pks).collect::<Vec<_>>(),
        );
        prop_assert!(
            inserted_pks.is_disjoint(&updated_pks),
            "inserted and updated not disjoint: {:?}",
            inserted_pks.intersection(&updated_pks).collect::<Vec<_>>(),
        );
        prop_assert!(
            deleted_pks.is_disjoint(&updated_pks),
            "deleted and updated not disjoint: {:?}",
            deleted_pks.intersection(&updated_pks).collect::<Vec<_>>(),
        );

        // `inserted` is next minus prev (by PK).
        let prev_pks: BTreeSet<Vec<i64>> = prev.iter().map(|r| pk_of(r, PK_COLS_SINGLE)).collect();
        let next_pks: BTreeSet<Vec<i64>> = next.iter().map(|r| pk_of(r, PK_COLS_SINGLE)).collect();
        let expected_inserts: BTreeSet<Vec<i64>> = next_pks.difference(&prev_pks).cloned().collect();
        prop_assert_eq!(&inserted_pks, &expected_inserts);

        // `deleted` is prev minus next.
        let expected_deletes: BTreeSet<Vec<i64>> = prev_pks.difference(&next_pks).cloned().collect();
        prop_assert_eq!(&deleted_pks, &expected_deletes);

        // `updated` is a subset of prev intersect next.
        let intersection: BTreeSet<Vec<i64>> =
            prev_pks.intersection(&next_pks).cloned().collect();
        prop_assert!(updated_pks.is_subset(&intersection));

        // For every "in both" PK, either it appears in `updated` (cells
        // changed) or it does not appear at all (cells unchanged).
        let prev_by_pk: BTreeMap<Vec<i64>, &RowImage> =
            prev.iter().map(|r| (pk_of(r, PK_COLS_SINGLE), r)).collect();
        let next_by_pk: BTreeMap<Vec<i64>, &RowImage> =
            next.iter().map(|r| (pk_of(r, PK_COLS_SINGLE), r)).collect();
        for pk in &intersection {
            let p = prev_by_pk[pk];
            let n = next_by_pk[pk];
            if cells_eq(p, n) {
                prop_assert!(
                    !updated_pks.contains(pk),
                    "unchanged row appeared in updated: pk={pk:?}",
                );
            } else {
                prop_assert!(
                    updated_pks.contains(pk),
                    "changed row missing from updated: pk={pk:?}",
                );
            }
        }
    }

    /// Reconstruction: applying the delta to `prev` reproduces `next`.
    #[test]
    fn applying_delta_reconstructs_next(
        prev in arb_single_pk_set(),
        next in arb_single_pk_set(),
    ) {
        let delta = row_set_delta(&prev, &next, PK_COLS_SINGLE);
        let rebuilt = apply_delta(&prev, &delta, PK_COLS_SINGLE);
        let expected: BTreeMap<Vec<i64>, RowImage> = next
            .iter()
            .map(|r| (pk_of(r, PK_COLS_SINGLE), r.clone()))
            .collect();

        prop_assert_eq!(rebuilt.len(), expected.len());
        for (pk, exp_row) in &expected {
            let got = rebuilt.get(pk).unwrap_or_else(|| {
                panic!("rebuilt set missing PK {pk:?} that exists in next")
            });
            prop_assert!(
                cells_eq(got, exp_row),
                "rebuilt row at PK {pk:?} differs from next: rebuilt={got:?} expected={exp_row:?}",
            );
        }
    }

    /// Same reconstruction property under a composite PK, which
    /// exercises the multi-column `pk_key` serialization path.
    #[test]
    fn applying_delta_reconstructs_next_composite_pk(
        prev in arb_composite_pk_set(),
        next in arb_composite_pk_set(),
    ) {
        let delta = row_set_delta(&prev, &next, PK_COLS_COMPOSITE);
        let rebuilt = apply_delta(&prev, &delta, PK_COLS_COMPOSITE);
        let expected: BTreeMap<Vec<i64>, RowImage> = next
            .iter()
            .map(|r| (pk_of(r, PK_COLS_COMPOSITE), r.clone()))
            .collect();

        prop_assert_eq!(rebuilt.len(), expected.len());
        for (pk, exp_row) in &expected {
            let got = rebuilt.get(pk).unwrap_or_else(|| {
                panic!("rebuilt set missing composite PK {pk:?} that exists in next")
            });
            prop_assert!(
                cells_eq(got, exp_row),
                "rebuilt row at PK {pk:?} differs from next under composite key",
            );
        }
    }
}

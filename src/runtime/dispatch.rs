//! Event dispatch pipeline

use super::{agg::agg_delta_for_row, ids::UserOrdinal, partition::TablePartition};
use crate::{
    compiler::{sql_shape::QueryProjection, Tri, Vm},
    AggDelta, DispatchError, EventKind, IdTypes, RowImage, WalEvent,
};
use ahash::AHashMap;
use roaring::RoaringBitmap;

/// User dictionary for ordinal ↔ UserId translation
///
/// Maps dense ordinals (0-based, used in bitmaps) to sparse UserIds.
/// Enables efficient RoaringBitmap operations while supporting arbitrary UserIds.
#[derive(Clone, Debug)]
pub struct UserDictionary<I: IdTypes> {
    /// UserOrdinal → UserId (dense, 0-indexed)
    ordinal_to_user: Vec<Option<I::UserId>>,
    /// UserId → UserOrdinal (for reverse lookup)
    user_to_ordinal: AHashMap<I::UserId, UserOrdinal>,
    /// Recycled ordinals from removed users, available for reuse
    free_list: Vec<UserOrdinal>,
}

impl<I: IdTypes> UserDictionary<I> {
    fn next_ordinal_for_len(len: u64) -> Result<UserOrdinal, &'static str> {
        let ordinal =
            u32::try_from(len).map_err(|_| "user ordinal capacity exceeded (u32::MAX)")?;
        Ok(UserOrdinal::new(ordinal))
    }

    /// Create new empty dictionary
    #[must_use]
    pub fn new() -> Self {
        Self {
            ordinal_to_user: Vec::new(),
            user_to_ordinal: AHashMap::new(),
            free_list: Vec::new(),
        }
    }

    /// Try to get/create ordinal for user, returning an error when capacity is exceeded.
    pub fn try_get_or_create(&mut self, user_id: I::UserId) -> Result<UserOrdinal, &'static str> {
        if let Some(&ordinal) = self.user_to_ordinal.get(&user_id) {
            return Ok(ordinal);
        }

        let ordinal = if let Some(recycled) = self.free_list.pop() {
            self.ordinal_to_user[recycled.get() as usize] = Some(user_id);
            recycled
        } else {
            let ord = Self::next_ordinal_for_len(self.ordinal_to_user.len() as u64)?;
            self.ordinal_to_user.push(Some(user_id));
            ord
        };
        self.user_to_ordinal.insert(user_id, ordinal);

        Ok(ordinal)
    }

    /// Get or create ordinal for user
    pub fn get_or_create(&mut self, user_id: I::UserId) -> UserOrdinal {
        self.try_get_or_create(user_id)
            .unwrap_or_else(|msg| panic!("{msg}"))
    }

    /// Get ordinal for user (if exists)
    #[must_use]
    pub fn get(&self, user_id: I::UserId) -> Option<UserOrdinal> {
        self.user_to_ordinal.get(&user_id).copied()
    }

    /// Get user by ordinal
    #[must_use]
    pub fn get_user(&self, ordinal: UserOrdinal) -> Option<I::UserId> {
        self.ordinal_to_user
            .get(ordinal.get() as usize)
            .copied()
            .flatten()
    }

    /// Remove user (for cleanup)
    pub fn remove(&mut self, user_id: I::UserId) -> Option<UserOrdinal> {
        let ordinal = self.user_to_ordinal.remove(&user_id)?;
        if let Some(slot) = self.ordinal_to_user.get_mut(ordinal.get() as usize) {
            *slot = None;
        }
        self.free_list.push(ordinal);
        Some(ordinal)
    }

    /// Get ordinal_to_user vector for serialization
    #[must_use]
    pub fn ordinal_to_user_vec(&self) -> Vec<I::UserId> {
        let mut by_ordinal: Vec<(u32, I::UserId)> = self
            .user_to_ordinal
            .iter()
            .map(|(&user_id, ordinal)| (ordinal.get(), user_id))
            .collect();
        by_ordinal.sort_unstable_by_key(|(ordinal, _)| *ordinal);
        by_ordinal.into_iter().map(|(_, user_id)| user_id).collect()
    }
}

impl<I: IdTypes> Default for UserDictionary<I> {
    fn default() -> Self {
        Self::new()
    }
}

/// Zero-allocation iterator over matched user IDs.
///
/// Owns the `RoaringBitmap` (already heap-allocated during dispatch — moving
/// it is just a pointer move) and borrows the `UserDictionary` to translate
/// ordinals into user IDs.
pub struct MatchedUsers<'a, I: IdTypes> {
    bitmap_iter: roaring::bitmap::IntoIter,
    dict: &'a UserDictionary<I>,
}

impl<I: IdTypes> Iterator for MatchedUsers<'_, I> {
    type Item = I::UserId;

    fn next(&mut self) -> Option<Self::Item> {
        for ord in self.bitmap_iter.by_ref() {
            if let Some(user_id) = self
                .dict
                .ordinal_to_user
                .get(ord as usize)
                .copied()
                .flatten()
            {
                return Some(user_id);
            }
        }
        None
    }
}

/// Select the row image used for dispatch based on event kind.
fn require_new_row<'a>(
    event: &'a WalEvent,
    message: &'static str,
) -> Result<&'a RowImage, DispatchError> {
    event
        .new_row
        .as_ref()
        .ok_or(DispatchError::MissingRequiredRowImage(message))
}

fn require_old_row<'a>(
    event: &'a WalEvent,
    message: &'static str,
) -> Result<&'a RowImage, DispatchError> {
    event
        .old_row
        .as_ref()
        .ok_or(DispatchError::MissingRequiredRowImage(message))
}

pub(crate) fn select_event_row(event: &WalEvent) -> Result<&RowImage, DispatchError> {
    match event.kind {
        EventKind::Insert => require_new_row(event, "INSERT requires new_row"),
        EventKind::Update => require_new_row(event, "UPDATE requires new_row"),
        EventKind::Delete => require_old_row(event, "DELETE requires old_row"),
        EventKind::Truncate => Err(DispatchError::MissingRequiredRowImage(
            "TRUNCATE has no row image",
        )),
    }
}

fn matched_users_for_truncate<'a, I: IdTypes>(
    partition: &TablePartition<I>,
    user_dict: &'a UserDictionary<I>,
) -> MatchedUsers<'a, I> {
    let snapshot = partition.load_snapshot();
    let mut ordinals = RoaringBitmap::new();
    for (pred_id, users) in &snapshot.predicates.predicate_users {
        let Some(pred) = snapshot.predicates.get_predicate(*pred_id) else {
            continue;
        };
        if matches!(pred.projection, QueryProjection::Rows) {
            ordinals |= users;
        }
    }
    MatchedUsers {
        bitmap_iter: ordinals.into_iter(),
        dict: user_dict,
    }
}

/// Dispatch event to interested users
///
/// Main dispatch algorithm:
/// 1. Validate event
/// 2. Select row image based on event kind
/// 3. Get table partition
/// 4. Select candidate predicates (index lookups)
/// 5. VM evaluation (filter to Tri::True)
/// 6. Return zero-alloc iterator over matched users
pub fn dispatch_users<'a, I: IdTypes>(
    event: &WalEvent,
    partition: &TablePartition<I>,
    user_dict: &'a UserDictionary<I>,
    vm: &mut Vm,
) -> Result<MatchedUsers<'a, I>, DispatchError> {
    // TRUNCATE events fan-out only to row subscribers (`SELECT * ...`).
    if event.kind == EventKind::Truncate {
        let _ = vm;
        return Ok(matched_users_for_truncate(partition, user_dict));
    }

    // 1. Get row image based on event kind (validates presence)
    let row = select_event_row(event)?;

    dispatch_users_with_row(event, row, partition, user_dict, vm)
}

pub(crate) fn dispatch_users_with_row<'a, I: IdTypes>(
    event: &WalEvent,
    row: &RowImage,
    partition: &TablePartition<I>,
    user_dict: &'a UserDictionary<I>,
    vm: &mut Vm,
) -> Result<MatchedUsers<'a, I>, DispatchError> {
    // 2. Select candidates (index lookups + fallback)
    let candidates = partition.select_candidates(row, event.kind, &event.changed_columns);

    // 3. VM evaluation (filter to Tri::True)
    let snapshot = partition.load_snapshot();
    let mut matching_ordinals = RoaringBitmap::new();

    for pred_id_u32 in &candidates {
        let Some(pred_id) = super::ids::PredicateId::try_from_u32(pred_id_u32) else {
            // Candidate IDs are engine-generated and expected to be non-zero.
            // Ignore invalid values defensively instead of panicking.
            continue;
        };

        if let Some(pred) = snapshot.predicates.get_predicate(pred_id) {
            // Hybrid path: for scan-required predicates, run cheap prefilter
            // first and skip VM if predicate cannot possibly be true.
            if pred.prefilter_plan.requires_prefilter_eval && !pred.prefilter_plan.may_match(row) {
                continue;
            }

            // Evaluate predicate against row
            let result = vm
                .eval(&pred.bytecode, row)
                .map_err(|e| DispatchError::VmError(format!("{e:?}")))?;

            // Only Tri::True is a match
            if result == Tri::True {
                // Collect user ordinals for this predicate
                if let Some(bitmap) = snapshot.predicates.predicate_users.get(&pred_id) {
                    matching_ordinals |= bitmap;
                }
            }
        }
    }

    // 4. Return zero-alloc iterator
    Ok(MatchedUsers {
        bitmap_iter: matching_ordinals.into_iter(),
        dict: user_dict,
    })
}

fn weighted_rows_for_agg(event: &WalEvent) -> Result<Vec<(i64, &RowImage)>, DispatchError> {
    match event.kind {
        EventKind::Insert => Ok(vec![(
            1,
            require_new_row(event, "INSERT requires new_row")?,
        )]),
        EventKind::Delete => Ok(vec![(
            -1,
            require_old_row(event, "DELETE requires old_row")?,
        )]),
        EventKind::Update => {
            let old_row = event
                .old_row
                .as_ref()
                .ok_or(DispatchError::AggregateUpdateRequiresOldRow(event.table_id))?;
            let new_row = require_new_row(event, "UPDATE requires new_row")?;
            Ok(vec![(-1, old_row), (1, new_row)])
        }
        EventKind::Truncate => Err(DispatchError::TruncateRequiresReset(event.table_id)),
    }
}

/// Compute typed signed deltas for aggregate subscriptions (COUNT(*), SUM(col), …).
///
/// Delta normalization per event kind:
/// - `Insert`   → `[(+1, new_row)]`
/// - `Delete`   → `[(-1, old_row)]`
/// - `Update`   → `[(-1, old_row), (+1, new_row)]`
/// - `Truncate` → `Err(TruncateRequiresReset)`
///
/// For each `(weight, row)` pair the function selects agg candidate
/// predicates, prefilters, VM-evaluates, and accumulates weight per user.
/// Zero-net entries are filtered out before returning.
/// The same user may appear multiple times (once per aggregate kind).
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
pub(crate) fn compute_agg_deltas<I: IdTypes>(
    event: &WalEvent,
    partition: &TablePartition<I>,
    user_dict: &UserDictionary<I>,
    vm: &mut Vm,
) -> Result<Vec<(I::UserId, AggDelta)>, DispatchError> {
    use crate::runtime::ids::PredicateId;

    let weighted_rows = weighted_rows_for_agg(event)?;

    // Separate accumulators for each aggregate kind (avoids mixed-type confusion).
    let mut count_weights: AHashMap<UserOrdinal, i64> = AHashMap::new();
    let mut sum_weights: AHashMap<UserOrdinal, f64> = AHashMap::new();
    // AVG accumulator: (sum_delta, count_delta)
    let mut avg_accum: AHashMap<UserOrdinal, (f64, i64)> = AHashMap::new();

    let snapshot = partition.load_snapshot();

    // For UPDATE, use dependency-aware candidate selection; for INSERT/DELETE
    // pass empty changed_cols to get all agg candidates.
    let changed_cols = if event.kind == EventKind::Update {
        &event.changed_columns[..]
    } else {
        &[]
    };
    let candidates = partition.select_agg_candidates(event.kind, changed_cols);

    for (weight, row) in weighted_rows {
        for pred_id_u32 in &candidates {
            let Some(pred_id) = PredicateId::try_from_u32(pred_id_u32) else {
                continue;
            };

            if let Some(pred) = snapshot.predicates.get_predicate(pred_id) {
                // Only process aggregate predicates.
                let QueryProjection::Aggregate(ref spec) = pred.projection else {
                    continue;
                };

                // Prefilter check
                if pred.prefilter_plan.requires_prefilter_eval
                    && !pred.prefilter_plan.may_match(row)
                {
                    continue;
                }

                // VM evaluation
                let result = vm
                    .eval(&pred.bytecode, row)
                    .map_err(|e| DispatchError::VmError(format!("{e:?}")))?;

                if result == Tri::True {
                    if let Some(bitmap) = snapshot.predicates.predicate_users.get(&pred_id) {
                        if let Some(delta) = agg_delta_for_row(spec, row, weight) {
                            for ord_u32 in bitmap {
                                let ord = UserOrdinal::new(ord_u32);
                                match &delta {
                                    AggDelta::Count(n) => {
                                        *count_weights.entry(ord).or_default() += *n;
                                    }
                                    AggDelta::Sum(v) => {
                                        *sum_weights.entry(ord).or_default() += *v;
                                    }
                                    AggDelta::Avg {
                                        sum_delta,
                                        count_delta,
                                    } => {
                                        let entry = avg_accum.entry(ord).or_default();
                                        entry.0 += *sum_delta;
                                        entry.1 += *count_delta;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Translate ordinals to user IDs; filter out zero-net entries.
    // Same user may appear multiple times (once per AggDelta variant).
    let mut result: Vec<(I::UserId, AggDelta)> = Vec::new();
    for (ord, n) in count_weights.into_iter().filter(|(_, n)| *n != 0) {
        if let Some(uid) = user_dict.get_user(ord) {
            result.push((uid, AggDelta::Count(n)));
        }
    }
    for (ord, v) in sum_weights.into_iter().filter(|(_, v)| *v != 0.0) {
        if let Some(uid) = user_dict.get_user(ord) {
            result.push((uid, AggDelta::Sum(v)));
        }
    }
    for (ord, (s, c)) in avg_accum
        .into_iter()
        .filter(|(_, (s, c))| *s != 0.0 || *c != 0)
    {
        if let Some(uid) = user_dict.get_user(ord) {
            result.push((
                uid,
                AggDelta::Avg {
                    sum_delta: s,
                    count_delta: c,
                },
            ));
        }
    }

    Ok(result)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::needless_collect)]
mod tests {
    use super::*;
    use crate::DefaultIds;
    use std::sync::Arc;

    #[test]
    fn test_user_dictionary_get_or_create() {
        let mut dict = UserDictionary::<DefaultIds>::new();

        let ord1 = dict.get_or_create(100);
        assert_eq!(ord1.get(), 0);

        let ord2 = dict.get_or_create(200);
        assert_eq!(ord2.get(), 1);

        // Same user returns same ordinal
        let ord1_again = dict.get_or_create(100);
        assert_eq!(ord1_again.get(), 0);
    }

    #[test]
    fn test_user_dictionary_next_ordinal_overflow_errors() {
        let err = UserDictionary::<DefaultIds>::next_ordinal_for_len(u64::from(u32::MAX) + 1)
            .expect_err("ordinal allocation beyond u32::MAX should fail");
        assert!(err.contains("ordinal capacity exceeded"));
    }

    #[test]
    fn test_user_dictionary_try_get_or_create_success() {
        let mut dict = UserDictionary::<DefaultIds>::new();
        let ord = dict
            .try_get_or_create(123)
            .expect("small dictionaries should allocate ordinals");
        assert_eq!(ord.get(), 0);
    }

    #[test]
    fn test_user_dictionary_get() {
        let mut dict = UserDictionary::<DefaultIds>::new();

        dict.get_or_create(42);

        assert_eq!(dict.get(42), Some(UserOrdinal::new(0)));
        assert_eq!(dict.get(99), None);
    }

    #[test]
    fn test_dispatch_insert_valid_event() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let partition = TablePartition::<DefaultIds>::new(1);
        let user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(100)]),
            }),
            changed_columns: Arc::from([]),
        };

        assert!(dispatch_users(&event, &partition, &user_dict, &mut vm).is_ok());
    }

    #[test]
    fn test_dispatch_insert_missing_new_row() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let partition = TablePartition::<DefaultIds>::new(1);
        let user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: None,
            changed_columns: Arc::from([]),
        };

        assert!(matches!(
            dispatch_users(&event, &partition, &user_dict, &mut vm),
            Err(DispatchError::MissingRequiredRowImage(_))
        ));
    }

    #[test]
    fn test_user_dictionary_remove() {
        let mut dict = UserDictionary::<DefaultIds>::new();

        let ord = dict.get_or_create(42);
        assert_eq!(dict.get(42), Some(ord));

        dict.remove(42);
        assert_eq!(dict.get(42), None);
    }

    #[test]
    fn test_user_dictionary_remove_clears_ordinal_lookup() {
        let mut dict = UserDictionary::<DefaultIds>::new();

        let ord = dict.get_or_create(42);
        assert_eq!(dict.get_user(ord), Some(42));

        dict.remove(42);
        assert_eq!(dict.get_user(ord), None);
    }

    #[test]
    fn test_user_dictionary_get_user() {
        let mut dict = UserDictionary::<DefaultIds>::new();

        let ord = dict.get_or_create(100);
        assert_eq!(dict.get_user(ord), Some(100));

        // Invalid ordinal
        assert_eq!(dict.get_user(UserOrdinal::new(999)), None);
    }

    #[test]
    fn test_user_dictionary_default() {
        let dict = UserDictionary::<DefaultIds>::default();
        assert_eq!(dict.get(42), None);
    }

    #[test]
    fn test_dispatch_update_missing_new_row() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let partition = TablePartition::<DefaultIds>::new(1);
        let user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(100)]),
            }),
            new_row: None,
            changed_columns: Arc::from([]),
        };

        assert!(matches!(
            dispatch_users(&event, &partition, &user_dict, &mut vm),
            Err(DispatchError::MissingRequiredRowImage(_))
        ));
    }

    #[test]
    fn test_dispatch_delete_missing_old_row() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let partition = TablePartition::<DefaultIds>::new(1);
        let user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let event = WalEvent {
            kind: EventKind::Delete,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(100)]),
            }),
            changed_columns: Arc::from([]),
        };

        assert!(matches!(
            dispatch_users(&event, &partition, &user_dict, &mut vm),
            Err(DispatchError::MissingRequiredRowImage(_))
        ));
    }

    #[test]
    fn test_dispatch_truncate_requires_no_row_and_returns_only_row_subscribers() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use super::super::predicate::{Binding, Predicate};
        use crate::compiler::sql_shape::QueryProjection;
        use crate::compiler::Vm;
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan};

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();

        // Row predicate bound to user 42.
        let row_pred = Predicate {
            id: super::super::ids::PredicateId::from_slab_index(0),
            hash: 0xABCD,
            normalized_sql: "true".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![Instruction::PushLiteral(
                crate::Cell::Bool(true),
            )])),
            dependency_columns: Arc::from([]),
            projection: QueryProjection::Rows,
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
            refcount: 1,
            updated_at_unix_ms: 1000,
        };
        let row_pred_id = row_pred.id;
        partition.add_predicate(row_pred, vec![IndexableAtom::Fallback]);

        let row_ord = user_dict.get_or_create(42);
        partition.add_binding(
            Binding {
                subscription_id: 100,
                predicate_id: row_pred_id,
                user_id: 42,
                user_ordinal: row_ord,
                session_id: None,
                updated_at_unix_ms: 1000,
            },
            row_pred_id,
        );

        // Aggregate predicate bound to user 77 must not leak into users().
        make_count_pred_and_binding(1, 101, 77, &mut partition, &mut user_dict);
        let mut vm = Vm::new();

        let event = WalEvent {
            kind: EventKind::Truncate,
            table_id: 1,
            pk: crate::PrimaryKey::empty(),
            old_row: None,
            new_row: None,
            changed_columns: Arc::from([]),
        };

        let mut users: Vec<_> = dispatch_users(&event, &partition, &user_dict, &mut vm)
            .expect("truncate should dispatch without row image")
            .collect();
        users.sort_unstable();
        assert_eq!(users, vec![42]);
    }

    #[test]
    fn test_user_dictionary_ordinal_to_user_vec() {
        let mut dict = UserDictionary::<DefaultIds>::new();
        dict.get_or_create(10);
        dict.get_or_create(20);
        dict.get_or_create(30);

        let vec = dict.ordinal_to_user_vec();
        assert_eq!(vec, vec![10, 20, 30]);
    }

    #[test]
    fn test_user_dictionary_ordinal_to_user_vec_excludes_removed_users() {
        let mut dict = UserDictionary::<DefaultIds>::new();
        dict.get_or_create(10);
        dict.get_or_create(20);
        dict.get_or_create(30);

        dict.remove(20);

        let vec = dict.ordinal_to_user_vec();
        assert_eq!(vec, vec![10, 30]);
    }

    #[test]
    fn test_matched_users_iterator() {
        let mut dict = UserDictionary::<DefaultIds>::new();
        dict.get_or_create(10);
        dict.get_or_create(20);
        dict.get_or_create(30);

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(0); // User 10
        bitmap.insert(2); // User 30

        let users: Vec<_> = (MatchedUsers {
            bitmap_iter: bitmap.into_iter(),
            dict: &dict,
        })
        .collect();
        assert_eq!(users, vec![10, 30]);
    }

    #[test]
    fn test_dispatch_users_update_event_matching() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use super::super::predicate::Predicate;
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan, Vm};

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let pred = Predicate {
            id: super::super::ids::PredicateId::from_slab_index(0),
            hash: 0x1234,
            normalized_sql: "age > 18".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(crate::Cell::Int(18)),
                Instruction::GreaterThan,
            ])),
            dependency_columns: Arc::from([1u16]),
            projection: crate::compiler::sql_shape::QueryProjection::Rows,
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
            refcount: 1,
            updated_at_unix_ms: 1000,
        };

        let pred_id = pred.id;
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let ord = user_dict.get_or_create(42);
        let binding = super::super::predicate::Binding {
            subscription_id: 100,
            predicate_id: pred_id,
            user_id: 42,
            user_ordinal: ord,
            session_id: None,
            updated_at_unix_ms: 1000,
        };
        partition.add_binding(binding, pred_id);

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(1), crate::Cell::Int(17)]),
            }),
            new_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(1), crate::Cell::Int(25)]),
            }),
            changed_columns: Arc::from([1u16]),
        };

        let result = dispatch_users(&event, &partition, &user_dict, &mut vm);
        assert!(result.is_ok());
        let users: Vec<_> = result.unwrap().collect();
        assert!(
            users.contains(&42),
            "User 42 should match age > 18 with age=25"
        );
    }

    #[test]
    fn test_dispatch_users_delete_event_matching() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use super::super::predicate::Predicate;
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan, Vm};

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let pred = Predicate {
            id: super::super::ids::PredicateId::from_slab_index(0),
            hash: 0x5678,
            normalized_sql: "age < 30".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(crate::Cell::Int(30)),
                Instruction::LessThan,
            ])),
            dependency_columns: Arc::from([1u16]),
            projection: crate::compiler::sql_shape::QueryProjection::Rows,
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
            refcount: 1,
            updated_at_unix_ms: 1000,
        };

        let pred_id = pred.id;
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let ord = user_dict.get_or_create(99);
        let binding = super::super::predicate::Binding {
            subscription_id: 200,
            predicate_id: pred_id,
            user_id: 99,
            user_ordinal: ord,
            session_id: None,
            updated_at_unix_ms: 1000,
        };
        partition.add_binding(binding, pred_id);

        let event = WalEvent {
            kind: EventKind::Delete,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(1), crate::Cell::Int(25)]),
            }),
            new_row: None,
            changed_columns: Arc::from([]),
        };

        let result = dispatch_users(&event, &partition, &user_dict, &mut vm);
        assert!(result.is_ok());
        let users: Vec<_> = result.unwrap().collect();
        assert!(
            users.contains(&99),
            "User 99 should match age < 30 with age=25"
        );
    }

    // --- Aggregate dispatch tests ---

    fn make_count_pred_and_binding(
        pred_slab: usize,
        sub_id: u64,
        user_id: u64,
        partition: &mut super::super::partition::TablePartition<DefaultIds>,
        user_dict: &mut UserDictionary<DefaultIds>,
    ) -> super::super::ids::PredicateId {
        use super::super::indexes::IndexableAtom;
        use super::super::predicate::{Binding, Predicate};
        use crate::compiler::sql_shape::{AggSpec, QueryProjection};
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan};

        // WHERE amount > 10
        let pred = Predicate {
            id: super::super::ids::PredicateId::from_slab_index(pred_slab),
            hash: 0xCCCC + pred_slab as u128,
            normalized_sql: "amount > 10".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(crate::Cell::Int(10)),
                Instruction::GreaterThan,
            ])),
            dependency_columns: Arc::from([1u16]),
            projection: QueryProjection::Aggregate(AggSpec::CountStar),
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
            refcount: 1,
            updated_at_unix_ms: 1000,
        };
        let pred_id = pred.id;
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let ord = user_dict.get_or_create(user_id);
        partition.add_binding(
            Binding {
                subscription_id: sub_id,
                predicate_id: pred_id,
                user_id,
                user_ordinal: ord,
                session_id: None,
                updated_at_unix_ms: 1000,
            },
            pred_id,
        );
        pred_id
    }

    /// Helper: row with cells [id=1, amount=value]
    fn make_row(amount: i64) -> crate::RowImage {
        crate::RowImage {
            cells: Arc::from([crate::Cell::Int(1), crate::Cell::Int(amount)]),
        }
    }

    #[test]
    fn test_agg_insert_matching_gives_plus_one() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(make_row(20)), // 20 > 10 → matches
            changed_columns: Arc::from([]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert_eq!(
            deltas,
            vec![(42, crate::AggDelta::Count(1))],
            "INSERT matching predicate should yield delta +1"
        );
    }

    #[test]
    fn test_agg_insert_non_matching_gives_no_delta() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(make_row(5)), // 5 ≤ 10 → no match
            changed_columns: Arc::from([]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert!(
            deltas.is_empty(),
            "INSERT not matching predicate should yield no delta"
        );
    }

    #[test]
    fn test_agg_delete_matching_gives_minus_one() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Delete,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(make_row(20)), // was matching
            new_row: None,
            changed_columns: Arc::from([]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert_eq!(
            deltas,
            vec![(42, crate::AggDelta::Count(-1))],
            "DELETE of matching row should yield delta -1"
        );
    }

    #[test]
    fn test_agg_update_old_matches_new_does_not_gives_minus_one() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(make_row(20)), // was matching (20 > 10)
            new_row: Some(make_row(5)),  // no longer matches (5 ≤ 10)
            changed_columns: Arc::from([1u16]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert_eq!(
            deltas,
            vec![(42, crate::AggDelta::Count(-1))],
            "UPDATE leaving predicate should yield delta -1"
        );
    }

    #[test]
    fn test_agg_update_old_does_not_match_new_does_gives_plus_one() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(make_row(5)),  // was not matching
            new_row: Some(make_row(20)), // now matches
            changed_columns: Arc::from([1u16]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert_eq!(
            deltas,
            vec![(42, crate::AggDelta::Count(1))],
            "UPDATE entering predicate should yield delta +1"
        );
    }

    #[test]
    fn test_agg_update_both_match_gives_zero_net_no_entry() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(make_row(15)), // matches (15 > 10)
            new_row: Some(make_row(20)), // also matches (20 > 10)
            changed_columns: Arc::from([1u16]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert!(
            deltas.is_empty(),
            "UPDATE where both old and new match should yield zero net delta (no entry)"
        );
    }

    #[test]
    fn test_agg_update_missing_old_row_returns_strict_error() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(make_row(20)),
            changed_columns: Arc::from([1u16]),
        };

        let err = compute_agg_deltas(&event, &partition, &user_dict, &mut vm)
            .expect_err("UPDATE without old_row must be rejected for aggregates");
        assert!(matches!(
            err,
            DispatchError::AggregateUpdateRequiresOldRow(1)
        ));
    }

    // --- SUM aggregate dispatch tests ---

    /// Helper: create a SUM predicate (WHERE amount > 10, SUM on column 1) and bind user.
    fn make_sum_pred_and_binding(
        pred_slab: usize,
        sub_id: u64,
        user_id: u64,
        sum_col: u16,
        partition: &mut super::super::partition::TablePartition<DefaultIds>,
        user_dict: &mut UserDictionary<DefaultIds>,
    ) -> super::super::ids::PredicateId {
        use super::super::indexes::IndexableAtom;
        use super::super::predicate::{Binding, Predicate};
        use crate::compiler::sql_shape::{AggSpec, QueryProjection};
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan};

        // WHERE amount > 10 (column 1), SUM on sum_col
        let pred = Predicate {
            id: super::super::ids::PredicateId::from_slab_index(pred_slab),
            hash: 0xDDDD + pred_slab as u128,
            normalized_sql: "amount > 10".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(crate::Cell::Int(10)),
                Instruction::GreaterThan,
            ])),
            dependency_columns: Arc::from([1u16, sum_col]),
            projection: QueryProjection::Aggregate(AggSpec::Sum { column: sum_col }),
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
            refcount: 1,
            updated_at_unix_ms: 1000,
        };
        let pred_id = pred.id;
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let ord = user_dict.get_or_create(user_id);
        partition.add_binding(
            Binding {
                subscription_id: sub_id,
                predicate_id: pred_id,
                user_id,
                user_ordinal: ord,
                session_id: None,
                updated_at_unix_ms: 1000,
            },
            pred_id,
        );
        pred_id
    }

    /// Helper: row with cells [id=1, amount=value]
    fn make_sum_row(amount: crate::Cell) -> crate::RowImage {
        crate::RowImage {
            cells: Arc::from([crate::Cell::Int(1), amount]),
        }
    }

    #[test]
    fn test_sum_insert_matching_gives_value_delta() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(make_sum_row(crate::Cell::Int(20))), // 20 > 10 → matches
            changed_columns: Arc::from([]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(20.0))]);
    }

    #[test]
    fn test_sum_insert_non_matching_no_delta() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(make_sum_row(crate::Cell::Int(5))), // 5 ≤ 10 → no match
            changed_columns: Arc::from([]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert!(deltas.is_empty());
    }

    #[test]
    fn test_sum_delete_matching_gives_neg_delta() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Delete,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(make_sum_row(crate::Cell::Int(20))),
            new_row: None,
            changed_columns: Arc::from([]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(-20.0))]);
    }

    #[test]
    fn test_sum_update_both_match_value_change() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(make_sum_row(crate::Cell::Int(15))), // matches, contributes -15
            new_row: Some(make_sum_row(crate::Cell::Int(25))), // matches, contributes +25
            changed_columns: Arc::from([1u16]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(10.0))]);
    }

    #[test]
    fn test_sum_update_same_value_zero_net() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(make_sum_row(crate::Cell::Int(20))),
            new_row: Some(make_sum_row(crate::Cell::Int(20))),
            changed_columns: Arc::from([1u16]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert!(
            deltas.is_empty(),
            "zero net SUM delta should be filtered out"
        );
    }

    #[test]
    fn test_sum_update_old_match_new_no_match() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(make_sum_row(crate::Cell::Int(20))), // matches → -20
            new_row: Some(make_sum_row(crate::Cell::Int(5))),  // no match → 0
            changed_columns: Arc::from([1u16]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(-20.0))]);
    }

    #[test]
    fn test_sum_update_old_no_match_new_match() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(make_sum_row(crate::Cell::Int(5))), // no match
            new_row: Some(make_sum_row(crate::Cell::Int(20))), // matches → +20
            changed_columns: Arc::from([1u16]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(20.0))]);
    }

    #[test]
    fn test_sum_null_cell_gives_no_delta() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut user_dict);

        // WHERE amount > 10 won't match NULL, so no delta
        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(make_sum_row(crate::Cell::Null)),
            changed_columns: Arc::from([]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert!(deltas.is_empty());
    }

    #[test]
    fn test_sum_missing_cell_gives_no_delta() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut user_dict);

        // Row too short — column 1 (amount) is missing
        // WHERE amount > 10 evaluates to Unknown → no match
        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(1)]), // only col 0, col 1 missing
            }),
            changed_columns: Arc::from([]),
        };

        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert!(deltas.is_empty());
    }

    #[test]
    fn test_sum_float_cell_value() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(make_sum_row(crate::Cell::Float(2.5))),
            changed_columns: Arc::from([]),
        };

        // WHERE amount > 10 won't match 2.5, so no delta
        let deltas = compute_agg_deltas(&event, &partition, &user_dict, &mut vm).unwrap();
        assert!(deltas.is_empty(), "2.5 ≤ 10, so WHERE does not match");
    }

    #[test]
    fn test_agg_truncate_returns_requires_reset_error() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut user_dict);

        let event = WalEvent {
            kind: EventKind::Truncate,
            table_id: 1,
            pk: crate::PrimaryKey::empty(),
            old_row: None,
            new_row: None,
            changed_columns: Arc::from([]),
        };

        let err = compute_agg_deltas(&event, &partition, &user_dict, &mut vm)
            .expect_err("TRUNCATE should return TruncateRequiresReset");
        assert!(
            matches!(err, DispatchError::TruncateRequiresReset(1)),
            "expected TruncateRequiresReset(1), got: {err:?}"
        );
    }

    #[test]
    fn test_agg_users_dispatch_skips_count_predicates() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        // Register a COUNT predicate for user 42
        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut user_dict);

        // Also register a Rows predicate for user 99 (to ensure dispatch still works)
        {
            use super::super::predicate::{Binding, Predicate};
            use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan};
            let pred = Predicate {
                id: super::super::ids::PredicateId::from_slab_index(1),
                hash: 0xAAAA,
                normalized_sql: "amount > 5".into(),
                bytecode: Arc::new(BytecodeProgram::new(vec![
                    Instruction::LoadColumn(1),
                    Instruction::PushLiteral(crate::Cell::Int(5)),
                    Instruction::GreaterThan,
                ])),
                dependency_columns: Arc::from([1u16]),
                projection: crate::compiler::sql_shape::QueryProjection::Rows,
                index_atoms: Arc::from([IndexableAtom::Fallback]),
                prefilter_plan: Arc::new(PrefilterPlan::default()),
                refcount: 1,
                updated_at_unix_ms: 1000,
            };
            let pred_id = pred.id;
            partition.add_predicate(pred, vec![IndexableAtom::Fallback]);
            let ord = user_dict.get_or_create(99);
            partition.add_binding(
                Binding {
                    subscription_id: 2,
                    predicate_id: pred_id,
                    user_id: 99,
                    user_ordinal: ord,
                    session_id: None,
                    updated_at_unix_ms: 1000,
                },
                pred_id,
            );
        }

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(make_row(20)), // matches both predicates
            changed_columns: Arc::from([]),
        };

        let users: Vec<_> = dispatch_users(&event, &partition, &user_dict, &mut vm)
            .expect("dispatch_users should succeed")
            .collect();

        // User 99 (Rows predicate) should appear; user 42 (COUNT predicate) must NOT
        assert!(users.contains(&99), "Rows subscriber should be dispatched");
        assert!(
            !users.contains(&42),
            "COUNT subscriber must not appear in users() dispatch"
        );
    }

    #[test]
    fn test_dispatch_users_no_match() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use super::super::predicate::Predicate;
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan, Vm};

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut user_dict = UserDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        let pred = Predicate {
            id: super::super::ids::PredicateId::from_slab_index(0),
            hash: 0xABCD,
            normalized_sql: "age > 50".into(),
            bytecode: Arc::new(BytecodeProgram::new(vec![
                Instruction::LoadColumn(1),
                Instruction::PushLiteral(crate::Cell::Int(50)),
                Instruction::GreaterThan,
            ])),
            dependency_columns: Arc::from([1u16]),
            projection: crate::compiler::sql_shape::QueryProjection::Rows,
            index_atoms: Arc::from([IndexableAtom::Fallback]),
            prefilter_plan: Arc::new(PrefilterPlan::default()),
            refcount: 1,
            updated_at_unix_ms: 1000,
        };

        let pred_id = pred.id;
        partition.add_predicate(pred, vec![IndexableAtom::Fallback]);

        let ord = user_dict.get_or_create(42);
        let binding = super::super::predicate::Binding {
            subscription_id: 300,
            predicate_id: pred_id,
            user_id: 42,
            user_ordinal: ord,
            session_id: None,
            updated_at_unix_ms: 1000,
        };
        partition.add_binding(binding, pred_id);

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(1), crate::Cell::Int(25)]),
            }),
            changed_columns: Arc::from([]),
        };

        let result = dispatch_users(&event, &partition, &user_dict, &mut vm);
        assert!(result.is_ok());
        let users: Vec<_> = result.unwrap().collect();
        assert!(
            users.is_empty(),
            "No users should match age > 50 with age=25"
        );
    }
}

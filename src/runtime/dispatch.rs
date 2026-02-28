//! Event dispatch pipeline

use super::{
    agg::agg_delta_for_row,
    ids::ConsumerOrdinal,
    partition::TablePartition,
    predicate::{Predicate, PredicateStore},
};
use crate::{
    compiler::{sql_shape::QueryProjection, Tri, Vm},
    AggDelta, Cell, ConsumerNotifications, DispatchError, EventKind, IdTypes, RowImage, WalEvent,
};
use ahash::AHashMap;
use roaring::RoaringBitmap;

/// Consumer dictionary for ordinal ↔ ConsumerId translation
///
/// Maps dense ordinals (0-based, used in bitmaps) to sparse ConsumerIds.
/// Enables efficient RoaringBitmap operations while supporting arbitrary ConsumerIds.
#[derive(Clone, Debug)]
pub struct ConsumerDictionary<I: IdTypes> {
    /// ConsumerOrdinal → ConsumerId (dense, 0-indexed)
    ordinal_to_consumer: Vec<Option<I::ConsumerId>>,
    /// ConsumerId → ConsumerOrdinal (for reverse lookup)
    consumer_to_ordinal: AHashMap<I::ConsumerId, ConsumerOrdinal>,
    /// Recycled ordinals from removed consumers, available for reuse
    free_list: Vec<ConsumerOrdinal>,
}

impl<I: IdTypes> ConsumerDictionary<I> {
    fn next_ordinal_for_len(len: u64) -> Result<ConsumerOrdinal, &'static str> {
        let ordinal =
            u32::try_from(len).map_err(|_| "consumer ordinal capacity exceeded (u32::MAX)")?;
        Ok(ConsumerOrdinal::new(ordinal))
    }

    /// Create new empty dictionary
    #[must_use]
    pub fn new() -> Self {
        Self {
            ordinal_to_consumer: Vec::new(),
            consumer_to_ordinal: AHashMap::new(),
            free_list: Vec::new(),
        }
    }

    /// Try to get/create ordinal for consumer, returning an error when capacity is exceeded.
    pub fn try_get_or_create(
        &mut self,
        consumer_id: I::ConsumerId,
    ) -> Result<ConsumerOrdinal, &'static str> {
        if let Some(&ordinal) = self.consumer_to_ordinal.get(&consumer_id) {
            return Ok(ordinal);
        }

        let ordinal = if let Some(recycled) = self.free_list.pop() {
            self.ordinal_to_consumer[recycled.get() as usize] = Some(consumer_id);
            recycled
        } else {
            let ord = Self::next_ordinal_for_len(self.ordinal_to_consumer.len() as u64)?;
            self.ordinal_to_consumer.push(Some(consumer_id));
            ord
        };
        self.consumer_to_ordinal.insert(consumer_id, ordinal);

        Ok(ordinal)
    }

    /// Get or create ordinal for consumer
    pub fn get_or_create(&mut self, consumer_id: I::ConsumerId) -> ConsumerOrdinal {
        self.try_get_or_create(consumer_id)
            .unwrap_or_else(|msg| panic!("{msg}"))
    }

    /// Get ordinal for consumer (if exists)
    #[must_use]
    pub fn get(&self, consumer_id: I::ConsumerId) -> Option<ConsumerOrdinal> {
        self.consumer_to_ordinal.get(&consumer_id).copied()
    }

    /// Get consumer by ordinal
    #[must_use]
    pub fn get_consumer(&self, ordinal: ConsumerOrdinal) -> Option<I::ConsumerId> {
        self.ordinal_to_consumer
            .get(ordinal.get() as usize)
            .copied()
            .flatten()
    }

    /// Remove consumer (for cleanup)
    pub fn remove(&mut self, consumer_id: I::ConsumerId) -> Option<ConsumerOrdinal> {
        let ordinal = self.consumer_to_ordinal.remove(&consumer_id)?;
        if let Some(slot) = self.ordinal_to_consumer.get_mut(ordinal.get() as usize) {
            *slot = None;
        }
        self.free_list.push(ordinal);
        Some(ordinal)
    }

    /// Get ordinal_to_consumer vector for serialization
    #[must_use]
    pub fn ordinal_to_consumer_vec(&self) -> Vec<I::ConsumerId> {
        let mut by_ordinal: Vec<(u32, I::ConsumerId)> = self
            .consumer_to_ordinal
            .iter()
            .map(|(&consumer_id, ordinal)| (ordinal.get(), consumer_id))
            .collect();
        by_ordinal.sort_unstable_by_key(|(ordinal, _)| *ordinal);
        by_ordinal
            .into_iter()
            .map(|(_, consumer_id)| consumer_id)
            .collect()
    }
}

impl<I: IdTypes> Default for ConsumerDictionary<I> {
    fn default() -> Self {
        Self::new()
    }
}

/// Zero-allocation iterator over matched consumer IDs.
///
/// Owns the `RoaringBitmap` (already heap-allocated during dispatch — moving
/// it is just a pointer move) and borrows the `ConsumerDictionary` to translate
/// ordinals into consumer IDs.
pub struct MatchedConsumers<'a, I: IdTypes> {
    bitmap_iter: roaring::bitmap::IntoIter,
    dict: &'a ConsumerDictionary<I>,
}

impl<I: IdTypes> Iterator for MatchedConsumers<'_, I> {
    type Item = I::ConsumerId;

    fn next(&mut self) -> Option<Self::Item> {
        for ord in self.bitmap_iter.by_ref() {
            if let Some(consumer_id) = self
                .dict
                .ordinal_to_consumer
                .get(ord as usize)
                .copied()
                .flatten()
            {
                return Some(consumer_id);
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

fn notifications_for_truncate<I: IdTypes>(
    partition: &TablePartition<I>,
    consumer_dict: &ConsumerDictionary<I>,
) -> ConsumerNotifications<I> {
    let snapshot = partition.load_snapshot();
    let mut ordinals = RoaringBitmap::new();
    for (pred_id, consumers) in &snapshot.predicates.predicate_consumers {
        let Some(pred) = snapshot.predicates.get_predicate(*pred_id) else {
            continue;
        };
        if matches!(pred.projection, QueryProjection::Rows) {
            ordinals |= consumers;
        }
    }
    let deleted = resolve_ordinals(ordinals, consumer_dict);
    ConsumerNotifications {
        inserted: Vec::new(),
        deleted,
        updated: Vec::new(),
    }
}

/// Resolve a `RoaringBitmap` of consumer ordinals into a `Vec` of consumer IDs.
fn resolve_ordinals<I: IdTypes>(
    bitmap: RoaringBitmap,
    dict: &ConsumerDictionary<I>,
) -> Vec<I::ConsumerId> {
    #[allow(clippy::cast_possible_truncation)]
    let mut result = Vec::with_capacity(bitmap.len() as usize);
    for ord in bitmap {
        if let Some(consumer_id) = dict
            .ordinal_to_consumer
            .get(ord as usize)
            .copied()
            .flatten()
        {
            result.push(consumer_id);
        }
    }
    result
}

/// Dispatch event to interested consumers, returning view-relative notifications.
///
/// Main dispatch algorithm:
/// 1. Validate event
/// 2. Route by event kind:
///    - INSERT: single-eval against new_row → all matches to `inserted`
///    - DELETE: single-eval against old_row → all matches to `deleted`
///    - UPDATE: dual-eval (old_row + new_row) → three-way split
///    - TRUNCATE: all row subscribers → `deleted`
/// 3. Return `ConsumerNotifications`
pub fn dispatch_consumers<I: IdTypes>(
    event: &WalEvent,
    partition: &TablePartition<I>,
    consumer_dict: &ConsumerDictionary<I>,
    vm: &mut Vm,
) -> Result<ConsumerNotifications<I>, DispatchError> {
    match event.kind {
        EventKind::Truncate => {
            let _ = vm;
            Ok(notifications_for_truncate(partition, consumer_dict))
        }
        EventKind::Insert => {
            let row = require_new_row(event, "INSERT requires new_row")?;
            let bitmap = dispatch_single_eval_bitmap(event, row, partition, vm)?;
            Ok(ConsumerNotifications {
                inserted: resolve_ordinals(bitmap, consumer_dict),
                deleted: Vec::new(),
                updated: Vec::new(),
            })
        }
        EventKind::Delete => {
            let row = require_old_row(event, "DELETE requires old_row")?;
            let bitmap = dispatch_single_eval_bitmap(event, row, partition, vm)?;
            Ok(ConsumerNotifications {
                inserted: Vec::new(),
                deleted: resolve_ordinals(bitmap, consumer_dict),
                updated: Vec::new(),
            })
        }
        EventKind::Update => dispatch_update(event, partition, consumer_dict, vm),
    }
}

/// Dual-eval dispatch for UPDATE events: evaluates both old and new rows to
/// produce view-relative `inserted` / `deleted` / `updated` sets.
fn dispatch_update<I: IdTypes>(
    event: &WalEvent,
    partition: &TablePartition<I>,
    consumer_dict: &ConsumerDictionary<I>,
    vm: &mut Vm,
) -> Result<ConsumerNotifications<I>, DispatchError> {
    // Hard error if old_row is absent.
    let old_row = event
        .old_row
        .as_ref()
        .ok_or(DispatchError::UpdateRequiresOldRow(event.table_id))?;

    // Hard error if old_row contains Cell::Missing (partial old image).
    if old_row.cells.iter().any(Cell::is_missing) {
        return Err(DispatchError::UpdateRequiresOldRow(event.table_id));
    }

    let new_row = require_new_row(event, "UPDATE requires new_row")?;

    // Use the UPDATE candidate set (dependency-aware).
    let candidates =
        partition.select_candidates(new_row, EventKind::Update, &event.changed_columns);
    let snapshot = partition.load_snapshot();

    let mut inserted_ordinals = RoaringBitmap::new();
    let mut deleted_ordinals = RoaringBitmap::new();
    let mut updated_ordinals = RoaringBitmap::new();

    for pred_id_u32 in &candidates {
        let Some(pred_id) = super::ids::PredicateId::try_from_u32(pred_id_u32) else {
            continue;
        };

        let Some(pred) = snapshot.predicates.get_predicate(pred_id) else {
            continue;
        };

        // Only row subscriptions participate in consumers().
        if !matches!(pred.projection, QueryProjection::Rows) {
            continue;
        }

        // Evaluate new_row.
        let new_match = {
            if pred.prefilter_plan.requires_prefilter_eval
                && !pred.prefilter_plan.may_match(new_row)
            {
                false
            } else {
                let result = vm
                    .eval(&pred.bytecode, new_row)
                    .map_err(|e| DispatchError::VmError(format!("{e:?}")))?;
                result == Tri::True
            }
        };

        // Evaluate old_row.
        let old_match = {
            if pred.prefilter_plan.requires_prefilter_eval
                && !pred.prefilter_plan.may_match(old_row)
            {
                false
            } else {
                let result = vm
                    .eval(&pred.bytecode, old_row)
                    .map_err(|e| DispatchError::VmError(format!("{e:?}")))?;
                result == Tri::True
            }
        };

        if let Some(bitmap) = snapshot.predicates.predicate_consumers.get(&pred_id) {
            match (new_match, old_match) {
                (true, false) => inserted_ordinals |= bitmap,
                (false, true) => deleted_ordinals |= bitmap,
                (true, true) => updated_ordinals |= bitmap,
                (false, false) => {}
            }
        }
    }

    Ok(ConsumerNotifications {
        inserted: resolve_ordinals(inserted_ordinals, consumer_dict),
        deleted: resolve_ordinals(deleted_ordinals, consumer_dict),
        updated: resolve_ordinals(updated_ordinals, consumer_dict),
    })
}

/// Single-eval dispatch: evaluate one row, return the matching ordinals bitmap.
/// Used for INSERT (new_row) and DELETE (old_row).
fn dispatch_single_eval_bitmap<I: IdTypes>(
    event: &WalEvent,
    row: &RowImage,
    partition: &TablePartition<I>,
    vm: &mut Vm,
) -> Result<RoaringBitmap, DispatchError> {
    let candidates = partition.select_candidates(row, event.kind, &event.changed_columns);
    let snapshot = partition.load_snapshot();
    let mut matching_ordinals = RoaringBitmap::new();

    for_each_matching_predicate(
        &candidates,
        &snapshot.predicates,
        row,
        vm,
        |pred, consumers| {
            if matches!(pred.projection, QueryProjection::Rows) {
                matching_ordinals |= consumers;
            }
            Ok(())
        },
    )?;

    Ok(matching_ordinals)
}

/// Iterate over candidate predicates, evaluate each against a row, and invoke
/// the callback for every match.
///
/// This is the inner hot-loop shared by both row dispatch
/// and `compute_agg_deltas` (aggregate subscriptions).
fn for_each_matching_predicate<I, F>(
    candidates: &RoaringBitmap,
    store: &PredicateStore<I>,
    row: &RowImage,
    vm: &mut Vm,
    mut on_match: F,
) -> Result<(), DispatchError>
where
    I: IdTypes,
    F: FnMut(&Predicate, &RoaringBitmap) -> Result<(), DispatchError>,
{
    for pred_id_u32 in candidates {
        let Some(pred_id) = super::ids::PredicateId::try_from_u32(pred_id_u32) else {
            continue;
        };

        let Some(pred) = store.get_predicate(pred_id) else {
            continue;
        };

        // Prefilter: skip VM when the plan can statically rule out the row.
        if pred.prefilter_plan.requires_prefilter_eval && !pred.prefilter_plan.may_match(row) {
            continue;
        }

        // VM evaluation
        let result = vm
            .eval(&pred.bytecode, row)
            .map_err(|e| DispatchError::VmError(format!("{e:?}")))?;

        if result == Tri::True {
            if let Some(bitmap) = store.predicate_consumers.get(&pred_id) {
                on_match(pred, bitmap)?;
            }
        }
    }
    Ok(())
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
            // Reject partial old rows — Cell::Missing would produce unsound deltas
            if old_row.cells.iter().any(Cell::is_missing) {
                return Err(DispatchError::AggregateUpdateRequiresOldRow(event.table_id));
            }
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
    consumer_dict: &ConsumerDictionary<I>,
    vm: &mut Vm,
) -> Result<Vec<(I::ConsumerId, AggDelta)>, DispatchError> {
    let weighted_rows = weighted_rows_for_agg(event)?;

    // Separate accumulators for each aggregate kind (avoids mixed-type confusion).
    let mut count_weights: AHashMap<ConsumerOrdinal, i64> = AHashMap::new();
    let mut sum_weights: AHashMap<ConsumerOrdinal, f64> = AHashMap::new();
    // AVG accumulator: (sum_delta, count_delta)
    let mut avg_accum: AHashMap<ConsumerOrdinal, (f64, i64)> = AHashMap::new();

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
        for_each_matching_predicate(
            &candidates,
            &snapshot.predicates,
            row,
            vm,
            |pred, consumers| {
                let QueryProjection::Aggregate(ref spec) = pred.projection else {
                    return Ok(());
                };

                if let Some(delta) = agg_delta_for_row(spec, row, weight) {
                    for ord_u32 in consumers {
                        let ord = ConsumerOrdinal::new(ord_u32);
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

                Ok(())
            },
        )?;
    }

    // Translate ordinals to user IDs; filter out zero-net entries.
    // Same user may appear multiple times (once per AggDelta variant).
    let mut result: Vec<(I::ConsumerId, AggDelta)> = Vec::new();
    for (ord, n) in count_weights.into_iter().filter(|(_, n)| *n != 0) {
        if let Some(uid) = consumer_dict.get_consumer(ord) {
            result.push((uid, AggDelta::Count(n)));
        }
    }
    for (ord, v) in sum_weights.into_iter().filter(|(_, v)| *v != 0.0) {
        if let Some(uid) = consumer_dict.get_consumer(ord) {
            result.push((uid, AggDelta::Sum(v)));
        }
    }
    for (ord, (s, c)) in avg_accum
        .into_iter()
        .filter(|(_, (s, c))| *s != 0.0 || *c != 0)
    {
        if let Some(uid) = consumer_dict.get_consumer(ord) {
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
    use crate::{DefaultIds, SubscriptionScope};
    use std::sync::Arc;

    #[test]
    fn test_consumer_dictionary_get_or_create() {
        let mut dict = ConsumerDictionary::<DefaultIds>::new();

        let ord1 = dict.get_or_create(100);
        assert_eq!(ord1.get(), 0);

        let ord2 = dict.get_or_create(200);
        assert_eq!(ord2.get(), 1);

        // Same consumer returns same ordinal
        let ord1_again = dict.get_or_create(100);
        assert_eq!(ord1_again.get(), 0);
    }

    #[test]
    fn test_consumer_dictionary_next_ordinal_overflow_errors() {
        let err = ConsumerDictionary::<DefaultIds>::next_ordinal_for_len(u64::from(u32::MAX) + 1)
            .expect_err("ordinal allocation beyond u32::MAX should fail");
        assert!(err.contains("ordinal capacity exceeded"));
    }

    #[test]
    fn test_consumer_dictionary_try_get_or_create_success() {
        let mut dict = ConsumerDictionary::<DefaultIds>::new();
        let ord = dict
            .try_get_or_create(123)
            .expect("small dictionaries should allocate ordinals");
        assert_eq!(ord.get(), 0);
    }

    #[test]
    fn test_consumer_dictionary_get() {
        let mut dict = ConsumerDictionary::<DefaultIds>::new();

        dict.get_or_create(42);

        assert_eq!(dict.get(42), Some(ConsumerOrdinal::new(0)));
        assert_eq!(dict.get(99), None);
    }

    #[test]
    fn test_dispatch_insert_valid_event() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let partition = TablePartition::<DefaultIds>::new(1);
        let consumer_dict = ConsumerDictionary::<DefaultIds>::new();
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

        assert!(dispatch_consumers(&event, &partition, &consumer_dict, &mut vm).is_ok());
    }

    #[test]
    fn test_dispatch_insert_missing_new_row() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let partition = TablePartition::<DefaultIds>::new(1);
        let consumer_dict = ConsumerDictionary::<DefaultIds>::new();
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
            dispatch_consumers(&event, &partition, &consumer_dict, &mut vm),
            Err(DispatchError::MissingRequiredRowImage(_))
        ));
    }

    #[test]
    fn test_consumer_dictionary_remove() {
        let mut dict = ConsumerDictionary::<DefaultIds>::new();

        let ord = dict.get_or_create(42);
        assert_eq!(dict.get(42), Some(ord));

        dict.remove(42);
        assert_eq!(dict.get(42), None);
    }

    #[test]
    fn test_consumer_dictionary_remove_clears_ordinal_lookup() {
        let mut dict = ConsumerDictionary::<DefaultIds>::new();

        let ord = dict.get_or_create(42);
        assert_eq!(dict.get_consumer(ord), Some(42));

        dict.remove(42);
        assert_eq!(dict.get_consumer(ord), None);
    }

    #[test]
    fn test_consumer_dictionary_get_consumer() {
        let mut dict = ConsumerDictionary::<DefaultIds>::new();

        let ord = dict.get_or_create(100);
        assert_eq!(dict.get_consumer(ord), Some(100));

        // Invalid ordinal
        assert_eq!(dict.get_consumer(ConsumerOrdinal::new(999)), None);
    }

    #[test]
    fn test_consumer_dictionary_default() {
        let dict = ConsumerDictionary::<DefaultIds>::default();
        assert_eq!(dict.get(42), None);
    }

    #[test]
    fn test_dispatch_update_missing_new_row() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let partition = TablePartition::<DefaultIds>::new(1);
        let consumer_dict = ConsumerDictionary::<DefaultIds>::new();
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
            dispatch_consumers(&event, &partition, &consumer_dict, &mut vm),
            Err(DispatchError::MissingRequiredRowImage(_))
        ));
    }

    #[test]
    fn test_dispatch_delete_missing_old_row() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let partition = TablePartition::<DefaultIds>::new(1);
        let consumer_dict = ConsumerDictionary::<DefaultIds>::new();
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
            dispatch_consumers(&event, &partition, &consumer_dict, &mut vm),
            Err(DispatchError::MissingRequiredRowImage(_))
        ));
    }

    #[test]
    fn test_dispatch_truncate_requires_no_row_and_returns_only_row_subscribers() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use super::super::predicate::{Predicate, SubscriptionBinding};
        use crate::compiler::sql_shape::QueryProjection;
        use crate::compiler::Vm;
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan};

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();

        // Row predicate bound to consumer 42.
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

        let row_ord = consumer_dict.get_or_create(42);
        partition.add_binding(
            SubscriptionBinding {
                subscription_id: 100,
                predicate_id: row_pred_id,
                consumer_id: 42,
                consumer_ordinal: row_ord,
                scope: SubscriptionScope::Durable,
                updated_at_unix_ms: 1000,
            },
            row_pred_id,
        );

        // Aggregate predicate bound to consumer 77 must not leak into consumers().
        make_count_pred_and_binding(1, 101, 77, &mut partition, &mut consumer_dict);
        let mut vm = Vm::new();

        let event = WalEvent {
            kind: EventKind::Truncate,
            table_id: 1,
            pk: crate::PrimaryKey::empty(),
            old_row: None,
            new_row: None,
            changed_columns: Arc::from([]),
        };

        let notifs = dispatch_consumers(&event, &partition, &consumer_dict, &mut vm)
            .expect("truncate should dispatch without row image");
        assert!(notifs.inserted.is_empty());
        assert!(notifs.updated.is_empty());
        let mut deleted = notifs.deleted;
        deleted.sort_unstable();
        assert_eq!(deleted, vec![42]);
    }

    #[test]
    fn test_consumer_dictionary_ordinal_to_consumer_vec() {
        let mut dict = ConsumerDictionary::<DefaultIds>::new();
        dict.get_or_create(10);
        dict.get_or_create(20);
        dict.get_or_create(30);

        let vec = dict.ordinal_to_consumer_vec();
        assert_eq!(vec, vec![10, 20, 30]);
    }

    #[test]
    fn test_consumer_dictionary_ordinal_to_consumer_vec_excludes_removed_users() {
        let mut dict = ConsumerDictionary::<DefaultIds>::new();
        dict.get_or_create(10);
        dict.get_or_create(20);
        dict.get_or_create(30);

        dict.remove(20);

        let vec = dict.ordinal_to_consumer_vec();
        assert_eq!(vec, vec![10, 30]);
    }

    #[test]
    fn test_matched_consumers_iterator() {
        let mut dict = ConsumerDictionary::<DefaultIds>::new();
        dict.get_or_create(10);
        dict.get_or_create(20);
        dict.get_or_create(30);

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(0); // Consumer 10
        bitmap.insert(2); // Consumer 30

        let consumers: Vec<_> = (MatchedConsumers {
            bitmap_iter: bitmap.into_iter(),
            dict: &dict,
        })
        .collect();
        assert_eq!(consumers, vec![10, 30]);
    }

    #[test]
    fn test_dispatch_consumers_update_event_matching() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use super::super::predicate::Predicate;
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan, Vm};

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
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

        let ord = consumer_dict.get_or_create(42);
        let binding = super::super::predicate::SubscriptionBinding {
            subscription_id: 100,
            predicate_id: pred_id,
            consumer_id: 42,
            consumer_ordinal: ord,
            scope: SubscriptionScope::Durable,
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

        let notifs = dispatch_consumers(&event, &partition, &consumer_dict, &mut vm).unwrap();
        // old_row age=17 (no match), new_row age=25 (match) → consumer enters (inserted)
        assert!(
            notifs.inserted.contains(&42),
            "Consumer 42 should be inserted (age > 18 with new age=25, old age=17)"
        );
        assert!(notifs.deleted.is_empty());
        assert!(notifs.updated.is_empty());
    }

    #[test]
    fn test_dispatch_consumers_delete_event_matching() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use super::super::predicate::Predicate;
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan, Vm};

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
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

        let ord = consumer_dict.get_or_create(99);
        let binding = super::super::predicate::SubscriptionBinding {
            subscription_id: 200,
            predicate_id: pred_id,
            consumer_id: 99,
            consumer_ordinal: ord,
            scope: SubscriptionScope::Durable,
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

        let notifs = dispatch_consumers(&event, &partition, &consumer_dict, &mut vm).unwrap();
        assert!(
            notifs.deleted.contains(&99),
            "Consumer 99 should see deletion (age < 30 with age=25)"
        );
        assert!(notifs.inserted.is_empty());
        assert!(notifs.updated.is_empty());
    }

    // --- Aggregate dispatch tests ---

    fn make_count_pred_and_binding(
        pred_slab: usize,
        sub_id: u64,
        consumer_id: u64,
        partition: &mut super::super::partition::TablePartition<DefaultIds>,
        consumer_dict: &mut ConsumerDictionary<DefaultIds>,
    ) -> super::super::ids::PredicateId {
        use super::super::indexes::IndexableAtom;
        use super::super::predicate::{Predicate, SubscriptionBinding};
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

        let ord = consumer_dict.get_or_create(consumer_id);
        partition.add_binding(
            SubscriptionBinding {
                subscription_id: sub_id,
                predicate_id: pred_id,
                consumer_id,
                consumer_ordinal: ord,
                scope: SubscriptionScope::Durable,
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
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
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
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
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
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
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
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
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
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
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
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
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
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut consumer_dict);

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

        let err = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm)
            .expect_err("UPDATE without old_row must be rejected for aggregates");
        assert!(matches!(
            err,
            DispatchError::AggregateUpdateRequiresOldRow(1)
        ));
    }

    #[test]
    fn test_agg_update_partial_old_row_returns_error() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut consumer_dict);

        // old_row has Cell::Missing at column 1 — partial image
        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            old_row: Some(crate::RowImage {
                cells: Arc::from([crate::Cell::Int(1), crate::Cell::Missing]),
            }),
            new_row: Some(make_row(20)),
            changed_columns: Arc::from([1u16]),
        };

        let err = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm)
            .expect_err("UPDATE with partial old_row must be rejected for aggregates");
        assert!(matches!(
            err,
            DispatchError::AggregateUpdateRequiresOldRow(1)
        ));
    }

    // --- SUM aggregate dispatch tests ---

    /// Helper: create a SUM predicate (WHERE amount > 10, SUM on column 1) and bind consumer.
    fn make_sum_pred_and_binding(
        pred_slab: usize,
        sub_id: u64,
        consumer_id: u64,
        sum_col: u16,
        partition: &mut super::super::partition::TablePartition<DefaultIds>,
        consumer_dict: &mut ConsumerDictionary<DefaultIds>,
    ) -> super::super::ids::PredicateId {
        use super::super::indexes::IndexableAtom;
        use super::super::predicate::{Predicate, SubscriptionBinding};
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

        let ord = consumer_dict.get_or_create(consumer_id);
        partition.add_binding(
            SubscriptionBinding {
                subscription_id: sub_id,
                predicate_id: pred_id,
                consumer_id,
                consumer_ordinal: ord,
                scope: SubscriptionScope::Durable,
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
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(20.0))]);
    }

    #[test]
    fn test_sum_insert_non_matching_no_delta() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
        assert!(deltas.is_empty());
    }

    #[test]
    fn test_sum_delete_matching_gives_neg_delta() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(-20.0))]);
    }

    #[test]
    fn test_sum_update_both_match_value_change() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(10.0))]);
    }

    #[test]
    fn test_sum_update_same_value_zero_net() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
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
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(-20.0))]);
    }

    #[test]
    fn test_sum_update_old_no_match_new_match() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(20.0))]);
    }

    #[test]
    fn test_sum_null_cell_gives_no_delta() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
        assert!(deltas.is_empty());
    }

    #[test]
    fn test_sum_missing_cell_gives_no_delta() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut consumer_dict);

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

        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
        assert!(deltas.is_empty());
    }

    #[test]
    fn test_sum_float_cell_value() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_sum_pred_and_binding(0, 1, 42, 1, &mut partition, &mut consumer_dict);

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
        let deltas = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm).unwrap();
        assert!(deltas.is_empty(), "2.5 ≤ 10, so WHERE does not match");
    }

    #[test]
    fn test_agg_truncate_returns_requires_reset_error() {
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut consumer_dict);

        let event = WalEvent {
            kind: EventKind::Truncate,
            table_id: 1,
            pk: crate::PrimaryKey::empty(),
            old_row: None,
            new_row: None,
            changed_columns: Arc::from([]),
        };

        let err = compute_agg_deltas(&event, &partition, &consumer_dict, &mut vm)
            .expect_err("TRUNCATE should return TruncateRequiresReset");
        assert!(
            matches!(err, DispatchError::TruncateRequiresReset(1)),
            "expected TruncateRequiresReset(1), got: {err:?}"
        );
    }

    #[test]
    fn test_agg_consumers_dispatch_skips_count_predicates() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use crate::compiler::Vm;

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
        let mut vm = Vm::new();

        // Register a COUNT predicate for consumer 42
        make_count_pred_and_binding(0, 1, 42, &mut partition, &mut consumer_dict);

        // Also register a Rows predicate for consumer 99 (to ensure dispatch still works)
        {
            use super::super::predicate::{Predicate, SubscriptionBinding};
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
            let ord = consumer_dict.get_or_create(99);
            partition.add_binding(
                SubscriptionBinding {
                    subscription_id: 2,
                    predicate_id: pred_id,
                    consumer_id: 99,
                    consumer_ordinal: ord,
                    scope: SubscriptionScope::Durable,
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

        let notifs = dispatch_consumers(&event, &partition, &consumer_dict, &mut vm)
            .expect("dispatch_consumers should succeed");

        // Consumer 99 (Rows predicate) should appear as inserted; consumer 42 (COUNT) must NOT
        assert!(
            notifs.inserted.contains(&99),
            "Rows subscriber should be dispatched"
        );
        let all: Vec<_> = notifs.into_iter().collect();
        assert!(
            !all.contains(&42),
            "COUNT subscriber must not appear in consumers() dispatch"
        );
    }

    #[test]
    fn test_dispatch_consumers_no_match() {
        use super::super::indexes::IndexableAtom;
        use super::super::partition::TablePartition;
        use super::super::predicate::Predicate;
        use crate::compiler::{BytecodeProgram, Instruction, PrefilterPlan, Vm};

        let mut partition = TablePartition::<DefaultIds>::new(1);
        let mut consumer_dict = ConsumerDictionary::<DefaultIds>::new();
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

        let ord = consumer_dict.get_or_create(42);
        let binding = super::super::predicate::SubscriptionBinding {
            subscription_id: 300,
            predicate_id: pred_id,
            consumer_id: 42,
            consumer_ordinal: ord,
            scope: SubscriptionScope::Durable,
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

        let notifs = dispatch_consumers(&event, &partition, &consumer_dict, &mut vm).unwrap();
        let all: Vec<_> = notifs.into_iter().collect();
        assert!(
            all.is_empty(),
            "No consumers should match age > 50 with age=25"
        );
    }
}

#![allow(clippy::type_complexity)]
//! Event dispatch pipeline

use super::{
    agg::{agg_delta_for_row, AggCellRead},
    ids::{ConsumerOrdinal, PredicateId},
    partition::{ColumnProbe, TablePartition},
    predicate::{Predicate, PredicateStore},
};
use crate::backend::{Backend, CdcEvent, Presence, RowKind, ScalarKind, Value};
use crate::runtime::indexes::IndexableCell;
use crate::{
    compiler::{sql_shape::QueryProjection, Tri, Vm},
    AggDelta, ConsumerNotifications, DispatchError, EventKind, IdTypes, SubscriptionId,
};
use alloc::vec::Vec;
use hashbrown::HashMap;
use roaring::RoaringBitmap;

/// Consumer dictionary translating between ordinals and ConsumerIds.
///
/// Maps dense ordinals (0-based, used in bitmaps) to sparse ConsumerIds, so
/// RoaringBitmap operations stay efficient for arbitrary ConsumerIds.
#[derive(Clone, Debug)]
pub struct ConsumerDictionary<I: IdTypes> {
    /// ConsumerOrdinal -> ConsumerId (dense, 0-indexed)
    ordinal_to_consumer: Vec<Option<I::ConsumerId>>,
    /// ConsumerId -> ConsumerOrdinal (for reverse lookup)
    consumer_to_ordinal: HashMap<I::ConsumerId, ConsumerOrdinal>,
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
            consumer_to_ordinal: HashMap::new(),
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
/// Owns the `RoaringBitmap` (already heap-allocated during dispatch, so moving
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

fn notifications_for_truncate_with_stamps<I: IdTypes, B: Backend, C>(
    partition: &TablePartition<I, B>,
    consumer_dict: &ConsumerDictionary<I>,
    stamps: &mut Vec<SubscriptionId>,
) -> ConsumerNotifications<I, C>
where
    C: crate::Checkpoint,
{
    let snapshot = partition.load_snapshot();
    let mut ordinals = RoaringBitmap::new();
    for (pred_id, consumers) in &snapshot.predicates.predicate_consumers {
        let Some(pred) = snapshot.predicates.get_predicate(*pred_id) else {
            continue;
        };
        if matches!(pred.projection, QueryProjection::Rows) {
            ordinals |= consumers;
            collect_stamps_for_predicate(&snapshot.predicates, *pred_id, consumers, stamps);
        }
    }
    let deleted = resolve_ordinals(ordinals, consumer_dict);
    ConsumerNotifications::from_parts(Vec::new(), deleted, Vec::new())
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

/// Dispatch an event to interested consumers, returning view-relative
/// notifications.
///
/// Routes by [`EventKind`]:
/// * `Insert` -> single-eval on [`RowKind::New`], all matches to `inserted`.
/// * `Delete` -> single-eval on [`RowKind::Old`], all matches to `deleted`.
/// * `Update` -> dual-eval (old + new), three-way split.
/// * `Truncate` -> all row subscribers to `deleted`.
///
/// `column_kinds` names the [`ScalarKind`] for each column of the target
/// table, index-aligned with [`crate::ColumnId`]. The dispatcher uses it to
/// route event scalar accessors correctly. Callers cache this per table
/// (via [`crate::catalog_helpers::column_scalar_kind`]).
pub fn dispatch_consumers<I, E>(
    event: &E,
    partition: &TablePartition<I, E::Backend>,
    consumer_dict: &ConsumerDictionary<I>,
    vm: &mut Vm<E::Backend>,
    column_kinds: &[ScalarKind],
) -> Result<ConsumerNotifications<I, E::Checkpoint>, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
{
    let (notifs, _) =
        dispatch_consumers_with_stamps(event, partition, consumer_dict, vm, column_kinds)?;
    Ok(notifs)
}

/// Like [`dispatch_consumers`] but also returns the `SubscriptionId`s
/// whose bindings contributed to a match.
///
/// Used by activity-aware eviction policies to stamp matched
/// subscriptions in O(1) per matched pair via the `binding_lookup` index
/// on `PredicateStore`.
pub fn dispatch_consumers_with_stamps<I, E>(
    event: &E,
    partition: &TablePartition<I, E::Backend>,
    consumer_dict: &ConsumerDictionary<I>,
    vm: &mut Vm<E::Backend>,
    column_kinds: &[ScalarKind],
) -> Result<(ConsumerNotifications<I, E::Checkpoint>, Vec<SubscriptionId>), DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
{
    let checkpoint = event.checkpoint().cloned();
    let mut stamps: Vec<SubscriptionId> = Vec::new();
    let notifs: ConsumerNotifications<I, E::Checkpoint> = match event.kind() {
        EventKind::Truncate => {
            let _ = vm;
            notifications_for_truncate_with_stamps(partition, consumer_dict, &mut stamps)
        }
        EventKind::Insert => {
            let bitmap = dispatch_single_eval_bitmap_with_stamps(
                event,
                RowKind::New,
                partition,
                vm,
                column_kinds,
                &mut stamps,
            )?;
            ConsumerNotifications::from_parts(
                resolve_ordinals(bitmap, consumer_dict),
                Vec::new(),
                Vec::new(),
            )
        }
        EventKind::Delete => {
            let bitmap = dispatch_single_eval_bitmap_with_stamps(
                event,
                RowKind::Old,
                partition,
                vm,
                column_kinds,
                &mut stamps,
            )?;
            ConsumerNotifications::from_parts(
                Vec::new(),
                resolve_ordinals(bitmap, consumer_dict),
                Vec::new(),
            )
        }
        EventKind::Update => dispatch_update_with_stamps(
            event,
            partition,
            consumer_dict,
            vm,
            column_kinds,
            &mut stamps,
        )?,
    };
    Ok((notifs.with_checkpoint(checkpoint), stamps))
}

fn collect_stamps_for_predicate<I: IdTypes, B: Backend>(
    predicates: &PredicateStore<I, B>,
    pred_id: PredicateId,
    consumers: &RoaringBitmap,
    out: &mut Vec<SubscriptionId>,
) {
    for ord_u32 in consumers {
        let ord = ConsumerOrdinal::new(ord_u32);
        if let Some(sub_ids) = predicates.binding_lookup.get(&(pred_id, ord)) {
            out.extend_from_slice(sub_ids);
        }
    }
}

/// Dual-eval dispatch for UPDATE events: evaluates both old and new rows
/// to produce view-relative `inserted` / `deleted` / `updated` sets.
///
/// Callers whose source cannot provide a complete old row (Postgres
/// REPLICA IDENTITY DEFAULT for example) will see `Presence::Missing` on
/// old-row accessors, causing the VM to return `Tri::Unknown`. Unknown
/// verdicts collapse to "did not match" in this splitter — that is
/// conservative-safe but may misclassify view membership. A future
/// enhancement would surface the incompleteness through a
/// `CdcEvent::has_complete_row` method and fall back to single-eval on
/// the new row (matches to `updated`, matching pre-Phase-5 behaviour).
fn dispatch_update_with_stamps<I, E>(
    event: &E,
    partition: &TablePartition<I, E::Backend>,
    consumer_dict: &ConsumerDictionary<I>,
    vm: &mut Vm<E::Backend>,
    column_kinds: &[ScalarKind],
    stamps: &mut Vec<SubscriptionId>,
) -> Result<ConsumerNotifications<I, E::Checkpoint>, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
{
    let candidates = partition.select_candidates(
        column_kinds.len(),
        |col| probe_column_for_index(event, RowKind::New, col, column_kinds),
        EventKind::Update,
        event.changed_columns(),
    );
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

        let new_match = vm
            .eval(&pred.bytecode, event, RowKind::New)
            .map_err(|e| DispatchError::VmError(format!("{e:?}")))?
            == Tri::True;

        let old_match = vm
            .eval(&pred.bytecode, event, RowKind::Old)
            .map_err(|e| DispatchError::VmError(format!("{e:?}")))?
            == Tri::True;

        if let Some(bitmap) = snapshot.predicates.predicate_consumers.get(&pred_id) {
            match (new_match, old_match) {
                (true, false) => {
                    inserted_ordinals |= bitmap;
                    collect_stamps_for_predicate(&snapshot.predicates, pred_id, bitmap, stamps);
                }
                (false, true) => {
                    deleted_ordinals |= bitmap;
                    collect_stamps_for_predicate(&snapshot.predicates, pred_id, bitmap, stamps);
                }
                (true, true) => {
                    updated_ordinals |= bitmap;
                    collect_stamps_for_predicate(&snapshot.predicates, pred_id, bitmap, stamps);
                }
                (false, false) => {}
            }
        }
    }

    Ok(ConsumerNotifications::from_parts(
        resolve_ordinals(inserted_ordinals, consumer_dict),
        resolve_ordinals(deleted_ordinals, consumer_dict),
        resolve_ordinals(updated_ordinals, consumer_dict),
    ))
}

/// Single-eval dispatch: evaluate one row_kind, return the matching
/// ordinals bitmap and accumulate matched subscription ids into `stamps`.
/// Used for INSERT (New) and DELETE (Old).
fn dispatch_single_eval_bitmap_with_stamps<I, E>(
    event: &E,
    row: RowKind,
    partition: &TablePartition<I, E::Backend>,
    vm: &mut Vm<E::Backend>,
    column_kinds: &[ScalarKind],
    stamps: &mut Vec<SubscriptionId>,
) -> Result<RoaringBitmap, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
{
    let candidates = partition.select_candidates(
        column_kinds.len(),
        |col| probe_column_for_index(event, row, col, column_kinds),
        event.kind(),
        event.changed_columns(),
    );
    let snapshot = partition.load_snapshot();
    let mut matching_ordinals = RoaringBitmap::new();

    for_each_matching_predicate(
        &candidates,
        &snapshot.predicates,
        event,
        row,
        vm,
        |pred, consumers| {
            if matches!(pred.projection, QueryProjection::Rows) {
                matching_ordinals |= consumers;
                collect_stamps_for_predicate(&snapshot.predicates, pred.id, consumers, stamps);
            }
            Ok(())
        },
    )?;

    Ok(matching_ordinals)
}

fn for_each_matching_predicate<I, E, F>(
    candidates: &RoaringBitmap,
    store: &PredicateStore<I, E::Backend>,
    event: &E,
    row: RowKind,
    vm: &mut Vm<E::Backend>,
    mut on_match: F,
) -> Result<(), DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    F: FnMut(&Predicate<E::Backend>, &RoaringBitmap) -> Result<(), DispatchError>,
{
    for pred_id_u32 in candidates {
        let Some(pred_id) = super::ids::PredicateId::try_from_u32(pred_id_u32) else {
            continue;
        };

        let Some(pred) = store.get_predicate(pred_id) else {
            continue;
        };

        let result = vm
            .eval(&pred.bytecode, event, row)
            .map_err(|e| DispatchError::VmError(format!("{e:?}")))?;

        if result == Tri::True {
            if let Some(bitmap) = store.predicate_consumers.get(&pred_id) {
                on_match(pred, bitmap)?;
            }
        }
    }
    Ok(())
}

/// Weighted-row pairs for aggregate delta computation.
///
/// Delta normalization per event kind:
/// * `Insert`   -> `[(+1, RowKind::New)]`
/// * `Delete`   -> `[(-1, RowKind::Old)]`
/// * `Update`   -> `[(-1, RowKind::Old), (+1, RowKind::New)]`
/// * `Truncate` -> `Err(TruncateRequiresReset)`
fn weighted_rows_for_agg<E: CdcEvent>(event: &E) -> Result<Vec<(i64, RowKind)>, DispatchError> {
    match event.kind() {
        EventKind::Insert => Ok(vec![(1, RowKind::New)]),
        EventKind::Delete => Ok(vec![(-1, RowKind::Old)]),
        EventKind::Update => Ok(vec![(-1, RowKind::Old), (1, RowKind::New)]),
        EventKind::Truncate => Err(DispatchError::TruncateRequiresReset(event.table_id())),
    }
}

/// Compute typed signed deltas for aggregate subscriptions
/// (`COUNT(*)`, `SUM(col)`, `AVG(col)`, ...).
///
/// See [`weighted_rows_for_agg`] for the per-event-kind normalization.
/// For each `(weight, row_kind)` pair, selects agg candidate predicates,
/// VM-evaluates them, and accumulates weight per user through the
/// appropriate [`AggDelta`] variant. Zero-net entries are filtered out.
/// The same user may appear multiple times in the result (once per
/// aggregate kind).
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
pub(crate) fn compute_agg_deltas<I, E>(
    event: &E,
    partition: &TablePartition<I, E::Backend>,
    consumer_dict: &ConsumerDictionary<I>,
    vm: &mut Vm<E::Backend>,
    column_kinds: &[ScalarKind],
) -> Result<Vec<(I::ConsumerId, AggDelta)>, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
{
    let weighted_rows = weighted_rows_for_agg(event)?;

    // Separate accumulators for each aggregate kind (avoids mixed-type confusion).
    let mut count_weights: HashMap<ConsumerOrdinal, i64> = HashMap::new();
    let mut sum_weights: HashMap<ConsumerOrdinal, f64> = HashMap::new();
    // AVG accumulator: (sum_delta, count_delta).
    let mut avg_accum: HashMap<ConsumerOrdinal, (f64, i64)> = HashMap::new();
    // VAR / STDDEV accumulator: (sum_delta, sum_sq_delta, count_delta).
    let mut stats_accum: HashMap<ConsumerOrdinal, (f64, f64, i64)> = HashMap::new();

    let snapshot = partition.load_snapshot();

    // For UPDATE, use dependency-aware candidate selection; for INSERT /
    // DELETE pass empty changed_cols to get all agg candidates.
    let changed_cols: &[crate::ColumnId] = if event.kind() == EventKind::Update {
        event.changed_columns()
    } else {
        &[]
    };
    let candidates = partition.select_agg_candidates(event.kind(), changed_cols);

    for (weight, row) in weighted_rows {
        for_each_matching_predicate(
            &candidates,
            &snapshot.predicates,
            event,
            row,
            vm,
            |pred, consumers| {
                let QueryProjection::Aggregate(spec) = &pred.projection else {
                    return Ok(());
                };

                let Some(delta) = agg_delta_for_row(spec, weight, |col| {
                    probe_column_for_agg(event, row, col, column_kinds)
                }) else {
                    return Ok(());
                };

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
                        AggDelta::Stats {
                            sum_delta,
                            sum_sq_delta,
                            count_delta,
                        } => {
                            let entry = stats_accum.entry(ord).or_default();
                            entry.0 += *sum_delta;
                            entry.1 += *sum_sq_delta;
                            entry.2 += *count_delta;
                        }
                    }
                }

                Ok(())
            },
        )?;
    }

    // Translate ordinals to user IDs; filter out zero-net entries.
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
    for (ord, (s, sq, c)) in stats_accum
        .into_iter()
        .filter(|(_, (s, sq, c))| *s != 0.0 || *sq != 0.0 || *c != 0)
    {
        if let Some(uid) = consumer_dict.get_consumer(ord) {
            result.push((
                uid,
                AggDelta::Stats {
                    sum_delta: s,
                    sum_sq_delta: sq,
                    count_delta: c,
                },
            ));
        }
    }

    Ok(result)
}

// ============================================================================
// Per-column probes (event -> ColumnProbe / AggCellRead)
// ============================================================================

/// Probe column `col` at the `row` view of `event` for the equality /
/// range / null indexes tracked by [`TablePartition::select_candidates`].
///
/// Dispatches on the pre-cached [`ScalarKind`] to the matching typed
/// accessor. Values whose scalar payload downcasts to one of the four
/// indexable primitives (`bool` / `i64` / `f64` / `String`) become
/// [`IndexableCell`] variants; every other scalar returns
/// [`ColumnProbe::present`] with `value: None`, causing the caller to
/// consult only the fallback index for that column.
fn probe_column_for_index<E: CdcEvent>(
    event: &E,
    row: RowKind,
    col: crate::ColumnId,
    column_kinds: &[ScalarKind],
) -> ColumnProbe {
    let Some(&kind) = column_kinds.get(col as usize) else {
        return ColumnProbe::missing();
    };
    match kind {
        ScalarKind::Bool => lift_present(event.bool_at(row, col), |v| {
            IndexableCell::from_value::<E::Backend>(&Value::Bool(v.clone()))
        }),
        ScalarKind::Int => lift_present(event.int_at(row, col), |v| {
            IndexableCell::from_value::<E::Backend>(&Value::Int(v.clone()))
        }),
        ScalarKind::Float => lift_present(event.float_at(row, col), |v| {
            IndexableCell::from_value::<E::Backend>(&Value::Float(v.clone()))
        }),
        ScalarKind::String => lift_present(event.string_at(row, col), |v| {
            IndexableCell::from_value::<E::Backend>(&Value::String(v.clone()))
        }),
        ScalarKind::Bytes => lift_present(event.bytes_at(row, col), |_| None),
        ScalarKind::Uuid => lift_present(event.uuid_at(row, col), |_| None),
        ScalarKind::Timestamp => lift_present(event.timestamp_at(row, col), |_| None),
        ScalarKind::TimestampTz => lift_present(event.timestamp_tz_at(row, col), |_| None),
        ScalarKind::Date => lift_present(event.date_at(row, col), |_| None),
        ScalarKind::Time => lift_present(event.time_at(row, col), |_| None),
        ScalarKind::Decimal => lift_present(event.decimal_at(row, col), |_| None),
        ScalarKind::Json => lift_present(event.json_at(row, col), |_| None),
        ScalarKind::Jsonb => lift_present(event.jsonb_at(row, col), |_| None),
    }
}

/// Lift a `Presence<&T>` into a [`ColumnProbe`] via a caller-supplied
/// indexable-payload extractor.
#[inline]
fn lift_present<T, F>(presence: Presence<&T>, extract: F) -> ColumnProbe
where
    F: FnOnce(&T) -> Option<IndexableCell>,
{
    match presence {
        Presence::Missing => ColumnProbe::missing(),
        Presence::Null => ColumnProbe::null(),
        Presence::Present(v) => ColumnProbe::present(extract(v)),
    }
}

/// Probe column `col` at the `row` view of `event` for aggregate delta
/// computation.
///
/// Dispatches on the pre-cached [`ScalarKind`] and downcasts the scalar
/// payload to `f64` when the column carries a numeric type
/// ([`ScalarKind::Int`] or [`ScalarKind::Float`]). Every other scalar is
/// reported as [`AggCellRead::NonNumeric`] when present.
fn probe_column_for_agg<E: CdcEvent>(
    event: &E,
    row: RowKind,
    col: crate::ColumnId,
    column_kinds: &[ScalarKind],
) -> AggCellRead {
    use core::any::Any;
    let Some(&kind) = column_kinds.get(col as usize) else {
        return AggCellRead::Missing;
    };
    match kind {
        ScalarKind::Int => match event.int_at(row, col) {
            Presence::Missing => AggCellRead::Missing,
            Presence::Null => AggCellRead::Null,
            Presence::Present(v) => (v as &dyn Any).downcast_ref::<i64>().map_or(
                AggCellRead::NonNumeric,
                #[allow(clippy::cast_precision_loss)]
                |i| AggCellRead::Numeric(*i as f64),
            ),
        },
        ScalarKind::Float => match event.float_at(row, col) {
            Presence::Missing => AggCellRead::Missing,
            Presence::Null => AggCellRead::Null,
            Presence::Present(v) => {
                (v as &dyn Any)
                    .downcast_ref::<f64>()
                    .map_or(AggCellRead::NonNumeric, |f| {
                        if f.is_finite() {
                            AggCellRead::Numeric(*f)
                        } else {
                            AggCellRead::NonNumeric
                        }
                    })
            }
        },
        ScalarKind::Bool => presence_only(event.bool_at(row, col)),
        ScalarKind::String => presence_only(event.string_at(row, col)),
        ScalarKind::Bytes => presence_only(event.bytes_at(row, col)),
        ScalarKind::Uuid => presence_only(event.uuid_at(row, col)),
        ScalarKind::Timestamp => presence_only(event.timestamp_at(row, col)),
        ScalarKind::TimestampTz => presence_only(event.timestamp_tz_at(row, col)),
        ScalarKind::Date => presence_only(event.date_at(row, col)),
        ScalarKind::Time => presence_only(event.time_at(row, col)),
        ScalarKind::Decimal => presence_only(event.decimal_at(row, col)),
        ScalarKind::Json => presence_only(event.json_at(row, col)),
        ScalarKind::Jsonb => presence_only(event.jsonb_at(row, col)),
    }
}

#[inline]
const fn presence_only<T>(presence: Presence<&T>) -> AggCellRead {
    match presence {
        Presence::Missing => AggCellRead::Missing,
        Presence::Null => AggCellRead::Null,
        Presence::Present(_) => AggCellRead::NonNumeric,
    }
}

// Test body deferred to Phase 10 per docs/refactor-cdc-event-handoff.md.

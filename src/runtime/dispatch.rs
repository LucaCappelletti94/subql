#![allow(clippy::type_complexity)]
//! Event dispatch pipeline

use super::{
    agg::{agg_delta_for_row, AggCellRead, DeltaSpec},
    aggregate::AggDelta,
    ids::{ConsumerOrdinal, PredicateId},
    partition::{ColumnProbe, TablePartition},
    predicate::{Predicate, PredicateStore},
};
use crate::backend::{Backend, CdcEvent, RowKind, Value};
use crate::runtime::indexes::IndexableCell;
use crate::term::TermLookup;
use crate::{
    compiler::{sql_shape::QueryProjection, Tri, Vm, VmError},
    ConsumerNotifications, DispatchError, EventKind, IdTypes, SubscriptionId,
};
use alloc::vec::Vec;
use hashbrown::{HashMap, HashSet};
use roaring::RoaringBitmap;
use sql_traits::prelude::DatabaseLike;

/// Map a [`VmError`] surfaced during evaluation into a [`DispatchError`],
/// preserving a structured cell-decode failure as [`DispatchError::Value`].
fn dispatch_vm_error(error: VmError) -> DispatchError {
    match error {
        VmError::Value(inner) => DispatchError::Value(inner),
        other => DispatchError::VmError(format!("{other:?}")),
    }
}

/// Which of a predicate's subscribers one row version reached.
///
/// A predicate is evaluated once for every subscriber sharing its text, so the
/// verdict is a *set* rather than a boolean as soon as a membership term is in
/// play: the term names which subscribers the changed row admits, and the
/// predicate's own subscribers are narrowed to that.
///
/// [`Matched::Every`] and [`Matched::Nobody`] borrow the predicate's bitmap
/// instead of copying it, so a predicate carrying no term costs exactly what it
/// did before terms existed. Only [`Matched::Narrowed`] owns a bitmap, and only
/// a term produces one.
enum Matched<'a> {
    /// The row test held and nothing narrowed it.
    Every(&'a RoaringBitmap),
    /// The row test did not hold, so no subscriber of this predicate matched.
    Nobody,
    /// A membership term narrowed the predicate's subscribers to these.
    Narrowed(RoaringBitmap),
}

impl Matched<'_> {
    /// The subscribers matched, or [`None`] when none did.
    const fn bitmap(&self) -> Option<&RoaringBitmap> {
        match self {
            Self::Every(all) => Some(all),
            Self::Nobody => None,
            Self::Narrowed(some) => Some(some),
        }
    }
}

/// Which view-relative set a split result belongs in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Slot {
    Inserted,
    Deleted,
    Updated,
}

/// Split one predicate's two row versions into the view-relative sets.
///
/// `inserted = new - old`, `deleted = old - new`, `updated = new & old`. With no
/// membership term the two sides are the whole subscriber bitmap or nothing,
/// which collapses to the four cases dispatch had before terms existed. Those
/// four are written out as their own arms so they borrow rather than compute,
/// leaving a term-free predicate exactly as cheap as it was.
///
/// Emits nothing for an empty set, so a predicate no subscriber matched neither
/// stamps nor touches an accumulator.
fn split_transition(
    new: &Matched<'_>,
    old: &Matched<'_>,
    mut emit: impl FnMut(Slot, &RoaringBitmap),
) {
    let mut emit_non_empty = |slot, set: &RoaringBitmap| {
        if !set.is_empty() {
            emit(slot, set);
        }
    };

    match (new, old) {
        (Matched::Nobody, Matched::Nobody) => {}
        (Matched::Every(all), Matched::Nobody) => emit_non_empty(Slot::Inserted, all),
        (Matched::Nobody, Matched::Every(all)) => emit_non_empty(Slot::Deleted, all),
        (Matched::Every(all), Matched::Every(_)) => emit_non_empty(Slot::Updated, all),
        _ => match (new.bitmap(), old.bitmap()) {
            (Some(n), None) => emit_non_empty(Slot::Inserted, n),
            (None, Some(o)) => emit_non_empty(Slot::Deleted, o),
            (Some(n), Some(o)) => {
                emit_non_empty(Slot::Inserted, &(n - o));
                emit_non_empty(Slot::Deleted, &(o - n));
                emit_non_empty(Slot::Updated, &(n & o));
            }
            (None, None) => {}
        },
    }
}

/// What one membership term says about one row version.
enum TermFacts<'a> {
    /// The term admits exactly these subscribers, and none when [`None`]: the
    /// row's compared value is one nobody stated, or it is SQL `NULL`, which
    /// `IN (SELECT ...)` never admits.
    Admits(Option<&'a RoaringBitmap>),
    /// The row does not carry the compared column, so the term cannot say. It
    /// answers `Tri::Unknown` for every subscriber alike, which composes through
    /// the filter's own three-valued logic rather than reading as "nobody".
    CannotSay,
}

/// Which of a predicate's subscribers `row` reached.
///
/// A term-free predicate is one evaluation and no allocation, exactly as before
/// terms existed. A filter naming `k` terms is `2^k` evaluations, because the
/// filter is a boolean formula whose only subscriber-dependent inputs are the
/// terms: partition the subscribers by their assignment over the `k` terms, and
/// the matched set is the union, over every assignment the formula accepts, of
/// the intersection of the term sets and their complements that assignment
/// describes. Exact for `AND`, `OR` and any nesting, and it does not grow with
/// subscriber count.
fn matched_for_row<'a, I, E, DB>(
    pred: &Predicate<E::Backend>,
    bitmap: &'a RoaringBitmap,
    store: &'a PredicateStore<I, E::Backend>,
    event: &E,
    row: RowKind,
    vm: &mut Vm<E::Backend>,
    db: &DB,
) -> Result<Matched<'a>, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    if pred.bytecode.term_columns.is_empty() {
        let held = vm
            .eval(&pred.bytecode, event, row, db)
            .map_err(dispatch_vm_error)?
            == Tri::True;
        return Ok(if held {
            Matched::Every(bitmap)
        } else {
            Matched::Nobody
        });
    }

    let mut facts: Vec<TermFacts<'a>> = Vec::with_capacity(pred.bytecode.term_columns.len());
    for (slot, columns) in pred.bytecode.term_columns.iter().enumerate() {
        let slot = u16::try_from(slot).unwrap_or(u16::MAX);
        // A NULL cell dominates an unreadable one: SQL never matches through a
        // NULL, so the term admits nobody whatever the other cells hold, while
        // an unreadable cell alone leaves the term unable to say.
        let mut keys = Vec::with_capacity(columns.len());
        let mut nobody = false;
        let mut unknown = false;
        for column in columns {
            let value = event
                .value_at(db, row, *column)
                .map_err(DispatchError::Value)?;
            match TermLookup::of(value) {
                TermLookup::Key(key) => keys.push(key),
                TermLookup::Nobody => nobody = true,
                TermLookup::Unknown => unknown = true,
            }
        }
        facts.push(if nobody {
            TermFacts::Admits(None)
        } else if unknown {
            TermFacts::CannotSay
        } else {
            TermFacts::Admits(
                store
                    .term_members(pred.id, slot)
                    .and_then(|members| members.admits(&keys)),
            )
        });
    }

    // Only a term the row can answer partitions the subscribers. One it cannot
    // answers alike for all of them, so it is a fixed `Unknown` rather than an
    // axis of the enumeration.
    let varying: Vec<usize> = facts
        .iter()
        .enumerate()
        .filter(|(_, fact)| matches!(fact, TermFacts::Admits(_)))
        .map(|(slot, _)| slot)
        .collect();

    let mut truths = alloc::vec![Tri::Unknown; facts.len()];
    let mut matched = RoaringBitmap::new();
    for assignment in 0..(1u32 << varying.len()) {
        for (bit, &slot) in varying.iter().enumerate() {
            truths[slot] = if assignment & (1 << bit) == 0 {
                Tri::False
            } else {
                Tri::True
            };
        }

        if vm
            .eval_with_terms(&pred.bytecode, event, row, db, &truths)
            .map_err(dispatch_vm_error)?
            != Tri::True
        {
            continue;
        }

        // The subscribers this assignment describes: in every term it says the
        // subscriber is in, out of every term it says they are not. Complemented
        // against the predicate's own subscribers, since a term set never reaches
        // outside them.
        let mut described = bitmap.clone();
        for (bit, &slot) in varying.iter().enumerate() {
            let TermFacts::Admits(admits) = facts[slot] else {
                continue;
            };
            let admits = admits.map_or_else(RoaringBitmap::new, Clone::clone);
            if assignment & (1 << bit) == 0 {
                described -= admits;
            } else {
                described &= admits;
            }
            if described.is_empty() {
                break;
            }
        }
        matched |= described;
    }

    Ok(Matched::Narrowed(matched))
}

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
) -> ConsumerNotifications<I, C, B>
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
/// `arity` is the target table's column count, used to bound index
/// candidate selection on INSERT and DELETE. An UPDATE selects without
/// probing, see [`TablePartition::select_update_candidates`].
pub fn dispatch_consumers<I, E, DB>(
    event: &E,
    partition: &TablePartition<I, E::Backend>,
    consumer_dict: &ConsumerDictionary<I>,
    vm: &mut Vm<E::Backend>,
    arity: usize,
    db: &DB,
) -> Result<ConsumerNotifications<I, E::Checkpoint, E::Backend>, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    let (notifs, _) =
        dispatch_consumers_with_stamps(event, partition, consumer_dict, vm, arity, db)?;
    Ok(notifs)
}

/// Like [`dispatch_consumers`] but also returns the `SubscriptionId`s
/// whose bindings contributed to a match.
///
/// Used by activity-aware eviction policies to stamp matched
/// subscriptions in O(1) per matched pair via the `binding_lookup` index
/// on `PredicateStore`.
pub fn dispatch_consumers_with_stamps<I, E, DB>(
    event: &E,
    partition: &TablePartition<I, E::Backend>,
    consumer_dict: &ConsumerDictionary<I>,
    vm: &mut Vm<E::Backend>,
    arity: usize,
    db: &DB,
) -> Result<
    (
        ConsumerNotifications<I, E::Checkpoint, E::Backend>,
        Vec<SubscriptionId>,
    ),
    DispatchError,
>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    let checkpoint = event.checkpoint();
    let mut stamps: Vec<SubscriptionId> = Vec::new();
    let notifs: ConsumerNotifications<I, E::Checkpoint, E::Backend> = match event.kind() {
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
                arity,
                db,
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
                arity,
                db,
                &mut stamps,
            )?;
            ConsumerNotifications::from_parts(
                Vec::new(),
                resolve_ordinals(bitmap, consumer_dict),
                Vec::new(),
            )
        }
        EventKind::Update => {
            dispatch_update_with_stamps(event, partition, consumer_dict, vm, db, &mut stamps)?
        }
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
/// REPLICA IDENTITY DEFAULT for example) will see `Value::Missing` on
/// old-row accessors, causing the VM to return `Tri::Unknown`. Unknown
/// verdicts collapse to "did not match" in this splitter — that is
/// conservative-safe but may misclassify view membership. A future
/// enhancement would surface the incompleteness through a
/// `CdcEvent::has_complete_row` method and fall back to single-eval on
/// the new row (matches to `updated`, matching pre-Phase-5 behaviour).
fn dispatch_update_with_stamps<I, E, DB>(
    event: &E,
    partition: &TablePartition<I, E::Backend>,
    consumer_dict: &ConsumerDictionary<I>,
    vm: &mut Vm<E::Backend>,
    db: &DB,
    stamps: &mut Vec<SubscriptionId>,
) -> Result<ConsumerNotifications<I, E::Checkpoint, E::Backend>, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    let candidates = partition.select_update_candidates();
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

        let Some(bitmap) = snapshot.predicates.predicate_consumers.get(&pred_id) else {
            continue;
        };

        let new_matched = matched_for_row(
            pred,
            bitmap,
            &snapshot.predicates,
            event,
            RowKind::New,
            vm,
            db,
        )?;
        let old_matched = matched_for_row(
            pred,
            bitmap,
            &snapshot.predicates,
            event,
            RowKind::Old,
            vm,
            db,
        )?;

        split_transition(&new_matched, &old_matched, |slot, set| {
            collect_stamps_for_predicate(&snapshot.predicates, pred_id, set, stamps);
            let target = match slot {
                Slot::Inserted => &mut inserted_ordinals,
                Slot::Deleted => &mut deleted_ordinals,
                Slot::Updated => &mut updated_ordinals,
            };
            *target |= set;
        });
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
fn dispatch_single_eval_bitmap_with_stamps<I, E, DB>(
    event: &E,
    row: RowKind,
    partition: &TablePartition<I, E::Backend>,
    vm: &mut Vm<E::Backend>,
    arity: usize,
    db: &DB,
    stamps: &mut Vec<SubscriptionId>,
) -> Result<RoaringBitmap, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    let candidates = partition.select_candidates(arity, |col| {
        probe_column_for_index(event, row, col, arity, db)
    });
    let snapshot = partition.load_snapshot();
    let mut matching_ordinals = RoaringBitmap::new();

    for_each_matching_predicate(
        &candidates,
        &snapshot.predicates,
        event,
        row,
        vm,
        db,
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

fn for_each_matching_predicate<I, E, F, DB>(
    candidates: &RoaringBitmap,
    store: &PredicateStore<I, E::Backend>,
    event: &E,
    row: RowKind,
    vm: &mut Vm<E::Backend>,
    db: &DB,
    mut on_match: F,
) -> Result<(), DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
    F: FnMut(&Predicate<E::Backend>, &RoaringBitmap) -> Result<(), DispatchError>,
{
    for pred_id_u32 in candidates {
        let Some(pred_id) = super::ids::PredicateId::try_from_u32(pred_id_u32) else {
            continue;
        };

        let Some(pred) = store.get_predicate(pred_id) else {
            continue;
        };

        let Some(bitmap) = store.predicate_consumers.get(&pred_id) else {
            continue;
        };

        // A term narrows which of the predicate's subscribers this one row
        // reaches, so the callback gets the narrowed set rather than the whole
        // bitmap. Without the narrowing an insert would reach every subscriber
        // sharing the filter, which is the failure the staged refusal existed to
        // prevent.
        let matched = matched_for_row(pred, bitmap, store, event, row, vm, db)?;
        if let Some(reached) = matched.bitmap().filter(|set| !set.is_empty()) {
            on_match(pred, reached)?;
        }
    }
    Ok(())
}

/// The delta shape an aggregate subscription folds, plus its group columns.
/// `None` for a row projection.
fn delta_spec_and_groups(
    projection: &QueryProjection,
) -> Option<(DeltaSpec<'_>, &[crate::ColumnId])> {
    match projection {
        QueryProjection::Aggregate(spec) => Some((DeltaSpec::Projected(spec), &[])),
        QueryProjection::GroupedAggregate {
            groups,
            agg,
            having,
        } => {
            let spec = match having {
                // Registration refuses a sibling on a columnless
                // projection, so the fallback arm is unreachable and
                // keeps the unwidened delta, which stays correct for
                // the projected value.
                Some(having) if having.widens(agg) => agg
                    .column()
                    .map_or(DeltaSpec::Projected(agg), DeltaSpec::FullStats),
                _ => DeltaSpec::Projected(agg),
            };
            Some((spec, groups))
        }
        QueryProjection::Rows => None,
    }
}

/// Weighted-row pairs for aggregate delta computation.
///
/// Delta normalization per event kind:
/// * `Insert`   -> `[(+1, RowKind::New)]`
/// * `Delete`   -> `[(-1, RowKind::Old)]`
/// * `Update`   -> `[(-1, RowKind::Old), (+1, RowKind::New)]`
/// * `Truncate` -> nothing, because emptying a table is not a row change.
///   [`SubscriptionEngine::aggregate_updates`](crate::SubscriptionEngine::aggregate_updates)
///   answers it from the held totals before reaching here.
fn weighted_rows_for_agg<E: CdcEvent>(event: &E) -> Vec<(i64, RowKind)> {
    match event.kind() {
        EventKind::Insert => vec![(1, RowKind::New)],
        EventKind::Delete => vec![(-1, RowKind::Old)],
        EventKind::Update => vec![(-1, RowKind::Old), (1, RowKind::New)],
        EventKind::Truncate => Vec::new(),
    }
}

/// Net aggregate change per subscription and group.
#[derive(Debug)]
pub(crate) struct AggregateDelta<B: crate::backend::Backend> {
    pub subscription: SubscriptionId,
    pub group: Option<crate::GroupIdentity<B>>,
    pub delta: Option<AggDelta>,
    /// Source-row change, independent of NULL aggregate cells.
    pub rows: i64,
}

pub(crate) struct AggregateComputation<B: crate::backend::Backend> {
    pub deltas: Vec<AggregateDelta<B>>,
    pub missing_old: Vec<SubscriptionId>,
    pub group_key_failed: Vec<SubscriptionId>,
}

type AggregateNet<B> = HashMap<
    (SubscriptionId, Option<Vec<u8>>),
    (Option<AggDelta>, i64, Option<crate::GroupIdentity<B>>),
>;

struct AggregateDeltaState<'a, I: IdTypes, B: crate::backend::Backend> {
    store: &'a PredicateStore<I, B>,
    missing_old: &'a HashSet<SubscriptionId>,
    net: &'a mut AggregateNet<B>,
}

fn accumulate_aggregate_deltas<I, B>(
    state: &mut AggregateDeltaState<'_, I, B>,
    consumers: &RoaringBitmap,
    predicate: PredicateId,
    group: Option<&crate::GroupIdentity<B>>,
    delta: Option<AggDelta>,
    rows: i64,
) where
    I: IdTypes,
    B: crate::backend::Backend,
{
    for ordinal in consumers {
        let ordinal = ConsumerOrdinal::new(ordinal);
        let Some(subscriptions) = state.store.binding_lookup.get(&(predicate, ordinal)) else {
            continue;
        };
        for &subscription in subscriptions {
            if state.missing_old.contains(&subscription) {
                continue;
            }
            let held = state
                .net
                .entry((subscription, group.map(|identity| identity.key.clone())))
                .or_insert_with(|| (None, 0, group.cloned()));
            held.1 += rows;
            if let Some(delta) = delta {
                match &mut held.0 {
                    Some(existing) => existing.merge(&delta),
                    slot @ None => *slot = Some(delta),
                }
            }
        }
    }
}

fn subscriptions_missing_old<I, E, DB>(
    event: &E,
    candidates: &RoaringBitmap,
    store: &PredicateStore<I, E::Backend>,
    db: &DB,
) -> HashSet<SubscriptionId>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    let mut missing = HashSet::new();
    if event.kind() != EventKind::Update {
        return missing;
    }
    for pred_id_u32 in candidates {
        let Some(pred_id) = PredicateId::try_from_u32(pred_id_u32) else {
            continue;
        };
        let Some(pred) = store.get_predicate(pred_id) else {
            continue;
        };
        if pred.dependency_columns.is_empty()
            || !matches!(
                pred.projection,
                QueryProjection::Aggregate(_) | QueryProjection::GroupedAggregate { .. }
            )
            || !pred.dependency_columns.iter().any(|column| {
                event
                    .value_at(db, RowKind::Old, *column)
                    .is_ok_and(|value| value.is_missing())
            })
        {
            continue;
        }
        let Some(consumers) = store.predicate_consumers.get(&pred_id) else {
            continue;
        };
        for ord_u32 in consumers {
            let ord = ConsumerOrdinal::new(ord_u32);
            if let Some(subscriptions) = store.binding_lookup.get(&(pred_id, ord)) {
                missing.extend(subscriptions.iter().copied());
            }
        }
    }
    missing
}

fn extend_bound_subscriptions<I, B>(
    consumers: &RoaringBitmap,
    predicate: PredicateId,
    store: &PredicateStore<I, B>,
    subscriptions: &mut HashSet<SubscriptionId>,
) where
    I: IdTypes,
    B: crate::backend::Backend,
{
    for ordinal in consumers {
        let ordinal = ConsumerOrdinal::new(ordinal);
        if let Some(bound) = store.binding_lookup.get(&(predicate, ordinal)) {
            subscriptions.extend(bound.iter().copied());
        }
    }
}

enum AggregateGroup<B: crate::backend::Backend> {
    Ungrouped,
    Group(crate::GroupIdentity<B>),
    Unencodable,
}

fn aggregate_group<E, DB>(
    event: &E,
    row: RowKind,
    columns: &[crate::ColumnId],
    encoder: Option<&crate::backend::GroupKeyEncoder<E::Backend>>,
    db: &DB,
) -> Result<AggregateGroup<E::Backend>, DispatchError>
where
    E: CdcEvent,
    DB: DatabaseLike,
{
    if columns.is_empty() {
        return Ok(AggregateGroup::Ungrouped);
    }
    let values = columns
        .iter()
        .map(|column| event.value_at(db, row, *column))
        .collect::<Result<Vec<_>, _>>()?;
    let Some(key) = encoder.and_then(|encoder| encoder.encode(&values)) else {
        return Ok(AggregateGroup::Unencodable);
    };
    Ok(AggregateGroup::Group(crate::GroupIdentity { key, values }))
}

#[allow(clippy::cast_precision_loss)]
pub(crate) fn compute_agg_deltas<I, E, DB>(
    event: &E,
    partition: &TablePartition<I, E::Backend>,
    vm: &mut Vm<E::Backend>,
    arity: usize,
    db: &DB,
) -> Result<AggregateComputation<E::Backend>, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    let weighted_rows = weighted_rows_for_agg(event);
    let mut net: AggregateNet<E::Backend> = HashMap::new();
    let snapshot = partition.load_snapshot();

    // For UPDATE, use dependency-aware candidate selection; for INSERT /
    // DELETE pass empty changed_cols to get all agg candidates.
    let changed_cols: Vec<crate::ColumnId> = if event.kind() == EventKind::Update {
        event.changed_columns(db)
    } else {
        Vec::new()
    };
    let candidates = partition.select_agg_candidates(event.kind(), &changed_cols);
    let missing_old = subscriptions_missing_old(event, &candidates, &snapshot.predicates, db);
    let mut group_key_failed = HashSet::new();

    for (weight, row) in weighted_rows {
        for_each_matching_predicate(
            &candidates,
            &snapshot.predicates,
            event,
            row,
            vm,
            db,
            |pred, consumers| {
                let Some((spec, groups)) = delta_spec_and_groups(&pred.projection) else {
                    return Ok(());
                };
                let mut decode_err: Option<crate::ValueError> = None;
                let maybe_delta =
                    agg_delta_for_row(spec, weight, |col| {
                        match probe_column_for_agg(event, row, col, arity, db) {
                            Ok(read) => read,
                            Err(error) => {
                                decode_err = Some(error);
                                AggCellRead::Missing
                            }
                        }
                    });
                if let Some(error) = decode_err {
                    return Err(DispatchError::Value(error));
                }
                let group =
                    match aggregate_group(event, row, groups, pred.group_key_encoder.as_ref(), db)?
                    {
                        AggregateGroup::Ungrouped => None,
                        AggregateGroup::Group(group) => Some(group),
                        AggregateGroup::Unencodable => {
                            extend_bound_subscriptions(
                                consumers,
                                pred.id,
                                &snapshot.predicates,
                                &mut group_key_failed,
                            );
                            return Ok(());
                        }
                    };

                accumulate_aggregate_deltas(
                    &mut AggregateDeltaState {
                        store: &snapshot.predicates,
                        missing_old: &missing_old,
                        net: &mut net,
                    },
                    consumers,
                    pred.id,
                    group.as_ref(),
                    maybe_delta,
                    weight,
                );

                Ok(())
            },
        )?;
    }

    let deltas = net
        .into_iter()
        .filter_map(|((subscription, _), (delta, rows, group))| {
            (rows != 0 || delta.as_ref().is_some_and(|delta| !delta.is_zero())).then_some(
                AggregateDelta {
                    subscription,
                    group,
                    delta,
                    rows,
                },
            )
        })
        .collect();
    let mut missing_old: Vec<_> = missing_old.into_iter().collect();
    missing_old.sort_unstable();
    let mut group_key_failed: Vec<_> = group_key_failed.into_iter().collect();
    group_key_failed.sort_unstable();
    Ok(AggregateComputation {
        deltas,
        missing_old,
        group_key_failed,
    })
}

// ============================================================================
// Per-column probes (event -> ColumnProbe / AggCellRead)
// ============================================================================

/// Probe column `col` at the `row` view of `event` for the equality /
/// range / null indexes tracked by [`TablePartition::select_candidates`].
///
/// Values whose scalar payload downcasts to one of the four indexable
/// primitives (`bool` / `i64` / `f64` / `String`) become
/// [`IndexableCell`] variants. Every other scalar returns
/// [`ColumnProbe::present`] with `value: None`, causing the caller to
/// consult only the fallback index for that column.
fn probe_column_for_index<E: CdcEvent, DB: DatabaseLike>(
    event: &E,
    row: RowKind,
    col: crate::ColumnId,
    arity: usize,
    db: &DB,
) -> ColumnProbe {
    if col as usize >= arity {
        return ColumnProbe::missing();
    }
    match event.value_at(db, row, col) {
        Ok(Value::Missing) => ColumnProbe::missing(),
        Ok(Value::Null) => ColumnProbe::null(),
        Ok(v) => ColumnProbe::present(IndexableCell::from_value::<E::Backend>(&v)),
        Err(_) => ColumnProbe::undecodable(),
    }
}

/// Probe column `col` at the `row` view of `event` for aggregate delta
/// computation.
///
/// Downcasts the scalar payload to `f64` when the column carries a
/// numeric type (`Value::Int` or `Value::Float`). Every other scalar is
/// reported as [`AggCellRead::NonNumeric`] when present.
fn probe_column_for_agg<E: CdcEvent, DB: DatabaseLike>(
    event: &E,
    row: RowKind,
    col: crate::ColumnId,
    arity: usize,
    db: &DB,
) -> Result<AggCellRead, crate::ValueError> {
    use core::any::Any;
    if usize::from(col) >= arity {
        return Ok(AggCellRead::Missing);
    }
    let value = event.value_at(db, row, col)?;
    Ok(match &value {
        Value::Missing => AggCellRead::Missing,
        Value::Null => AggCellRead::Null,
        Value::Int(i) => {
            (i as &dyn Any)
                .downcast_ref::<i64>()
                .map_or(AggCellRead::NonNumeric, |i64_ref| {
                    #[allow(clippy::cast_precision_loss)]
                    AggCellRead::Numeric(*i64_ref as f64)
                })
        }
        Value::Float(f) => {
            (f as &dyn Any)
                .downcast_ref::<f64>()
                .map_or(AggCellRead::NonNumeric, |f64_ref| {
                    if f64_ref.is_finite() {
                        AggCellRead::Numeric(*f64_ref)
                    } else {
                        AggCellRead::NonNumeric
                    }
                })
        }
        _ => AggCellRead::NonNumeric,
    })
}

// Test body deferred to Phase 10 per docs/refactor-cdc-event-handoff.md.

#[cfg(test)]
mod transition_tests {
    use super::{split_transition, Matched, Slot};
    use alloc::vec::Vec;
    use roaring::RoaringBitmap;

    fn bits(ordinals: &[u32]) -> RoaringBitmap {
        ordinals.iter().copied().collect()
    }

    /// Every non-empty set the split emits, as `(slot, ordinals)`.
    fn split(new: &Matched<'_>, old: &Matched<'_>) -> Vec<(Slot, Vec<u32>)> {
        let mut out = Vec::new();
        split_transition(new, old, |slot, set| out.push((slot, set.iter().collect())));
        out
    }

    /// The four cases dispatch had before terms existed, unchanged. A predicate
    /// with no term reaches exactly these, so this is what says the set algebra
    /// did not move the old behaviour.
    #[test]
    fn a_predicate_with_no_term_splits_as_it_always_did() {
        let all = bits(&[1, 4, 9]);
        let every = Matched::Every(&all);

        assert_eq!(
            split(&every, &Matched::Nobody),
            vec![(Slot::Inserted, vec![1, 4, 9])],
            "matching now and not before is an insert for every subscriber"
        );
        assert_eq!(
            split(&Matched::Nobody, &every),
            vec![(Slot::Deleted, vec![1, 4, 9])],
            "matching before and not now is a delete for every subscriber"
        );
        assert_eq!(
            split(&every, &every),
            vec![(Slot::Updated, vec![1, 4, 9])],
            "matching both times is an update for every subscriber"
        );
        assert!(
            split(&Matched::Nobody, &Matched::Nobody).is_empty(),
            "matching neither time emits nothing at all"
        );
    }

    /// The case a term introduces: the same row is an insert for one subscriber,
    /// a delete for another and an update for a third, out of one evaluation.
    #[test]
    fn a_narrowed_split_separates_subscribers_of_one_predicate() {
        let new = Matched::Narrowed(bits(&[1, 2]));
        let old = Matched::Narrowed(bits(&[2, 3]));

        let mut got = split(&new, &old);
        got.sort_by_key(|(slot, _)| format!("{slot:?}"));

        assert_eq!(
            got,
            vec![
                (Slot::Deleted, vec![3]),
                (Slot::Inserted, vec![1]),
                (Slot::Updated, vec![2]),
            ],
            "one predicate, three different verdicts, split by subscriber"
        );
    }

    /// A narrowed side against a whole side still subtracts, which is what makes
    /// a term able to remove a subscriber the row test alone would have kept.
    #[test]
    fn narrowing_one_side_against_every_still_subtracts() {
        let all = bits(&[1, 2, 3]);
        let narrowed = Matched::Narrowed(bits(&[2]));

        assert_eq!(
            split(&narrowed, &Matched::Every(&all)),
            vec![(Slot::Deleted, vec![1, 3]), (Slot::Updated, vec![2])],
            "the subscribers the term dropped leave the view"
        );
    }

    /// An empty narrowing is not the same as nobody matching, and must not
    /// emit an empty set into an accumulator.
    #[test]
    fn an_empty_narrowing_emits_nothing() {
        let empty = Matched::Narrowed(RoaringBitmap::new());
        let all = bits(&[7]);

        assert_eq!(
            split(&empty, &Matched::Every(&all)),
            vec![(Slot::Deleted, vec![7])],
            "the row left the view for everyone, and no empty set is emitted"
        );
        assert!(
            split(&empty, &empty).is_empty(),
            "two empty narrowings emit nothing"
        );
    }
}

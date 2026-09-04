#![allow(clippy::type_complexity)]
//! Event dispatch pipeline

use super::{
    agg::{agg_delta_for_row, AggCellRead, DeltaSpec},
    aggregate::AggDelta,
    ids::{ConsumerOrdinal, PredicateId},
    partition::TablePartition,
    predicate::{Predicate, PredicateStore},
};
use crate::backend::{Backend, CdcEvent, RowKind};
use crate::term::TermLookup;
use crate::{
    compiler::{sql_shape::QueryProjection, vm::refusal::EvaluationRefusal, Tri, Vm, VmError},
    ConsumerNotifications, DispatchError, EventKind, IdTypes, SubscriptionId,
};
use alloc::vec::Vec;
use hashbrown::{HashMap, HashSet};
use roaring::RoaringBitmap;
use sql_traits::prelude::DatabaseLike;

pub(crate) mod column_probes;

use self::column_probes::{probe_column_for_agg, probe_column_for_index};

/// Map a [`VmError`] surfaced during evaluation into a [`DispatchError`],
/// preserving a structured cell-decode failure as [`DispatchError::Value`].
fn dispatch_vm_error(error: VmError) -> DispatchError {
    match error {
        VmError::Value(inner) => DispatchError::Value(inner),
        other => DispatchError::VmError(format!("{other:?}")),
    }
}

/// What a dispatch pass reports besides the notification sets: the
/// subscriptions it could not evaluate, and those whose cell the event did
/// not carry. Both are per subscription, and both are empty for almost
/// every event, so they travel together rather than as two parameters.
#[derive(Default)]
struct DispatchReports {
    refusals: Vec<(ConsumerOrdinal, SubscriptionId, EvaluationRefusal)>,
    unanswered: Vec<(ConsumerOrdinal, SubscriptionId, crate::ColumnId)>,
}

/// The four values every evaluation pass carries: which event, which row
/// version of it, the VM to run, and the catalog to read cells through.
struct EvalContext<'a, E: CdcEvent, DB: DatabaseLike> {
    event: &'a E,
    row: RowKind,
    vm: &'a mut Vm<E::Backend>,
    db: &'a DB,
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

/// One predicate's verdict for one row version.
///
/// Both halves at once, because a predicate carrying a membership term can
/// answer some of its subscribers and be refused for others: with
/// `term OR overflowing`, the assignment where the term holds short-circuits
/// and never reaches the arithmetic.
struct RowVerdict<'a> {
    /// The subscribers the row test reached.
    matched: Matched<'a>,
    /// The subscribers whose evaluation the engine refuses, and why.
    refused: Option<(RoaringBitmap, EvaluationRefusal)>,
    /// The subscribers whose evaluation read a cell the event does not
    /// carry, and which column. Not a refusal: the engine would answer
    /// this comparison, the stream simply did not carry the cell.
    unanswered: Option<(RoaringBitmap, crate::ColumnId)>,
}

impl Matched<'_> {
    /// This verdict without the subscribers whose evaluation was refused.
    ///
    /// A refused subscription gets the failure and nothing else: reporting a
    /// transition as well would tell the caller both that the row left the
    /// answer and that the answer could not be computed.
    fn without(self, refused: &RoaringBitmap) -> Self {
        if refused.is_empty() {
            return self;
        }
        match self {
            Self::Every(all) => Self::Narrowed(all - refused),
            Self::Nobody => Self::Nobody,
            Self::Narrowed(some) => Self::Narrowed(some - refused),
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
) -> Result<RowVerdict<'a>, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    if pred.bytecode.term_columns.is_empty() {
        let verdict = match vm.eval(&pred.bytecode, event, row, db) {
            Ok(verdict) => verdict,
            // No term, so the refusal is the whole predicate's.
            Err(VmError::Refused(failure)) => {
                return Ok(RowVerdict {
                    matched: Matched::Nobody,
                    refused: Some((bitmap.clone(), failure)),
                    unanswered: None,
                })
            }
            Err(other) => return Err(dispatch_vm_error(other)),
        };
        // Only an unknown verdict is unanswerable. A decisive `false`
        // read the absent cell and SQL's three-valued logic settled the
        // answer anyway, as in `body = 'x' AND tag = 'no'`, so there is
        // nothing a read would tell the caller.
        let absent = (verdict == Tri::Unknown)
            .then(|| vm.absent_column())
            .flatten()
            .map(|column| (bitmap.clone(), column));
        return Ok(RowVerdict {
            matched: if verdict == Tri::True {
                Matched::Every(bitmap)
            } else {
                Matched::Nobody
            },
            refused: None,
            unanswered: absent,
        });
    }

    let (facts, term_absent) = term_facts_for_row(pred, store, event, row, db)?;

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
    // The subscribers some assignment was refused for, and the cause. Every
    // refusal in one predicate has the same cause: the arithmetic is the
    // row's, and only whether it is reached varies.
    let mut refused = RoaringBitmap::new();
    let mut cause: Option<EvaluationRefusal> = None;
    // The same, per assignment, for a cell the event does not carry: an
    // assignment that short-circuits before reading it is answered.
    let mut unanswered = RoaringBitmap::new();
    let mut absent: Option<crate::ColumnId> = None;
    // Reused across assignments: each one starts from the predicate's own
    // subscribers, so the buffer is refilled rather than reallocated.
    let mut described = RoaringBitmap::new();
    for assignment in 0..(1u32 << varying.len()) {
        for (bit, &slot) in varying.iter().enumerate() {
            truths[slot] = if assignment & (1 << bit) == 0 {
                Tri::False
            } else {
                Tri::True
            };
        }

        // Only an assignment that matched, was refused, or could not be
        // answered needs its subscriber set computed, so this costs what it
        // did before either report existed.
        let mut missing_here = None;
        let refusal = match vm.eval_with_terms(&pred.bytecode, event, row, db, &truths) {
            Ok(Tri::True) => None,
            // Same rule per assignment: a decisive `false` is an answer.
            Ok(Tri::Unknown) => match vm.absent_column().or(term_absent) {
                Some(column) => {
                    missing_here = Some(column);
                    None
                }
                None => continue,
            },
            Ok(_) => continue,
            Err(VmError::Refused(failure)) => Some(failure),
            Err(other) => return Err(dispatch_vm_error(other)),
        };

        // The subscribers this assignment describes: in every term it says the
        // subscriber is in, out of every term it says they are not. Complemented
        // against the predicate's own subscribers, since a term set never reaches
        // outside them.
        described.clone_from(bitmap);
        for (bit, &slot) in varying.iter().enumerate() {
            let TermFacts::Admits(admits) = facts[slot] else {
                continue;
            };
            match (assignment & (1 << bit) == 0, admits) {
                // Out of a term is out of nothing when the term admits nobody,
                // and in it describes nobody at all.
                (true, Some(admits)) => described -= admits,
                (true, None) => {}
                (false, Some(admits)) => described &= admits,
                (false, None) => described.clear(),
            }
            if described.is_empty() {
                break;
            }
        }
        if let Some(failure) = refusal {
            refused |= &described;
            cause = Some(failure);
        } else if let Some(column) = missing_here {
            unanswered |= &described;
            absent = Some(column);
        } else {
            matched |= &described;
        }
    }

    Ok(RowVerdict {
        matched: Matched::Narrowed(matched),
        refused: cause
            .filter(|_| !refused.is_empty())
            .map(|failure| (refused, failure)),
        unanswered: absent
            .filter(|_| !unanswered.is_empty())
            .map(|column| (unanswered, column)),
    })
}

/// What every membership term of `pred` says about one row version, and the
/// first column the event did not carry.
///
/// A term's columns are read here rather than by the VM, so an absent one is
/// invisible to [`Vm::absent_column`] and is carried out alongside the facts.
fn term_facts_for_row<'a, I, E, DB>(
    pred: &Predicate<E::Backend>,
    store: &'a PredicateStore<I, E::Backend>,
    event: &E,
    row: RowKind,
    db: &DB,
) -> Result<(Vec<TermFacts<'a>>, Option<crate::ColumnId>), DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    let mut facts: Vec<TermFacts<'a>> = Vec::with_capacity(pred.bytecode.term_columns.len());
    let mut term_absent: Option<crate::ColumnId> = None;
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
            let missing = value.is_missing();
            match TermLookup::of(value) {
                TermLookup::Key(key) => keys.push(key),
                TermLookup::Nobody => nobody = true,
                TermLookup::Unknown => {
                    unknown = true;
                    if missing && term_absent.is_none() {
                        term_absent = Some(*column);
                    }
                }
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
    Ok((facts, term_absent))
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
    let mut reports = DispatchReports::default();
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
                (&mut stamps, &mut reports),
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
                (&mut stamps, &mut reports),
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
            db,
            &mut stamps,
            &mut reports,
        )?,
    };
    let failures = reports
        .refusals
        .into_iter()
        .filter_map(|(ordinal, subscription_id, failure)| {
            Some(crate::types::EvaluationFailure {
                subscription_id,
                consumer_id: consumer_dict.get_consumer(ordinal)?,
                refusal: failure,
            })
        })
        .collect();
    let unanswered = reports
        .unanswered
        .into_iter()
        .filter_map(|(ordinal, subscription_id, column)| {
            Some(crate::types::UnansweredCell {
                subscription_id,
                consumer_id: consumer_dict.get_consumer(ordinal)?,
                column,
            })
        })
        .collect();
    Ok((
        notifs
            .with_checkpoint(checkpoint)
            .with_evaluation_failures(failures)
            .with_unanswered(unanswered),
        stamps,
    ))
}

/// Record one unanswerable predicate against every subscription bound to
/// it, for the same reason refusals are recorded per subscription.
fn collect_unanswered_for_predicate<I: IdTypes, B: Backend>(
    predicates: &PredicateStore<I, B>,
    pred_id: PredicateId,
    consumers: &RoaringBitmap,
    column: crate::ColumnId,
    out: &mut Vec<(ConsumerOrdinal, SubscriptionId, crate::ColumnId)>,
) {
    for ord_u32 in consumers {
        let ord = ConsumerOrdinal::new(ord_u32);
        if let Some(sub_ids) = predicates.binding_lookup.get(&(pred_id, ord)) {
            out.extend(sub_ids.iter().map(|sub_id| (ord, *sub_id, column)));
        }
    }
}

/// Record one refused predicate against every subscription bound to it.
///
/// The failure is per subscription, not per consumer: one consumer can hold
/// several subscriptions, and only the ones whose predicate was refused
/// failed.
fn collect_refusals_for_predicate<I: IdTypes, B: Backend>(
    predicates: &PredicateStore<I, B>,
    pred_id: PredicateId,
    consumers: &RoaringBitmap,
    failure: EvaluationRefusal,
    out: &mut Vec<(ConsumerOrdinal, SubscriptionId, EvaluationRefusal)>,
) {
    for ord_u32 in consumers {
        let ord = ConsumerOrdinal::new(ord_u32);
        if let Some(sub_ids) = predicates.binding_lookup.get(&(pred_id, ord)) {
            out.extend(sub_ids.iter().map(|sub_id| (ord, *sub_id, failure)));
        }
    }
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
/// verdicts still collapse to "did not match" here, which is
/// indistinguishable from a genuine non-match.
/// [`CdcEvent::presence_at`](crate::backend::CdcEvent::presence_at) is what
/// tells the two apart; no caller acts on it yet.
fn dispatch_update_with_stamps<I, E, DB>(
    event: &E,
    partition: &TablePartition<I, E::Backend>,
    consumer_dict: &ConsumerDictionary<I>,
    vm: &mut Vm<E::Backend>,
    db: &DB,
    stamps: &mut Vec<SubscriptionId>,
    reports: &mut DispatchReports,
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

        // A row version refused for some subscribers is reported for those,
        // and the transition is computed for the rest. A subscription refused
        // on either version is refused for the update: an answer needs both
        // versions, so it gets the failure and no transition.
        let mut refused_here = RoaringBitmap::new();
        for (consumers, failure) in [new_matched.refused, old_matched.refused]
            .into_iter()
            .flatten()
        {
            collect_refusals_for_predicate(
                &snapshot.predicates,
                pred_id,
                &consumers,
                failure,
                &mut reports.refusals,
            );
            refused_here |= consumers;
        }

        // The same rule for a cell the event did not carry, which is the
        // case that produces one in production: an unchanged TOASTed column
        // is omitted from an update's message whatever the replica identity
        // says.
        //
        // The new version only. An absent cell in the old image is the
        // replica-identity story, which `REPLICA_IDENTITY_AUDIT_SQL` covers
        // and the transition rules already account for: under the default
        // identity the old image is the key alone, so treating that as
        // unanswerable would report every update on such a table.
        if let Some((consumers, column)) = new_matched.unanswered {
            collect_unanswered_for_predicate(
                &snapshot.predicates,
                pred_id,
                &consumers,
                column,
                &mut reports.unanswered,
            );
            refused_here |= consumers;
        }

        let new_served = new_matched.matched.without(&refused_here);
        let old_served = old_matched.matched.without(&refused_here);
        split_transition(&new_served, &old_served, |slot, set| {
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
    accumulators: (&mut Vec<SubscriptionId>, &mut DispatchReports),
) -> Result<RoaringBitmap, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    let (stamps, reports) = accumulators;
    let candidates = partition.select_candidates(arity, |col| {
        probe_column_for_index(event, row, col, arity, db)
    });
    let snapshot = partition.load_snapshot();
    let mut matching_ordinals = RoaringBitmap::new();

    for_each_matching_predicate(
        &candidates,
        &snapshot.predicates,
        &mut EvalContext { event, row, vm, db },
        reports,
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
    context: &mut EvalContext<'_, E, DB>,
    reports: &mut DispatchReports,
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
        let verdict = matched_for_row(
            pred,
            bitmap,
            store,
            context.event,
            context.row,
            context.vm,
            context.db,
        )?;
        if let Some((consumers, failure)) = verdict.refused {
            collect_refusals_for_predicate(
                store,
                pred_id,
                &consumers,
                failure,
                &mut reports.refusals,
            );
        }
        if let Some((consumers, column)) = verdict.unanswered {
            collect_unanswered_for_predicate(
                store,
                pred_id,
                &consumers,
                column,
                &mut reports.unanswered,
            );
        }
        if let Some(reached) = verdict.matched.bitmap().filter(|set| !set.is_empty()) {
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

/// The refused subscriptions of one event, with their deltas removed.
///
/// A refused subscription contributes no delta, even when one row version
/// was served: an update's fold needs both versions, and folding half of it
/// would move the total by a row the filter never judged. The caller
/// applies the stop after folding, so the removal happens here.
fn refused_subscriptions<B: crate::backend::Backend>(
    refusals: Vec<(ConsumerOrdinal, SubscriptionId, EvaluationRefusal)>,
    deltas: &mut Vec<AggregateDelta<B>>,
) -> Vec<(SubscriptionId, EvaluationRefusal)> {
    let mut refused: Vec<(SubscriptionId, EvaluationRefusal)> = refusals
        .into_iter()
        .map(|(_, subscription, failure)| (subscription, failure))
        .collect();
    refused.sort_unstable_by_key(|(subscription, _)| *subscription);
    refused.dedup_by_key(|(subscription, _)| *subscription);
    if !refused.is_empty() {
        deltas.retain(|delta| {
            refused
                .binary_search_by_key(&delta.subscription, |(subscription, _)| *subscription)
                .is_err()
        });
    }
    refused
}

pub(crate) struct AggregateComputation<B: crate::backend::Backend> {
    pub deltas: Vec<AggregateDelta<B>>,
    pub missing_old: Vec<SubscriptionId>,
    pub group_key_failed: Vec<SubscriptionId>,
    /// Subscriptions whose filter the engine refuses to evaluate for this
    /// event. An aggregate emits maintenance rather than rows, so these
    /// surface as maintenance stops rather than in a notification set.
    pub evaluation_refused: Vec<(SubscriptionId, EvaluationRefusal)>,
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

    let kind = event.kind();
    let candidates = if kind == EventKind::Update {
        event.with_changed_columns(db, |changed| partition.select_agg_candidates(kind, changed))
    } else {
        partition.select_agg_candidates(kind, &[])
    };
    let missing_old = subscriptions_missing_old(event, &candidates, &snapshot.predicates, db);
    let mut group_key_failed = HashSet::new();
    let mut reports = DispatchReports::default();

    for (weight, row) in weighted_rows {
        for_each_matching_predicate(
            &candidates,
            &snapshot.predicates,
            &mut EvalContext { event, row, vm, db },
            &mut reports,
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

    let mut deltas: Vec<AggregateDelta<E::Backend>> = net
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
    let evaluation_refused = refused_subscriptions(reports.refusals, &mut deltas);

    Ok(AggregateComputation {
        deltas,
        missing_old,
        group_key_failed,
        evaluation_refused,
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

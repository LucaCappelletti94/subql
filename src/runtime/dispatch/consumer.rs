use super::{
    dispatch_vm_error, probe_column_for_index, projected_change, term_fact_for_row, DispatchError,
    EvalContext, EventKind, IdTypes, Predicate, PredicateStore, ProjectedChange, RowKind, Slot,
    TermFacts, Tri, Vm, VmError,
};
use crate::backend::{CdcEvent, CellPresence};
use crate::runtime::{ids::ConsumerOrdinal, partition::TablePartition};
use crate::types::{ConsumerMatch, EvaluationFailure, UnansweredCell};
use sql_traits::prelude::DatabaseLike;

struct Verdict {
    matched: bool,
    refused: Option<crate::compiler::vm::refusal::EvaluationRefusal>,
    unanswered: Option<crate::ColumnId>,
}

fn evaluate<I, E, DB>(
    pred: &Predicate<E::Backend>,
    store: &PredicateStore<I, E::Backend>,
    ordinal: ConsumerOrdinal,
    context: &mut EvalContext<'_, E, DB>,
) -> Result<Verdict, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    let mut truths = [Tri::Unknown; crate::compiler::MAX_TERMS_PER_FILTER];
    let mut term_absent = None;
    let count = pred.bytecode.term_columns.len();
    let truths = truths
        .get_mut(..count)
        .ok_or_else(|| DispatchError::VmError("membership term limit exceeded".into()))?;
    for (slot, (truth, columns)) in truths
        .iter_mut()
        .zip(pred.bytecode.term_columns.iter())
        .enumerate()
    {
        let (fact, absent) = term_fact_for_row(
            (pred.id, slot, columns),
            store,
            context.event,
            context.row,
            context.db,
        )?;
        term_absent = term_absent.or(absent);
        *truth = match fact {
            TermFacts::Admits(Some(admitted)) if admitted.contains(ordinal.get()) => Tri::True,
            TermFacts::Admits(_) => Tri::False,
            TermFacts::CannotSay => Tri::Unknown,
        };
    }
    let result = context.vm.eval_with_terms(
        &pred.bytecode,
        context.event,
        context.row,
        context.db,
        truths,
        term_absent,
    );
    match result {
        Ok(truth) => Ok(Verdict {
            matched: truth == Tri::True,
            refused: None,
            unanswered: context.vm.absent_column(),
        }),
        Err(VmError::Refused(refused)) => Ok(Verdict {
            matched: false,
            refused: Some(refused),
            unanswered: None,
        }),
        Err(error) => Err(dispatch_vm_error(error)),
    }
}

fn candidate<E, DB>(
    pred: &Predicate<E::Backend>,
    context: &EvalContext<'_, E, DB>,
    arity: &mut Option<usize>,
) -> Result<bool, DispatchError>
where
    E: CdcEvent,
    DB: DatabaseLike,
{
    use crate::runtime::indexes::IndexableAtom;
    let mut probe =
        |column| {
            let width = if let Some(width) = *arity {
                width
            } else {
                let width = crate::catalog_helpers::table_arity(
                    context.db,
                    context.event.table_id(context.db),
                )?;
                *arity = Some(width);
                width
            };
            Ok::<_, DispatchError>((usize::from(column) < width).then(|| {
                probe_column_for_index(context.event, context.row, column, width, context.db)
            }))
        };
    for atom in pred.index_atoms.iter() {
        let column = match atom {
            IndexableAtom::Equality { column_id, .. }
            | IndexableAtom::Range { column_id, .. }
            | IndexableAtom::Null { column_id, .. } => *column_id,
            IndexableAtom::Fallback => return Ok(true),
        };
        if probe(column)?
            .as_ref()
            .is_some_and(|probe| atom.admits(probe))
        {
            return Ok(true);
        }
    }
    for &column in pred.dependency_columns.iter() {
        if probe(column)?.is_some_and(|probe| {
            matches!(
                probe.presence,
                CellPresence::Missing | CellPresence::Undecodable
            )
        }) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn report<I, E>(
    store: &PredicateStore<I, E::Backend>,
    pred: &Predicate<E::Backend>,
    ordinal: ConsumerOrdinal,
    consumer: I::ConsumerId,
    verdict: &Verdict,
    out: &mut ConsumerMatch<I, E::Checkpoint>,
) where
    I: IdTypes,
    E: CdcEvent,
{
    if let Some(refusal) = verdict.refused {
        out.evaluation_failures
            .extend(
                store
                    .subscription_ids_of(pred.id, ordinal)
                    .map(|subscription_id| EvaluationFailure {
                        subscription_id,
                        consumer_id: consumer,
                        refusal,
                    }),
            );
    }
    if let Some(column) = verdict.unanswered {
        report_unanswered::<I, E>(store, pred, ordinal, consumer, column, out);
    }
}

fn report_unanswered<I, E>(
    store: &PredicateStore<I, E::Backend>,
    pred: &Predicate<E::Backend>,
    ordinal: ConsumerOrdinal,
    consumer: I::ConsumerId,
    column: crate::ColumnId,
    out: &mut ConsumerMatch<I, E::Checkpoint>,
) where
    I: IdTypes,
    E: CdcEvent,
{
    out.unanswered.extend(
        store
            .subscription_ids_of(pred.id, ordinal)
            .map(|subscription_id| UnansweredCell {
                subscription_id,
                consumer_id: consumer,
                column,
            }),
    );
}

fn update<I, E, DB>(
    pred: &Predicate<E::Backend>,
    store: &PredicateStore<I, E::Backend>,
    ordinal: ConsumerOrdinal,
    consumer: I::ConsumerId,
    context: &mut EvalContext<'_, E, DB>,
    out: &mut ConsumerMatch<I, E::Checkpoint>,
) -> Result<Option<Slot>, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    context.row = RowKind::New;
    let new = evaluate(pred, store, ordinal, context)?;
    context.row = RowKind::Old;
    let old = evaluate(pred, store, ordinal, context)?;
    report::<I, E>(store, pred, ordinal, consumer, &new, out);
    let old_report = Verdict {
        matched: old.matched,
        refused: old.refused,
        unanswered: old
            .unanswered
            .filter(|_| new.refused.is_none() && old.refused.is_none() && new.unanswered.is_none()),
    };
    report::<I, E>(store, pred, ordinal, consumer, &old_report, out);
    let subset = projected_change(&pred.projection, context.event, context.db)?;
    if new.refused.is_some()
        || old.refused.is_some()
        || new.unanswered.is_some()
        || old.unanswered.is_some()
    {
        return Ok(None);
    }
    let slot = match (new.matched, old.matched) {
        (true, false) => Some(Slot::Inserted),
        (false, true) => Some(Slot::Deleted),
        (true, true) => Some(Slot::Updated),
        (false, false) => None,
    };
    Ok(match (slot, subset) {
        (Some(Slot::Deleted | Slot::Updated), Some(ProjectedChange::OldLacks(column))) => {
            report_unanswered::<I, E>(store, pred, ordinal, consumer, column, out);
            None
        }
        (Some(Slot::Updated), Some(ProjectedChange::Unchanged)) => None,
        _ => slot,
    })
}

pub fn match_consumer<I, E, DB>(
    event: &E,
    partition: &TablePartition<I, E::Backend>,
    ordinal: ConsumerOrdinal,
    consumer: I::ConsumerId,
    vm: &mut Vm<E::Backend>,
    db: &DB,
) -> Result<ConsumerMatch<I, E::Checkpoint>, DispatchError>
where
    I: IdTypes,
    E: CdcEvent,
    DB: DatabaseLike,
{
    let snapshot = partition.load_snapshot();
    let store = &snapshot.predicates;
    let mut out = ConsumerMatch::empty().with_checkpoint(event.checkpoint());
    let mut arity = None;
    let mut context = EvalContext {
        event,
        row: RowKind::New,
        vm,
        db,
    };
    for id in store.predicate_ids_for(ordinal) {
        let Some(pred) = store.get_predicate(id) else {
            continue;
        };
        if !pred.projection.delivers_rows() {
            continue;
        }
        let slot = match event.kind() {
            EventKind::Truncate => Some(Slot::Deleted),
            EventKind::Insert | EventKind::Delete => {
                context.row = if event.kind() == EventKind::Insert {
                    RowKind::New
                } else {
                    RowKind::Old
                };
                if !candidate(pred, &context, &mut arity)? {
                    continue;
                }
                let verdict = evaluate(pred, store, ordinal, &mut context)?;
                report::<I, E>(store, pred, ordinal, consumer, &verdict, &mut out);
                if !verdict.matched {
                    continue;
                }
                if context.row == RowKind::Old {
                    if let Some(ProjectedChange::OldLacks(column)) =
                        projected_change(&pred.projection, event, db)?
                    {
                        report_unanswered::<I, E>(store, pred, ordinal, consumer, column, &mut out);
                        continue;
                    }
                    Some(Slot::Deleted)
                } else {
                    Some(Slot::Inserted)
                }
            }
            EventKind::Update => update(pred, store, ordinal, consumer, &mut context, &mut out)?,
        };
        let reached = match slot {
            Some(Slot::Inserted) => &mut out.inserted,
            Some(Slot::Deleted) => &mut out.deleted,
            Some(Slot::Updated) => &mut out.updated,
            None => continue,
        };
        *reached = true;
    }
    Ok(out)
}

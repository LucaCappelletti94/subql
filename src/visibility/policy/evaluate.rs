use alloc::vec::Vec;

use rls2fga_types::RecordDescription;
use rls2fga_types::{ActionAnswer, ActionJudgement, ActionStatement, RowVersion};
use rls2fga_types::{RequestComparison, RowDecision};
use sql_traits::prelude::DatabaseLike;

use crate::visibility::records::records_from_row_view;
use crate::visibility::{RowView, RowWrite, Verdict, VisibilityPolicy};
use crate::TableId;

use super::request_values::RequestValues;
use super::row_policy::RowPolicy;
use super::subject::Subject;
use super::write_statement::{image_of, statement_of, table_of};

/// What the changed row settles about one watcher.
///
/// Three answers rather than two, because "the row does not say" is not a
/// denial. Collapsing it into one would either delegate a question the row
/// answered, which costs a round trip, or answer one it did not, which is a
/// wrong verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Local {
    /// An arm the row settles grants this watcher.
    Allow,
    /// Every arm was read and none grants this watcher.
    Deny,
    /// Something the row does not settle, so the service has to be asked.
    Unresolved,
}

/// What the model says answering one statement on one table takes.
///
/// Three states rather than a list of judgements, because two of them are not
/// lists: a table the database restricts nothing on has nothing to satisfy and
/// grants, and one the model refuses grants nobody. An empty list cannot spell
/// both, and reading it as either is a wrong verdict half the time.
#[derive(Clone, Copy, Debug)]
enum Requirement<'a> {
    /// Nothing has to be satisfied, so every watcher is granted.
    Granted,
    /// Nobody is granted, whatever the row says.
    Refused,
    /// Every judgement has to grant, read against the version it names. Never
    /// empty.
    Judged(&'a [ActionJudgement]),
}

/// Evaluate `decision` against `row` for one watcher.
///
/// Per watcher rather than per row, which a request-gated arm forces: its
/// answer is not a set of names but a comparison against each watcher's own
/// values, so there is nothing to return once for everybody.
fn evaluate<R, DB, S>(
    decision: &RowDecision,
    row: &R,
    db: &DB,
    watcher: &S,
    values: &mut RequestValues,
) -> Local
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
    S: Subject + ?Sized,
{
    match decision {
        RowDecision::Leaf { shapes, .. } => {
            let mut granted = false;
            for shape in shapes {
                let Ok(records) = records_from_row_view::<R, DB>(shape, row, db) else {
                    return Local::Unresolved;
                };
                granted |= records.iter().any(|record| {
                    watcher
                        .subjects()
                        .any(|name| name.as_ref() == record.subject)
                });
            }
            if granted {
                Local::Allow
            } else {
                Local::Deny
            }
        }
        RowDecision::RequestGated {
            shapes,
            context_key,
            request_parameter,
            comparison,
            ..
        } => request_gated::<R, DB, S>(
            shapes,
            context_key,
            request_parameter,
            *comparison,
            row,
            db,
            watcher,
            values,
        ),
        RowDecision::Any(children) => {
            let mut unresolved = false;
            for child in children {
                match evaluate(child, row, db, watcher, values) {
                    // One arm granting settles a union, whatever the others
                    // say. This is what keeps one unreadable arm from
                    // disabling the whole table.
                    Local::Allow => return Local::Allow,
                    Local::Unresolved => unresolved = true,
                    Local::Deny => {}
                }
            }
            if unresolved {
                Local::Unresolved
            } else {
                Local::Deny
            }
        }
        RowDecision::All(children) => {
            let mut denied = false;
            let mut unresolved = false;
            for child in children {
                match evaluate(child, row, db, watcher, values) {
                    Local::Deny => denied = true,
                    Local::Unresolved => unresolved = true,
                    Local::Allow => {}
                }
            }
            // A denying arm beside an unreadable one is deliberately not a
            // local refusal. It would be correct, and a subtly wrong exclusion
            // is a silent wrong refusal, so it waits for a restrictive policy
            // to exist and be tested against.
            if unresolved {
                Local::Unresolved
            } else if denied {
                Local::Deny
            } else {
                Local::Allow
            }
        }
        // `RowDecision` is `#[non_exhaustive]`: a composition this does not
        // understand is delegated rather than guessed at. Such a recipe is not
        // indexed either, so this is defence in depth rather than the only
        // guard.
        _ => Local::Unresolved,
    }
}

/// Whether the caller's own value completes the comparison this row's side
/// half-settles.
///
/// The records here grant `user:*`, so taking their subject at face value
/// grants everyone. The row's side is in the record's condition context, under
/// `context_key`, and only the caller's value decides it.
#[allow(clippy::too_many_arguments)]
fn request_gated<R, DB, S>(
    shapes: &[RecordDescription],
    context_key: &str,
    request_parameter: &str,
    comparison: RequestComparison,
    row: &R,
    db: &DB,
    watcher: &S,
    values: &mut RequestValues,
) -> Local
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
    S: Subject + ?Sized,
{
    values.reset();
    if !watcher.request_value(request_parameter, values) {
        // The watcher cannot say what it sent, so nothing completes the
        // comparison here. It loses speed and never correctness.
        return Local::Unresolved;
    }

    let mut granted = false;
    for shape in shapes {
        let Ok(records) = records_from_row_view::<R, DB>(shape, row, db) else {
            return Local::Unresolved;
        };
        for record in records {
            let Some(context) = record.context.as_ref() else {
                // A record with no context cannot be the row's side of a
                // comparison, and its subject is the wildcard, so reading it
                // as a grant would grant everyone.
                return Local::Unresolved;
            };
            // A parameter this comparison does not read is a condition half
            // nothing here evaluates, the clock being the live case, so a
            // context carrying more than the one compared key delegates.
            if context.values.len() != 1 {
                return Local::Unresolved;
            }
            let Some(value) = context.values.get(context_key) else {
                return Local::Unresolved;
            };
            granted |= match comparison {
                RequestComparison::CallerSetHolds => values.holds(value),
                // One value, not one of several: a watcher that sent a set
                // where the policy compares a single value has not satisfied
                // it, and reading any element as a match is a wrong allow.
                RequestComparison::CallerValueEquals => values.len() == 1 && values.holds(value),
                // `RequestComparison` is `#[non_exhaustive]`: a comparison this
                // does not know cannot be applied, and its records grant
                // everyone until one is.
                _ => return Local::Unresolved,
            };
        }
    }

    if granted {
        Local::Allow
    } else {
        Local::Deny
    }
}

impl<DB, P> VisibilityPolicy for RowPolicy<DB, P>
where
    DB: DatabaseLike + Send + Sync,
    P: VisibilityPolicy,
    P::Watcher: Subject + Clone,
{
    type Watcher = P::Watcher;
    type Error = P::Error;
    type Backend = P::Backend;

    fn may_see<R>(
        &self,
        row: &R,
        watchers: &[Self::Watcher],
        verdicts: &mut [Verdict],
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send
    where
        R: RowView<Backend = Self::Backend> + Sync + ?Sized,
    {
        // Reading the row happens here rather than in the block, so an event
        // answered for every watcher never suspends.
        let mut leftover: Vec<Self::Watcher> = Vec::new();
        let mut places: Vec<usize> = Vec::new();
        match self.requirement(row.table_id(), ActionStatement::Select) {
            None => {
                leftover.extend_from_slice(watchers);
                places.extend(0..watchers.len());
            }
            Some(required) => {
                let mut values = RequestValues::new();
                for (place, (watcher, verdict)) in
                    watchers.iter().zip(verdicts.iter_mut()).enumerate()
                {
                    // A read judges the row as it is, so every judgement reads
                    // the one image a read has.
                    match self.judge(required, |_| Some(row), watcher, &mut values) {
                        Local::Allow => *verdict = Verdict::Allow,
                        // The caller pre-filled a denial, so a local refusal is
                        // already written.
                        Local::Deny => {}
                        Local::Unresolved => {
                            leftover.push(watcher.clone());
                            places.push(place);
                        }
                    }
                }
            }
        }

        async move {
            if leftover.is_empty() {
                return Ok(());
            }
            // Only the watchers the row left unresolved are asked about, and
            // their answers land back where they came from.
            let mut answers = Vec::new();
            Verdict::reset(&mut answers, leftover.len());
            self.inner().may_see(row, &leftover, &mut answers).await?;
            for (place, answer) in places.into_iter().zip(answers) {
                if let Some(verdict) = verdicts.get_mut(place) {
                    *verdict = answer;
                }
            }
            Ok(())
        }
    }

    fn may_write<R>(
        &self,
        write: RowWrite<'_, R>,
        watcher: &Self::Watcher,
    ) -> impl core::future::Future<Output = Result<Verdict, Self::Error>> + Send
    where
        R: RowView<Backend = Self::Backend> + Sync + ?Sized,
    {
        let answered = self.settles(write, watcher);
        async move {
            match answered {
                Some(verdict) => Ok(verdict),
                None => self.inner().may_write(write, watcher).await,
            }
        }
    }
}

impl<DB, P> RowPolicy<DB, P>
where
    DB: DatabaseLike + Send + Sync,
    P: VisibilityPolicy,
    P::Watcher: Subject,
{
    /// The verdict the changed row settles for `write`, or [`None`] to delegate.
    ///
    /// Reading the row happens here rather than inside the returned future, so
    /// a locally answered write never suspends.
    ///
    /// Which relations answer the statement, and which version each judges, is
    /// the model's to say rather than this one's. A policy giving one condition
    /// is answered by one relation against both versions, because Postgres
    /// applies a lone `USING` clause to the result as well, and a policy giving
    /// both clauses is answered by one relation per version. Choosing here
    /// instead names relations the model does not always define.
    fn settles<R>(&self, write: RowWrite<'_, R>, watcher: &P::Watcher) -> Option<Verdict>
    where
        R: RowView<Backend = P::Backend> + ?Sized,
    {
        let required = self.requirement(table_of(&write), statement_of(&write))?;
        let mut values = RequestValues::new();
        let local = self.judge(
            required,
            |version| image_of(&write, version),
            watcher,
            &mut values,
        );
        match local {
            Local::Allow => Some(Verdict::Allow),
            Local::Deny => Some(Verdict::Deny),
            Local::Unresolved => None,
        }
    }

    /// What answering `statement` on rows of `table` takes, or [`None`] when
    /// nothing here can answer it.
    ///
    /// The wildcard covers a relation that fuses both versions, which no single
    /// image answers, and every answer a later revision adds. Neither is reached
    /// by the four statements a write asks about today, since the fused one is
    /// reported for an update naming no rows and nothing here asks that. It is
    /// the guard for the next variant rather than a live branch.
    fn requirement(&self, table: TableId, statement: ActionStatement) -> Option<Requirement<'_>> {
        Some(match self.shapes().answer(table, statement)? {
            ActionAnswer::Unrestricted => Requirement::Granted,
            ActionAnswer::Denied => Requirement::Refused,
            ActionAnswer::Judged(judges) if !judges.is_empty() => Requirement::Judged(judges),
            _ => return None,
        })
    }

    /// Read `required` against the row versions `image` hands out.
    ///
    /// The two states that are not lists answer without reading anything: one
    /// grants because there is no rule, the other refuses because no rule can
    /// ever grant. A refusal on any judgement is definite whatever the others
    /// say, so it answers rather than delegating, and an unreadable one is not
    /// an answer.
    fn judge<'a, R, F>(
        &self,
        required: Requirement<'_>,
        image: F,
        watcher: &P::Watcher,
        values: &mut RequestValues,
    ) -> Local
    where
        R: RowView + ?Sized + 'a,
        F: Fn(RowVersion) -> Option<&'a R>,
    {
        let judges = match required {
            Requirement::Granted => return Local::Allow,
            Requirement::Refused => return Local::Deny,
            Requirement::Judged(judges) => judges,
        };
        for judge in judges {
            let Some(row) = image(judge.version) else {
                return Local::Unresolved;
            };
            let Some(decision) = self.shapes().recipe(row.table_id(), &judge.relation) else {
                return Local::Unresolved;
            };
            match evaluate::<R, _, _>(decision, row, self.shapes().catalog(), watcher, values) {
                Local::Deny => return Local::Deny,
                Local::Unresolved => return Local::Unresolved,
                Local::Allow => {}
            }
        }
        Local::Allow
    }
}

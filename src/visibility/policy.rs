//! A [`VisibilityPolicy`] that answers from the changed row where the
//! schema allows, and delegates the rest.
//!
//! [`rls2fga`] reports, per relation of the model it emits, whether one row
//! decides who the relation grants to and how those subjects compose. Where
//! it does, this answers with no round trip: evaluate the recipe against the
//! row image and test the resulting subjects against the watcher list.
//! Where it does not, the question goes to another [`VisibilityPolicy`] the
//! caller supplies.
//!
//! # The delegate is the seam, and it is the one already here
//!
//! subql names no authorization protocol. Everything it cannot answer is
//! handed to an inner [`VisibilityPolicy`], which a separate crate
//! implements against whatever service the caller runs.
//!
//! That reuse is what makes the cost measurable in the only place it counts.
//! [`VisibilityPolicy::may_see`] takes every watcher at once, so a delegated
//! event is one call to the backend however large the audience, and a
//! counter on the delegate reads the round trips rather than the questions.
//! A counter on [`RowPolicy`] itself would read one per event from the day
//! it landed and prove nothing.
//!
//! # Refusing is delegating, never answering empty
//!
//! A recipe subql cannot evaluate is not indexed at all, so its rows are
//! delegated rather than answered from an empty subject set, which would
//! read as "this row grants nobody". [`RowPolicy::new`] settles that once
//! per relation instead of per row. What it cannot settle in advance is a
//! cell that fails to decode, so that falls back to the delegate too.

use alloc::borrow::Cow;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use rls2fga::generator::action_relations::{
    ActionAnswer, ActionJudgement, ActionStatement, RowVersion,
};
use rls2fga::generator::records::RecordDescription;
use rls2fga::generator::relations::{RequestComparison, RowDecision};
use sql_traits::prelude::DatabaseLike;

use crate::visibility::records::records_from_row_view;
use crate::visibility::shapes::SharedShapes;
use crate::visibility::{RowView, RowWrite, Verdict, VisibilityPolicy};
use crate::TableId;

// ---------------------------------------------------------------------------
// Subject
// ---------------------------------------------------------------------------
/// The principals a watcher authenticates as, and the values it sent.
///
/// Two methods because subql needs two different things of a watcher and they
/// are not the same thing spelled twice. A record's subject side is a
/// `type:key` name, so answering from the row is a set-membership test over
/// [`subjects`](Self::subjects). A request-gated grant is not a stored fact at
/// all: the row settles one side of a comparison the caller's own value
/// completes, and that value is bare and typed by the policy rather than by
/// the model. Feeding one where the other belongs is a wrong comparison, and a
/// bare key colliding with a name would be a wrong allow.
///
/// [`VisibilityPolicy::Watcher`] stays opaque to subql everywhere else.
///
/// # Why names are [`Cow`] and values are written rather than returned
///
/// A consumer whose identity is a typed id has no rendered name to lend, and
/// `&str` would force it to keep one on its own type, which is a design
/// decision subql has no business making for it. [`Cow`] leaves the choice
/// where it belongs.
///
/// Values go the other way, into a buffer subql owns, because they are asked
/// for once per watcher per changed row on the path that exists to cost
/// nothing. Returning them owned would allocate per watcher per event.
pub trait Subject {
    /// Every `type:key` this watcher is known by, exactly as a record spells
    /// it (`user:alice`).
    ///
    /// A principal carrying an identity and further subjects yields all of
    /// them: naming only the first would deny a holder the model grants.
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>>;

    /// Write what this watcher sent under `parameter` into `out`, bare and
    /// untyped, and report whether it could answer at all.
    ///
    /// `false` means "cannot answer", which delegates, so a watcher that
    /// omits a parameter loses speed and never correctness. Answering with no
    /// values is a different thing and is an answer: a caller holding no keys
    /// is granted by no key.
    ///
    /// `out` arrives empty. The default cannot answer, so a watcher with no
    /// request values at all implements nothing.
    fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
        let _ = (parameter, out);
        false
    }
}

impl Subject for str {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, Self>> {
        core::iter::once(Cow::Borrowed(self))
    }
}

impl Subject for String {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
        core::iter::once(Cow::Borrowed(self.as_str()))
    }
}

impl<T: Subject + ?Sized> Subject for &T {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
        (*self).subjects()
    }

    fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
        (*self).request_value(parameter, out)
    }
}

/// A consumer whose watcher is a shared handle over its own type cannot
/// write this itself: [`Arc`] is not `#[fundamental]`, so `Arc<Local>` is
/// not a local type and the orphan rule refuses a foreign trait on it. It
/// lives here so that consumer implements [`Subject`] on its own type and
/// needs no newtype.
impl<T: Subject + ?Sized> Subject for Arc<T> {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
        (**self).subjects()
    }

    fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
        (**self).request_value(parameter, out)
    }
}

// ---------------------------------------------------------------------------
// RequestValues
// ---------------------------------------------------------------------------

/// What one watcher sent under one parameter, in a buffer subql reuses.
///
/// Reused across every watcher of one changed row, so it is filled and read
/// once per watcher and allocates only while it grows. Each slot keeps its
/// capacity through [`reset`](Self::reset), so a watcher writing a key the
/// same length as the last one allocates nothing.
///
/// The values are bare, as the caller sent them. A held key reaching Postgres
/// inside `app.subjects` is bare there too, and that is the spelling the
/// comparison was translated against.
#[derive(Clone, Debug, Default)]
pub struct RequestValues {
    /// Slots, of which the first `len` are live. Retained past `len` so a
    /// later fill reuses the allocation rather than making a new one.
    slots: Vec<String>,
    len: usize,
}

impl RequestValues {
    /// An empty buffer.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            slots: Vec::new(),
            len: 0,
        }
    }

    /// Forget every value, keeping the allocations to fill again.
    pub const fn reset(&mut self) {
        self.len = 0;
    }

    /// Add one value the watcher sent.
    pub fn push(&mut self, value: &str) {
        match self.slots.get_mut(self.len) {
            Some(slot) => {
                slot.clear();
                slot.push_str(value);
            }
            None => self.slots.push(String::from(value)),
        }
        self.len += 1;
    }

    /// Whether the watcher sent `value`.
    #[must_use]
    pub fn holds(&self, value: &str) -> bool {
        self.values().any(|held| held == value)
    }

    /// Every value the watcher sent, in the order it wrote them.
    pub fn values(&self) -> impl Iterator<Item = &str> {
        self.slots[..self.len].iter().map(String::as_str)
    }

    /// How many values the watcher sent.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether the watcher sent none, which is an answer rather than a
    /// refusal to answer.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Slots the buffer is holding on to, which is what a reuse test reads.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.slots.len()
    }
}

// ---------------------------------------------------------------------------
// Which statement a write asks about
// ---------------------------------------------------------------------------

/// The statement `write` is, so the model can be asked what answers it.
///
/// A replacement judged on the row as it stands is a locking read, which is what
/// Postgres filters by the update rule's `USING` clause as well, so that is the
/// statement it asks about rather than a replacement it cannot complete.
pub(crate) const fn statement_of<R: ?Sized>(write: &RowWrite<'_, R>) -> ActionStatement {
    match write {
        RowWrite::Insert { .. } => ActionStatement::Insert,
        RowWrite::Update { .. } => ActionStatement::Update,
        RowWrite::UpdateUsing { .. } => ActionStatement::SelectForUpdate,
        RowWrite::Delete { .. } => ActionStatement::Delete,
    }
}

/// The row image `version` names, or [`None`] when this write carries none.
///
/// A creation has no existing row and a removal no resulting one, so a judgement
/// naming a version the write does not carry cannot be answered here.
pub(crate) const fn image_of<'a, R: ?Sized>(
    write: &RowWrite<'a, R>,
    version: RowVersion,
) -> Option<&'a R> {
    match (write, version) {
        (RowWrite::Insert { new } | RowWrite::Update { new, .. }, RowVersion::Resulting) => {
            Some(new)
        }
        (
            RowWrite::Delete { old } | RowWrite::UpdateUsing { old } | RowWrite::Update { old, .. },
            RowVersion::Existing,
        ) => Some(old),
        // `RowVersion` is `#[non_exhaustive]`, and a version this does not know
        // is one it cannot supply an image for either way.
        _ => None,
    }
}

/// The table every image of `write` belongs to.
pub(crate) fn table_of<R>(write: &RowWrite<'_, R>) -> TableId
where
    R: RowView + ?Sized,
{
    match write {
        RowWrite::Insert { new } => new.table_id(),
        RowWrite::Update { old, .. } | RowWrite::UpdateUsing { old } | RowWrite::Delete { old } => {
            old.table_id()
        }
    }
}

// ---------------------------------------------------------------------------
// RowPolicy
// ---------------------------------------------------------------------------

/// Answers from the row where the schema decides it, delegates the rest.
///
/// Build it from [`rls2fga::translator::Translation::relations`] and the
/// catalog those relations were planned against.
///
/// # Examples
///
/// ```
/// # use core::convert::Infallible;
/// # use core::future::Future;
/// use rls2fga::classifier::patterns::ConfidenceLevel;
/// use rls2fga::translator::TranslatorBuilder;
/// use sqlparser::dialect::PostgreSqlDialect;
/// use subql::backend::{Postgres, Value};
/// use subql::testing::TestEvent;
/// use std::sync::Arc;
/// use rls2fga::generator::action_relations::ActionStatement;
/// use subql::visibility::policy::RowPolicy;
/// use subql::visibility::shapes::Shapes;
/// use subql::visibility::{EventRow, RowView, RowWrite, Verdict, VisibilityPolicy};
/// use subql::{catalog_helpers, ParserDB};
///
/// // The delegate answers whatever the row cannot. This one is never asked.
/// struct Backend;
/// impl VisibilityPolicy for Backend {
///     type Watcher = String;
///     type Error = Infallible;
///     type Backend = Postgres;
///     fn may_see<R>(
///         &self,
///         _row: &R,
///         _watchers: &[String],
///         _verdicts: &mut [Verdict],
///     ) -> impl Future<Output = Result<(), Infallible>> + Send
///     where
///         R: RowView<Backend = Postgres> + Sync + ?Sized,
///     {
///         async { Ok(()) }
///     }
///     fn may_write<R>(
///         &self,
///         _write: RowWrite<'_, R>,
///         _watcher: &String,
///     ) -> impl Future<Output = Result<Verdict, Infallible>> + Send
///     where
///         R: RowView<Backend = Postgres> + Sync + ?Sized,
///     {
///         async { Ok(Verdict::Deny) }
///     }
/// }
///
/// let db = ParserDB::parse::<PostgreSqlDialect>(
///     "CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT);
///      ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
///      CREATE POLICY p ON docs FOR SELECT USING (owner_id = current_user);",
/// )?;
/// let docs = catalog_helpers::table_id(&db, "docs").expect("docs is in the catalog");
///
/// // `can_select: owner`, and one row decides it.
/// let translator = TranslatorBuilder::new()
///     .with_min_confidence(ConfidenceLevel::B)
///     .build();
/// let translation = translator.translate(&db)?;
/// let relations = translation.relations();
/// let naming = translation.row_naming();
/// let answers = translation.action_relations();
/// let shapes = Arc::new(
///     Shapes::new::<Postgres>(db, &relations)
///         .with_row_naming(&naming)
///         .with_action_relations(&answers),
/// );
/// let policy = RowPolicy::new(shapes, Backend);
/// assert!(policy.answers_locally(docs, ActionStatement::Select));
///
/// let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::String("alice".into())])
///     .with_pk_columns([0u16]);
/// let row = EventRow::current(&event, policy.catalog()).expect("an insert carries a post-image");
///
/// let watchers = ["user:alice".to_string(), "user:bob".to_string()];
/// let mut verdicts = Vec::new();
/// Verdict::reset(&mut verdicts, watchers.len());
///
/// let runtime = tokio::runtime::Builder::new_current_thread().build()?;
/// runtime.block_on(policy.may_see(&row, &watchers, &mut verdicts))?;
/// assert_eq!(verdicts, [Verdict::Allow, Verdict::Deny]);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct RowPolicy<DB, P> {
    shapes: SharedShapes<DB>,
    inner: P,
}

impl<DB: DatabaseLike, P> RowPolicy<DB, P> {
    /// Answer from `shapes`, delegating whatever it does not settle to `inner`.
    ///
    /// The index is shared rather than built here, so this and the policy behind
    /// it read one catalog and one set of descriptions. Two of them built apart
    /// could disagree, and every record would then name rows that do not exist.
    #[must_use]
    pub const fn new(shapes: SharedShapes<DB>, inner: P) -> Self {
        Self { shapes, inner }
    }

    /// The index, so a caller shares it with the policy behind this one.
    #[must_use]
    pub const fn shapes(&self) -> &SharedShapes<DB> {
        &self.shapes
    }

    /// The catalog, so the caller builds its [`EventRow`](crate::visibility::EventRow)
    /// views against the same one the recipes were indexed with.
    pub fn catalog(&self) -> &DB {
        self.shapes.catalog()
    }

    /// The delegate.
    pub const fn inner(&self) -> &P {
        &self.inner
    }

    /// Whether a question about `action` on a row of `table` can be answered
    /// without a round trip.
    ///
    /// Reports the recipe, not the row: a row whose cell fails to decode is
    /// still delegated, and so is a watcher that cannot supply a value the
    /// recipe compares against.
    #[must_use]
    pub fn answers_locally(&self, table: TableId, statement: ActionStatement) -> bool {
        self.shapes.answers_locally(table, statement)
    }
}

// ---------------------------------------------------------------------------
// Evaluating one recipe for one watcher
// ---------------------------------------------------------------------------

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
            self.inner.may_see(row, &leftover, &mut answers).await?;
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
                None => self.inner.may_write(write, watcher).await,
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
        Some(match self.shapes.answer(table, statement)? {
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
            let Some(decision) = self.shapes.recipe(row.table_id(), &judge.relation) else {
                return Local::Unresolved;
            };
            match evaluate::<R, _, _>(decision, row, self.shapes.catalog(), watcher, values) {
                Local::Deny => return Local::Deny,
                Local::Unresolved => return Local::Unresolved,
                Local::Allow => {}
            }
        }
        Local::Allow
    }
}
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use crate::visibility::test_names;
    use alloc::borrow::Cow;
    use alloc::string::{String, ToString};
    use alloc::sync::Arc;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::future::Future;
    use core::pin::{pin, Pin};
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::task::{Context, Poll, Waker};

    use rls2fga::classifier::function_registry::{SessionAttribute, SessionAttributeKind};
    use rls2fga::classifier::patterns::ConfidenceLevel;
    use rls2fga::generator::records::{
        ColumnKind, ContextRendering, ObjectKey, RecordContext, RecordContextEntry,
        RecordDerivation, RecordDescription, RecordTemplate, SubjectKey, ValueSource,
    };
    use rls2fga::generator::relations::{RelationShapes, RowDecision};
    use rls2fga::generator::well_known::can_select_relation;
    use rls2fga::translator::TranslatorBuilder;
    use sqlparser::dialect::PostgreSqlDialect;

    use super::{image_of, RequestValues, RowPolicy, Subject};
    use crate::backend::{Postgres, Value};
    use crate::testing::TestEvent;
    use crate::visibility::shapes::{Shapes, SharedShapes};
    use crate::visibility::{EventRow, RowView, RowWrite, Verdict, VisibilityPolicy};
    use crate::{catalog_helpers, ColumnId, ParserDB, TableId, ValueError};
    use rls2fga::generator::action_relations::{ActionStatement, RowVersion};
    use rls2fga::generator::relations::RequestComparison;

    // -----------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------

    fn block_on<F: Future>(fut: F) -> F::Output {
        let mut ctx = Context::from_waker(Waker::noop());
        let mut pinned = pin!(fut);
        loop {
            if let Poll::Ready(v) = pinned.as_mut().poll(&mut ctx) {
                return v;
            }
        }
    }

    /// Returns `Pending` once so the delegate genuinely suspends, which is
    /// what a real round trip does.
    struct YieldOnce(bool);
    impl Future for YieldOnce {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                return Poll::Ready(());
            }
            self.0 = true;
            ctx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Unreachable;

    /// The backend. Counts its own calls, which is where requirement 4's
    /// criterion has to be measured: a counter on [`RowPolicy`] itself reads
    /// one per event from the day it lands and proves nothing.
    #[derive(Default)]
    struct Delegate {
        see_calls: AtomicUsize,
        write_calls: AtomicUsize,
        grant: Option<String>,
        fail: bool,
    }

    impl Delegate {
        fn granting(subject: &str) -> Self {
            Self {
                grant: Some(subject.to_string()),
                ..Self::default()
            }
        }

        fn failing() -> Self {
            Self {
                fail: true,
                ..Self::default()
            }
        }

        fn see_calls(&self) -> usize {
            self.see_calls.load(Ordering::Relaxed)
        }

        fn write_calls(&self) -> usize {
            self.write_calls.load(Ordering::Relaxed)
        }
    }

    impl VisibilityPolicy for Delegate {
        type Watcher = String;
        type Error = Unreachable;
        type Backend = Postgres;

        fn may_see<R>(
            &self,
            _row: &R,
            watchers: &[String],
            verdicts: &mut [Verdict],
        ) -> impl Future<Output = Result<(), Unreachable>> + Send
        where
            R: RowView<Backend = Postgres> + Sync + ?Sized,
        {
            self.see_calls.fetch_add(1, Ordering::Relaxed);
            let fail = self.fail;
            let grant = self.grant.clone();
            async move {
                YieldOnce(false).await;
                if fail {
                    return Err(Unreachable);
                }
                for (watcher, verdict) in watchers.iter().zip(verdicts.iter_mut()) {
                    if grant.as_deref() == Some(watcher.as_str()) {
                        *verdict = Verdict::Allow;
                    }
                }
                Ok(())
            }
        }

        fn may_write<R>(
            &self,
            _write: RowWrite<'_, R>,
            watcher: &String,
        ) -> impl Future<Output = Result<Verdict, Unreachable>> + Send
        where
            R: RowView<Backend = Postgres> + Sync + ?Sized,
        {
            self.write_calls.fetch_add(1, Ordering::Relaxed);
            let allowed = self.grant.as_deref() == Some(watcher.as_str());
            let fail = self.fail;
            async move {
                YieldOnce(false).await;
                if fail {
                    return Err(Unreachable);
                }
                Ok(if allowed {
                    Verdict::Allow
                } else {
                    Verdict::Deny
                })
            }
        }
    }

    /// Counts its calls and the watchers they named, generic over the watcher,
    /// so a test about a watcher shape writes only that shape.
    ///
    /// The watcher count is what decision 4's criterion reads: it says the
    /// backend was asked about the leftovers alone rather than about everybody.
    struct Named<W> {
        calls: AtomicUsize,
        seen: AtomicUsize,
        grant: Option<String>,
        watcher: core::marker::PhantomData<fn(W)>,
    }

    impl<W> Default for Named<W> {
        fn default() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                seen: AtomicUsize::new(0),
                grant: None,
                watcher: core::marker::PhantomData,
            }
        }
    }

    impl<W> Named<W> {
        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        fn seen(&self) -> usize {
            self.seen.load(Ordering::Relaxed)
        }
    }

    /// A backend granting exactly one name, to prove a delegated verdict lands
    /// on the watcher it was asked about.
    type Granting<W> = Named<W>;

    impl<W: Subject> Named<W> {
        fn to(name: &str) -> Self {
            Self {
                grant: Some(name.to_string()),
                ..Self::default()
            }
        }
    }

    impl<W: Subject + Send + Sync> VisibilityPolicy for Named<W> {
        type Watcher = W;
        type Error = Unreachable;
        type Backend = Postgres;

        fn may_see<R>(
            &self,
            _row: &R,
            watchers: &[W],
            verdicts: &mut [Verdict],
        ) -> impl Future<Output = Result<(), Unreachable>> + Send
        where
            R: RowView<Backend = Postgres> + Sync + ?Sized,
        {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.seen.fetch_add(watchers.len(), Ordering::Relaxed);
            let granted: Vec<bool> = watchers
                .iter()
                .map(|watcher| {
                    self.grant.as_ref().is_some_and(|name| {
                        watcher
                            .subjects()
                            .any(|held| held.as_ref() == name.as_str())
                    })
                })
                .collect();
            async move {
                YieldOnce(false).await;
                for (allow, verdict) in granted.into_iter().zip(verdicts.iter_mut()) {
                    if allow {
                        *verdict = Verdict::Allow;
                    }
                }
                Ok(())
            }
        }

        fn may_write<R>(
            &self,
            _write: RowWrite<'_, R>,
            _watcher: &W,
        ) -> impl Future<Output = Result<Verdict, Unreachable>> + Send
        where
            R: RowView<Backend = Postgres> + Sync + ?Sized,
        {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.seen.fetch_add(1, Ordering::Relaxed);
            async { Ok(Verdict::Deny) }
        }
    }

    // -----------------------------------------------------------------
    // Schemas, translated by the real producer
    // -----------------------------------------------------------------

    /// `can_select: owner`, one leaf. `can_delete: no_access`, no recipe.
    const OWNERSHIP: &str = "
CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT, editor_id TEXT);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (owner_id = current_user);
";

    /// `can_select: owner or editor`.
    const UNION: &str = "
CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT, editor_id TEXT);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (owner_id = current_user OR editor_id = current_user);
";

    /// `can_select: owner and editor`, through a restrictive barrier.
    const INTERSECTION: &str = "
CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT, editor_id TEXT);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p1 ON docs FOR SELECT USING (owner_id = current_user);
CREATE POLICY p2 ON docs AS RESTRICTIVE FOR SELECT USING (editor_id = current_user);
";

    /// `can_select: member from teams`, a tuple-to-userset, never decidable.
    const TEAM: &str = "
CREATE TABLE teams(id INTEGER PRIMARY KEY);
CREATE TABLE team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT);
CREATE TABLE docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_user));
";

    /// Row-level security on with no policy at all, which `PostgreSQL` reads as
    /// showing nobody anything, and the model reports refused for every
    /// statement.
    const CLOSED: &str = "
CREATE TABLE ledger(id INTEGER PRIMARY KEY, amount INTEGER);
ALTER TABLE ledger ENABLE ROW LEVEL SECURITY;
";

    fn translated(sql: &str) -> (ParserDB, Vec<RelationShapes>) {
        let db = ParserDB::parse::<PostgreSqlDialect>(sql).unwrap();
        let relations = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build()
            .translate(&db)
            .unwrap()
            .relations();
        (db, relations)
    }

    /// The two session attributes almost every fixture here declares.
    fn common_attributes() -> [SessionAttribute; 2] {
        [
            SessionAttribute::setting("app.user_id", SessionAttributeKind::CallerId),
            SessionAttribute::setting("app.subjects", SessionAttributeKind::SetAttribute),
        ]
    }

    /// The index both readers share, built the way a real caller builds it.
    ///
    /// The naming and the statement answers come from translating the catalog,
    /// not from `relations`, because several tests hand-build a recipe to reach a
    /// shape a real schema does not produce. Which relation answers a statement
    /// is the model's either way.
    fn shared(db: ParserDB, relations: &[RelationShapes]) -> SharedShapes<ParserDB> {
        shared_declaring(db, relations, common_attributes())
    }

    /// The same, for a fixture whose policies read a session attribute
    /// [`common_attributes`] does not carry.
    ///
    /// **The answers have to come from a translator that reads the policy the
    /// same way the recipes did.** One that cannot read it drops the policy, and
    /// a table with row-level security on and no policy left grants nobody, so
    /// the report says the statement is refused rather than describing the rule
    /// the fixture actually wrote.
    fn shared_declaring(
        db: ParserDB,
        relations: &[RelationShapes],
        attributes: impl IntoIterator<Item = SessionAttribute>,
    ) -> SharedShapes<ParserDB> {
        let translator = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .with_session_attributes(attributes)
            .build();
        let (naming, answers, notes) = {
            let translation = translator.translate(&db).unwrap();
            (
                translation.row_naming(),
                translation.action_relations(),
                translation.notes().to_vec(),
            )
        };
        Arc::new(
            Shapes::new::<Postgres>(db, relations)
                .with_row_naming(&naming)
                .with_action_relations(&answers)
                .with_required_parameters(&notes),
        )
    }

    fn docs_id(db: &ParserDB) -> TableId {
        catalog_helpers::table_id(db, "docs").unwrap()
    }

    /// A `docs` insert: id 4, the named owner and editor.
    fn docs_row(owner: Value<Postgres>, editor: Value<Postgres>) -> Vec<Value<Postgres>> {
        vec![Value::Int(4), owner, editor]
    }

    fn insert(table: TableId, row: Vec<Value<Postgres>>) -> TestEvent<Postgres> {
        TestEvent::insert(table, row).with_pk_columns([0u16])
    }

    fn text(value: &str) -> Value<Postgres> {
        Value::String(value.into())
    }

    /// Ask `policy` about `event`'s post-image and read the verdicts back.
    fn see(
        policy: &RowPolicy<ParserDB, Delegate>,
        event: &TestEvent<Postgres>,
        watchers: &[String],
    ) -> Result<Vec<Verdict>, Unreachable> {
        let view = EventRow::current(event, policy.catalog()).unwrap();
        let mut verdicts = Vec::new();
        Verdict::reset(&mut verdicts, watchers.len());
        block_on(policy.may_see(&view, watchers, &mut verdicts))?;
        Ok(verdicts)
    }

    fn watchers(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    // -----------------------------------------------------------------
    // Requirement 3: answered from the row, no round trip
    // -----------------------------------------------------------------

    /// The whole point. The row names alice, so alice is allowed and the
    /// backend is never asked.
    #[test]
    fn a_row_naming_the_watcher_is_allowed_without_a_round_trip() {
        let (db, relations) = translated(OWNERSHIP);
        let event = insert(docs_id(&db), docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::default());

        let got = see(&policy, &event, &watchers(&["user:alice"])).unwrap();

        assert_eq!(got, [Verdict::Allow]);
        assert_eq!(policy.inner().see_calls(), 0, "answered from the row");
    }

    /// A watcher the row does not name is denied locally, not delegated.
    /// Delegating here would make every event a round trip.
    #[test]
    fn a_row_naming_somebody_else_is_denied_without_a_round_trip() {
        let (db, relations) = translated(OWNERSHIP);
        let event = insert(docs_id(&db), docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:bob"));

        let got = see(&policy, &event, &watchers(&["user:bob"])).unwrap();

        assert_eq!(got, [Verdict::Deny]);
        assert_eq!(policy.inner().see_calls(), 0);
    }

    /// The half that matters: a relation the schema does not decide from one
    /// row is never answered locally. Answering it would be a wrong allow.
    #[test]
    fn a_relation_with_no_recipe_is_never_answered_locally() {
        let (db, relations) = translated(TEAM);
        let docs = docs_id(&db);
        let event = insert(docs, vec![Value::Int(4), Value::Int(3)]);
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:alice"));

        assert!(!policy.answers_locally(docs, ActionStatement::Select));
        let got = see(&policy, &event, &watchers(&["user:alice"])).unwrap();

        assert_eq!(got, [Verdict::Allow], "the backend answered");
        assert_eq!(policy.inner().see_calls(), 1);
    }

    /// A table the model refuses shows nobody anything, which is an answer and
    /// the cheapest one there is. Delegating it costs a round trip whose answer
    /// is always no, and a caller that fails closed on a failure to answer holds
    /// the event forever.
    #[test]
    fn a_table_the_model_refuses_denies_every_watcher_without_asking() {
        let (db, relations) = translated(CLOSED);
        let ledger = catalog_helpers::table_id(&db, "ledger").unwrap();
        let event = insert(ledger, vec![Value::Int(4), Value::Int(7)]);
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:alice"));

        assert!(policy.answers_locally(ledger, ActionStatement::Select));
        let got = see(&policy, &event, &watchers(&["user:alice", "user:bob"])).unwrap();

        assert_eq!(
            got,
            [Verdict::Deny, Verdict::Deny],
            "nobody reads this table, and the delegate would have granted alice"
        );
        assert_eq!(
            policy.inner().see_calls(),
            0,
            "and the refusal cost no round trip"
        );
    }

    /// The same refusal answers a write, which is the question asked before a
    /// change is delivered at all.
    #[test]
    fn a_write_the_model_refuses_is_denied_without_asking() {
        let (db, relations) = translated(CLOSED);
        let ledger = catalog_helpers::table_id(&db, "ledger").unwrap();
        let event = insert(ledger, vec![Value::Int(4), Value::Int(7)]);
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:alice"));
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        assert!(policy.answers_locally(ledger, ActionStatement::Insert));
        let got =
            block_on(policy.may_write(RowWrite::Insert { new: &view }, &"user:alice".to_string()));

        assert_eq!(got, Ok(Verdict::Deny));
        assert_eq!(policy.inner().write_calls(), 0);
    }

    /// A refusal is reported per statement, so one table can be answered locally
    /// twice over with opposite verdicts: the owner reads her row and still
    /// deletes nothing.
    #[test]
    fn a_table_that_refuses_writes_still_answers_reads_from_the_row() {
        let (db, relations) = translated(OWNERSHIP);
        let docs = docs_id(&db);
        let event = insert(docs, docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:alice"));
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        let read = see(&policy, &event, &watchers(&["user:alice"])).unwrap();
        let removal =
            block_on(policy.may_write(RowWrite::Delete { old: &view }, &"user:alice".to_string()));

        assert_eq!(
            read,
            [Verdict::Allow],
            "the policy names alice as the owner"
        );
        assert_eq!(
            removal,
            Ok(Verdict::Deny),
            "and no policy admits a delete, whoever asks"
        );
        assert_eq!(
            (policy.inner().see_calls(), policy.inner().write_calls()),
            (0, 0),
            "neither answer needed the service"
        );
    }

    /// A guard that fails yields no subject, which is a local deny rather
    /// than a question.
    #[test]
    fn a_row_whose_grant_column_is_null_is_denied_without_a_round_trip() {
        let (db, relations) = translated(OWNERSHIP);
        let event = insert(docs_id(&db), docs_row(Value::Null, Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:alice"));

        let got = see(&policy, &event, &watchers(&["user:alice"])).unwrap();

        assert_eq!(got, [Verdict::Deny]);
        assert_eq!(policy.inner().see_calls(), 0);
    }

    // -----------------------------------------------------------------
    // Composition
    // -----------------------------------------------------------------

    /// `owner or editor`: either leaf grants.
    #[test]
    fn a_union_recipe_allows_a_watcher_named_by_either_leaf() {
        let (db, relations) = translated(UNION);
        let event = insert(docs_id(&db), docs_row(text("alice"), text("bob")));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::default());

        let got = see(
            &policy,
            &event,
            &watchers(&["user:alice", "user:bob", "user:carol"]),
        )
        .unwrap();

        assert_eq!(got, [Verdict::Allow, Verdict::Allow, Verdict::Deny]);
        assert_eq!(policy.inner().see_calls(), 0);
    }

    /// `owner and editor`: naming only one side grants nothing. Flattening
    /// the recipe into one list would allow both of the first two, which is
    /// the wrong allow this whole design exists to remove.
    #[test]
    fn an_intersection_recipe_denies_a_watcher_named_by_only_one_leaf() {
        let (db, relations) = translated(INTERSECTION);
        let event = insert(docs_id(&db), docs_row(text("alice"), text("bob")));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::default());

        let got = see(&policy, &event, &watchers(&["user:alice", "user:bob"])).unwrap();

        assert_eq!(got, [Verdict::Deny, Verdict::Deny]);
        assert_eq!(policy.inner().see_calls(), 0);
    }

    /// The same row naming one user on both sides satisfies the intersection.
    #[test]
    fn an_intersection_recipe_allows_a_watcher_named_by_every_leaf() {
        let (db, relations) = translated(INTERSECTION);
        let event = insert(docs_id(&db), docs_row(text("alice"), text("alice")));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::default());

        let got = see(&policy, &event, &watchers(&["user:alice"])).unwrap();

        assert_eq!(got, [Verdict::Allow]);
        assert_eq!(policy.inner().see_calls(), 0);
    }

    /// An intersection over nothing is "everybody" if you fold it, so it is
    /// refused instead. The producer does not emit one today, and a
    /// fail-open answer must not wait on that staying true.
    #[test]
    fn an_intersection_over_no_children_is_refused_rather_than_granting_everyone() {
        let (db, _) = translated(OWNERSHIP);
        let docs = docs_id(&db);
        let relations = vec![RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: true,
            shapes: Vec::new(),
            decision: Some(RowDecision::All(Vec::new())),
            grants_nobody: false,
        }];
        let event = insert(docs, docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:alice"));

        assert!(!policy.answers_locally(docs, ActionStatement::Select));
        let got = see(&policy, &event, &watchers(&["user:alice"])).unwrap();

        assert_eq!(got, [Verdict::Allow], "the backend answered");
        assert_eq!(policy.inner().see_calls(), 1);
    }

    // -----------------------------------------------------------------
    // Requirement 4: round trips do not grow with the audience
    // -----------------------------------------------------------------

    /// Measured at the backend, at two counts an order of magnitude apart.
    #[test]
    fn delegated_round_trips_do_not_grow_with_the_watcher_count() {
        let mut counts = Vec::new();
        for size in [10usize, 1000] {
            let (db, relations) = translated(TEAM);
            let event = insert(docs_id(&db), vec![Value::Int(4), Value::Int(3)]);
            let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:7"));
            let audience: Vec<String> = (0..size).map(|i| alloc::format!("user:{i}")).collect();

            let got = see(&policy, &event, &audience).unwrap();

            assert_eq!(got.len(), size);
            assert_eq!(got[7], Verdict::Allow);
            counts.push(policy.inner().see_calls());
        }
        assert_eq!(counts, [1, 1], "one call per event, whatever the audience");
    }

    /// And a decidable relation costs none at either count.
    #[test]
    fn a_locally_answered_event_makes_no_round_trip_at_any_watcher_count() {
        let mut counts = Vec::new();
        for size in [10usize, 1000] {
            let (db, relations) = translated(OWNERSHIP);
            let event = insert(docs_id(&db), docs_row(text("7"), Value::Null));
            let policy = RowPolicy::new(shared(db, &relations), Delegate::default());
            let audience: Vec<String> = (0..size).map(|i| alloc::format!("user:{i}")).collect();

            let got = see(&policy, &event, &audience).unwrap();

            assert_eq!(got[7], Verdict::Allow);
            assert_eq!(got[8], Verdict::Deny);
            counts.push(policy.inner().see_calls());
        }
        assert_eq!(counts, [0, 0]);
    }

    // -----------------------------------------------------------------
    // Routing
    // -----------------------------------------------------------------

    /// A write question reads the write relation. `can_select` grants the owner
    /// here and no policy admits a delete, so answering a delete from the read
    /// recipe would grant a delete the model denies.
    #[test]
    fn a_write_question_reads_the_write_relation_not_the_read_one() {
        let (db, relations) = translated(OWNERSHIP);
        let docs = docs_id(&db);
        let event = insert(docs, docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:alice"));

        assert!(policy.answers_locally(docs, ActionStatement::Select));
        assert!(policy.answers_locally(docs, ActionStatement::Delete));

        let view = EventRow::current(&event, policy.catalog()).unwrap();
        let got =
            block_on(policy.may_write(RowWrite::Delete { old: &view }, &"user:alice".to_string()))
                .unwrap();

        assert_eq!(
            got,
            Verdict::Deny,
            "alice reads the row she owns and still deletes nothing"
        );
        assert_eq!(policy.inner().write_calls(), 0);
    }

    /// A table no recipe covers is delegated whole.
    #[test]
    fn a_table_outside_every_recipe_is_delegated() {
        let (_, relations) = translated(OWNERSHIP);
        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT, editor_id TEXT);
             CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT);",
        )
        .unwrap();
        let notes = catalog_helpers::table_id(&catalog, "notes").unwrap();
        let event = insert(notes, vec![Value::Int(1), text("hi")]);
        let policy = RowPolicy::new(
            shared(catalog, &relations),
            Delegate::granting("user:alice"),
        );

        assert!(!policy.answers_locally(notes, ActionStatement::Select));
        let got = see(&policy, &event, &watchers(&["user:alice"])).unwrap();

        assert_eq!(got, [Verdict::Allow]);
        assert_eq!(policy.inner().see_calls(), 1);
    }

    // -----------------------------------------------------------------
    // Refusing, never answering empty
    // -----------------------------------------------------------------

    /// A list subject cannot be read from a row image, and an empty subject
    /// set would read as "this row grants nobody". The recipe is dropped at
    /// construction so every such row is delegated.
    #[test]
    fn a_list_subject_is_delegated_rather_than_answered_empty() {
        let (db, _) = translated(OWNERSHIP);
        let docs = docs_id(&db);
        let relations = vec![RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: true,
            shapes: Vec::new(),
            decision: Some(RowDecision::Leaf {
                relation: test_names::relation("owner"),
                shapes: vec![RecordDescription {
                    tables: vec!["docs".to_string()],
                    derivation: RecordDerivation::FromRow {
                        table: "docs".to_string(),
                        template: Box::new(RecordTemplate {
                            object_type: "docs".to_string(),
                            object_key: id_key(),
                            relation: test_names::relation("owner"),
                            subject_type: "user".to_string(),
                            subject_key: SubjectKey::new(ValueSource::ListElements(
                                test_names::column_read("owner_id"),
                            )),
                            context: None,
                        }),
                        guards: Vec::new(),
                    },
                }],
            }),
            grants_nobody: false,
        }];
        let event = insert(docs, docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:alice"));

        assert!(!policy.answers_locally(docs, ActionStatement::Select));
        let got = see(&policy, &event, &watchers(&["user:alice"])).unwrap();

        assert_eq!(got, [Verdict::Allow]);
        assert_eq!(policy.inner().see_calls(), 1);
    }

    /// A cell the row carried but could not be decoded is not a row that
    /// grants nobody. Denying from it would silently withdraw access.
    #[test]
    fn an_undecodable_cell_is_delegated_rather_than_denied() {
        struct Corrupt(TableId);
        impl RowView for Corrupt {
            type Backend = Postgres;
            fn table_id(&self) -> TableId {
                self.0
            }
            fn value_at(&self, col: ColumnId) -> Result<Value<Postgres>, ValueError> {
                if col == 0 {
                    return Ok(Value::Int(4));
                }
                Err(ValueError::Builtin {
                    column: col,
                    kind: crate::backend::ScalarKind::String,
                })
            }
        }

        let (db, relations) = translated(OWNERSHIP);
        let docs = docs_id(&db);
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:alice"));
        let row = Corrupt(docs);
        let audience = watchers(&["user:alice"]);
        let mut verdicts = Vec::new();
        Verdict::reset(&mut verdicts, 1);

        block_on(policy.may_see(&row, &audience, &mut verdicts)).unwrap();

        assert_eq!(verdicts, [Verdict::Allow], "the backend answered");
        assert_eq!(policy.inner().see_calls(), 1);
    }

    // -----------------------------------------------------------------
    // Plumbing the trait already promises
    // -----------------------------------------------------------------

    /// Verdicts are positional, so one client with two subscriptions is two
    /// watchers and gets two answers.
    #[test]
    fn the_same_watcher_twice_gets_the_same_answer_twice() {
        let (db, relations) = translated(OWNERSHIP);
        let event = insert(docs_id(&db), docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::default());

        let got = see(
            &policy,
            &event,
            &watchers(&["user:alice", "user:bob", "user:alice"]),
        )
        .unwrap();

        assert_eq!(got, [Verdict::Allow, Verdict::Deny, Verdict::Allow]);
    }

    /// A backend that cannot answer leaves every watcher at the caller's
    /// pre-filled denial, and says so rather than reporting a decision.
    #[test]
    fn a_delegated_failure_leaves_every_watcher_denied() {
        let (db, relations) = translated(TEAM);
        let event = insert(docs_id(&db), vec![Value::Int(4), Value::Int(3)]);
        let policy = RowPolicy::new(shared(db, &relations), Delegate::failing());
        let audience = watchers(&["user:alice", "user:bob"]);
        let mut verdicts = Vec::new();
        Verdict::reset(&mut verdicts, audience.len());

        let got = block_on(policy.may_see(&event_view(&policy, &event), &audience, &mut verdicts));

        assert_eq!(got, Err(Unreachable));
        assert_eq!(verdicts, [Verdict::Deny, Verdict::Deny]);
    }

    fn event_view<'a>(
        policy: &'a RowPolicy<ParserDB, Delegate>,
        event: &'a TestEvent<Postgres>,
    ) -> EventRow<'a, TestEvent<Postgres>, ParserDB> {
        EventRow::current(event, policy.catalog()).unwrap()
    }

    /// A buffer shorter than the watcher list answers what it can rather
    /// than panicking on the caller's behalf.
    #[test]
    fn a_short_verdict_buffer_answers_what_it_can() {
        let (db, relations) = translated(OWNERSHIP);
        let event = insert(docs_id(&db), docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::default());
        let audience = watchers(&["user:bob", "user:alice"]);
        let mut verdicts = vec![Verdict::Deny];

        block_on(policy.may_see(&event_view(&policy, &event), &audience, &mut verdicts)).unwrap();

        assert_eq!(verdicts, [Verdict::Deny]);
    }

    // -----------------------------------------------------------------
    // The write path
    // -----------------------------------------------------------------

    /// `can_delete: editor and can_select` over `can_select: owner`, so the
    /// delete recipe is an intersection the row decides.
    const DELETE_GATED: &str = "
CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT, editor_id TEXT);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY ps ON docs FOR SELECT USING (owner_id = current_user);
CREATE POLICY pd ON docs FOR DELETE USING (editor_id = current_user);
";

    /// A write the schema decides costs no round trip either, and it is the
    /// write recipe that decides it.
    #[test]
    fn a_decidable_write_relation_is_answered_without_a_round_trip() {
        let (db, relations) = translated(DELETE_GATED);
        let docs = docs_id(&db);
        let event = insert(docs, docs_row(text("alice"), text("alice")));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:bob"));

        assert!(policy.answers_locally(docs, ActionStatement::Delete));
        let view = event_view(&policy, &event);

        let allowed =
            block_on(policy.may_write(RowWrite::Delete { old: &view }, &"user:alice".to_string()));
        let denied =
            block_on(policy.may_write(RowWrite::Delete { old: &view }, &"user:bob".to_string()));

        assert_eq!(allowed, Ok(Verdict::Allow));
        assert_eq!(
            denied,
            Ok(Verdict::Deny),
            "the delegate would have allowed bob"
        );
        assert_eq!(policy.inner().write_calls(), 0);
    }

    /// Only the editor may delete, even though the owner may read. Reading
    /// the wrong recipe would grant a delete the model denies.
    #[test]
    fn a_delete_recipe_denies_a_watcher_the_read_recipe_allows() {
        let (db, relations) = translated(DELETE_GATED);
        let docs = docs_id(&db);
        let event = insert(docs, docs_row(text("alice"), text("bob")));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::default());
        let view = event_view(&policy, &event);

        let mut verdicts = vec![Verdict::Deny];
        block_on(policy.may_see(&view, &watchers(&["user:alice"]), &mut verdicts)).unwrap();

        assert_eq!(verdicts, [Verdict::Allow], "alice owns it, so she reads it");
        assert_eq!(
            block_on(policy.may_write(RowWrite::Delete { old: &view }, &"user:alice".to_string())),
            Ok(Verdict::Deny),
            "she is not the editor, so she does not delete it"
        );
    }

    /// `can_update` is an intersection across two row versions:
    /// `can_update_using` judges the row as it is and `can_update_check` the
    /// row as it will be. The `SELECT` policy is what makes it decidable:
    /// Postgres reads a row before updating it, so rls2fga folds `can_select`
    /// into `can_update_using`, and without it the whole recipe delegates.
    const UPDATE_TWO_SIDED: &str = "
CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT, editor_id TEXT);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY ps ON docs FOR SELECT USING (owner_id = current_user);
CREATE POLICY pu ON docs FOR UPDATE
  USING (owner_id = current_user) WITH CHECK (editor_id = current_user);
";

    /// Only `can_update_using` is decidable. The `SELECT` policy is what makes
    /// it so, and the `WITH CHECK` clause reaching team membership is what
    /// leaves the other half undecidable: it grants whoever shares the team,
    /// which no single row names.
    const UPDATE_USING_ONLY: &str = "
CREATE TABLE teams(id INTEGER PRIMARY KEY);
CREATE TABLE team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT);
CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT, editor_id TEXT,
                  team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY ps ON docs FOR SELECT USING (owner_id = current_user);
CREATE POLICY pu ON docs FOR UPDATE USING (owner_id = current_user)
  WITH CHECK (EXISTS (SELECT 1 FROM team_members
    WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_user));
";

    /// The mirror: only `can_update_check` is decidable, because the clause
    /// choosing which rows may be touched reaches team membership.
    const UPDATE_CHECK_ONLY: &str = "
CREATE TABLE teams(id INTEGER PRIMARY KEY);
CREATE TABLE team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT);
CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT, editor_id TEXT,
                  team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY ps ON docs FOR SELECT USING (owner_id = current_user);
CREATE POLICY pu ON docs FOR UPDATE USING (EXISTS (SELECT 1 FROM team_members
    WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_user))
  WITH CHECK (editor_id = current_user);
";

    /// A `docs` row of the two fixtures above: id 4, in team 3.
    fn docs_team_row(owner: Value<Postgres>, editor: Value<Postgres>) -> Vec<Value<Postgres>> {
        vec![Value::Int(4), owner, editor, Value::Int(3)]
    }

    /// A replacement is answered from both versions, and the caller who
    /// writes themselves in is refused by the version they do not hold.
    ///
    /// This is the wrong allow the shape invites. Judged on the new image
    /// alone, bob names himself owner and editor and is granted a row alice
    /// holds.
    #[test]
    fn a_replacement_is_answered_from_both_versions() {
        let (db, relations) = translated(UPDATE_TWO_SIDED);
        let docs = docs_id(&db);
        // alice owns it, and bob rewrites it naming himself owner and editor.
        let event = TestEvent::update(
            docs,
            docs_row(text("alice"), Value::Null),
            docs_row(text("bob"), text("bob")),
        )
        .with_pk_columns([0u16]);
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:bob"));

        assert!(policy.answers_locally(docs, ActionStatement::SelectForUpdate));
        assert!(policy.answers_locally(docs, ActionStatement::Update));

        let old = EventRow::previous(&event, policy.catalog()).unwrap();
        let new = EventRow::current(&event, policy.catalog()).unwrap();

        let got = block_on(policy.may_write(
            RowWrite::Update {
                old: &old,
                new: &new,
            },
            &"user:bob".to_string(),
        ));

        assert_eq!(
            got,
            Ok(Verdict::Deny),
            "bob does not hold the row as it is, so he cannot replace it"
        );
        assert_eq!(
            policy.inner().write_calls(),
            0,
            "and the schema settled it with no round trip, though the \
             delegate would have granted bob"
        );
    }

    /// The version that refuses is the new one here, which is the half a
    /// policy reading only the row as it is would miss.
    ///
    /// alice holds the row and hands the editorship to bob, and the rule
    /// admitting the result wants the caller to be the editor.
    #[test]
    fn a_replacement_the_new_version_refuses_is_refused() {
        let (db, relations) = translated(UPDATE_TWO_SIDED);
        let docs = docs_id(&db);
        let event = TestEvent::update(
            docs,
            docs_row(text("alice"), text("alice")),
            docs_row(text("alice"), text("bob")),
        )
        .with_pk_columns([0u16]);
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:alice"));

        let old = EventRow::previous(&event, policy.catalog()).unwrap();
        let new = EventRow::current(&event, policy.catalog()).unwrap();
        let got = block_on(policy.may_write(
            RowWrite::Update {
                old: &old,
                new: &new,
            },
            &"user:alice".to_string(),
        ));

        assert_eq!(
            got,
            Ok(Verdict::Deny),
            "she holds it now and would not be its editor after"
        );
        assert_eq!(policy.inner().write_calls(), 0);
    }

    /// The half that judges the row as it stands answers on its own, which is
    /// what a caller holding only that version asks.
    #[test]
    fn the_row_as_it_stands_answers_a_replacement_question_alone() {
        let (db, relations) = translated(UPDATE_TWO_SIDED);
        let event = insert(docs_id(&db), docs_row(text("alice"), text("bob")));
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:bob"));
        let view = event_view(&policy, &event);

        let alice = block_on(policy.may_write(
            RowWrite::UpdateUsing { old: &view },
            &"user:alice".to_string(),
        ));
        let bob = block_on(policy.may_write(
            RowWrite::UpdateUsing { old: &view },
            &"user:bob".to_string(),
        ));

        assert_eq!(alice, Ok(Verdict::Allow), "alice owns it");
        assert_eq!(
            bob,
            Ok(Verdict::Deny),
            "bob is only the editor, and the delegate would have granted him"
        );
        assert_eq!(policy.inner().write_calls(), 0);
    }

    /// A replacement whose first half refuses is a definite refusal, so the
    /// second half is never consulted and nothing is delegated.
    ///
    /// Both halves are readable here, and the half admitting the result would
    /// have granted bob, who writes himself in as editor. Consulting it after a
    /// refusal is the wrong allow this defends.
    #[test]
    fn a_refusal_on_the_row_as_it_is_needs_no_second_half() {
        let (db, relations) = translated(UPDATE_TWO_SIDED);
        let docs = docs_id(&db);
        let event = TestEvent::update(
            docs,
            docs_row(text("alice"), Value::Null),
            docs_row(text("alice"), text("bob")),
        )
        .with_pk_columns([0u16]);
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:bob"));

        assert!(policy.answers_locally(docs, ActionStatement::SelectForUpdate));
        assert!(policy.answers_locally(docs, ActionStatement::Update));

        let old = EventRow::previous(&event, policy.catalog()).unwrap();
        let new = EventRow::current(&event, policy.catalog()).unwrap();
        let got = block_on(policy.may_write(
            RowWrite::Update {
                old: &old,
                new: &new,
            },
            &"user:bob".to_string(),
        ));

        assert_eq!(
            got,
            Ok(Verdict::Deny),
            "bob does not own the row as it stands, whatever he writes into it"
        );
        assert_eq!(
            policy.inner().write_calls(),
            0,
            "a refused first half settles the conjunction"
        );
    }

    /// A replacement whose first half grants and whose second half is not
    /// decidable is not an answer, so it delegates.
    #[test]
    fn a_grant_on_the_row_as_it_is_still_needs_the_second_half() {
        let (db, relations) = translated(UPDATE_USING_ONLY);
        let docs = docs_id(&db);
        let event = TestEvent::update(
            docs,
            docs_team_row(text("alice"), Value::Null),
            docs_team_row(text("alice"), text("alice")),
        )
        .with_pk_columns([0u16]);
        let policy = RowPolicy::new(shared(db, &relations), Delegate::granting("user:alice"));

        assert!(
            policy.answers_locally(docs, ActionStatement::SelectForUpdate),
            "the half judging the row as it stands is readable"
        );
        assert!(
            !policy.answers_locally(docs, ActionStatement::Update),
            "and the half admitting the result reaches team membership"
        );

        let old = EventRow::previous(&event, policy.catalog()).unwrap();
        let new = EventRow::current(&event, policy.catalog()).unwrap();
        let got = block_on(policy.may_write(
            RowWrite::Update {
                old: &old,
                new: &new,
            },
            &"user:alice".to_string(),
        ));

        assert_eq!(got, Ok(Verdict::Allow), "the delegate answered");
        assert_eq!(policy.inner().write_calls(), 1);
    }

    /// A replacement whose first half is undecidable delegates, even though the
    /// second half is decidable and would have granted.
    ///
    /// This is the wrong allow in the other direction. The row as it will be
    /// names bob as editor, so answering from the half that is readable grants
    /// bob a row alice holds.
    #[test]
    fn an_undecidable_first_half_delegates_the_whole_replacement() {
        let (db, relations) = translated(UPDATE_CHECK_ONLY);
        let docs = docs_id(&db);
        let event = TestEvent::update(
            docs,
            docs_team_row(text("alice"), Value::Null),
            docs_team_row(text("alice"), text("bob")),
        )
        .with_pk_columns([0u16]);
        let policy = RowPolicy::new(shared(db, &relations), Delegate::default());

        assert!(
            !policy.answers_locally(docs, ActionStatement::SelectForUpdate),
            "the half judging the row as it is delegates here"
        );
        assert!(
            !policy.answers_locally(docs, ActionStatement::Update),
            "so the replacement as a whole is not answerable either, even though \
             the half admitting the result is readable and would have granted bob"
        );

        let old = EventRow::previous(&event, policy.catalog()).unwrap();
        let new = EventRow::current(&event, policy.catalog()).unwrap();
        let got = block_on(policy.may_write(
            RowWrite::Update {
                old: &old,
                new: &new,
            },
            &"user:bob".to_string(),
        ));

        assert_eq!(got, Ok(Verdict::Deny), "the delegate refused bob");
        assert_eq!(
            policy.inner().write_calls(),
            1,
            "the readable half alone is not an answer"
        );
    }

    // -----------------------------------------------------------------
    // Watchers that are not owned strings
    // -----------------------------------------------------------------

    /// A watcher held by reference is named the same way an owned one is,
    /// so a caller keeping its subject strings elsewhere pays no clone.
    #[test]
    fn a_borrowed_watcher_is_named_the_same_way() {
        /// Counts, like [`Delegate`], so the assertion below is that a
        /// borrowed watcher took the local path rather than this one.
        #[derive(Default)]
        struct Borrowed(AtomicUsize);

        impl VisibilityPolicy for Borrowed {
            type Watcher = &'static str;
            type Error = Unreachable;
            type Backend = Postgres;

            fn may_see<R>(
                &self,
                _row: &R,
                _watchers: &[&'static str],
                _verdicts: &mut [Verdict],
            ) -> impl Future<Output = Result<(), Unreachable>> + Send
            where
                R: RowView<Backend = Postgres> + Sync + ?Sized,
            {
                self.0.fetch_add(1, Ordering::Relaxed);
                async { Ok(()) }
            }

            fn may_write<R>(
                &self,
                _write: RowWrite<'_, R>,
                _watcher: &&'static str,
            ) -> impl Future<Output = Result<Verdict, Unreachable>> + Send
            where
                R: RowView<Backend = Postgres> + Sync + ?Sized,
            {
                self.0.fetch_add(1, Ordering::Relaxed);
                async { Ok(Verdict::Deny) }
            }
        }

        let (db, relations) = translated(OWNERSHIP);
        let event = insert(docs_id(&db), docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Borrowed::default());
        let view = EventRow::current(&event, policy.catalog()).unwrap();
        let mut verdicts = vec![Verdict::Deny; 2];

        block_on(policy.may_see(&view, &["user:alice", "user:bob"], &mut verdicts)).unwrap();

        assert_eq!(verdicts, [Verdict::Allow, Verdict::Deny]);
        assert_eq!(policy.inner().0.load(Ordering::Relaxed), 0);
    }

    /// A watcher that holds no text at all is still answerable.
    ///
    /// This is the shape a real consumer has: an identity is a typed id, and
    /// text belongs at the edges, so the subject is rendered rather than
    /// stored. Returning `&str` would force every such consumer to keep a
    /// rendered copy on its own type whether it wanted one or not.
    #[test]
    fn a_watcher_that_renders_its_subject_on_demand_is_answered() {
        #[derive(Clone)]
        struct Typed(u64);

        impl Subject for Typed {
            fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
                core::iter::once(Cow::Owned(alloc::format!("user:{}", self.0)))
            }
        }

        #[derive(Default)]
        struct Backend(AtomicUsize);

        impl VisibilityPolicy for Backend {
            type Watcher = Typed;
            type Error = Unreachable;
            type Backend = Postgres;

            fn may_see<R>(
                &self,
                _row: &R,
                _watchers: &[Typed],
                _verdicts: &mut [Verdict],
            ) -> impl Future<Output = Result<(), Unreachable>> + Send
            where
                R: RowView<Backend = Postgres> + Sync + ?Sized,
            {
                self.0.fetch_add(1, Ordering::Relaxed);
                async { Ok(()) }
            }

            fn may_write<R>(
                &self,
                _write: RowWrite<'_, R>,
                _watcher: &Typed,
            ) -> impl Future<Output = Result<Verdict, Unreachable>> + Send
            where
                R: RowView<Backend = Postgres> + Sync + ?Sized,
            {
                self.0.fetch_add(1, Ordering::Relaxed);
                async { Ok(Verdict::Deny) }
            }
        }

        let (db, relations) = translated(OWNERSHIP);
        // The row grants the user whose rendered subject is `user:7`.
        let event = insert(docs_id(&db), docs_row(text("7"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Backend::default());
        let view = EventRow::current(&event, policy.catalog()).unwrap();
        let mut verdicts = vec![Verdict::Deny; 2];

        block_on(policy.may_see(&view, &[Typed(7), Typed(8)], &mut verdicts)).unwrap();

        assert_eq!(verdicts, [Verdict::Allow, Verdict::Deny]);
        assert_eq!(policy.inner().0.load(Ordering::Relaxed), 0);
    }

    /// A watcher held behind a shared handle is named the same way too.
    ///
    /// This is not symmetry for its own sake. A consumer whose watcher is
    /// `Arc<ItsOwnType>` cannot write the impl itself: `Arc` is not
    /// `#[fundamental]`, so `Arc<Local>` is not a local type and the orphan
    /// rule refuses a foreign trait on it. Without this the consumer needs
    /// a newtype at every site that names the watcher.
    #[test]
    fn a_watcher_behind_a_shared_handle_is_named_the_same_way() {
        #[derive(Default)]
        struct Shared(AtomicUsize);

        impl VisibilityPolicy for Shared {
            type Watcher = Arc<String>;
            type Error = Unreachable;
            type Backend = Postgres;

            fn may_see<R>(
                &self,
                _row: &R,
                _watchers: &[Arc<String>],
                _verdicts: &mut [Verdict],
            ) -> impl Future<Output = Result<(), Unreachable>> + Send
            where
                R: RowView<Backend = Postgres> + Sync + ?Sized,
            {
                self.0.fetch_add(1, Ordering::Relaxed);
                async { Ok(()) }
            }

            fn may_write<R>(
                &self,
                _write: RowWrite<'_, R>,
                _watcher: &Arc<String>,
            ) -> impl Future<Output = Result<Verdict, Unreachable>> + Send
            where
                R: RowView<Backend = Postgres> + Sync + ?Sized,
            {
                self.0.fetch_add(1, Ordering::Relaxed);
                async { Ok(Verdict::Deny) }
            }
        }

        let (db, relations) = translated(OWNERSHIP);
        let event = insert(docs_id(&db), docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Shared::default());
        let view = EventRow::current(&event, policy.catalog()).unwrap();
        let audience = [
            Arc::new("user:alice".to_string()),
            Arc::new("user:bob".to_string()),
        ];
        let mut verdicts = vec![Verdict::Deny; 2];

        block_on(policy.may_see(&view, &audience, &mut verdicts)).unwrap();

        assert_eq!(verdicts, [Verdict::Allow, Verdict::Deny]);
        assert_eq!(policy.inner().0.load(Ordering::Relaxed), 0);
    }

    // -----------------------------------------------------------------
    // What a watcher exposes: names, and the values it sent
    // -----------------------------------------------------------------

    /// A watcher authenticating as several names is granted when the row
    /// names any one of them.
    ///
    /// A real consumer's principal carries an identity and zero or more
    /// further subjects, so naming only the first would deny a holder the
    /// model grants.
    #[test]
    fn a_watcher_with_several_names_is_granted_by_any_of_them() {
        #[derive(Clone)]
        struct Several(Vec<String>);

        impl Subject for Several {
            fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
                self.0.iter().map(|name| Cow::Borrowed(name.as_str()))
            }
        }

        let (db, relations) = translated(OWNERSHIP);
        let event = insert(docs_id(&db), docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Named::<Several>::default());
        let view = EventRow::current(&event, policy.catalog()).unwrap();
        let watchers = [
            Several(vec!["user:bob".to_string(), "user:alice".to_string()]),
            Several(vec!["user:bob".to_string()]),
        ];
        let mut verdicts = vec![Verdict::Deny; 2];

        block_on(policy.may_see(&view, &watchers, &mut verdicts)).unwrap();

        assert_eq!(
            verdicts,
            [Verdict::Allow, Verdict::Deny],
            "the second name is what grants the first watcher"
        );
        assert_eq!(policy.inner().calls(), 0);
    }

    /// A watcher that cannot answer for a parameter is distinguishable from
    /// one that answers with nothing.
    ///
    /// The first delegates, because the row settles only one side of the
    /// comparison and nothing completed the other. The second is an answer:
    /// a caller holding no keys is granted by no key.
    #[test]
    fn answering_nothing_and_being_unable_to_answer_are_different() {
        struct Holder(Option<Vec<String>>);

        impl Subject for Holder {
            fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
                core::iter::empty()
            }

            fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
                assert_eq!(parameter, "app_subjects");
                let Some(held) = self.0.as_ref() else {
                    return false;
                };
                for key in held {
                    out.push(key);
                }
                true
            }
        }

        let mut values = RequestValues::new();

        assert!(
            !Holder(None).request_value("app_subjects", &mut values),
            "a watcher that cannot answer says so"
        );

        values.reset();
        assert!(Holder(Some(Vec::new())).request_value("app_subjects", &mut values));
        assert!(!values.holds("abc"), "it holds nothing, which is an answer");

        values.reset();
        assert!(Holder(Some(vec!["abc".to_string()])).request_value("app_subjects", &mut values));
        assert!(values.holds("abc"));
        assert!(!values.holds("def"));
    }

    /// The buffer carries nothing from one parameter into the next.
    ///
    /// It is reused across every watcher of one event, so a stale value left
    /// behind would grant the next watcher what the previous one held.
    #[test]
    fn the_buffer_keeps_nothing_from_the_previous_watcher() {
        let mut values = RequestValues::new();
        values.push("abc");
        assert!(values.holds("abc"));

        values.reset();

        assert!(!values.holds("abc"));
        assert_eq!(values.values().count(), 0);
    }

    /// Reuse costs no allocation after the first fill, which is the whole
    /// reason the watcher writes into a buffer rather than returning one.
    #[test]
    fn reuse_reallocates_nothing() {
        let mut values = RequestValues::new();
        values.push("a-key-long-enough-to-have-a-heap-buffer");
        let capacity = values.capacity();

        for _ in 0..8 {
            values.reset();
            values.push("a-key-long-enough-to-have-a-heap-buffer");
        }

        assert_eq!(values.capacity(), capacity, "the buffer was reused");
    }

    /// A watcher reached through a wrapper is asked for its values too.
    ///
    /// Forgetting to forward this is silent: the wrapper would report that it
    /// cannot answer, and every request-gated grant would delegate instead of
    /// being decided locally.
    ///
    /// Asked through a generic bound, because that is the only way to reach
    /// the wrapper's own impl. Calling `(&watcher).request_value(..)` directly
    /// resolves to the inner type's impl instead, since the method takes
    /// `&self` and the receiver already has that shape, so a test written that
    /// way passes with the forwarding deleted.
    #[test]
    fn a_wrapped_watcher_forwards_the_values_it_holds() {
        struct Keyed;

        impl Subject for Keyed {
            fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
                core::iter::empty()
            }

            fn request_value(&self, _parameter: &str, out: &mut RequestValues) -> bool {
                out.push("abc");
                true
            }
        }

        /// Reaches the watcher exactly as the policy does.
        fn ask<S: Subject>(watcher: S, out: &mut RequestValues) -> bool {
            watcher.request_value("app_subjects", out)
        }

        let mut values = RequestValues::new();
        assert!(ask(Arc::new(Keyed), &mut values));
        assert!(values.holds("abc"), "the shared handle forwarded it");

        values.reset();
        assert!(ask(&Keyed, &mut values));
        assert!(values.holds("abc"), "the reference forwarded it");
    }

    // -----------------------------------------------------------------
    // Recipes this does not recognise
    // -----------------------------------------------------------------

    /// The shape every connetto table has: one policy whose `USING` is an `OR`
    /// of the caller's identity and the keys it holds.
    ///
    /// Verified against connetto's own fixtures rather than invented, at
    /// `connetto-server/tests/rls_write_filter.rs:58-60`.
    const HELD_KEYS: &str = "
CREATE TABLE notes(id INTEGER PRIMARY KEY, owner TEXT, body TEXT);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_p ON notes USING (
  owner = current_setting('app.user_id', true)
  OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));
";

    /// Translate with the request-scoped values declared, which is what makes
    /// the held-keys arm classify at all. Without the declarations rls2fga
    /// refuses that arm and the model keeps only the owner.
    fn translated_with_keys(sql: &str) -> (ParserDB, Vec<RelationShapes>) {
        let db = ParserDB::parse::<PostgreSqlDialect>(sql).unwrap();
        let relations = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .with_session_attributes([
                SessionAttribute::setting("app.user_id", SessionAttributeKind::CallerId),
                SessionAttribute::setting("app.subjects", SessionAttributeKind::SetAttribute),
            ])
            .build()
            .translate(&db)
            .unwrap()
            .relations();
        (db, relations)
    }

    /// A watcher with a name and, when it can say so, the keys it sent.
    #[derive(Clone)]
    struct Principal {
        name: String,
        keys: Option<Vec<String>>,
    }

    impl Principal {
        /// Holds keys and can say which.
        fn holding(name: &str, keys: &[&str]) -> Self {
            Self {
                name: name.to_string(),
                keys: Some(keys.iter().map(|key| (*key).to_string()).collect()),
            }
        }

        /// Cannot say what it sent, which delegates.
        fn silent(name: &str) -> Self {
            Self {
                name: name.to_string(),
                keys: None,
            }
        }
    }

    impl Subject for Principal {
        fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
            core::iter::once(Cow::Borrowed(self.name.as_str()))
        }

        fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
            if parameter != "app_subjects" {
                return false;
            }
            let Some(keys) = self.keys.as_ref() else {
                return false;
            };
            for key in keys {
                out.push(key);
            }
            true
        }
    }

    fn notes_id(db: &ParserDB) -> TableId {
        catalog_helpers::table_id(db, "notes").unwrap()
    }

    /// A `notes` row: id 4, owned by `owner`.
    fn notes_row(owner: &str) -> Vec<Value<Postgres>> {
        vec![Value::Int(4), text(owner), Value::Null]
    }

    /// The whole point of requirements 3 and 4 for the only consumer. A row
    /// shared through a held key is answered from the watcher's own values,
    /// with no round trip.
    #[test]
    fn a_shared_row_is_answered_from_the_watchers_own_keys() {
        let (db, relations) = translated_with_keys(HELD_KEYS);
        let notes = notes_id(&db);
        let event = insert(notes, notes_row("share-key-7"));
        let policy = RowPolicy::new(shared(db, &relations), Named::<Principal>::default());
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        assert!(
            policy.answers_locally(notes, ActionStatement::Select),
            "the recipe carries an owner arm and a request-gated arm, and both read this row"
        );

        let watchers = [
            Principal::holding("user:bob", &["share-key-7"]),
            Principal::holding("user:eve", &["share-key-9"]),
        ];
        let mut verdicts = vec![Verdict::Deny; 2];
        block_on(policy.may_see(&view, &watchers, &mut verdicts)).unwrap();

        assert_eq!(
            verdicts,
            [Verdict::Allow, Verdict::Deny],
            "bob holds the key the row names and eve holds another"
        );
        assert_eq!(
            policy.inner().calls(),
            0,
            "and neither answer cost a round trip"
        );
    }

    /// The identity arm answers on its own, so a watcher holding no keys at all
    /// is still decided locally.
    #[test]
    fn the_identity_arm_answers_without_any_keys() {
        let (db, relations) = translated_with_keys(HELD_KEYS);
        let notes = notes_id(&db);
        // The column holds the bare id, which is what the record names as
        // `user:alice`. A column already carrying the prefix would encode the
        // colon and name somebody else.
        let event = insert(notes, notes_row("alice"));
        let policy = RowPolicy::new(shared(db, &relations), Named::<Principal>::default());
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        let watchers = [Principal::holding("user:alice", &[])];
        let mut verdicts = vec![Verdict::Deny];
        block_on(policy.may_see(&view, &watchers, &mut verdicts)).unwrap();

        assert_eq!(verdicts, [Verdict::Allow]);
        assert_eq!(policy.inner().calls(), 0);
    }

    /// Decision 4's headline: a union whose other arm grants is answered
    /// ALLOW without resolving the arm the row does not settle.
    ///
    /// alice owns the row and cannot say what she sent, so the request-gated arm
    /// is unresolved and the owner arm decides it anyway. Requiring every arm to
    /// resolve would delegate this, which is the pathology that made one
    /// request-gated arm disable the fast path for a whole table.
    #[test]
    fn a_granting_arm_answers_beside_an_unresolved_one() {
        let (db, relations) = translated_with_keys(HELD_KEYS);
        let notes = notes_id(&db);
        let event = insert(notes, notes_row("alice"));
        let policy = RowPolicy::new(shared(db, &relations), Named::<Principal>::default());
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        let watchers = [Principal::silent("user:alice")];
        let mut verdicts = vec![Verdict::Deny];
        block_on(policy.may_see(&view, &watchers, &mut verdicts)).unwrap();

        assert_eq!(verdicts, [Verdict::Allow], "the owner arm settled it");
        assert_eq!(
            policy.inner().calls(),
            0,
            "and the unresolved arm cost no round trip"
        );
    }

    /// A policy comparing one value is not satisfied by a watcher that sent a
    /// set holding it.
    ///
    /// Reading any element as a match is a wrong allow: the deployment declared
    /// this parameter as one value, and a caller carrying several has not met a
    /// rule that compares against one.
    const SCALAR_GATE: &str = "
CREATE TABLE notes(id INTEGER PRIMARY KEY, owner TEXT, body TEXT);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_p ON notes USING (owner = current_setting('app.department', true));
";

    #[test]
    fn a_single_value_comparison_refuses_a_watcher_that_sent_several() {
        let db = ParserDB::parse::<PostgreSqlDialect>(SCALAR_GATE).unwrap();
        let department =
            SessionAttribute::setting("app.department", SessionAttributeKind::ScalarAttribute)
                .with_parameter("app_subjects");
        let relations = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .with_session_attributes([department.clone()])
            .build()
            .translate(&db)
            .unwrap()
            .relations();
        let notes = notes_id(&db);
        let event = insert(notes, notes_row("physics"));
        let policy = RowPolicy::new(
            shared_declaring(db, &relations, [department]),
            Named::<Principal>::default(),
        );
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        assert!(policy.answers_locally(notes, ActionStatement::Select));

        let watchers = [
            Principal::holding("user:one", &["physics"]),
            Principal::holding("user:many", &["physics", "chemistry"]),
        ];
        let mut verdicts = vec![Verdict::Deny; 2];
        block_on(policy.may_see(&view, &watchers, &mut verdicts)).unwrap();

        assert_eq!(
            verdicts,
            [Verdict::Allow, Verdict::Deny],
            "one value matches, and a set holding it does not"
        );
        assert_eq!(policy.inner().calls(), 0);
    }

    /// A replacement on connetto's own shape is answered from both versions by
    /// the one relation the model defines for it.
    ///
    /// This is the defect the upstream report was filed for. With one condition
    /// and no explicit `WITH CHECK`, rls2fga emits `can_update` alone and neither
    /// half, so a reader naming the halves asks for relations the model never
    /// defined: the local path answered nothing and the service rejected the
    /// question. Postgres applies a lone `USING` clause to the result as well, so
    /// the one relation judges both versions and both must grant.
    #[test]
    fn a_replacement_on_one_condition_is_answered_from_both_versions() {
        let (db, relations) = translated_with_keys(HELD_KEYS);
        let notes = notes_id(&db);
        let policy = RowPolicy::new(shared(db, &relations), Named::<Principal>::default());

        assert!(
            policy.answers_locally(notes, ActionStatement::Update),
            "one relation answers it, against both versions"
        );

        // alice holds the row and hands it to bob, so the row as it will be
        // refuses her even though the row as it is grants her.
        let handover =
            TestEvent::update(notes, notes_row("alice"), notes_row("bob")).with_pk_columns([0u16]);
        let old = EventRow::previous(&handover, policy.catalog()).unwrap();
        let new = EventRow::current(&handover, policy.catalog()).unwrap();
        let alice = Principal::holding("user:alice", &[]);

        let got = block_on(policy.may_write(
            RowWrite::Update {
                old: &old,
                new: &new,
            },
            &alice,
        ));

        assert_eq!(
            got,
            Ok(Verdict::Deny),
            "she holds it now and would not after"
        );
        assert_eq!(
            policy.inner().calls(),
            0,
            "and no round trip was needed to say so"
        );

        // Keeping it is granted, since both versions name her.
        let kept = TestEvent::update(notes, notes_row("alice"), notes_row("alice"))
            .with_pk_columns([0u16]);
        let old = EventRow::previous(&kept, policy.catalog()).unwrap();
        let new = EventRow::current(&kept, policy.catalog()).unwrap();
        assert_eq!(
            block_on(policy.may_write(
                RowWrite::Update {
                    old: &old,
                    new: &new
                },
                &alice
            )),
            Ok(Verdict::Allow)
        );
        assert_eq!(policy.inner().calls(), 0);
    }

    /// Each verb carries only the versions it has, so a judgement naming one it
    /// does not carry finds nothing rather than the other one.
    ///
    /// A creation has no existing row and a removal no resulting one. Substituting
    /// the version that is present would judge the rule admitting a result against
    /// the row being removed, which answers a question nobody asked.
    #[test]
    fn a_verb_carries_only_the_versions_it_has() {
        let (db, relations) = translated(OWNERSHIP);
        let docs = docs_id(&db);
        let policy = RowPolicy::new(shared(db, &relations), Delegate::default());
        let event = insert(docs, docs_row(text("alice"), Value::Null));
        let view = event_view(&policy, &event);

        let created = RowWrite::Insert { new: &view };
        assert!(image_of(&created, RowVersion::Resulting).is_some());
        assert!(
            image_of(&created, RowVersion::Existing).is_none(),
            "a creation has no existing row"
        );

        let removed = RowWrite::Delete { old: &view };
        assert!(image_of(&removed, RowVersion::Existing).is_some());
        assert!(
            image_of(&removed, RowVersion::Resulting).is_none(),
            "a removal has no resulting row"
        );

        let taking = RowWrite::UpdateUsing { old: &view };
        assert!(image_of(&taking, RowVersion::Existing).is_some());
        assert!(
            image_of(&taking, RowVersion::Resulting).is_none(),
            "a caller holding one version has only that one"
        );
    }

    /// A table the model reports as restricting nothing grants every write, with
    /// nothing to ask and no round trip.
    ///
    /// `teams` carries no row-level security and reaches the model only because a
    /// protected table points at it, which is the shape that is reported
    /// unrestricted. A table the model knows nothing about at all is a different
    /// thing and delegates, since silence is not a grant.
    #[test]
    fn a_table_restricting_nothing_grants_without_asking() {
        let (db, relations) = translated(TEAM);
        let teams = catalog_helpers::table_id(&db, "teams").unwrap();
        let policy = RowPolicy::new(shared(db, &relations), Named::<Principal>::default());
        let event = TestEvent::insert(teams, vec![Value::Int(1)]).with_pk_columns([0u16]);
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        assert!(policy.answers_locally(teams, ActionStatement::Insert));
        let got = block_on(policy.may_write(
            RowWrite::Insert { new: &view },
            &Principal::silent("user:nobody"),
        ));

        assert_eq!(
            got,
            Ok(Verdict::Allow),
            "no row-level security means no rule to satisfy"
        );
        assert_eq!(policy.inner().calls(), 0);
    }

    /// A watcher that cannot say what it sent loses speed, never correctness:
    /// its question is delegated rather than answered from the row alone.
    #[test]
    fn a_watcher_that_cannot_say_what_it_sent_is_delegated() {
        let (db, relations) = translated_with_keys(HELD_KEYS);
        let notes = notes_id(&db);
        let event = insert(notes, notes_row("share-key-7"));
        let policy = RowPolicy::new(shared(db, &relations), Named::<Principal>::default());
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        let watchers = [Principal::silent("user:bob")];
        let mut verdicts = vec![Verdict::Deny];
        block_on(policy.may_see(&view, &watchers, &mut verdicts)).unwrap();

        assert_eq!(policy.inner().calls(), 1, "the backend was asked");
        assert_eq!(policy.inner().seen(), 1);
    }

    /// Only the watchers the row left unresolved are forwarded, which is what
    /// keeps one unreadable arm from making every watcher a round trip.
    #[test]
    fn only_the_unresolved_watchers_are_delegated() {
        let (db, relations) = translated_with_keys(HELD_KEYS);
        let notes = notes_id(&db);
        let event = insert(notes, notes_row("share-key-7"));
        let policy = RowPolicy::new(shared(db, &relations), Named::<Principal>::default());
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        let watchers = [
            Principal::holding("user:bob", &["share-key-7"]),
            Principal::silent("user:eve"),
            Principal::holding("user:dan", &["share-key-9"]),
        ];
        let mut verdicts = vec![Verdict::Deny; 3];
        block_on(policy.may_see(&view, &watchers, &mut verdicts)).unwrap();

        assert_eq!(verdicts[0], Verdict::Allow, "answered from the row");
        assert_eq!(verdicts[2], Verdict::Deny, "answered from the row");
        assert_eq!(policy.inner().calls(), 1, "one call for the leftovers");
        assert_eq!(
            policy.inner().seen(),
            1,
            "and it named only the watcher the row could not settle"
        );
    }

    /// A verdict the backend returns lands on the watcher it was asked about,
    /// not on the position it occupied in the delegated batch.
    #[test]
    fn a_delegated_verdict_lands_on_the_right_watcher() {
        let (db, relations) = translated_with_keys(HELD_KEYS);
        let notes = notes_id(&db);
        let event = insert(notes, notes_row("share-key-7"));
        let policy = RowPolicy::new(
            shared(db, &relations),
            Granting::<Principal>::to("user:eve"),
        );
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        let watchers = [
            Principal::holding("user:bob", &["share-key-9"]),
            Principal::silent("user:eve"),
            Principal::silent("user:dan"),
        ];
        let mut verdicts = vec![Verdict::Deny; 3];
        block_on(policy.may_see(&view, &watchers, &mut verdicts)).unwrap();

        assert_eq!(
            verdicts,
            [Verdict::Deny, Verdict::Allow, Verdict::Deny],
            "the grant belongs to eve, who sat second"
        );
    }

    /// A request-gated record carrying no condition context is refused rather
    /// than read as a grant.
    ///
    /// Its subject is the wildcard, so taking it at face value grants everyone.
    /// The shapes rls2fga emits for this variant always carry a context, so this
    /// is defence in depth against a later one that does not.
    #[test]
    fn a_request_gated_record_without_a_context_is_delegated() {
        let (db, _) = translated(OWNERSHIP);
        let docs = docs_id(&db);
        let relations = vec![RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: true,
            shapes: Vec::new(),
            decision: Some(RowDecision::RequestGated {
                relation: test_names::relation("owner"),
                shapes: vec![shape_on("docs", "owner_id")],
                context_key: "row_department".to_string(),
                // A parameter the watcher below does answer, so evaluation
                // reaches the records rather than stopping at the watcher.
                request_parameter: "app_subjects".to_string(),
                comparison: RequestComparison::CallerValueEquals,
            }),
            grants_nobody: false,
        }];
        let event = insert(docs, docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(
            shared(db, &relations),
            Granting::<Principal>::to("user:alice"),
        );
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        let watchers = [Principal::holding("user:alice", &["alice"])];
        let mut verdicts = vec![Verdict::Deny];
        block_on(policy.may_see(&view, &watchers, &mut verdicts)).unwrap();

        assert_eq!(verdicts, [Verdict::Allow], "the backend answered");
        assert_eq!(policy.inner().calls(), 1);
    }

    /// A record carrying the row's side under the key the recipe names is
    /// compared against the watcher's own value.
    #[test]
    fn a_context_the_recipe_names_is_compared() {
        let (db, _) = translated(OWNERSHIP);
        let docs = docs_id(&db);
        let relations = vec![RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: true,
            shapes: Vec::new(),
            decision: Some(RowDecision::RequestGated {
                relation: test_names::relation("owner"),
                shapes: vec![gated_shape_on("docs", "row_owner", "owner_id")],
                context_key: "row_owner".to_string(),
                request_parameter: "app_subjects".to_string(),
                comparison: RequestComparison::CallerSetHolds,
            }),
            grants_nobody: false,
        }];
        let event = insert(docs, docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(shared(db, &relations), Named::<Principal>::default());
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        let watchers = [
            Principal::holding("user:one", &["alice"]),
            Principal::holding("user:two", &["bob"]),
        ];
        let mut verdicts = vec![Verdict::Deny; 2];
        block_on(policy.may_see(&view, &watchers, &mut verdicts)).unwrap();

        assert_eq!(verdicts, [Verdict::Allow, Verdict::Deny]);
        assert_eq!(policy.inner().calls(), 0);
    }

    /// A record carrying its side under a different key than the recipe names is
    /// refused rather than compared.
    ///
    /// The two come from one traversal upstream, so this is defence in depth.
    /// Comparing anyway would test the caller's value against whatever column
    /// the record happened to carry, which is a comparison nobody wrote.
    #[test]
    fn a_context_under_another_key_is_delegated() {
        let (db, _) = translated(OWNERSHIP);
        let docs = docs_id(&db);
        let relations = vec![RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: true,
            shapes: Vec::new(),
            decision: Some(RowDecision::RequestGated {
                relation: test_names::relation("owner"),
                shapes: vec![gated_shape_on("docs", "row_editor", "owner_id")],
                context_key: "row_owner".to_string(),
                request_parameter: "app_subjects".to_string(),
                comparison: RequestComparison::CallerSetHolds,
            }),
            grants_nobody: false,
        }];
        let event = insert(docs, docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(
            shared(db, &relations),
            Granting::<Principal>::to("user:one"),
        );
        let view = EventRow::current(&event, policy.catalog()).unwrap();

        let watchers = [Principal::holding("user:one", &["alice"])];
        let mut verdicts = vec![Verdict::Deny];
        block_on(policy.may_see(&view, &watchers, &mut verdicts)).unwrap();

        assert_eq!(verdicts, [Verdict::Allow], "the backend answered");
        assert_eq!(policy.inner().calls(), 1);
    }

    fn id_key() -> ObjectKey {
        ObjectKey::new(vec![ValueSource::typed_column(
            test_names::column("id"),
            ColumnKind::Integer,
        )])
    }

    // -----------------------------------------------------------------
    // Recipes no single table keys
    // -----------------------------------------------------------------

    /// One request-gated shape reading `table`, carrying `column`'s value as the
    /// row's side of a comparison under `key`.
    ///
    /// Its subject is the wildcard, exactly as rls2fga emits for this variant,
    /// so nothing but the context can decide it.
    fn gated_shape_on(table: &str, key: &str, column: &str) -> RecordDescription {
        RecordDescription {
            tables: vec![table.to_string()],
            derivation: RecordDerivation::FromRow {
                table: table.to_string(),
                template: Box::new(RecordTemplate {
                    object_type: table.to_string(),
                    object_key: id_key(),
                    relation: test_names::relation("owner"),
                    subject_type: "user".to_string(),
                    subject_key: SubjectKey::wildcard(),
                    context: Some(RecordContext {
                        condition: format!("when_{key}"),
                        entries: vec![RecordContextEntry {
                            key: key.to_string(),
                            value: ValueSource::column(column),
                            rendering: ContextRendering::SqlText,
                        }],
                    }),
                }),
                guards: Vec::new(),
            },
        }
    }

    /// One leaf shape reading `table`, granting `relation` to its `subject`
    /// column.
    fn shape_on(table: &str, subject: &str) -> RecordDescription {
        RecordDescription {
            tables: vec![table.to_string()],
            derivation: RecordDerivation::FromRow {
                table: table.to_string(),
                template: Box::new(RecordTemplate {
                    object_type: table.to_string(),
                    object_key: id_key(),
                    relation: test_names::relation("owner"),
                    subject_type: "user".to_string(),
                    subject_key: SubjectKey::column(subject),
                    context: None,
                }),
                guards: Vec::new(),
            },
        }
    }

    fn leaf(shapes: Vec<RecordDescription>) -> RowDecision {
        RowDecision::Leaf {
            relation: test_names::relation("owner"),
            shapes,
        }
    }

    fn can_select(decision: RowDecision) -> Vec<RelationShapes> {
        vec![RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: true,
            shapes: Vec::new(),
            decision: Some(decision),
            grants_nobody: false,
        }]
    }

    /// A recipe is keyed by the one table its leaves read, so a recipe whose
    /// leaves disagree has no key. Picking either table would evaluate it
    /// against a row from the other one.
    #[test]
    fn a_recipe_no_single_table_keys_is_delegated() {
        let joined = RecordDescription {
            tables: vec!["docs".to_string()],
            derivation: RecordDerivation::Joined {
                queries: Vec::new(),
                reason: "two rows".to_string(),
            },
        };
        let cases = [
            (
                "one leaf shape that has to be queried beside one that does not",
                leaf(vec![joined, shape_on("docs", "owner_id")]),
            ),
            (
                "two leaf shapes on different tables",
                leaf(vec![
                    shape_on("docs", "owner_id"),
                    shape_on("notes", "owner_id"),
                ]),
            ),
            (
                "two children on different tables",
                RowDecision::Any(vec![
                    leaf(vec![shape_on("docs", "owner_id")]),
                    leaf(vec![shape_on("notes", "owner_id")]),
                ]),
            ),
        ];

        for (label, decision) in cases {
            let (db, _) = translated(OWNERSHIP);
            let docs = docs_id(&db);
            let event = insert(docs, docs_row(text("alice"), Value::Null));
            let policy = RowPolicy::new(
                shared(db, &can_select(decision)),
                Delegate::granting("user:alice"),
            );

            assert!(
                !policy.answers_locally(docs, ActionStatement::Select),
                "{label}"
            );
            let got = see(&policy, &event, &watchers(&["user:alice"])).unwrap();

            assert_eq!(got, [Verdict::Allow], "{label}: the backend answered");
            assert_eq!(policy.inner().see_calls(), 1, "{label}");
        }
    }
}

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
use alloc::collections::BTreeSet;
use alloc::string::String;
use alloc::sync::Arc;

use hashbrown::HashMap;
use rls2fga::generator::records::RecordDerivation;
use rls2fga::generator::relations::{RelationShapes, RowDecision};
use rls2fga::generator::well_known::{
    CAN_DELETE_RELATION, CAN_INSERT_RELATION, CAN_SELECT_RELATION, CAN_UPDATE_RELATION,
};
use sql_traits::prelude::DatabaseLike;

use crate::catalog_helpers;
use crate::visibility::records::{is_evaluable, records_from_row_view};
use crate::visibility::{RowView, Verdict, VisibilityPolicy, WriteOp};
use crate::TableId;

// ---------------------------------------------------------------------------
// Subject
// ---------------------------------------------------------------------------
/// The principal a watcher authenticates as.
///
/// A record's subject side is a `type:key` string, so answering from the row
/// is a set-membership test and needs the watcher spelled the same way.
/// [`VisibilityPolicy::Watcher`] stays opaque to subql everywhere else.
///
/// # Why [`Cow`] and not `&str`
///
/// A consumer whose identity is a typed id rather than text has no rendered
/// subject to lend. `&str` would force it to keep one on its own type, which
/// is a design decision subql has no business making for it. Returning
/// [`Cow`] leaves the choice where it belongs: a watcher that already holds
/// the text lends it and pays nothing, and one that renders per call says so.
pub trait Subject {
    /// `type:key`, exactly as a record spells it (`user:alice`).
    fn subject(&self) -> Cow<'_, str>;
}

impl Subject for str {
    fn subject(&self) -> Cow<'_, str> {
        Cow::Borrowed(self)
    }
}

impl Subject for String {
    fn subject(&self) -> Cow<'_, str> {
        Cow::Borrowed(self)
    }
}

impl<T: Subject + ?Sized> Subject for &T {
    fn subject(&self) -> Cow<'_, str> {
        (*self).subject()
    }
}

/// A consumer whose watcher is a shared handle over its own type cannot
/// write this itself: [`Arc`] is not `#[fundamental]`, so `Arc<Local>` is
/// not a local type and the orphan rule refuses a foreign trait on it. It
/// lives here so that consumer implements [`Subject`] on its own type and
/// needs no newtype.
impl<T: Subject + ?Sized> Subject for Arc<T> {
    fn subject(&self) -> Cow<'_, str> {
        (**self).subject()
    }
}

// ---------------------------------------------------------------------------
// Action
// ---------------------------------------------------------------------------

/// Which permission a question is about.
///
/// Names the relation of the emitted model that answers it, so a read and a
/// delete consult different recipes. Answering a delete from the read recipe
/// would grant a write the model denies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    /// Reading the row.
    Select,
    /// Creating it.
    Insert,
    /// Replacing its values.
    Update,
    /// Removing it.
    Delete,
}

impl Action {
    /// The relation of the emitted model that answers this action.
    #[must_use]
    pub const fn relation(self) -> &'static str {
        match self {
            Self::Select => CAN_SELECT_RELATION,
            Self::Insert => CAN_INSERT_RELATION,
            Self::Update => CAN_UPDATE_RELATION,
            Self::Delete => CAN_DELETE_RELATION,
        }
    }

    /// The action a write question is about.
    #[must_use]
    pub const fn of(op: WriteOp) -> Self {
        match op {
            WriteOp::Insert => Self::Insert,
            WriteOp::Update => Self::Update,
            WriteOp::Delete => Self::Delete,
        }
    }

    /// The action `relation` answers, or [`None`] for a relation that is not
    /// one of the four.
    fn from_relation(relation: &str) -> Option<Self> {
        [Self::Select, Self::Insert, Self::Update, Self::Delete]
            .into_iter()
            .find(|action| action.relation() == relation)
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
/// use subql::visibility::policy::{Action, RowPolicy};
/// use subql::visibility::{EventRow, RowView, Verdict, VisibilityPolicy, WriteOp};
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
///         _row: &R,
///         _watcher: &String,
///         _op: WriteOp,
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
/// let relations = TranslatorBuilder::new()
///     .with_min_confidence(ConfidenceLevel::B)
///     .build()
///     .translate(&db)
///     .relations();
/// let policy = RowPolicy::new(db, &relations, Backend);
/// assert!(policy.answers_locally(docs, Action::Select));
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
    db: DB,
    /// Only the recipes this can actually evaluate, keyed by the table its
    /// leaves read. Absent means delegate.
    recipes: HashMap<(TableId, Action), RowDecision>,
    inner: P,
}

impl<DB: DatabaseLike, P> RowPolicy<DB, P> {
    /// Index the recipes in `relations` against the catalog `db`.
    ///
    /// Keeps a relation only when it is one of the four actions, one row
    /// decides it, every leaf shape is evaluable from a row image, and every
    /// leaf reads the one table the recipe is then keyed by. Everything else
    /// is simply absent, and an absent recipe delegates.
    #[must_use]
    pub fn new(db: DB, relations: &[RelationShapes], inner: P) -> Self {
        let mut recipes = HashMap::new();
        for entry in relations {
            let Some(decision) = entry.decision.as_ref() else {
                continue;
            };
            let Some(action) = Action::from_relation(&entry.relation) else {
                continue;
            };
            let Some(table) =
                usable_table(decision).and_then(|name| catalog_helpers::table_id(&db, name))
            else {
                continue;
            };
            recipes.insert((table, action), decision.clone());
        }
        Self { db, recipes, inner }
    }

    /// The catalog, so the caller builds its [`EventRow`](crate::visibility::EventRow)
    /// views against the same one the recipes were indexed with.
    pub const fn catalog(&self) -> &DB {
        &self.db
    }

    /// The delegate.
    pub const fn inner(&self) -> &P {
        &self.inner
    }

    /// Whether a question about `action` on a row of `table` can be answered
    /// without a round trip.
    ///
    /// Reports the recipe, not the row: a row whose cell fails to decode is
    /// still delegated.
    #[must_use]
    pub fn answers_locally(&self, table: TableId, action: Action) -> bool {
        self.recipes.contains_key(&(table, action))
    }

    /// The subjects this row grants `action` to, or [`None`] to delegate.
    fn granted<R>(&self, action: Action, row: &R) -> Option<BTreeSet<String>>
    where
        R: RowView + ?Sized,
    {
        let decision = self.recipes.get(&(row.table_id(), action))?;
        subjects(decision, row, &self.db)
    }
}

/// The one table every leaf of `decision` reads, or [`None`] when the recipe
/// cannot be answered from a row image at all.
///
/// Refuses a composition over no children, because folding an intersection
/// of nothing yields every subject, which is the one error direction that
/// matters. It names no table either, so both fall out of the same walk
/// rather than needing a guard that says so twice.
///
/// A recipe shape this does not recognise, `RequestGated` among them, falls
/// to the wildcard and delegates. That is the whole protection against a
/// composition a later rls2fga adds: a grant the request completes is not
/// settled by the row, and answering it here would grant on the row alone.
fn usable_table(decision: &RowDecision) -> Option<&str> {
    let mut table: Option<&str> = None;
    match decision {
        RowDecision::Leaf { shapes, .. } => {
            for shape in shapes {
                if !is_evaluable(shape) {
                    return None;
                }
                let RecordDerivation::FromRow { table: name, .. } = &shape.derivation else {
                    return None;
                };
                if *table.get_or_insert(name) != name.as_str() {
                    return None;
                }
            }
        }
        RowDecision::Any(children) | RowDecision::All(children) => {
            for child in children {
                let name = usable_table(child)?;
                if *table.get_or_insert(name) != name {
                    return None;
                }
            }
        }
        // `RowDecision` is `#[non_exhaustive]`: a composition this does not
        // understand is delegated rather than guessed at.
        _ => return None,
    }
    table
}

/// Evaluate `decision` against `row`: a leaf is the subjects its shapes
/// yield, [`RowDecision::Any`] their union and [`RowDecision::All`] their
/// intersection. [`None`] means the row could not be read, never that it
/// granted nobody.
fn subjects<R, DB>(decision: &RowDecision, row: &R, db: &DB) -> Option<BTreeSet<String>>
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    match decision {
        RowDecision::Leaf { shapes, .. } => {
            let mut out = BTreeSet::new();
            for shape in shapes {
                for record in records_from_row_view(shape, row, db).ok()? {
                    out.insert(record.subject);
                }
            }
            Some(out)
        }
        RowDecision::Any(children) => {
            let mut out = BTreeSet::new();
            for child in children {
                out.extend(subjects(child, row, db)?);
            }
            Some(out)
        }
        RowDecision::All(children) => {
            let mut out: Option<BTreeSet<String>> = None;
            for child in children {
                let next = subjects(child, row, db)?;
                match out.as_mut() {
                    Some(kept) => kept.retain(|subject| next.contains(subject)),
                    None => out = Some(next),
                }
            }
            out
        }
        _ => None,
    }
}

impl<DB, P> VisibilityPolicy for RowPolicy<DB, P>
where
    DB: DatabaseLike + Send + Sync,
    P: VisibilityPolicy,
    P::Watcher: Subject,
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
        // Reading the row happens here rather than in the block, so a
        // locally answered event never suspends.
        let answered = self.granted(Action::Select, row).inspect(|granted| {
            for (watcher, verdict) in watchers.iter().zip(verdicts.iter_mut()) {
                if granted.contains(watcher.subject().as_ref()) {
                    *verdict = Verdict::Allow;
                }
            }
        });
        async move {
            if answered.is_some() {
                return Ok(());
            }
            self.inner.may_see(row, watchers, verdicts).await
        }
    }

    fn may_write<R>(
        &self,
        row: &R,
        watcher: &Self::Watcher,
        op: WriteOp,
    ) -> impl core::future::Future<Output = Result<Verdict, Self::Error>> + Send
    where
        R: RowView<Backend = Self::Backend> + Sync + ?Sized,
    {
        let answered = self.granted(Action::of(op), row).map(|granted| {
            if granted.contains(watcher.subject().as_ref()) {
                Verdict::Allow
            } else {
                Verdict::Deny
            }
        });
        async move {
            match answered {
                Some(verdict) => Ok(verdict),
                None => self.inner.may_write(row, watcher, op).await,
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloc::borrow::Cow;
    use alloc::string::{String, ToString};
    use alloc::sync::Arc;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::future::Future;
    use core::pin::{pin, Pin};
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::task::{Context, Poll, Waker};

    use rls2fga::classifier::patterns::ConfidenceLevel;
    use rls2fga::generator::records::{
        ObjectKey, RecordDerivation, RecordDescription, RecordTemplate, SubjectKey, ValueSource,
    };
    use rls2fga::generator::relations::{RelationShapes, RowDecision};
    use rls2fga::translator::TranslatorBuilder;
    use sqlparser::dialect::PostgreSqlDialect;

    use super::{Action, RowPolicy, Subject};
    use crate::backend::{Postgres, Value};
    use crate::testing::TestEvent;
    use crate::visibility::{EventRow, RowView, Verdict, VisibilityPolicy, WriteOp};
    use crate::{catalog_helpers, ColumnId, ParserDB, TableId, ValueError};
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
            _row: &R,
            watcher: &String,
            _op: WriteOp,
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

    fn translated(sql: &str) -> (ParserDB, Vec<RelationShapes>) {
        let db = ParserDB::parse::<PostgreSqlDialect>(sql).unwrap();
        let relations = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build()
            .translate(&db)
            .relations();
        (db, relations)
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
        let policy = RowPolicy::new(db, &relations, Delegate::default());

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
        let policy = RowPolicy::new(db, &relations, Delegate::granting("user:bob"));

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
        let policy = RowPolicy::new(db, &relations, Delegate::granting("user:alice"));

        assert!(!policy.answers_locally(docs, Action::Select));
        let got = see(&policy, &event, &watchers(&["user:alice"])).unwrap();

        assert_eq!(got, [Verdict::Allow], "the backend answered");
        assert_eq!(policy.inner().see_calls(), 1);
    }

    /// A guard that fails yields no subject, which is a local deny rather
    /// than a question.
    #[test]
    fn a_row_whose_grant_column_is_null_is_denied_without_a_round_trip() {
        let (db, relations) = translated(OWNERSHIP);
        let event = insert(docs_id(&db), docs_row(Value::Null, Value::Null));
        let policy = RowPolicy::new(db, &relations, Delegate::granting("user:alice"));

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
        let policy = RowPolicy::new(db, &relations, Delegate::default());

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
        let policy = RowPolicy::new(db, &relations, Delegate::default());

        let got = see(&policy, &event, &watchers(&["user:alice", "user:bob"])).unwrap();

        assert_eq!(got, [Verdict::Deny, Verdict::Deny]);
        assert_eq!(policy.inner().see_calls(), 0);
    }

    /// The same row naming one user on both sides satisfies the intersection.
    #[test]
    fn an_intersection_recipe_allows_a_watcher_named_by_every_leaf() {
        let (db, relations) = translated(INTERSECTION);
        let event = insert(docs_id(&db), docs_row(text("alice"), text("alice")));
        let policy = RowPolicy::new(db, &relations, Delegate::default());

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
            type_name: "docs".to_string(),
            relation: "can_select".to_string(),
            from_one_row: true,
            shapes: Vec::new(),
            decision: Some(RowDecision::All(Vec::new())),
        }];
        let event = insert(docs, docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(db, &relations, Delegate::granting("user:alice"));

        assert!(!policy.answers_locally(docs, Action::Select));
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
            let policy = RowPolicy::new(db, &relations, Delegate::granting("user:7"));
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
            let policy = RowPolicy::new(db, &relations, Delegate::default());
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

    /// A write question reads the write relation. `can_select` is decidable
    /// here and `can_delete` is not, so answering a delete from the read
    /// recipe would grant a delete the model denies.
    #[test]
    fn a_write_question_reads_the_write_relation_not_the_read_one() {
        let (db, relations) = translated(OWNERSHIP);
        let docs = docs_id(&db);
        let event = insert(docs, docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(db, &relations, Delegate::default());

        assert!(policy.answers_locally(docs, Action::Select));
        assert!(!policy.answers_locally(docs, Action::Delete));

        let view = EventRow::current(&event, policy.catalog()).unwrap();
        let got =
            block_on(policy.may_write(&view, &"user:alice".to_string(), WriteOp::Delete)).unwrap();

        assert_eq!(got, Verdict::Deny);
        assert_eq!(policy.inner().write_calls(), 1, "the backend was asked");
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
        let policy = RowPolicy::new(catalog, &relations, Delegate::granting("user:alice"));

        assert!(!policy.answers_locally(notes, Action::Select));
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
            type_name: "docs".to_string(),
            relation: "can_select".to_string(),
            from_one_row: true,
            shapes: Vec::new(),
            decision: Some(RowDecision::Leaf {
                relation: "members".to_string(),
                shapes: vec![RecordDescription {
                    tables: vec!["docs".to_string()],
                    derivation: RecordDerivation::FromRow {
                        table: "docs".to_string(),
                        template: Box::new(RecordTemplate {
                            object_type: "docs".to_string(),
                            object_key: ObjectKey::column("id"),
                            relation: "members".to_string(),
                            subject_type: "user".to_string(),
                            subject_key: SubjectKey::new(ValueSource::ListElements(
                                "owner_id".to_string(),
                            )),
                            context: None,
                        }),
                        guards: Vec::new(),
                    },
                }],
            }),
        }];
        let event = insert(docs, docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(db, &relations, Delegate::granting("user:alice"));

        assert!(!policy.answers_locally(docs, Action::Select));
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
                Err(ValueError {
                    column: col,
                    kind: crate::backend::ScalarKind::String,
                })
            }
        }

        let (db, relations) = translated(OWNERSHIP);
        let docs = docs_id(&db);
        let policy = RowPolicy::new(db, &relations, Delegate::granting("user:alice"));
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
        let policy = RowPolicy::new(db, &relations, Delegate::default());

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
        let policy = RowPolicy::new(db, &relations, Delegate::failing());
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
        let policy = RowPolicy::new(db, &relations, Delegate::default());
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
        let policy = RowPolicy::new(db, &relations, Delegate::granting("user:bob"));

        assert!(policy.answers_locally(docs, Action::Delete));
        let view = event_view(&policy, &event);

        let allowed = block_on(policy.may_write(&view, &"user:alice".to_string(), WriteOp::Delete));
        let denied = block_on(policy.may_write(&view, &"user:bob".to_string(), WriteOp::Delete));

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
        let policy = RowPolicy::new(db, &relations, Delegate::default());
        let view = event_view(&policy, &event);

        let mut verdicts = vec![Verdict::Deny];
        block_on(policy.may_see(&view, &watchers(&["user:alice"]), &mut verdicts)).unwrap();

        assert_eq!(verdicts, [Verdict::Allow], "alice owns it, so she reads it");
        assert_eq!(
            block_on(policy.may_write(&view, &"user:alice".to_string(), WriteOp::Delete)),
            Ok(Verdict::Deny),
            "she is not the editor, so she does not delete it"
        );
    }

    /// Each verb reads its own relation. Swapping two of them would answer
    /// an insert from the update rules.
    #[test]
    fn every_write_verb_reads_its_own_relation() {
        assert_eq!(Action::Select.relation(), "can_select");
        assert_eq!(Action::of(WriteOp::Insert).relation(), "can_insert");
        assert_eq!(Action::of(WriteOp::Update).relation(), "can_update");
        assert_eq!(Action::of(WriteOp::Delete).relation(), "can_delete");
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
                _row: &R,
                _watcher: &&'static str,
                _op: WriteOp,
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
        let policy = RowPolicy::new(db, &relations, Borrowed::default());
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
        struct Typed(u64);

        impl Subject for Typed {
            fn subject(&self) -> Cow<'_, str> {
                Cow::Owned(alloc::format!("user:{}", self.0))
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
                _row: &R,
                _watcher: &Typed,
                _op: WriteOp,
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
        let policy = RowPolicy::new(db, &relations, Backend::default());
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
                _row: &R,
                _watcher: &Arc<String>,
                _op: WriteOp,
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
        let policy = RowPolicy::new(db, &relations, Shared::default());
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
    // Recipes this does not recognise
    // -----------------------------------------------------------------

    /// A grant the caller's own request completes is not settled by the row,
    /// so it must reach the backend even though it arrives as a recipe.
    ///
    /// `RowDecision` is `#[non_exhaustive]` and gained this variant after
    /// `RowPolicy` was written. The wildcard is what kept that safe, and
    /// this is the test that says so, because the next variant will arrive
    /// the same way.
    #[test]
    fn a_recipe_the_request_completes_is_delegated() {
        let (db, _) = translated(OWNERSHIP);
        let docs = docs_id(&db);
        let relations = vec![RelationShapes {
            type_name: "docs".to_string(),
            relation: "can_select".to_string(),
            from_one_row: true,
            shapes: Vec::new(),
            decision: Some(RowDecision::RequestGated {
                relation: "gated".to_string(),
                shapes: vec![shape_on("docs", "owner_id")],
                context_key: "row_department".to_string(),
                request_parameter: "department".to_string(),
                comparison: RequestComparison::CallerValueEquals,
            }),
        }];
        let event = insert(docs, docs_row(text("alice"), Value::Null));
        let policy = RowPolicy::new(db, &relations, Delegate::granting("user:alice"));

        assert!(!policy.answers_locally(docs, Action::Select));
        let got = see(&policy, &event, &watchers(&["user:alice"])).unwrap();

        assert_eq!(got, [Verdict::Allow], "the backend answered");
        assert_eq!(policy.inner().see_calls(), 1);
    }

    // -----------------------------------------------------------------
    // Recipes no single table keys
    // -----------------------------------------------------------------

    /// One leaf shape reading `table`, granting `relation` to its `subject`
    /// column.
    fn shape_on(table: &str, subject: &str) -> RecordDescription {
        RecordDescription {
            tables: vec![table.to_string()],
            derivation: RecordDerivation::FromRow {
                table: table.to_string(),
                template: Box::new(RecordTemplate {
                    object_type: table.to_string(),
                    object_key: ObjectKey::column("id"),
                    relation: "owner".to_string(),
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
            relation: "owner".to_string(),
            shapes,
        }
    }

    fn can_select(decision: RowDecision) -> Vec<RelationShapes> {
        vec![RelationShapes {
            type_name: "docs".to_string(),
            relation: "can_select".to_string(),
            from_one_row: true,
            shapes: Vec::new(),
            decision: Some(decision),
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
            let policy =
                RowPolicy::new(db, &can_select(decision), Delegate::granting("user:alice"));

            assert!(!policy.answers_locally(docs, Action::Select), "{label}");
            let got = see(&policy, &event, &watchers(&["user:alice"])).unwrap();

            assert_eq!(got, [Verdict::Allow], "{label}: the backend answered");
            assert_eq!(policy.inner().see_calls(), 1, "{label}");
        }
    }
}

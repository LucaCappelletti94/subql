//! Whether one changed row enters, leaves or stays outside each
//! watcher's reach.
//!
//! [`VisibilityPolicy`] answers about one version of a row. A change
//! stream carries two, and the difference between them is what a client
//! has to be told: a row that moved out of reach must be taken back, not
//! silently dropped, or the client keeps data it may no longer see.
//!
//! This is the same shape subql already computes for a subscription
//! predicate, one layer up.
//!
//! # The current version is asked first, and often alone
//!
//! A watcher who can see the row as it is now receives it, whatever was
//! true before, so the previous version is consulted only when the
//! current one is absent or invisible to somebody. An event every
//! watcher can see costs one call. An event nobody can see costs two.
//! Neither grows with the size of the audience.
//!
//! The second call asks about every watcher rather than only the denied
//! ones, because [`VisibilityPolicy::may_see`] takes a contiguous slice
//! and [`Watcher`](VisibilityPolicy::Watcher) carries no `Clone` bound to
//! build a subset with. That costs extra questions inside one call, never
//! extra calls.
//!
//! # Withdrawing is the safe direction
//!
//! [`Transitions::reset`] pre-fills [`Transition::Withdraw`], and the
//! combination only ever upgrades out of it. So a call that fails partway
//! leaves every watcher it did not reach withdrawing, and a caller that
//! ignores the error cannot leak a row. A client that receives a removal
//! for a row it never held applies a no-op, while the opposite assumption
//! leaves it holding a row it may no longer see.
//!
//! # What this cannot see
//!
//! Under Postgres `REPLICA IDENTITY DEFAULT` an update carries no old
//! image, and nothing in the event distinguishes that from an old image
//! whose values the policy genuinely refuses. Such a row is reported as
//! [`Transition::Nothing`] and the watcher is never told it lost access.
//! Only `REPLICA IDENTITY FULL` closes that, which is why subql ships
//! [`REPLICA_IDENTITY_AUDIT_SQL`](crate::REPLICA_IDENTITY_AUDIT_SQL).

use alloc::vec::Vec;

use sql_traits::prelude::DatabaseLike;

use crate::backend::{CdcEvent, Value};
use crate::visibility::{EventRow, RowView, Verdict, VisibilityPolicy};
use crate::{ColumnId, EventKind};

/// What one watcher must be told about one changed row.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Transition {
    /// The watcher could not see the row before and cannot see it now.
    /// Telling it anything would disclose that the row exists.
    Nothing,
    /// The watcher may see the row as it is now.
    Deliver,
    /// The watcher could see the row and cannot any more, so its copy has
    /// to be taken back. The default, so an unanswered question never
    /// leaks.
    #[default]
    Withdraw,
}

/// Why a transition could not be computed.
///
/// Distinct from a policy that answered "denied": each of these means the
/// question was never put, so a caller must not read the buffer as an
/// answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransitionError<E> {
    /// The event names no row, which today means a truncate. Reporting
    /// [`Transition::Nothing`] for it would read as "no watcher is
    /// affected", the opposite of what a truncate does.
    NotARowEvent,
    /// The event's table is not in the catalog, so no version of the row
    /// can be read.
    UnknownTable,
    /// The previous version of the row carries its key and nothing else,
    /// so whether a watcher could see it before is not knowable from this
    /// event.
    ///
    /// Postgres produces that under `REPLICA IDENTITY DEFAULT`. Reading
    /// it as [`Transition::Nothing`] would drop the row from a watcher's
    /// copy without telling it, so the question is refused instead. See
    /// [`REPLICA_IDENTITY_AUDIT_SQL`](crate::REPLICA_IDENTITY_AUDIT_SQL)
    /// for catching a misconfigured table once at startup rather than
    /// per event.
    IncompletePreviousImage,
    /// The policy could not reach an answer.
    Policy(E),
}

/// Scratch for [`transitions`], kept across events so no event allocates.
#[derive(Clone, Debug, Default)]
pub struct Transitions {
    current: Vec<Verdict>,
    previous: Vec<Verdict>,
    out: Vec<Transition>,
}

impl Transitions {
    /// An empty buffer. Size it with [`reset`](Self::reset) before use.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            current: Vec::new(),
            previous: Vec::new(),
            out: Vec::new(),
        }
    }

    /// Prepare to answer `watchers` questions, fail-closed.
    ///
    /// Sizes every buffer to exactly `watchers` entries, pre-fills the
    /// verdicts with [`Verdict::Deny`] and the transitions with
    /// [`Transition::Withdraw`]. A stale answer never survives.
    pub fn reset(&mut self, watchers: usize) {
        Verdict::reset(&mut self.current, watchers);
        Verdict::reset(&mut self.previous, watchers);
        self.out.clear();
        self.out.resize(watchers, Transition::Withdraw);
    }

    /// One transition per watcher, positionally.
    #[must_use]
    pub fn get(&self) -> &[Transition] {
        &self.out
    }
}

/// Decide, for each watcher, what `event` does to its copy of the row.
///
/// Writes one [`Transition`] per watcher into `buffers`, positionally.
/// Size the buffer with [`Transitions::reset`] first.
///
/// # Errors
///
/// [`TransitionError::NotARowEvent`] for a truncate,
/// [`TransitionError::UnknownTable`] when the catalog does not know the
/// event's table, and [`TransitionError::Policy`] when the policy could
/// not answer. In every case the buffer keeps whatever it had reached,
/// with the rest still withdrawing.
pub async fn transitions<P, E, DB>(
    policy: &P,
    event: &E,
    db: &DB,
    watchers: &[P::Watcher],
    buffers: &mut Transitions,
) -> Result<(), TransitionError<P::Error>>
where
    P: VisibilityPolicy,
    E: CdcEvent<Backend = P::Backend> + Sync,
    DB: DatabaseLike,
{
    if event.kind() == EventKind::Truncate {
        return Err(TransitionError::NotARowEvent);
    }
    let current = EventRow::current(event, db);
    let previous = EventRow::previous(event, db);
    if current.is_none() && previous.is_none() {
        return Err(TransitionError::UnknownTable);
    }

    // A delete carries no current version, so its pre-filled denials
    // already say nobody can see the row as it is now.
    if let Some(row) = &current {
        policy
            .may_see(row, watchers, &mut buffers.current)
            .await
            .map_err(TransitionError::Policy)?;
    }
    for (verdict, out) in buffers.current.iter().zip(buffers.out.iter_mut()) {
        if verdict.allowed() {
            *out = Transition::Deliver;
        }
    }
    if buffers.current.iter().all(|v| v.allowed()) {
        return Ok(());
    }

    // An insert has no previous version because the row did not
    // exist, which is not the same as an image the event failed to
    // carry: nobody can lose what they never had.
    if let Some(row) = &previous {
        if is_key_only(row, event, db) {
            return Err(TransitionError::IncompletePreviousImage);
        }
        policy
            .may_see(row, watchers, &mut buffers.previous)
            .await
            .map_err(TransitionError::Policy)?;
    }
    for ((current, previous), out) in buffers
        .current
        .iter()
        .zip(buffers.previous.iter())
        .zip(buffers.out.iter_mut())
    {
        if !current.allowed() && !previous.allowed() {
            *out = Transition::Nothing;
        }
    }
    Ok(())
}

/// Whether `row` carries its key and nothing else.
///
/// Postgres under `REPLICA IDENTITY DEFAULT` emits exactly that for an
/// update or a delete, and it is indistinguishable from a complete image
/// only if you ignore [`Value::Missing`], which means "the source did not
/// carry this cell" and is a different variant from SQL NULL.
///
/// One absent column beside a present one is not key-only: a single
/// unchanged large value can go unsent under `FULL`, and refusing that
/// would refuse a correctly configured database. A table whose every
/// column is part of the key has nothing to be missing, so it is never
/// key-only.
///
/// Stops at the first non-key cell the row does carry, so a correctly
/// configured table pays one read.
pub(crate) fn is_key_only<R, E, DB>(row: &R, event: &E, db: &DB) -> bool
where
    R: RowView,
    E: CdcEvent,
    DB: DatabaseLike,
{
    // Unreachable through `transitions`, which resolves the table before
    // building either view. Answering "complete" here would send the
    // caller on to judge an image it cannot measure, and a wrong answer
    // in that direction is a row dropped without notice.
    let Ok(arity) = crate::catalog_helpers::table_arity(db, row.table_id()) else {
        return true;
    };
    let keys = event.pk_columns(db);
    let mut non_key = (0..arity)
        .filter_map(|ordinal| ColumnId::try_from(ordinal).ok())
        .filter(|ordinal| !keys.contains(ordinal))
        .peekable();
    // An all-key table has nothing that could be missing, so it is
    // complete by construction. `all` on an empty iterator is `true`,
    // which would refuse it.
    if non_key.peek().is_none() {
        return false;
    }
    non_key.all(|ordinal| matches!(row.value_at(ordinal), Ok(Value::Missing)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloc::vec;
    use alloc::vec::Vec;
    use core::future::Future;
    use core::pin::{pin, Pin};
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::task::{Context, Poll, Waker};

    use sqlparser::dialect::PostgreSqlDialect;

    use super::{transitions, Transition, TransitionError, Transitions};
    use crate::backend::{Postgres, Value};
    use crate::testing::TestEvent;
    use crate::visibility::{RowView, RowWrite, Verdict, VisibilityPolicy};
    use crate::{catalog_helpers, ParserDB, TableId};

    // Harness

    fn block_on<F: Future>(fut: F) -> F::Output {
        let mut ctx = Context::from_waker(Waker::noop());
        let mut pinned = pin!(fut);
        loop {
            if let Poll::Ready(v) = pinned.as_mut().poll(&mut ctx) {
                return v;
            }
        }
    }

    /// Returns `Pending` once so the policy genuinely suspends, which is
    /// what every real implementation does on its round trip.
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

    /// Grants the row to the watcher its `owner` column names, and counts
    /// how many times it was asked, which is how the tests prove the
    /// previous image is consulted only when it has to be.
    #[derive(Default)]
    struct OwnerPolicy {
        calls: AtomicUsize,
        fail: bool,
    }

    impl OwnerPolicy {
        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    fn owner_of<R: RowView<Backend = Postgres> + ?Sized>(row: &R) -> Option<i64> {
        match row.value_at(1) {
            Ok(Value::Int(id)) => Some(id),
            _ => None,
        }
    }

    impl VisibilityPolicy for OwnerPolicy {
        type Watcher = i64;
        type Error = Unreachable;
        type Backend = Postgres;

        fn may_see<R>(
            &self,
            row: &R,
            watchers: &[i64],
            verdicts: &mut [Verdict],
        ) -> impl Future<Output = Result<(), Unreachable>> + Send
        where
            R: RowView<Backend = Postgres> + Sync + ?Sized,
        {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let owner = owner_of(row);
            let fail = self.fail;
            async move {
                YieldOnce(false).await;
                if fail {
                    return Err(Unreachable);
                }
                for (watcher, verdict) in watchers.iter().zip(verdicts.iter_mut()) {
                    if owner == Some(*watcher) {
                        *verdict = Verdict::Allow;
                    }
                }
                Ok(())
            }
        }

        fn may_write<R>(
            &self,
            _write: RowWrite<'_, R>,
            _watcher: &i64,
        ) -> impl Future<Output = Result<Verdict, Unreachable>> + Send
        where
            R: RowView<Backend = Postgres> + Sync + ?Sized,
        {
            // A statement before the block keeps this the same shape as
            // the read path: work happens eagerly, the await does not.
            let verdict = Verdict::Deny;
            async move {
                YieldOnce(false).await;
                Ok(verdict)
            }
        }
    }

    fn catalog() -> (ParserDB, TableId) {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE docs (id INT PRIMARY KEY, owner INT);",
        )
        .unwrap();
        let docs = catalog_helpers::table_id(&db, "docs").unwrap();
        (db, docs)
    }

    fn row(id: i64, owner: i64) -> Vec<Value<Postgres>> {
        vec![Value::Int(id), Value::Int(owner)]
    }

    /// Run one event past `watchers` and read the transitions back.
    fn run(
        policy: &OwnerPolicy,
        event: &TestEvent<Postgres>,
        db: &ParserDB,
        watchers: &[i64],
    ) -> Result<Vec<Transition>, TransitionError<Unreachable>> {
        let mut buffers = Transitions::new();
        buffers.reset(watchers.len());
        block_on(transitions(policy, event, db, watchers, &mut buffers))?;
        Ok(buffers.get().to_vec())
    }

    // The four cases

    /// A watcher who can see the row as it is now receives it, and the
    /// previous version is never consulted, which is the whole cost
    /// argument: one question, not two.
    #[test]
    fn visible_now_delivers_without_asking_about_before() {
        let (db, docs) = catalog();
        let event = TestEvent::update(docs, row(1, 7), row(1, 7)).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[7]).unwrap(),
            [Transition::Deliver]
        );
        assert_eq!(policy.calls(), 1, "the previous image must not be asked");
    }

    /// The confidentiality defect this requirement exists to close: the
    /// row moved out of the watcher's reach, so it has to be taken back
    /// rather than silently dropped.
    #[test]
    fn losing_access_withdraws_the_row() {
        let (db, docs) = catalog();
        let event = TestEvent::update(docs, row(1, 7), row(1, 9)).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[7]).unwrap(),
            [Transition::Withdraw]
        );
        assert_eq!(policy.calls(), 2, "the previous image decides this one");
    }

    /// A watcher who could never see the row is not told anything, which
    /// is what stops a deletion disclosing a key to everybody.
    #[test]
    fn never_had_access_says_nothing() {
        let (db, docs) = catalog();
        let event = TestEvent::update(docs, row(1, 7), row(1, 9)).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[42]).unwrap(),
            [Transition::Nothing]
        );
    }

    /// A delete has no current version, so the previous one decides. The
    /// owner is told, and nobody else is.
    #[test]
    fn a_delete_withdraws_only_from_who_could_see_it() {
        let (db, docs) = catalog();
        let event = TestEvent::delete(docs, row(1, 7)).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[7, 42]).unwrap(),
            [Transition::Withdraw, Transition::Nothing]
        );
    }

    /// An insert has no previous version because the row did not exist,
    /// which is different from an image the event failed to carry. A
    /// watcher who cannot see it is told nothing rather than sent a
    /// removal for a row nobody ever held.
    #[test]
    fn an_insert_never_withdraws() {
        let (db, docs) = catalog();
        let event = TestEvent::insert(docs, row(1, 7)).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[7, 42]).unwrap(),
            [Transition::Deliver, Transition::Nothing]
        );
        assert_eq!(policy.calls(), 1, "there is no previous image to ask about");
    }

    // Cost

    /// One call covers every watcher who can see the current row. The
    /// second call happens once, not once per denied watcher.
    #[test]
    fn a_mixed_audience_costs_exactly_two_calls() {
        let (db, docs) = catalog();
        let event = TestEvent::update(docs, row(1, 7), row(1, 9)).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        let watchers: Vec<i64> = (0..64).collect();
        let got = run(&policy, &event, &db, &watchers).unwrap();
        assert_eq!(policy.calls(), 2, "must not grow with the audience");
        assert_eq!(got[9], Transition::Deliver);
        assert_eq!(got[7], Transition::Withdraw);
        assert_eq!(got[42], Transition::Nothing);
    }

    /// One client with two subscriptions over one table is two watchers,
    /// and they stay two answers.
    #[test]
    fn duplicate_watchers_keep_independent_transitions() {
        let (db, docs) = catalog();
        let event = TestEvent::update(docs, row(1, 7), row(1, 9)).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[7, 9, 7]).unwrap(),
            [
                Transition::Withdraw,
                Transition::Deliver,
                Transition::Withdraw
            ]
        );
    }

    // Refusals and failure

    /// A truncate names no row, so there is nothing to judge. Answering
    /// `Nothing` would read as "no watcher is affected", which is the
    /// opposite of what a truncate does.
    #[test]
    fn a_truncate_is_refused_rather_than_answered() {
        let (db, docs) = catalog();
        let event = TestEvent::<Postgres>::truncate(docs);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[7]),
            Err(TransitionError::NotARowEvent)
        );
        assert_eq!(policy.calls(), 0);
    }

    /// A table the catalog does not know is refused, not read as a row
    /// nobody may see.
    #[test]
    fn an_unknown_table_is_refused() {
        let (db, _) = catalog();
        let event = TestEvent::insert(TableId::MAX, row(1, 7)).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[7]),
            Err(TransitionError::UnknownTable)
        );
    }

    /// A policy that cannot answer surfaces as an error, and the buffer
    /// it leaves behind withdraws rather than delivers, so a caller that
    /// ignores the error cannot leak a row.
    #[test]
    fn a_failing_policy_errors_and_leaves_the_buffer_fail_closed() {
        let (db, docs) = catalog();
        let event: TestEvent<_> =
            TestEvent::update(docs, row(1, 7), row(1, 7)).with_pk_columns([0u16]);
        let policy = OwnerPolicy {
            fail: true,
            ..OwnerPolicy::default()
        };
        let watchers = [7i64, 9];
        let mut buffers = Transitions::new();
        buffers.reset(watchers.len());
        let outcome = block_on(transitions(&policy, &event, &db, &watchers, &mut buffers));
        assert_eq!(outcome, Err(TransitionError::Policy(Unreachable)));
        assert_eq!(
            buffers.get(),
            [Transition::Withdraw, Transition::Withdraw],
            "an unanswered question must never deliver"
        );
    }

    /// The scratch buffer is meant to be kept across events, so a stale
    /// answer must not survive a reset.
    #[test]
    fn reset_clears_a_previous_events_answers() {
        let (db, docs) = catalog();
        let policy = OwnerPolicy::default();
        let mut buffers = Transitions::new();

        let first: TestEvent<_> = TestEvent::insert(docs, row(1, 7)).with_pk_columns([0u16]);
        buffers.reset(1);
        block_on(transitions(&policy, &first, &db, &[7], &mut buffers)).unwrap();
        assert_eq!(buffers.get(), [Transition::Deliver]);

        let second: TestEvent<_> = TestEvent::insert(docs, row(2, 9)).with_pk_columns([0u16]);
        buffers.reset(1);
        block_on(transitions(&policy, &second, &db, &[7], &mut buffers)).unwrap();
        assert_eq!(buffers.get(), [Transition::Nothing], "no stale Deliver");
    }

    /// Sizing the buffer to a different audience must not carry answers
    /// across, and must not panic.
    #[test]
    fn reset_resizes_between_events() {
        let mut buffers = Transitions::new();
        buffers.reset(3);
        assert_eq!(buffers.get().len(), 3);
        buffers.reset(1);
        assert_eq!(buffers.get(), [Transition::Withdraw]);
        buffers.reset(0);
        assert!(
            buffers.get().is_empty(),
            "reset to zero produces an empty transition buffer"
        );
    }

    // An old image that cannot be judged

    /// Under Postgres `REPLICA IDENTITY DEFAULT` an update's old image
    /// carries the key and nothing else, so whether the watcher could see
    /// the row before is not knowable from the event. Reporting
    /// [`Transition::Nothing`] there loses the row from the watcher's copy
    /// without telling it, so the question is refused instead.
    #[test]
    fn a_key_only_old_image_is_refused_rather_than_read_as_invisible() {
        let (db, docs) = catalog();
        let key_only = vec![Value::Int(1), Value::Missing];
        let event = TestEvent::update(docs, key_only, row(1, 9)).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[7]),
            Err(TransitionError::IncompletePreviousImage)
        );
    }

    /// A delete carries the same key-only image under `DEFAULT`, and is
    /// refused for the same reason. Forwarding it regardless would
    /// disclose the deleted row's key to every watcher.
    #[test]
    fn a_key_only_delete_is_refused() {
        let (db, docs) = catalog();
        let event =
            TestEvent::delete(docs, vec![Value::Int(1), Value::Missing]).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[7]),
            Err(TransitionError::IncompletePreviousImage)
        );
    }

    /// The check must not fire on a complete image, which is the whole
    /// population of a correctly configured database.
    #[test]
    fn a_complete_old_image_is_judged_normally() {
        let (db, docs) = catalog();
        let event = TestEvent::update(docs, row(1, 7), row(1, 9)).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[7]).unwrap(),
            [Transition::Withdraw]
        );
    }

    /// A row nobody has lost still costs nothing: the old image is never
    /// looked at, so its shape cannot refuse an event that needs no
    /// withdrawal.
    #[test]
    fn a_key_only_old_image_does_not_refuse_when_nobody_lost_access() {
        let (db, docs) = catalog();
        let key_only = vec![Value::Int(1), Value::Missing];
        let event = TestEvent::update(docs, key_only, row(1, 9)).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[9]).unwrap(),
            [Transition::Deliver]
        );
        assert_eq!(policy.calls(), 1);
    }

    /// A single unchanged large column is absent from an otherwise
    /// complete image, which is not the same thing as a key-only image
    /// and must not be refused.
    #[test]
    fn one_absent_column_beside_a_present_one_is_not_key_only() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE docs (id INT PRIMARY KEY, owner INT, body TEXT);",
        )
        .unwrap();
        let docs = catalog_helpers::table_id(&db, "docs").unwrap();
        let partial = vec![Value::Int(1), Value::Int(7), Value::Missing];
        let after = vec![Value::Int(1), Value::Int(9), Value::Missing];
        let event = TestEvent::update(docs, partial, after).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[7]).unwrap(),
            [Transition::Withdraw]
        );
    }

    /// A table that is all key has no non-key column to be missing, so
    /// its old image is complete by construction and must not be refused.
    #[test]
    fn an_all_key_table_is_never_refused() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE edges (src INT, dst INT, PRIMARY KEY (src, dst));",
        )
        .unwrap();
        let edges = catalog_helpers::table_id(&db, "edges").unwrap();
        let event = TestEvent::delete(edges, vec![Value::Int(1), Value::Int(2)])
            .with_pk_columns([0u16, 1u16]);
        let policy = OwnerPolicy::default();
        assert_eq!(
            run(&policy, &event, &db, &[7]).unwrap(),
            [Transition::Nothing]
        );
    }

    /// The signature no longer spells `+ Send`, so a hold across the
    /// await that is not `Send` would silently make this undrivable from
    /// a multi-threaded runtime. Pin it here instead.
    #[test]
    fn the_returned_future_is_send() {
        const fn assert_send<T: Send>(_: &T) {}
        let (db, docs) = catalog();
        let event: TestEvent<_> = TestEvent::insert(docs, row(1, 7)).with_pk_columns([0u16]);
        let policy = OwnerPolicy::default();
        let watchers = [7i64];
        let mut buffers = Transitions::new();
        buffers.reset(1);
        let future = transitions(&policy, &event, &db, &watchers, &mut buffers);
        assert_send(&future);
        block_on(future).unwrap();
        assert_eq!(buffers.get(), [Transition::Deliver]);
    }

    /// A view whose table the catalog cannot measure is treated as
    /// key-only, because "complete" would send the caller on to judge an
    /// image whose shape is unknown. `transitions` resolves the table
    /// first so this cannot be reached through it, which is exactly why
    /// the direction needs pinning here.
    #[test]
    fn an_unmeasurable_table_is_treated_as_key_only() {
        struct Detached;
        impl RowView for Detached {
            type Backend = Postgres;
            fn table_id(&self) -> TableId {
                TableId::MAX
            }
            fn value_at(&self, _: crate::ColumnId) -> Result<Value<Postgres>, crate::ValueError> {
                Ok(Value::Int(1))
            }
        }
        let (db, docs) = catalog();
        let event: TestEvent<_> =
            TestEvent::update(docs, row(1, 7), row(1, 9)).with_pk_columns([0u16]);
        assert!(
            super::is_key_only(&Detached, &event, &db),
            "an unmeasurable image must not be judged"
        );
    }
}

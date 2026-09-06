//! Row visibility: which watchers may see one changed row.
//!
//! # Why the seam is here
//!
//! Authorization on a change path is asked once per changed row, and the
//! answer differs per watcher. Putting the question behind a trait subql
//! owns makes the backend answering it an implementation detail rather
//! than a structural commitment of whoever drives the loop.
//!
//! subql defines [`VisibilityPolicy`] and ships nothing behind it. The
//! backend (Postgres row-level security, an authorization service, a
//! local evaluation of the row's own columns) is the implementor's
//! choice, exactly as [`Connector`](crate::reexec::Connector) leaves the
//! query and its retry to the caller.
//!
//! # Shape
//!
//! One question per row naming every watcher, answered per watcher:
//!
//! * The row arrives as a [`RowView`], a lazy per-column accessor bound
//!   to one version of the row. Nothing is decoded until a column is
//!   asked for, so an implementation that reads two columns of twenty
//!   pays for two.
//! * Watchers arrive as a slice of [`VisibilityPolicy::Watcher`], an
//!   opaque type subql never inspects.
//! * Verdicts are written positionally into a caller-owned buffer, so
//!   the same watcher appearing twice stays two answers and no
//!   allocation happens per event.
//!
//! Asking about the previous version of a row means handing over a
//! [`RowView`] bound to that version, so no version argument is needed.
//! [`EventRow::previous`] and [`EventRow::current`] build those two views
//! over a [`CdcEvent`](crate::backend::CdcEvent).
//!
//! # Failure is per watcher, not per call
//!
//! The caller pre-fills the buffer with [`Verdict::Deny`] through
//! [`Verdict::reset`], so a call that fails partway leaves everything it
//! did not reach already denied, and verdicts it did write stay valid.
//! An implementation that answers some watchers locally and others over
//! a network therefore does not discard correct local answers when the
//! network call fails.

// `records` carries its own `//!` docs, so no outer doc here: a module
// with both resolves its intra-doc links in this file's scope instead of
// its own, and the types it links to live there.
#[cfg(feature = "visibility-records")]
pub mod records;

// `policy` carries its own `//!` docs, for the same reason.
#[cfg(feature = "visibility-records")]
pub mod policy;

// `shapes` carries its own `//!` docs, for the same reason.
#[cfg(feature = "visibility-records")]
pub mod shapes;

// `openfga` carries its own `//!` docs, for the same reason.
#[cfg(feature = "visibility-openfga")]
pub mod openfga;

// `store` carries its own `//!` docs, for the same reason.
#[cfg(feature = "visibility-records")]
pub mod store;

// `transition` carries its own `//!` docs, for the same reason.
pub mod transition;

// Names a test needs, taken from a translation rather than spelled. Test-only, since
// nothing outside rls2fga may mint one.
#[cfg(all(test, feature = "visibility-records"))]
pub(crate) mod test_names;

pub(crate) mod event_row;
pub(crate) mod row_write;
pub(crate) mod verdict;
pub(crate) mod visibility_policy;

pub use event_row::EventRow;
pub use row_write::{RowView, RowWrite};
pub use verdict::{Verdict, WriteOp};
pub use visibility_policy::VisibilityPolicy;

#[cfg(test)]
mod tests {
    use super::{EventRow, RowView, RowWrite, Verdict, VisibilityPolicy, WriteOp};
    use crate::backend::{CdcEvent, Postgres, RowKind, Value};
    use crate::testing::TestEvent;
    use crate::{catalog_helpers, TableId};
    use alloc::sync::Arc;
    use alloc::task::Wake;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::future::Future;
    use core::pin::{pin, Pin};
    use core::task::{Context, Poll};
    use pg_walstream::{ChangeEvent, ColumnValue, EventType, Lsn, RowData};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    /// No-op `Wake`: the test policies never park. Built on the safe
    /// `Wake` trait so the crate's `forbid(unsafe_code)` holds.
    struct NoopWake;
    #[allow(unknown_lints, clippy::manual_noop_waker)]
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    /// Drive a never-parking future to completion from a `#[test]`, which
    /// is a top-level sync boundary rather than a runtime worker.
    fn block_on<F: Future>(fut: F) -> F::Output {
        let waker = Arc::new(NoopWake).into();
        let mut ctx = Context::from_waker(&waker);
        let mut pinned = pin!(fut);
        loop {
            if let Poll::Ready(v) = pinned.as_mut().poll(&mut ctx) {
                return v;
            }
        }
    }

    /// Returns `Pending` exactly once, so the policy under test really
    /// suspends. A policy that resolves in a single poll would let a
    /// future that is not resumable elsewhere pass unnoticed, and every
    /// real implementation of this trait makes a round trip.
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

    fn catalog() -> (ParserDB, TableId) {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE docs (id INT PRIMARY KEY, owner INT);",
        )
        .expect("catalog DDL parses");
        let docs = catalog_helpers::table_id(&db, "docs").expect("docs is in the catalog");
        (db, docs)
    }

    /// Grants to the watcher named in the row's `owner` column, and
    /// refuses to answer any watcher at or after `fail_from`.
    struct OwnerPolicy {
        fail_from: Option<usize>,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Unreachable;

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
            let owner = owner_of(row);
            let fail_from = self.fail_from;
            async move {
                YieldOnce(false).await;
                for (i, (watcher, verdict)) in watchers.iter().zip(verdicts.iter_mut()).enumerate()
                {
                    if fail_from == Some(i) {
                        return Err(Unreachable);
                    }
                    if owner == Some(*watcher) {
                        *verdict = Verdict::Allow;
                    }
                }
                Ok(())
            }
        }

        /// Deletes are refused outright, so the verb is the only thing
        /// that differs between two otherwise identical questions. A
        /// replacement has to own the row both before and after.
        fn may_write<R>(
            &self,
            write: RowWrite<'_, R>,
            watcher: &i64,
        ) -> impl Future<Output = Result<Verdict, Unreachable>> + Send
        where
            R: RowView<Backend = Postgres> + Sync + ?Sized,
        {
            let owns = match write {
                RowWrite::Insert { new } => owner_of(new) == Some(*watcher),
                RowWrite::Update { old, new } => {
                    owner_of(old) == Some(*watcher) && owner_of(new) == Some(*watcher)
                }
                RowWrite::UpdateUsing { old } | RowWrite::Delete { old } => {
                    owner_of(old) == Some(*watcher)
                }
            };
            let allowed = owns && !matches!(write.op(), WriteOp::Delete);
            let fails = self.fail_from == Some(0);
            async move {
                YieldOnce(false).await;
                if fails {
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

    #[test]
    fn reset_sizes_the_buffer_and_denies_every_slot() {
        let mut buffer = vec![Verdict::Allow; 5];
        Verdict::reset(&mut buffer, 2);
        assert_eq!(buffer, [Verdict::Deny; 2], "shrink clears stale grants");

        buffer[0] = Verdict::Allow;
        Verdict::reset(&mut buffer, 4);
        assert_eq!(buffer, [Verdict::Deny; 4], "grow clears stale grants");

        Verdict::reset(&mut buffer, 0);
        assert!(
            buffer.is_empty(),
            "reset to zero produces an empty verdict buffer"
        );
    }

    #[test]
    fn verdict_defaults_to_deny() {
        assert_eq!(Verdict::default(), Verdict::Deny);
        assert!(!Verdict::Deny.allowed());
        assert!(Verdict::Allow.allowed());
    }

    /// A route is per subscription, so one client watching one table
    /// twice appears twice. Positional verdicts must keep both.
    #[test]
    fn duplicate_watchers_get_independent_verdicts() {
        let (db, docs) = catalog();
        let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(7)]);
        let row = EventRow::current(&event, &db).expect("insert carries a post-image");

        let watchers = [7i64, 9, 7];
        let mut verdicts = Vec::new();
        Verdict::reset(&mut verdicts, watchers.len());

        block_on(OwnerPolicy { fail_from: None }.may_see(&row, &watchers, &mut verdicts))
            .expect("policy answers");
        assert_eq!(verdicts, [Verdict::Allow, Verdict::Deny, Verdict::Allow]);
    }

    /// Decision 7: a failure denies only what it did not reach, and the
    /// verdicts already written stay valid.
    #[test]
    fn failure_leaves_reached_verdicts_and_denies_the_rest() {
        let (db, docs) = catalog();
        let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(7)]);
        let row = EventRow::current(&event, &db).expect("insert carries a post-image");

        let watchers = [7i64, 7, 7];
        let mut verdicts = Vec::new();
        Verdict::reset(&mut verdicts, watchers.len());

        let err =
            block_on(OwnerPolicy { fail_from: Some(1) }.may_see(&row, &watchers, &mut verdicts))
                .expect_err("policy gives up at watcher 1");
        assert_eq!(err, Unreachable);
        assert_eq!(
            verdicts,
            [Verdict::Allow, Verdict::Deny, Verdict::Deny],
            "watcher 0 was answered before the failure, 1 and 2 stay denied"
        );
    }

    /// A short buffer is a caller bug, but the policy must not panic on
    /// its behalf: it answers what it can.
    #[test]
    fn short_verdict_buffer_answers_what_it_can() {
        let (db, docs) = catalog();
        let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(7)]);
        let row = EventRow::current(&event, &db).expect("insert carries a post-image");

        let watchers = [7i64, 7, 7];
        let mut verdicts = vec![Verdict::Deny; 2];
        block_on(OwnerPolicy { fail_from: None }.may_see(&row, &watchers, &mut verdicts))
            .expect("policy answers");
        assert_eq!(verdicts, [Verdict::Allow, Verdict::Allow]);
    }

    /// The two constructors are how a caller names a version, and they
    /// read different images of the same event.
    #[test]
    fn current_and_previous_read_different_images() {
        let (db, docs) = catalog();
        // `update` takes the old image first, then the new one.
        let event = TestEvent::<Postgres>::update(
            docs,
            vec![Value::Int(4), Value::Int(7)],
            vec![Value::Int(4), Value::Int(9)],
        );

        let current = EventRow::current(&event, &db).expect("update carries a post-image");
        let previous = EventRow::previous(&event, &db).expect("update carries a pre-image");

        assert_eq!(current.row_kind(), RowKind::New);
        assert_eq!(previous.row_kind(), RowKind::Old);
        assert_eq!(current.table_id(), docs);
        assert_eq!(previous.table_id(), docs);
        assert_eq!(owner_of(&current), Some(9));
        assert_eq!(owner_of(&previous), Some(7));
    }

    /// The write path is one watcher, one verb, one verdict, and its
    /// error means "could not determine" rather than "denied".
    #[test]
    fn write_verdict_is_single_and_its_error_is_distinct_from_denial() {
        let (db, docs) = catalog();
        let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(7)]);
        let row = EventRow::current(&event, &db).expect("insert carries a post-image");

        let allowed =
            block_on(OwnerPolicy { fail_from: None }.may_write(RowWrite::Insert { new: &row }, &7))
                .expect("policy answers");
        assert_eq!(allowed, Verdict::Allow);

        let denied =
            block_on(OwnerPolicy { fail_from: None }.may_write(RowWrite::Delete { old: &row }, &9))
                .expect("policy answers");
        assert_eq!(denied, Verdict::Deny);

        let undetermined = block_on(OwnerPolicy { fail_from: Some(0) }.may_write(
            RowWrite::Update {
                old: &row,
                new: &row,
            },
            &7,
        ));
        assert_eq!(undetermined, Err(Unreachable));
    }

    /// A view may be taken through a trait object, which is the option
    /// the design left open beside the generic parameter.
    #[test]
    fn row_view_is_usable_behind_a_trait_object() {
        let (db, docs) = catalog();
        let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(7)]);
        let concrete = EventRow::current(&event, &db).expect("insert carries a post-image");
        let row: &(dyn RowView<Backend = Postgres> + Sync) = &concrete;

        let watchers = [7i64];
        let mut verdicts = Vec::new();
        Verdict::reset(&mut verdicts, watchers.len());
        block_on(OwnerPolicy { fail_from: None }.may_see(row, &watchers, &mut verdicts))
            .expect("policy answers");
        assert_eq!(verdicts, [Verdict::Allow]);
    }

    /// The production Postgres CDC event, not just the test fixture,
    /// fits [`EventRow`]. This is the type connetto's change path feeds,
    /// so if it did not compose the seam would be unusable where it is
    /// meant to live.
    #[test]
    fn real_change_event_composes_with_event_row() {
        let (db, docs) = catalog();
        let event = ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "docs".into(),
                relation_oid: 1,
                data: RowData::from_pairs(vec![
                    ("id", ColumnValue::text("4")),
                    ("owner", ColumnValue::text("7")),
                ]),
            },
            lsn: Lsn::new(0x10),
            metadata: None,
        };
        let row = EventRow::current(&event, &db).expect("insert on a known table builds");
        assert_eq!(row.table_id(), docs);
        assert_eq!(owner_of(&row), Some(7));

        let watchers = [7i64, 9];
        let mut verdicts = Vec::new();
        Verdict::reset(&mut verdicts, watchers.len());
        block_on(OwnerPolicy { fail_from: None }.may_see(&row, &watchers, &mut verdicts))
            .expect("policy answers");
        assert_eq!(verdicts, [Verdict::Allow, Verdict::Deny]);
    }

    /// The `Send` bound on the returned futures is the reason a consumer
    /// can hold this seam inside a spawned task on a multi-threaded
    /// runtime, which is where connetto's fan-out runs. The policy
    /// suspends mid-answer through `YieldOnce`, so the task really can
    /// resume on a different worker.
    #[test]
    fn policy_future_runs_inside_a_spawned_task() {
        let (db, docs) = catalog();
        let db = Arc::new(db);
        let event = Arc::new(TestEvent::<Postgres>::insert(
            docs,
            vec![Value::Int(4), Value::Int(7)],
        ));

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .expect("runtime builds");

        let verdicts = runtime.block_on(async move {
            tokio::spawn(async move {
                let row = EventRow::current(&*event, &*db).expect("insert carries a post-image");
                let watchers = [7i64, 9, 7];
                let mut verdicts = Vec::new();
                Verdict::reset(&mut verdicts, watchers.len());
                OwnerPolicy { fail_from: None }
                    .may_see(&row, &watchers, &mut verdicts)
                    .await
                    .expect("policy answers");
                verdicts
            })
            .await
            .expect("task completes")
        });

        assert_eq!(verdicts, [Verdict::Allow, Verdict::Deny, Verdict::Allow]);
    }

    /// A version the event does not carry must not produce a view that
    /// silently reads every column as absent. Before this refused, an
    /// implementation reading "no rule found" as "no restriction" would
    /// have failed open on a row nobody meant to ask about.
    #[test]
    fn no_view_for_a_version_the_event_does_not_carry() {
        let (db, docs) = catalog();

        let insert = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(7)]);
        assert!(EventRow::current(&insert, &db).is_some());
        assert!(
            EventRow::previous(&insert, &db).is_none(),
            "an insert has no pre-image"
        );

        let delete = TestEvent::<Postgres>::delete(docs, vec![Value::Int(4), Value::Int(7)]);
        assert!(EventRow::previous(&delete, &db).is_some());
        assert!(
            EventRow::current(&delete, &db).is_none(),
            "a delete has no post-image"
        );

        let truncate = TestEvent::<Postgres>::truncate(docs);
        assert!(EventRow::current(&truncate, &db).is_none());
        assert!(EventRow::previous(&truncate, &db).is_none());
        assert!(EventRow::new(&truncate, &db, RowKind::Pk).is_none());
    }

    /// A table the catalog does not know must not produce a view either.
    /// A name-resolving event reports `TableId::MAX` for one, which is
    /// the sentinel the engine turns into `UnknownTableId` and which a
    /// policy must never be handed as though it were a real table.
    #[test]
    fn no_view_for_a_table_the_catalog_does_not_know() {
        let (db, _docs) = catalog();
        let stranger = ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "not_in_the_catalog".into(),
                relation_oid: 9,
                data: RowData::from_pairs(vec![("id", ColumnValue::text("1"))]),
            },
            lsn: Lsn::new(1),
            metadata: None,
        };
        assert_eq!(
            stranger.table_id(&db),
            TableId::MAX,
            "the event itself still reports the sentinel"
        );
        assert!(
            EventRow::current(&stranger, &db).is_none(),
            "the view refuses the sentinel rather than passing it to a policy"
        );
    }

    /// The verb is what justifies a second method, so it has to reach the
    /// implementation. Same watcher, same row, and the verb is the only thing
    /// that differs.
    #[test]
    fn the_write_verb_reaches_the_policy() {
        let (db, docs) = catalog();
        let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(7)]);
        let row = EventRow::current(&event, &db).expect("insert carries a post-image");

        let answer = |write| {
            block_on(OwnerPolicy { fail_from: None }.may_write(write, &7)).expect("policy answers")
        };
        assert_eq!(answer(RowWrite::Insert { new: &row }), Verdict::Allow);
        assert_eq!(
            answer(RowWrite::Update {
                old: &row,
                new: &row
            }),
            Verdict::Allow
        );
        assert_eq!(answer(RowWrite::UpdateUsing { old: &row }), Verdict::Allow);
        assert_eq!(
            answer(RowWrite::Delete { old: &row }),
            Verdict::Deny,
            "the policy refuses deletes, so the verb must have arrived"
        );
    }

    /// A replacement carries both versions, and a policy reading only the new
    /// one grants a caller who writes themselves in as the owner.
    #[test]
    fn a_replacement_is_judged_on_both_versions() {
        let (db, docs) = catalog();
        let held = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(7)]);
        let taken = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(9)]);
        let old = EventRow::current(&held, &db).expect("insert carries a post-image");
        let new = EventRow::current(&taken, &db).expect("insert carries a post-image");

        let policy = OwnerPolicy { fail_from: None };

        // 9 does not hold the row and writes itself in as the new owner.
        assert_eq!(
            block_on(policy.may_write(
                RowWrite::Update {
                    old: &old,
                    new: &new
                },
                &9
            )),
            Ok(Verdict::Deny),
            "the row as it is refuses 9, so the replacement refuses"
        );
        // 7 holds it and would hand it to 9, which the new version refuses.
        assert_eq!(
            block_on(policy.may_write(
                RowWrite::Update {
                    old: &old,
                    new: &new
                },
                &7
            )),
            Ok(Verdict::Deny),
            "the row as it will be refuses 7"
        );
        // Asked only about the row as it stands, 7 holds it.
        assert_eq!(
            block_on(policy.may_write(RowWrite::UpdateUsing { old: &old }, &7)),
            Ok(Verdict::Allow)
        );
    }

    /// A cell that was carried but cannot be decoded is an error, not an
    /// absence, and the view forwards it per column rather than
    /// collapsing the whole row. The neighbouring column still reads,
    /// which is the laziness the design pays for.
    #[test]
    fn a_corrupt_cell_surfaces_as_an_error_not_an_absence() {
        let (db, _docs) = catalog();
        let event = ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "docs".into(),
                relation_oid: 1,
                data: RowData::from_pairs(vec![
                    ("id", ColumnValue::text("4")),
                    ("owner", ColumnValue::text("not-a-number")),
                ]),
            },
            lsn: Lsn::new(1),
            metadata: None,
        };
        let row = EventRow::current(&event, &db).expect("insert on a known table builds");

        assert_eq!(row.value_at(0), Ok(Value::Int(4)), "the good column reads");
        let err = row.value_at(1).expect_err("an undecodable INT cell errors");
        assert_eq!(
            err,
            crate::ValueError::Builtin {
                column: 1,
                kind: crate::backend::ScalarFamily::Int
            }
        );

        // An implementation that does not inspect the error still fails
        // closed, because the caller pre-filled the buffer.
        let watchers = [7i64];
        let mut verdicts = Vec::new();
        Verdict::reset(&mut verdicts, watchers.len());
        block_on(OwnerPolicy { fail_from: None }.may_see(&row, &watchers, &mut verdicts))
            .expect("policy answers");
        assert_eq!(verdicts, [Verdict::Deny]);
    }
}

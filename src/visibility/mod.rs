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
//! over a [`CdcEvent`].
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

use alloc::vec::Vec;

use crate::backend::{Backend, CdcEvent, RowKind, Value};
use crate::{ColumnId, EventKind, TableId, ValueError};
use sql_traits::prelude::DatabaseLike;

// ---------------------------------------------------------------------------
// Verdict
// ---------------------------------------------------------------------------

/// One authorization answer.
///
/// Two states only. "Could not determine" is not a verdict: it is a
/// returned error, which on the read path leaves the unreached watchers
/// at their pre-filled [`Verdict::Deny`] and on the write path is the
/// whole answer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Verdict {
    /// The watcher may not see the row, or may not perform the write.
    ///
    /// The default, so a buffer that an implementation never touched
    /// fails closed.
    #[default]
    Deny,
    /// The watcher may see the row, or may perform the write.
    Allow,
}

impl Verdict {
    /// Whether this verdict permits the action.
    #[must_use]
    pub const fn allowed(self) -> bool {
        matches!(self, Self::Allow)
    }

    /// Prepare `buffer` to answer `watchers` questions, fail-closed.
    ///
    /// Sizes the buffer to exactly `watchers` entries and sets every one
    /// to [`Verdict::Deny`]. Call this on a buffer kept across events
    /// rather than allocating a fresh one, which is the whole reason
    /// [`VisibilityPolicy::may_see`] writes into a caller-owned slice.
    ///
    /// # Examples
    ///
    /// ```
    /// use subql::visibility::Verdict;
    ///
    /// let mut buffer = Vec::new();
    /// Verdict::reset(&mut buffer, 3);
    /// assert_eq!(buffer, [Verdict::Deny; 3]);
    ///
    /// // A stale Allow from a previous event never survives the reset.
    /// buffer[0] = Verdict::Allow;
    /// Verdict::reset(&mut buffer, 2);
    /// assert_eq!(buffer, [Verdict::Deny; 2]);
    /// ```
    pub fn reset(buffer: &mut Vec<Self>, watchers: usize) {
        buffer.clear();
        buffer.resize(watchers, Self::Deny);
    }
}

// ---------------------------------------------------------------------------
// WriteOp
// ---------------------------------------------------------------------------

/// The verb a write authorization question is about.
///
/// Distinct from [`EventKind`], which describes an
/// observed change and includes `Truncate`. A write question is always
/// about one row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WriteOp {
    /// Create the row.
    Insert,
    /// Replace the row's values.
    Update,
    /// Remove the row.
    Delete,
}

// ---------------------------------------------------------------------------
// RowWrite
// ---------------------------------------------------------------------------

/// The write a question is about, carrying the row versions its verb needs.
///
/// A replacement is judged on two versions rather than one. The rule choosing
/// which rows a caller may touch reads the row as it is, and the rule admitting
/// the result reads the row as it will be, so one version cannot answer both:
/// judging the new one asks whether the **new** owner is the caller, which
/// grants a caller who holds nothing and writes themselves in as owner.
///
/// The verb and its versions therefore travel together, and the pairings that
/// cannot be answered are not constructible.
///
/// # Judging a replacement on one version, deliberately
///
/// [`UpdateUsing`](Self::UpdateUsing) is what a caller asks when it holds only
/// the row as it stands and wants to know whether a watcher may replace it at
/// all, which is the question handing on a delegated permission over a row
/// needs. It is the first rule alone, so it is a **weaker** question than
/// [`Update`](Self::Update) rather than a cheaper spelling of it.
///
/// `#[non_exhaustive]`: a verb this learns adds a variant, and an
/// implementation keeps a wildcard arm, which has to refuse rather than guess.
#[non_exhaustive]
pub enum RowWrite<'a, R: ?Sized> {
    /// Creating the row, judged on the row being created.
    Insert {
        /// The row as it will be.
        new: &'a R,
    },
    /// Replacing the row, judged on both versions.
    Update {
        /// The row as it is.
        old: &'a R,
        /// The row as it will be.
        new: &'a R,
    },
    /// Whether the row as it stands may be replaced at all, for a caller
    /// holding no other version.
    UpdateUsing {
        /// The row as it is.
        old: &'a R,
    },
    /// Removing the row, judged on the row being removed.
    Delete {
        /// The row as it is.
        old: &'a R,
    },
}

// `Clone` and `Copy` by hand rather than derived: this holds references, which
// are `Copy` whatever they point at, and the derive would demand `R: Copy` and
// so refuse every real row view.
impl<R: ?Sized> Clone for RowWrite<'_, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R: ?Sized> Copy for RowWrite<'_, R> {}

impl<R: ?Sized> RowWrite<'_, R> {
    /// The verb, for an implementation wanting it without matching every
    /// version the question carries.
    ///
    /// [`UpdateUsing`](Self::UpdateUsing) reports [`WriteOp::Update`] because
    /// it is an update question. It is not the same question, so an
    /// implementation deciding what to consult matches the variant instead of
    /// reading this.
    #[must_use]
    pub const fn op(&self) -> WriteOp {
        match self {
            Self::Insert { .. } => WriteOp::Insert,
            Self::Update { .. } | Self::UpdateUsing { .. } => WriteOp::Update,
            Self::Delete { .. } => WriteOp::Delete,
        }
    }
}

// ---------------------------------------------------------------------------
// RowView
// ---------------------------------------------------------------------------

/// Lazy per-column read of one row, bound to one version of it.
///
/// Implemented by subql over a [`CdcEvent`] through [`EventRow`], and by
/// a consumer over whatever it holds instead (a client-uploaded write,
/// say). Nothing is decoded until [`value_at`](Self::value_at) asks.
///
/// # The view must be complete
///
/// Every column of the table must be readable, not only the ones a
/// particular implementation happens to want. A row-level-security
/// backend reads the key alone, while a backend that evaluates rules
/// against the row reads whichever columns the rules mention, and a view
/// that could answer only some columns would make "this answer needed no
/// round trip" depend on which caller asked.
///
/// A caller holding only a key therefore reads the row before asking,
/// rather than handing over a view that reports the rest as
/// [`Value::Missing`].
///
/// [`EventRow`] enforces as much of this as an event's shape can settle:
/// it refuses to build a view for a version the event does not carry, or
/// for a table the catalog does not know. What it cannot settle is a
/// Postgres update whose old image is absent because the table runs
/// `REPLICA IDENTITY DEFAULT`, so a caller that asks about previous
/// versions has to require `FULL`
/// (see [`REPLICA_IDENTITY_AUDIT_SQL`](crate::REPLICA_IDENTITY_AUDIT_SQL)).
///
/// A cell that was carried but cannot be decoded is an `Err`, not a
/// [`Value::Missing`], so a corrupt row is distinguishable from an
/// absent one and an implementation can refuse rather than deny.
///
/// # Static or dynamic dispatch
///
/// [`VisibilityPolicy`] takes the view as a generic parameter, so the
/// usual call monomorphises and costs no vtable. The trait is also
/// dyn-compatible, so a caller that would rather erase the view passes
/// `&(dyn RowView<Backend = _> + Sync)`. The `Sync` is not optional
/// there: the policy's futures are `Send`, and they hold the view.
pub trait RowView {
    /// The database whose [`Value`] shape this row carries.
    type Backend: Backend;

    /// Table the row belongs to.
    fn table_id(&self) -> TableId;

    /// Decode one cell of this row.
    ///
    /// Returns [`Value::Null`] for SQL NULL and `Err` for a cell that
    /// carried a value its catalog type cannot decode. Per the
    /// completeness contract above, a well-formed view does not answer
    /// [`Value::Missing`] for a column of its own table.
    fn value_at(&self, col: ColumnId) -> Result<Value<Self::Backend>, ValueError>;
}

// ---------------------------------------------------------------------------
// EventRow
// ---------------------------------------------------------------------------

/// Whether `kind` carries the row image `row` names at all.
///
/// Structural only: it reports what the event kind can carry, not what a
/// particular source populated. An update whose old image is absent
/// because the table runs Postgres `REPLICA IDENTITY DEFAULT` still
/// answers `true` here, which is the residual [`EventRow`] documents.
const fn image_exists(kind: EventKind, row: RowKind) -> bool {
    !matches!(
        (kind, row),
        (EventKind::Truncate, _)
            | (EventKind::Insert, RowKind::Old)
            | (EventKind::Delete, RowKind::New)
    )
}

/// [`RowView`] over one version of a [`CdcEvent`].
///
/// Borrows the event and the catalog and resolves the table once at
/// construction. Reading a column forwards to
/// [`CdcEvent::value_at`], so no row image is materialised.
///
/// Build it with [`current`](Self::current) for the post-image and
/// [`previous`](Self::previous) for the pre-image. Those two names are
/// how a caller says which version a question is about.
///
/// # Construction can fail, on purpose
///
/// The constructors return [`None`] rather than a view that reads every
/// column as [`Value::Missing`], which is what a version the event does
/// not carry, or a table the catalog does not know, would otherwise
/// produce. Silence there is the wrong default for an authorization
/// seam: an implementation that reads "no rule found for this table" as
/// "no restriction" would fail open on a view nobody meant to build.
/// This is also how decision 3's "never ask about a version you have no
/// values for" stops being a rule a caller has to remember.
///
/// **The guarantee is structural, not per column.** It covers the two
/// cases the event's own shape settles: an image the event kind cannot
/// carry (a pre-image of an insert, a post-image of a delete, either of
/// a truncate) and a table absent from the catalog. It does not cover a
/// Postgres update whose old image is missing because the table runs
/// `REPLICA IDENTITY DEFAULT`, since that is indistinguishable from a
/// populated one without decoding. A caller that asks about previous
/// versions still has to require `FULL`
/// (see [`REPLICA_IDENTITY_AUDIT_SQL`](crate::REPLICA_IDENTITY_AUDIT_SQL)).
pub struct EventRow<'a, E, DB> {
    event: &'a E,
    db: &'a DB,
    row: RowKind,
    table: TableId,
}

impl<'a, E: CdcEvent, DB: DatabaseLike> EventRow<'a, E, DB> {
    /// View of the row image `row` selects, or [`None`] when the event
    /// does not carry it or its table is not in `db`.
    ///
    /// Prefer [`current`](Self::current) or [`previous`](Self::previous).
    /// This constructor exists for [`RowKind::Pk`], which projects the
    /// key alone and so does not satisfy [`RowView`]'s completeness
    /// contract even when it builds.
    ///
    /// Reads [`CdcEvent::kind`], so the event must be a row or truncate
    /// event. Sources reduce a raw stream with `into_engine_events`
    /// before anything reaches the engine, and the same applies here.
    #[must_use]
    pub fn new(event: &'a E, db: &'a DB, row: RowKind) -> Option<Self> {
        if !image_exists(event.kind(), row) {
            return None;
        }
        let table = event.table_id(db);
        // Catches the `TableId::MAX` sentinel a name-resolving event
        // returns for an unknown table, and any id the catalog lost.
        crate::catalog_helpers::table_arity(db, table)?;
        Some(Self {
            event,
            db,
            row,
            table,
        })
    }

    /// View of the row as it is after the change ([`RowKind::New`]).
    ///
    /// [`None`] for a delete or a truncate, which have no post-image.
    #[must_use]
    pub fn current(event: &'a E, db: &'a DB) -> Option<Self> {
        Self::new(event, db, RowKind::New)
    }

    /// View of the row as it was before the change ([`RowKind::Old`]).
    ///
    /// [`None`] for an insert or a truncate, which have no pre-image.
    /// Complete per column only under `REPLICA IDENTITY FULL`. See
    /// [`RowView`] and the note on [`EventRow`].
    #[must_use]
    pub fn previous(event: &'a E, db: &'a DB) -> Option<Self> {
        Self::new(event, db, RowKind::Old)
    }

    /// Which row image this view reads.
    #[must_use]
    pub const fn row_kind(&self) -> RowKind {
        self.row
    }
}

impl<E: CdcEvent, DB: DatabaseLike> RowView for EventRow<'_, E, DB> {
    type Backend = E::Backend;

    fn table_id(&self) -> TableId {
        self.table
    }

    fn value_at(&self, col: ColumnId) -> Result<Value<Self::Backend>, ValueError> {
        self.event.value_at(self.db, self.row, col)
    }
}

// ---------------------------------------------------------------------------
// VisibilityPolicy
// ---------------------------------------------------------------------------

/// Answers who may see a row, and who may write one.
///
/// Two methods because the two questions differ irreducibly: reading asks
/// about many watchers at once and wants a verdict each, writing asks
/// about one caller performing one verb and wants one verdict. Folding
/// them would make every read on the change path carry a verb it ignores.
///
/// Futures are `Send` so a policy can be driven from a multi-threaded
/// runtime, following [`AsyncConnector`](crate::reexec::AsyncConnector).
/// The `impl Future + Send` return shape is deliberate: `async fn` in
/// trait cannot promise `Send`.
///
/// # Watchers
///
/// [`Watcher`](Self::Watcher) is opaque and supplied by the implementor,
/// like [`Connector::AuthContext`](crate::reexec::Connector::AuthContext).
/// It carries no equality, ordering or hashing bound, because verdicts
/// are positional rather than keyed. `Send + Sync` are there only so the
/// returned futures can be `Send`.
///
/// # Examples
///
/// A policy that grants a row to the watcher named in its `owner` column.
///
/// ```
/// use core::convert::Infallible;
/// use core::future::Future;
///
/// use sqlparser::dialect::PostgreSqlDialect;
/// use subql::backend::{Postgres, Value};
/// use subql::testing::TestEvent;
/// use subql::visibility::{EventRow, RowView, RowWrite, Verdict, VisibilityPolicy};
/// use subql::{catalog_helpers, ParserDB};
///
/// struct OwnerPolicy;
///
/// /// Read the `owner` column (ordinal 1) off any row view.
/// fn owner_of<R: RowView<Backend = Postgres> + ?Sized>(row: &R) -> Option<i64> {
///     match row.value_at(1) {
///         Ok(Value::Int(id)) => Some(id),
///         _ => None,
///     }
/// }
///
/// impl VisibilityPolicy for OwnerPolicy {
///     type Watcher = i64;
///     type Error = Infallible;
///     type Backend = Postgres;
///
///     fn may_see<R>(
///         &self,
///         row: &R,
///         watchers: &[i64],
///         verdicts: &mut [Verdict],
///     ) -> impl Future<Output = Result<(), Infallible>> + Send
///     where
///         R: RowView<Backend = Postgres> + Sync + ?Sized,
///     {
///         let owner = owner_of(row);
///         async move {
///             for (watcher, verdict) in watchers.iter().zip(verdicts.iter_mut()) {
///                 if owner == Some(*watcher) {
///                     *verdict = Verdict::Allow;
///                 }
///             }
///             Ok(())
///         }
///     }
///
///     fn may_write<R>(
///         &self,
///         write: RowWrite<'_, R>,
///         watcher: &i64,
///     ) -> impl Future<Output = Result<Verdict, Infallible>> + Send
///     where
///         R: RowView<Backend = Postgres> + Sync + ?Sized,
///     {
///         // A replacement is granted only if this watcher owns the row both
///         // before and after, so it cannot write itself in as the new owner.
///         let allowed = match write {
///             RowWrite::Insert { new } => owner_of(new) == Some(*watcher),
///             RowWrite::Update { old, new } => {
///                 owner_of(old) == Some(*watcher) && owner_of(new) == Some(*watcher)
///             }
///             RowWrite::UpdateUsing { old } | RowWrite::Delete { old } => {
///                 owner_of(old) == Some(*watcher)
///             }
///             // A verb this does not know refuses rather than guessing.
///             _ => false,
///         };
///         async move { Ok(if allowed { Verdict::Allow } else { Verdict::Deny }) }
///     }
/// }
///
/// let db = ParserDB::parse::<PostgreSqlDialect>(
///     "CREATE TABLE docs (id INT PRIMARY KEY, owner INT);",
/// )?;
/// let docs = catalog_helpers::table_id(&db, "docs").expect("docs is in the catalog");
///
/// // Row 4 is owned by watcher 7.
/// let event =
///     TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(7)]).with_pk_columns([0u16]);
/// let row = EventRow::current(&event, &db).expect("an insert carries a post-image");
///
/// // Watcher 7 twice, because one client with two subscriptions over one
/// // table is two watchers, and positional verdicts keep them distinct.
/// let watchers = [7i64, 9, 7];
/// let mut verdicts = Vec::new();
/// Verdict::reset(&mut verdicts, watchers.len());
///
/// let runtime = tokio::runtime::Builder::new_current_thread().build()?;
/// runtime.block_on(OwnerPolicy.may_see(&row, &watchers, &mut verdicts))?;
/// assert_eq!(verdicts, [Verdict::Allow, Verdict::Deny, Verdict::Allow]);
///
/// let write = runtime.block_on(OwnerPolicy.may_write(RowWrite::Delete { old: &row }, &9))?;
/// assert_eq!(write, Verdict::Deny);
///
/// // Watcher 7 owns row 4 and may delete it, and may not hand it to 9.
/// let handover =
///     TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(9)]).with_pk_columns([0u16]);
/// let next = EventRow::current(&handover, &db).expect("an insert carries a post-image");
/// let rewrite = runtime.block_on(
///     OwnerPolicy.may_write(RowWrite::Update { old: &row, new: &next }, &7),
/// )?;
/// assert_eq!(rewrite, Verdict::Deny, "7 owns it now and would not after");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait VisibilityPolicy: Send + Sync {
    /// Who is asking. Opaque to subql, interpreted by the implementor.
    type Watcher: Send + Sync;
    /// Failure to reach an answer, never an answer of "denied".
    type Error: Send;
    /// Database whose [`Value`] shape the row views carry.
    type Backend: Backend;

    /// Decide, for each watcher, whether it may see `row`.
    ///
    /// Writes one verdict per watcher into `verdicts`, positionally:
    /// `verdicts[i]` answers `watchers[i]`. The caller sizes and
    /// pre-fills the buffer with [`Verdict::reset`], so an implementation
    /// only ever has to write the grants.
    ///
    /// `verdicts.len()` must equal `watchers.len()`. An implementation
    /// that is handed a shorter buffer must not panic on the caller's
    /// behalf: zipping the two slices answers what it can and leaves the
    /// rest denied.
    ///
    /// Returning `Err` means the policy could not reach an answer for
    /// everything it had left. Whatever it already wrote stands, and the
    /// rest are the caller's pre-filled denials. An implementation that
    /// wants a transient per-watcher failure to deny only that watcher
    /// writes [`Verdict::Deny`] for it and carries on rather than
    /// returning.
    fn may_see<R>(
        &self,
        row: &R,
        watchers: &[Self::Watcher],
        verdicts: &mut [Verdict],
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send
    where
        R: RowView<Backend = Self::Backend> + Sync + ?Sized;

    /// Decide whether `watcher` may perform `write`.
    ///
    /// One watcher, one write, one verdict. `Err` here is unambiguous:
    /// the policy could not determine an answer, which is a different
    /// thing to tell a client than "you are not allowed".
    ///
    /// [`RowWrite`] carries the row versions the verb is judged on, so a
    /// replacement arrives with both and an implementation cannot be handed
    /// half of what that question needs.
    fn may_write<R>(
        &self,
        write: RowWrite<'_, R>,
        watcher: &Self::Watcher,
    ) -> impl core::future::Future<Output = Result<Verdict, Self::Error>> + Send
    where
        R: RowView<Backend = Self::Backend> + Sync + ?Sized;
}

#[cfg(test)]
mod tests {
    use super::{EventRow, RowView, RowWrite, Verdict, VisibilityPolicy, WriteOp};
    use crate::backend::{CdcEvent, Postgres, RowKind, Value};
    use crate::testing::TestEvent;
    use crate::{catalog_helpers, TableId};
    use alloc::sync::Arc;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::future::Future;
    use core::pin::{pin, Pin};
    use core::task::{Context, Poll};
    use pg_walstream::{ChangeEvent, ColumnValue, EventType, Lsn, RowData};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;
    use std::task::Wake;

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
                kind: crate::backend::ScalarKind::Int
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

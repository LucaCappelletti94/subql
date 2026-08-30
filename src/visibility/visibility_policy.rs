use crate::backend::Backend;

use super::{RowView, RowWrite, Verdict};

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
    /// Database whose [`Value`](crate::backend::Value) shape the row views carry.
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

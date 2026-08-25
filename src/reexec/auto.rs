#![allow(clippy::type_complexity)]
//! Connector-calling wrapper over [`SubscriptionEngine`](crate::SubscriptionEngine).
//!
//! Use this when subql should call a database connector for every
//! [`ReExecutionTrigger`](super::ReExecutionTrigger). Use
//! `SubscriptionEngine` directly when downstream Rust code executes the SQL.
//!
//! See [`super::connector`] for the trait contract and error semantics.
//!
//! [`ReExecutionTrigger`]: super::ReExecutionTrigger

use super::connector::{Connector, ReExecError};
use super::engine::{BatchOutcome, ReExecNotifications, RowDelta, RowsUpdate};
use crate::backend::{Backend, BuiltinKind, CdcEvent, Value};
use crate::clock::{duration_between, ClockHandle};
use crate::compiler::literals::SqlLiteralParse;
#[cfg(test)]
use crate::SubscriptionRequest;
use crate::{
    IdTypes, RegisterError, Registered, SubscriptionId, SubscriptionScope, Tier, UnregisterReport,
};
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;
use hashbrown::HashMap;

/// Default byte budget for one page of a re-read captured query.
///
/// A quarter of a mebibyte: large enough that a small result arrives in one
/// page, small enough that a large one cannot exhaust memory before the caller
/// sees anything.
pub const DEFAULT_PAGE_BYTES: usize = 256 * 1024;

/// Keys named in one scoped read by default.
///
/// A keyed read's duration tracks how many keys it names, and a caller's
/// statement timeout applies per statement, so an unbounded request disables
/// the only read ceiling the caller has. Measured against Postgres 16: a 50,000
/// key request is cancelled outright under a 25 ms ceiling that an ordinary
/// read clears in under 1 ms, and because a failed read returns its keys, the
/// next ordinary change carries the whole backlog and fails again.
///
/// The value is a measured optimum rather than a round number. Splitting 10,000
/// keys was fastest near this size, 28 percent quicker than one statement,
/// while 1,000 was 14 percent slower than not splitting at all and 200 was
/// three times slower. The curve is shallow above this point and steep below
/// it, so err upwards. That measurement ran over a local socket where round
/// trips are nearly free, and real network latency moves the optimum higher,
/// which is why this is configurable.
pub const DEFAULT_MAX_KEYS_PER_READ: usize = 4096;
use sql_traits::prelude::DatabaseLike;

/// Carry a connector's read position into the event-checkpoint domain.
///
/// Identity when the two domains are the same type, which is every shipped
/// pairing (`PgLsn` reads with `PgLsn` events, and so on). `None` when one
/// side has no position domain (`NoCheckpoint`): a Maxwell-fed MySQL engine
/// reads binlog positions its events cannot spell, and a positionless seed
/// is what the install layer already handles. Two DIFFERENT real position
/// domains are a wiring mistake, caught by the debug assertion rather than
/// degraded into a silent `None`.
pub(super) fn reconcile_checkpoint<F: crate::Checkpoint, T: crate::Checkpoint>(
    checkpoint: Option<&F>,
) -> Option<T> {
    use core::any::{Any, TypeId};
    debug_assert!(
        TypeId::of::<F>() == TypeId::of::<T>()
            || TypeId::of::<F>() == TypeId::of::<crate::NoCheckpoint>()
            || TypeId::of::<T>() == TypeId::of::<crate::NoCheckpoint>(),
        "a connector and its events speak different position domains"
    );
    checkpoint
        .and_then(|value| (value as &dyn Any).downcast_ref::<T>())
        .cloned()
}

/// Result of [`AutoResolvingEngine::snapshot`]: the captured query's current value.
///
/// Tagged so future captured-query flavors (single-table row re-execution,
/// multi-table aggregate re-execution) can be added without changing the
/// engine method's signature.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SnapshotResult<B: Backend, C: crate::Checkpoint, I: IdTypes = crate::DefaultIds> {
    /// A scalar captured query: a `MIN`/`MAX` value.
    Scalar(Value<B>, Option<C>),
    /// Initial grouped aggregate rows installed under one checkpoint.
    GroupedAggregate {
        updates: Vec<crate::AggregateValueUpdate<I, B>>,
        checkpoint: Option<C>,
    },
    /// A whole-re-read captured query: its answer, in pages, all read from one
    /// snapshot so they describe a single instant.
    ///
    /// Every page carries that snapshot's position, so a caller can anchor the
    /// answer to the change stream and know which events follow it.
    Rows {
        /// Column names as the database reported them, in projection order.
        columns: Vec<String>,
        /// Every row of the answer, in `columns` order, pages concatenated.
        rows: Vec<Vec<Value<B>>>,
        /// Position the snapshot was read at, when the connector reports one.
        checkpoint: Option<C>,
    },
}

/// Per-query state needed to drive an automatic re-execution.
///
/// Shared by both the sync and async engines. Private to the `reexec`
/// module so the engine internals can read its fields directly.
pub(super) struct ResolveContext<I: IdTypes, A> {
    /// Re-execution SQL produced by the plan.
    pub(super) sql: String,
    /// Decode kind for the scalar result. Meaningless for a whole re-read,
    /// which has no single column.
    pub(super) column_kind: BuiltinKind,
    /// Initial grouped extreme read, present only for that tier.
    pub(super) grouped_bootstrap: Option<crate::AggregateBootstrap>,
    /// Whether resolving means reading one scalar or re-reading every row.
    /// The trigger does not say, and the two are resolved differently.
    pub(super) whole_result: bool,
    /// Whether resolving means asking only about the rows that changed.
    pub(super) keyed: bool,
    /// Which re-read the next page belongs to, so a consumer can tell a new
    /// answer from a continuation of the old one.
    pub(super) generation: u64,
    /// Session owning the query, used to drop contexts on
    /// [`unregister_session`](AutoResolvingEngine::unregister_session).
    pub(super) session: Option<I::SessionId>,
    /// Per-subscription auth state, passed verbatim to the connector.
    pub(super) auth: A,
}

/// Connector execution mode used by [`AutoResolvingEngine`].
///
/// The associated type is the authorization value stored per subscription.
pub trait ResolverMode<B: Backend> {
    /// Per-subscription authorization value stored by the wrapper.
    type AuthContext;
}

/// Synchronous [`Connector`] mode.
pub struct SyncMode<X>(pub X);

impl<B: Backend, X: Connector<Backend = B>> ResolverMode<B> for SyncMode<X> {
    type AuthContext = X::AuthContext;
}

/// Calls a database connector for every read requested by a
/// [`SubscriptionEngine`](crate::SubscriptionEngine).
///
/// `SyncMode<X>` provides synchronous methods for `X: Connector`.
/// [`AsyncMode`](super::AsyncMode) provides asynchronous methods for
/// `X: AsyncConnector`.
pub struct AutoResolvingEngine<E, I, DB, M>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike,
    M: ResolverMode<E::Backend>,
{
    pub(super) inner: crate::SubscriptionEngine<E, I, DB>,
    pub(super) mode: M,
    pub(super) contexts: HashMap<SubscriptionId, ResolveContext<I, M::AuthContext>>,
    /// Byte budget for one page of a re-read captured query.
    pub(super) max_page_bytes: usize,
    /// Keys named in one scoped read of the keyed tier.
    pub(super) max_keys_per_read: usize,
    /// Optional clock used for per-query debounce.
    pub(super) clock: Option<ClockHandle>,
    /// Minimum interval between two re-executions of the same query.
    pub(super) debounce: Option<Duration>,
    /// Last execution time per subscription and optional group.
    pub(super) last_reexec_at: HashMap<(SubscriptionId, Option<Vec<u8>>), u64>,
}

impl<E, I, DB, M> AutoResolvingEngine<E, I, DB, M>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
    M: ResolverMode<E::Backend>,
{
    /// Wrap a registry shell and one explicit connector mode.
    pub fn new(inner: crate::SubscriptionEngine<E, I, DB>, mode: M) -> Self {
        Self {
            inner,
            mode,
            contexts: HashMap::new(),
            max_page_bytes: DEFAULT_PAGE_BYTES,
            max_keys_per_read: DEFAULT_MAX_KEYS_PER_READ,
            clock: None,
            debounce: None,
            last_reexec_at: HashMap::new(),
        }
    }

    /// Update connector-call metadata after the registry changes a subscription
    /// tier under the same identity.
    pub(super) fn apply_transitions(&mut self, transitions: &[crate::MaintenanceTransition]) {
        for transition in transitions {
            let Some(context) = self.contexts.get_mut(&transition.subscription_id) else {
                continue;
            };
            match &transition.to {
                Tier::Scalar { sql, column_kind } => {
                    context.sql.clone_from(sql);
                    context.column_kind = *column_kind;
                    context.whole_result = false;
                    context.keyed = false;
                    context.grouped_bootstrap = None;
                }
                Tier::GroupedScalar { bootstrap } => {
                    context.sql.clone_from(&bootstrap.sql);
                    context.column_kind = bootstrap
                        .kinds
                        .get(bootstrap.group_columns)
                        .copied()
                        .unwrap_or(BuiltinKind::String);
                    context.grouped_bootstrap = Some(bootstrap.clone());
                    context.whole_result = false;
                    context.keyed = false;
                }
                Tier::KeyedRows { sql, .. } => {
                    context.sql.clone_from(sql);
                    context.column_kind = BuiltinKind::String;
                    context.whole_result = false;
                    context.keyed = true;
                    context.grouped_bootstrap = None;
                }
                Tier::WholeRows { sql, .. } => {
                    context.sql.clone_from(sql);
                    context.column_kind = BuiltinKind::String;
                    context.whole_result = true;
                    context.keyed = false;
                    context.grouped_bootstrap = None;
                }
                Tier::InProcess(_) => {}
            }
            context.generation = 0;
        }
    }

    /// Set how many keys one scoped read of the keyed tier may name.
    ///
    /// Bounds statement size and therefore statement duration, which is what
    /// keeps a caller's statement timeout meaningful under a burst: that
    /// ceiling applies per statement, so one unbounded request puts the
    /// duration under the burst's control rather than the caller's.
    ///
    /// Lower costs round trips, and below roughly a thousand keys that cost
    /// dominates. Zero is clamped to one rather than meaning "no limit".
    /// Defaults to [`DEFAULT_MAX_KEYS_PER_READ`].
    #[must_use]
    pub const fn with_max_keys_per_read(mut self, max_keys: usize) -> Self {
        // Zero would make no progress possible, so it means one at a time.
        self.max_keys_per_read = if max_keys == 0 { 1 } else { max_keys };
        self
    }

    /// Set the byte budget for one page of a re-read captured query.
    ///
    /// A smaller budget bounds memory and wire size per message at the cost of
    /// more round trips. Zero is clamped to one rather than meaning "no limit",
    /// and a page always carries at least one row whatever the budget, because
    /// a budget smaller than a single row would otherwise make no progress.
    /// Defaults to [`DEFAULT_PAGE_BYTES`].
    #[must_use]
    pub const fn with_max_page_bytes(mut self, max_bytes: usize) -> Self {
        // Zero would make no progress, and the read guarantees at least one row
        // per page anyway, so clamp rather than accept a budget that lies.
        self.max_page_bytes = if max_bytes == 0 { 1 } else { max_bytes };
        self
    }

    /// Attach a [`Clock`](crate::Clock) for time-based decisions (per-query
    /// debounce). Defaults to no clock. Without one, debounce is silently
    /// disabled even if
    /// [`with_debounce_per_query`](Self::with_debounce_per_query) is set.
    #[must_use]
    pub fn with_clock(mut self, clock: ClockHandle) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Configure a minimum interval between two re-executions of the
    /// same captured query.
    ///
    /// Triggers within the window are silently dropped: the connector is
    /// not called and no [`ScalarUpdate`](super::ScalarUpdate) is emitted (the engine's stored
    /// value, set by the prior re-execution, remains current). Requires
    /// [`with_clock`](Self::with_clock). Without a clock the debounce
    /// config is ignored.
    #[must_use]
    pub const fn with_debounce_per_query(mut self, debounce: Duration) -> Self {
        self.debounce = Some(debounce);
        self
    }

    /// Number of captured re-execution queries (matches the inner engine).
    pub fn reexec_query_count(&self) -> usize {
        self.inner.reread_count()
    }

    pub(super) fn debounce_skip(
        &self,
        subscription_id: SubscriptionId,
        read: &super::ReExecutionRead,
    ) -> bool {
        let (Some(clock), Some(window)) = (self.clock.as_ref(), self.debounce) else {
            return false;
        };
        let key = (subscription_id, read.group_key().map(<[u8]>::to_vec));
        let Some(last_micros) = self.last_reexec_at.get(&key).copied() else {
            return false;
        };
        duration_between(last_micros, clock.now_micros()) < window
    }

    pub(super) fn stamp_reexec(
        &mut self,
        subscription_id: SubscriptionId,
        read: &super::ReExecutionRead,
    ) {
        if let Some(clock) = self.clock.as_ref() {
            self.last_reexec_at.insert(
                (subscription_id, read.group_key().map(<[u8]>::to_vec)),
                clock.now_micros(),
            );
        }
    }

    /// Register a subscription. `auth` is stored alongside the captured
    /// query and re-presented to the connector on each re-execution.
    /// Engine-supported queries pass through unchanged (no auth stored).
    /// Sync in both modes because registration only touches in-memory
    /// engine state.
    pub fn register<R>(
        &mut self,
        spec: R,
        auth: M::AuthContext,
    ) -> Result<Registered, RegisterError>
    where
        R: crate::RegistrationRequest<I, E::Backend>,
    {
        let session = match spec.scope() {
            SubscriptionScope::Session(s) => Some(s),
            SubscriptionScope::Durable => None,
        };
        let result = self.inner.register(spec)?;
        match &result.tier {
            Tier::Scalar { sql, column_kind } => {
                self.contexts.insert(
                    result.subscription_id,
                    ResolveContext {
                        sql: sql.clone(),
                        column_kind: *column_kind,
                        grouped_bootstrap: None,
                        whole_result: false,
                        keyed: false,
                        generation: 0,
                        session,
                        auth,
                    },
                );
            }
            Tier::GroupedScalar { bootstrap } => {
                self.contexts.insert(
                    result.subscription_id,
                    ResolveContext {
                        sql: bootstrap.sql.clone(),
                        column_kind: bootstrap
                            .kinds
                            .get(bootstrap.group_columns)
                            .copied()
                            .unwrap_or(BuiltinKind::String),
                        grouped_bootstrap: Some(bootstrap.clone()),
                        whole_result: false,
                        keyed: false,
                        generation: 0,
                        session,
                        auth,
                    },
                );
            }
            Tier::KeyedRows { sql, .. } | Tier::WholeRows { sql, .. } => {
                self.contexts.insert(
                    result.subscription_id,
                    ResolveContext {
                        sql: sql.clone(),
                        // No single column to decode: the rows carry their own
                        // shape, which is why `RowPage` reports column names.
                        column_kind: BuiltinKind::String,
                        grouped_bootstrap: None,
                        // The tier decides which read serves a change, so it
                        // comes from the registration rather than a default.
                        // Defaulting here once made every keyed capture resolve
                        // as a whole re-read, which is correct output produced
                        // the expensive way, so nothing failed and nothing said
                        // so.
                        whole_result: matches!(result.tier, Tier::WholeRows { .. }),
                        keyed: matches!(result.tier, Tier::KeyedRows { .. }),
                        generation: 0,
                        session,
                        auth,
                    },
                );
            }
            Tier::InProcess(_) => {}
        }
        Ok(result)
    }
}

/// Walks a key set in bounded batches.
///
/// Bounded because statement duration tracks how many keys a statement names,
/// and a caller's statement timeout applies per statement, so one unbounded
/// request puts the duration under the burst's control rather than the
/// caller's.
pub(super) struct KeyBatches<'a, B: Backend> {
    keys: &'a [Vec<Value<B>>],
    size: usize,
    at: usize,
}

impl<'a, B: Backend> KeyBatches<'a, B> {
    pub(super) const fn new(keys: &'a [Vec<Value<B>>], size: usize) -> Self {
        Self {
            keys,
            // Zero would never advance.
            size: if size == 0 { 1 } else { size },
            at: 0,
        }
    }
}

impl<'a, B: Backend> Iterator for KeyBatches<'a, B> {
    type Item = &'a [Vec<Value<B>>];

    fn next(&mut self) -> Option<Self::Item> {
        if self.at >= self.keys.len() {
            return None;
        }
        let start = self.at;
        let end = self.keys.len().min(start + self.size);
        self.at = end;
        Some(&self.keys[start..end])
    }
}

/// Closes a cursor if the read using it is abandoned by an unwinding panic.
///
/// The ordinary path closes explicitly and disarms this, which is what lets a
/// close failure be reported instead of swallowed. Unwinding runs nothing but
/// destructors, so without this a panic mid-read would strand the connector's
/// map entry, holding a pooled connection inside an open transaction forever.
struct CloseOnUnwind<'a, X: Connector> {
    connector: &'a X,
    cursor: super::CursorId,
    armed: bool,
}

impl<X: Connector> Drop for CloseOnUnwind<'_, X> {
    fn drop(&mut self) {
        if self.armed {
            // Best-effort: there is no caller left to report to, and nothing
            // here may panic.
            let _ = self.connector.close_cursor(self.cursor);
        }
    }
}

impl<E, I, DB, X> AutoResolvingEngine<E, I, DB, SyncMode<X>>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
    X: Connector<Backend = E::Backend>,
{
    /// The connector this engine drives.
    pub const fn connector(&self) -> &X {
        &self.mode.0
    }

    /// Bootstrap a captured query by reading its current answer through the
    /// connector.
    ///
    /// Returns a [`SnapshotResult`] tagged with the connector's
    /// [`Checkoint`](Connector::Checkpoint): `Scalar` for a scalar capture,
    /// `Rows` for either row tier.
    ///
    /// Without this a captured subscription would deliver nothing until
    /// something happened to change a table it reads, so a caller registering
    /// against a quiet database would sit empty holding a correct answer it
    /// had never been told. A keyed capture cannot recover from that later
    /// either: no change ever fires for a row that was already in the answer
    /// and stayed there.
    ///
    /// A scalar's value is also installed via [`Install::install`](crate::Install::install), so the
    /// engine is fully primed once this returns. Neither row tier installs
    /// anything, because neither holds an answer: the rows go to the caller.
    ///
    /// # Errors
    ///
    /// Returns [`ReExecError::Connector`] if the connector fails, and
    /// [`ReExecError::Cursor`] if a row tier's read fails or the connector
    /// holds no cursors. Returns `Ok(None)` if `subscription_id` does not exist. The
    /// absence is signaled (rather than panicking) so callers can race a
    /// snapshot against an `unregister_*` without crashing.
    pub fn snapshot(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> Result<Option<SnapshotResult<E::Backend, X::Checkpoint, I>>, ReExecError<X::Error>> {
        let Some(context) = self.contexts.get(&subscription_id) else {
            return Ok(None);
        };
        let grouped_bootstrap = context.grouped_bootstrap.clone();
        if let Some(bootstrap) = grouped_bootstrap {
            let (_, rows, checkpoint) = self.read_whole(&bootstrap.sql, subscription_id)?;
            let mut installed = crate::Install::install(
                &mut self.inner,
                subscription_id,
                crate::GroupedScalarSeedInstall {
                    rows,
                    read_at: reconcile_checkpoint(checkpoint.as_ref()),
                },
            )?;
            let mut pending = core::mem::take(&mut installed.triggers);
            while let Some(trigger) = pending.pop() {
                match &trigger.read {
                    super::ReExecutionRead::GroupedScalar {
                        group,
                        sql,
                        column_kinds,
                    } => {
                        let resolved = self.resolve_grouped_scalar(
                            subscription_id,
                            group,
                            sql,
                            *column_kinds,
                            trigger.checkpoint.clone(),
                        )?;
                        self.apply_transitions(&resolved.transitions);
                        pending.extend(resolved.triggers);
                        installed.updates.extend(resolved.updates);
                        installed.transitions.extend(resolved.transitions);
                    }
                    super::ReExecutionRead::Subscription => {
                        let sql = self
                            .contexts
                            .get(&subscription_id)
                            .expect("a transitioned read keeps its connector context")
                            .sql
                            .clone();
                        let (columns, rows, checkpoint) = self.read_whole(&sql, subscription_id)?;
                        return Ok(Some(SnapshotResult::Rows {
                            columns,
                            rows,
                            checkpoint,
                        }));
                    }
                }
            }
            return Ok(Some(SnapshotResult::GroupedAggregate {
                updates: installed.updates,
                checkpoint,
            }));
        }
        if context.whole_result || context.keyed {
            let sql = context.sql.clone();
            let (columns, rows, checkpoint) = self.read_whole(&sql, subscription_id)?;
            return Ok(Some(SnapshotResult::Rows {
                columns,
                rows,
                checkpoint,
            }));
        }
        let (value, checkpoint) = self
            .mode
            .0
            .execute_scalar(&context.sql, context.column_kind, &context.auth)
            .map_err(ReExecError::Connector)?;
        let _installed = crate::Install::install(
            &mut self.inner,
            subscription_id,
            crate::ScalarInstall {
                value: value.clone(),
                checkpoint: checkpoint.clone(),
            },
        )?;
        Ok(Some(SnapshotResult::Scalar(value, checkpoint)))
    }

    /// Read a captured query's whole answer from one cursor, concatenating its
    /// pages.
    ///
    /// One snapshot for the lot, which is what makes the concatenation mean
    /// something: pages from separate reads would describe no single instant.
    /// The page budget still bounds each round trip, so a large answer is read
    /// in bounded steps even though it is returned whole.
    fn read_whole(
        &self,
        sql: &str,
        subscription_id: SubscriptionId,
    ) -> Result<
        (
            Vec<String>,
            Vec<Vec<Value<E::Backend>>>,
            Option<X::Checkpoint>,
        ),
        ReExecError<X::Error>,
    > {
        let auth = &self
            .contexts
            .get(&subscription_id)
            .expect("the caller just read this context")
            .auth;
        let cursor = self
            .mode
            .0
            .open_cursor(sql, auth)
            .map_err(ReExecError::Cursor)?;

        // The cursor is closed on every exit from here, including a panic. An
        // early return is handled by closing before `outcome?` below, but a
        // panic unwinds straight past that, and the connector's map would keep
        // the entry alive forever with a pooled connection inside an open
        // transaction. `Drop` is the only thing unwinding runs.
        let mut guard = CloseOnUnwind {
            connector: &self.mode.0,
            cursor,
            armed: true,
        };

        let mut columns = Vec::new();
        let mut rows = Vec::new();
        let mut checkpoint = None;
        let outcome = (|| -> Result<(), ReExecError<X::Error>> {
            loop {
                let page = self
                    .mode
                    .0
                    .fetch_cursor(cursor, self.max_page_bytes)
                    .map_err(ReExecError::Cursor)?;
                if columns.is_empty() {
                    columns = page.value.columns;
                }
                checkpoint = page.checkpoint;
                let more = page.value.more;
                rows.extend(page.value.rows);
                if !more {
                    return Ok(());
                }
            }
        })();
        let closed = self
            .mode
            .0
            .close_cursor(cursor)
            .map_err(ReExecError::Cursor);
        guard.armed = false;
        outcome?;
        closed?;
        Ok((columns, rows, checkpoint))
    }

    /// Dispatch a CDC event.
    ///
    /// For every [`ReExecutionTrigger`] the inner engine emits, this method
    /// looks up the captured query's auth context, calls
    /// [`Connector::execute_scalar`] with the plan's SQL and decode kind,
    /// installs the result via [`Install::install`](crate::Install::install), and pushes a
    /// [`ScalarUpdate`](super::ScalarUpdate) in the trigger's place. The returned
    /// [`ReExecNotifications::triggers`] is always empty under this engine.
    ///
    /// The first connector failure aborts the rest of the batch and is
    /// surfaced as [`ReExecError::Connector`].
    ///
    /// [`ReExecutionTrigger`]: super::ReExecutionTrigger
    pub fn consumers(
        &mut self,
        event: &E,
    ) -> Result<ReExecNotifications<I, E::Backend, E::Checkpoint>, ReExecError<X::Error>> {
        let ReExecNotifications {
            engine,
            mut aggregate_updates,
            mut scalar_updates,
            mut rows_updates,
            mut row_deltas,
            triggers,
            mut transitions,
        } = self
            .inner
            .reread_notifications(event)
            .map_err(ReExecError::Dispatch)?;
        self.apply_transitions(&transitions);
        let mut pending = triggers;
        while let Some(trigger) = pending.pop() {
            if self.debounce_skip(trigger.subscription_id, &trigger.read) {
                continue;
            }
            if let super::ReExecutionRead::GroupedScalar {
                group,
                sql,
                column_kinds,
            } = &trigger.read
            {
                let installed = self.resolve_grouped_scalar(
                    trigger.subscription_id,
                    group,
                    sql,
                    *column_kinds,
                    trigger.checkpoint.clone(),
                )?;
                self.apply_transitions(&installed.transitions);
                pending.extend(installed.triggers);
                aggregate_updates.extend(installed.updates);
                transitions.extend(installed.transitions);
                self.stamp_reexec(trigger.subscription_id, &trigger.read);
                continue;
            }
            let ctx = self
                .contexts
                .get(&trigger.subscription_id)
                .expect("every read tier stores its connector context at registration");
            if ctx.keyed {
                let deltas = self.resolve_keyed(
                    trigger.subscription_id,
                    trigger.consumer_id,
                    trigger.checkpoint.as_ref(),
                )?;
                self.stamp_reexec(trigger.subscription_id, &trigger.read);
                row_deltas.extend(deltas);
                continue;
            }
            if ctx.whole_result {
                let pages = self.reread(
                    trigger.subscription_id,
                    trigger.consumer_id,
                    trigger.checkpoint.as_ref(),
                )?;
                self.stamp_reexec(trigger.subscription_id, &trigger.read);
                rows_updates.extend(pages);
                continue;
            }
            let (value, _db_checkpoint) = self
                .mode
                .0
                .execute_scalar(&ctx.sql, ctx.column_kind, &ctx.auth)
                .map_err(ReExecError::Connector)?;
            let update = crate::Install::install(
                &mut self.inner,
                trigger.subscription_id,
                crate::ScalarInstall {
                    value,
                    checkpoint: trigger.checkpoint.clone(),
                },
            )?;
            self.stamp_reexec(trigger.subscription_id, &trigger.read);
            scalar_updates.push(update);
        }

        Ok(ReExecNotifications {
            engine,
            aggregate_updates,
            scalar_updates,
            rows_updates,
            row_deltas,
            triggers: Vec::new(),
            transitions,
        })
    }

    fn resolve_grouped_scalar(
        &mut self,
        subscription_id: SubscriptionId,
        group: &[u8],
        sql: &str,
        _column_kinds: [BuiltinKind; 2],
        checkpoint: Option<E::Checkpoint>,
    ) -> Result<
        crate::AggregateMaintenanceOutput<I, E::Backend, E::Checkpoint>,
        ReExecError<X::Error>,
    > {
        let context = self
            .contexts
            .get(&subscription_id)
            .expect("a grouped scalar read stores its connector context");
        let snapshot = self
            .mode
            .0
            .read_page(sql, self.max_page_bytes, &context.auth)
            .map_err(ReExecError::Connector)?;
        if snapshot.value.more || snapshot.value.rows.len() != 1 {
            return Err(crate::AggregateInstallError::RowCount {
                subscription: subscription_id,
                rows: snapshot.value.rows.len(),
            }
            .into());
        }
        let row = snapshot
            .value
            .rows
            .into_iter()
            .next()
            .expect("the row count was checked");
        crate::Install::install(
            &mut self.inner,
            subscription_id,
            crate::GroupedScalarInstall {
                group: group.to_vec(),
                row,
                checkpoint,
            },
        )
        .map_err(Into::into)
    }

    /// Ask the database which of the changed rows are in the answer, and turn
    /// that into one delta per row.
    ///
    /// A key that comes back is in the answer and is delivered as its current
    /// row. A key that comes back empty is not, and is delivered as a removal,
    /// which is harmless for a row the caller never held. That is why this tier
    /// holds no state: the answer to "is it in" is asked rather than remembered.
    fn resolve_keyed(
        &mut self,
        subscription_id: SubscriptionId,
        consumer_id: I::ConsumerId,
        checkpoint: Option<&E::Checkpoint>,
    ) -> Result<Vec<RowDelta<I, E::Backend, E::Checkpoint>>, ReExecError<X::Error>> {
        let keys = self.inner.take_pending_keys(subscription_id);
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let Some(plan) = self.inner.keyed_plan(subscription_id) else {
            return Ok(Vec::new());
        };
        let key_positions = plan.key_positions.clone();
        let plan_ref = plan.clone();

        // Asked in bounded batches. Statement duration tracks how many keys a
        // statement names, and a caller's statement timeout applies per
        // statement, so one unbounded request disables the only read ceiling
        // the caller has. Each key appears in exactly one batch, so no key is
        // asked about twice however many rows come back.
        let mut columns = Vec::new();
        let mut present: Vec<(Vec<Value<E::Backend>>, Vec<Value<E::Backend>>)> = Vec::new();
        for batch in KeyBatches::new(&keys, self.max_keys_per_read) {
            if let Err(e) = self.read_one_batch(
                subscription_id,
                &plan_ref,
                &key_positions,
                batch,
                &mut columns,
                &mut present,
            ) {
                // Every key goes back, including those of batches that already
                // succeeded. A failure fails the whole call, so their rows were
                // never delivered, and keeping their keys would leave those rows
                // stale for good. Asking again is the cost of an all-or-nothing
                // result.
                self.inner.restore_pending_keys(subscription_id, keys);
                return Err(e);
            }
        }

        Ok(Self::deltas_from(
            subscription_id,
            consumer_id,
            checkpoint,
            &keys,
            &present,
            &columns,
        ))
    }

    /// Read one bounded batch of keys, resuming inside the batch if its rows do
    /// not fit one page.
    ///
    /// Split out so the batch loop reads as the one thing it is, and so the
    /// caller owns the decision about which keys go back on a failure.
    #[allow(clippy::too_many_arguments)]
    fn read_one_batch(
        &self,
        subscription_id: SubscriptionId,
        plan: &crate::reexec::plan::KeyedPlan,
        key_positions: &[usize],
        batch: &[Vec<Value<E::Backend>>],
        columns: &mut Vec<String>,
        present: &mut Vec<(Vec<Value<E::Backend>>, Vec<Value<E::Backend>>)>,
    ) -> Result<(), ReExecError<X::Error>> {
        let auth = &self
            .contexts
            .get(&subscription_id)
            .expect("a captured query stores its context at register time")
            .auth;
        let render = |keys: &[Vec<Value<E::Backend>>]| {
            crate::reexec::plan::render_scoped_read::<E::Backend>(plan, keys).map_err(|e| {
                ReExecError::Dispatch(crate::DispatchError::VmError(alloc::format!("{e}")))
            })
        };
        let Some(mut page_sql) = render(batch)? else {
            return Ok(());
        };
        // Accumulated across pages, not per page. Resetting it would let a key
        // answered on an earlier page back into the next statement, which
        // delivers it twice and, with a stable row order, never terminates:
        // the remaining sets oscillate between the halves of the batch.
        let mut seen: Vec<Vec<Value<E::Backend>>> = Vec::new();
        loop {
            let page = self
                .mode
                .0
                .read_page(&page_sql, self.max_page_bytes, auth)
                .map_err(ReExecError::Connector)?;
            if columns.is_empty() {
                columns.clone_from(&page.value.columns);
            }
            let before = seen.len();
            for row in page.value.rows {
                let key: Vec<Value<E::Backend>> = key_positions
                    .iter()
                    .filter_map(|i| row.get(*i).cloned())
                    .collect();
                seen.push(key.clone());
                present.push((key, row));
            }
            // A page with no rows ends the read whatever it claims about there
            // being more. Our own reader cannot report that combination, but
            // this trait has outside implementors, and without this a connector
            // that did would loop here forever.
            if !page.value.more || seen.len() == before {
                return Ok(());
            }
            // Resume within the batch, excluding the keys already returned, so
            // the statement stays bounded by the batch. `seen` accumulates
            // across pages, so `remaining` strictly shrinks and the loop ends.
            let remaining: Vec<Vec<Value<E::Backend>>> = batch
                .iter()
                .filter(|k| !seen.contains(k))
                .cloned()
                .collect();
            if remaining.is_empty() {
                return Ok(());
            }
            match render(&remaining)? {
                Some(next) => page_sql = next,
                None => return Ok(()),
            }
        }
    }

    /// Turn "these keys were asked about, these rows came back" into one delta
    /// per key. Shared with the async engine, which asks the same question.
    fn deltas_from(
        subscription_id: SubscriptionId,
        consumer_id: I::ConsumerId,
        checkpoint: Option<&E::Checkpoint>,
        keys: &[Vec<Value<E::Backend>>],
        present: &[(Vec<Value<E::Backend>>, Vec<Value<E::Backend>>)],
        columns: &[String],
    ) -> Vec<RowDelta<I, E::Backend, E::Checkpoint>> {
        deltas_from(
            subscription_id,
            consumer_id,
            checkpoint,
            keys,
            present,
            columns,
        )
    }
}

/// Turn "these keys were asked about, these rows came back" into one delta per
/// key: present is an upsert, absent is a removal.
///
/// A free function because both engines produce it from the same answer, and a
/// method on the sync engine would drag its [`Connector`] bound into the async
/// one. `columns` is empty on a removal: there is no row to describe.
pub(super) fn deltas_from<I, B, C>(
    subscription_id: SubscriptionId,
    consumer_id: I::ConsumerId,
    checkpoint: Option<&C>,
    keys: &[Vec<Value<B>>],
    present: &[(Vec<Value<B>>, Vec<Value<B>>)],
    columns: &[String],
) -> Vec<RowDelta<I, B, C>>
where
    I: IdTypes,
    B: Backend,
    C: crate::Checkpoint,
{
    let mut deltas = Vec::with_capacity(keys.len());
    // Which keys came back, by encoded form. `Value` carries floats so it has
    // neither `Hash` nor `Ord`, and scanning the returned rows once per key
    // asked about is the product of the two counts.
    let mut returned: hashbrown::HashSet<Vec<u8>> = hashbrown::HashSet::new();
    for (key, row) in present {
        if let Some(encoded) = crate::backend::encode_value_key(key) {
            returned.insert(encoded);
        }
        deltas.push(RowDelta {
            subscription_id,
            consumer_id,
            key: key.clone(),
            columns: columns.to_vec(),
            row: Some(row.clone()),
            checkpoint: checkpoint.cloned(),
        });
    }
    for key in keys {
        // A key that could not be encoded falls back to the scan, which is
        // correct and merely slower, rather than being reported as removed.
        let came_back = crate::backend::encode_value_key(key).map_or_else(
            || present.iter().any(|(k, _)| k == key),
            |encoded| returned.contains(&encoded),
        );
        if !came_back {
            deltas.push(RowDelta {
                subscription_id,
                consumer_id,
                key: key.clone(),
                columns: Vec::new(),
                row: None,
                checkpoint: checkpoint.cloned(),
            });
        }
    }
    deltas
}

impl<E, I, DB, X> AutoResolvingEngine<E, I, DB, SyncMode<X>>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
    X: Connector<Backend = E::Backend>,
{
    /// Re-read a captured query in full and hand back its pages.
    ///
    /// Every page comes from one cursor in one transaction, which is what makes
    /// the pages add up to a single instant. A keyed result could instead be
    /// paged statelessly through [`Connector::read_page`], avoiding the
    /// transaction, and that is an optimisation to add per shape rather than a
    /// different delivered contract: these same pages, same generation, same
    /// `more`.
    fn reread(
        &mut self,
        subscription_id: SubscriptionId,
        consumer_id: I::ConsumerId,
        checkpoint: Option<&E::Checkpoint>,
    ) -> Result<Vec<RowsUpdate<I, E::Backend, E::Checkpoint>>, ReExecError<X::Error>> {
        let ctx = self
            .contexts
            .get_mut(&subscription_id)
            .expect("every read tier stores its connector context at registration");
        let sql = ctx.sql.clone();
        ctx.generation = ctx.generation.saturating_add(1);
        let generation = ctx.generation;
        let cursor = self
            .mode
            .0
            .open_cursor(&sql, &self.contexts[&subscription_id].auth)
            .map_err(ReExecError::Cursor)?;
        let mut guard = CloseOnUnwind {
            connector: &self.mode.0,
            cursor,
            armed: true,
        };
        let mut pages = Vec::new();
        let outcome = (|| -> Result<(), ReExecError<X::Error>> {
            loop {
                let page = self
                    .mode
                    .0
                    .fetch_cursor(cursor, self.max_page_bytes)
                    .map_err(ReExecError::Cursor)?;
                let more = page.value.more;
                pages.push(RowsUpdate {
                    subscription_id,
                    consumer_id,
                    generation,
                    columns: page.value.columns,
                    rows: page.value.rows,
                    more,
                    checkpoint: checkpoint.cloned(),
                });
                if !more {
                    return Ok(());
                }
            }
        })();
        let closed = self
            .mode
            .0
            .close_cursor(cursor)
            .map_err(ReExecError::Cursor);
        guard.armed = false;
        outcome?;
        closed?;
        Ok(pages)
    }

    /// Batch variant of [`consumers`](Self::consumers).
    ///
    /// Runs each event through the inner trigger-emitting engine in input
    /// order, then resolves the **deduplicated** triggers serially via the
    /// connector. With N events that displace the same captured query K
    /// times, the connector is called once instead of K times. Per-event
    /// engine notifications stay in input order. The returned
    /// [`BatchOutcome::triggers`] is always empty after resolution.
    ///
    /// The first connector failure aborts the whole batch. Partial
    /// notifications are dropped. The caller is expected to retry the
    /// batch.
    pub fn consumers_batch(
        &mut self,
        events: &[E],
    ) -> Result<BatchOutcome<I, E::Backend, E::Checkpoint>, ReExecError<X::Error>> {
        let BatchOutcome {
            per_event,
            mut aggregate_updates,
            mut scalar_updates,
            mut rows_updates,
            mut row_deltas,
            triggers,
            mut transitions,
        } = self
            .inner
            .reread_batch(events)
            .map_err(ReExecError::Dispatch)?;
        self.apply_transitions(&transitions);
        let mut pending = triggers;
        while let Some(trigger) = pending.pop() {
            if self.debounce_skip(trigger.subscription_id, &trigger.read) {
                continue;
            }
            if let super::ReExecutionRead::GroupedScalar {
                group,
                sql,
                column_kinds,
            } = &trigger.read
            {
                let installed = self.resolve_grouped_scalar(
                    trigger.subscription_id,
                    group,
                    sql,
                    *column_kinds,
                    trigger.checkpoint.clone(),
                )?;
                self.apply_transitions(&installed.transitions);
                pending.extend(installed.triggers);
                aggregate_updates.extend(installed.updates);
                transitions.extend(installed.transitions);
                self.stamp_reexec(trigger.subscription_id, &trigger.read);
                continue;
            }
            let ctx = self
                .contexts
                .get(&trigger.subscription_id)
                .expect("every read tier stores its connector context at registration");
            if ctx.keyed {
                let deltas = self.resolve_keyed(
                    trigger.subscription_id,
                    trigger.consumer_id,
                    trigger.checkpoint.as_ref(),
                )?;
                self.stamp_reexec(trigger.subscription_id, &trigger.read);
                row_deltas.extend(deltas);
                continue;
            }
            if ctx.whole_result {
                let pages = self.reread(
                    trigger.subscription_id,
                    trigger.consumer_id,
                    trigger.checkpoint.as_ref(),
                )?;
                self.stamp_reexec(trigger.subscription_id, &trigger.read);
                rows_updates.extend(pages);
                continue;
            }
            let (value, _db_checkpoint) = self
                .mode
                .0
                .execute_scalar(&ctx.sql, ctx.column_kind, &ctx.auth)
                .map_err(ReExecError::Connector)?;
            let update = crate::Install::install(
                &mut self.inner,
                trigger.subscription_id,
                crate::ScalarInstall {
                    value,
                    checkpoint: trigger.checkpoint.clone(),
                },
            )?;
            self.stamp_reexec(trigger.subscription_id, &trigger.read);
            scalar_updates.push(update);
        }

        Ok(BatchOutcome {
            per_event,
            aggregate_updates,
            scalar_updates,
            rows_updates,
            row_deltas,
            triggers: Vec::new(),
            transitions,
        })
    }

    /// Unregister a session and drop every stored auth context that
    /// belonged to it.
    pub fn unregister_session(&mut self, session_id: I::SessionId) -> UnregisterReport {
        let engine = self.inner.unregister_session(session_id);
        self.contexts
            .retain(|_, ctx| ctx.session != Some(session_id));
        engine
    }

    /// Unregister a subscription by id, resolving whichever registry holds
    /// it. Returns false if no such subscription existed.
    ///
    /// One id counter serves both registries (`next_subscription_id` lives
    /// only on the inner engine), so an id cannot be claimed by both and the
    /// order below is a resolution, not a precedence. The read registry is
    /// tried first, and when it claims the id the stored resolve context is
    /// dropped with it.
    pub fn unregister_subscription(&mut self, subscription_id: SubscriptionId) -> bool {
        if self.inner.unregister_reread(subscription_id) {
            self.contexts.remove(&subscription_id);
            return true;
        }
        self.inner.unregister_subscription(subscription_id)
    }

    /// Unregister an in-process subscription by `(consumer_id, sql)`.
    pub fn unregister_query(
        &mut self,
        consumer_id: I::ConsumerId,
        sql: &str,
    ) -> Result<UnregisterReport, RegisterError> {
        self.inner.unregister_query(consumer_id, sql)
    }

    /// Advance the resume cursor for `(session_id, sub_id)`. Passthrough to
    /// [`SubscriptionEngine::advance_cursor`](crate::SubscriptionEngine::advance_cursor).
    ///
    /// # Errors
    ///
    /// [`crate::AdvanceCursorError::NonMonotonic`] when `checkpoint` rewinds.
    pub fn advance_cursor(
        &mut self,
        session_id: I::SessionId,
        sub_id: SubscriptionId,
        checkpoint: crate::OpaqueCheckpoint,
    ) -> Result<Option<crate::OpaqueCheckpoint>, crate::AdvanceCursorError> {
        self.inner.advance_cursor(session_id, sub_id, checkpoint)
    }

    /// Set the resume cursor for `(session_id, sub_id)` unconditionally.
    /// Passthrough to
    /// [`SubscriptionEngine::force_set_cursor`](crate::SubscriptionEngine::force_set_cursor).
    pub fn force_set_cursor(
        &mut self,
        session_id: I::SessionId,
        sub_id: SubscriptionId,
        checkpoint: crate::OpaqueCheckpoint,
    ) -> Option<crate::OpaqueCheckpoint> {
        self.inner.force_set_cursor(session_id, sub_id, checkpoint)
    }

    /// Read the resume cursor for `(session_id, sub_id)`. Passthrough to
    /// [`SubscriptionEngine::cursor_for`](crate::SubscriptionEngine::cursor_for).
    #[must_use]
    pub fn cursor_for(
        &self,
        session_id: I::SessionId,
        sub_id: SubscriptionId,
    ) -> Option<&crate::OpaqueCheckpoint> {
        self.inner.cursor_for(session_id, sub_id)
    }

    /// Iterate `(subscription_id, cursor)` for every cursor stored against
    /// `session_id`. Passthrough to
    /// [`SubscriptionEngine::cursors_for_session`](crate::SubscriptionEngine::cursors_for_session).
    pub fn cursors_for_session(
        &self,
        session_id: I::SessionId,
    ) -> impl Iterator<Item = (SubscriptionId, &crate::OpaqueCheckpoint)> + '_ {
        self.inner.cursors_for_session(session_id)
    }

    /// Remove the resume cursor for `(session_id, sub_id)`. Passthrough to
    /// [`SubscriptionEngine::drop_cursor`](crate::SubscriptionEngine::drop_cursor).
    pub fn drop_cursor(
        &mut self,
        session_id: I::SessionId,
        sub_id: SubscriptionId,
    ) -> Option<crate::OpaqueCheckpoint> {
        self.inner.drop_cursor(session_id, sub_id)
    }

    /// Match `event` against the registered subscriptions without reading or
    /// folding, for catchup replay of an event the caller already dispatched.
    ///
    /// Delegates straight to
    /// [`SubscriptionEngine::consumers`](crate::SubscriptionEngine::consumers),
    /// so no connector call is made and no aggregate fold advances. The return
    /// carries more than its name suggests: alongside the row verdicts it
    /// reports term-membership narrowings, which a replay announces a second
    /// time. Re-applying them is a set union or difference that does not move
    /// the stored state, but a caller acting on the announcement itself must
    /// treat a replay's narrowings as possibly-stale repeats.
    ///
    /// # Errors
    ///
    /// [`crate::DispatchError`] when the event cannot be matched.
    pub fn match_rows(
        &mut self,
        event: &E,
    ) -> Result<crate::ConsumerNotifications<I, E::Checkpoint, E::Backend>, crate::DispatchError>
    {
        self.inner.consumers(event)
    }
}

impl<E, I, DB, M, T> crate::Install<T> for AutoResolvingEngine<E, I, DB, M>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
    M: ResolverMode<E::Backend>,
    crate::SubscriptionEngine<E, I, DB>: crate::Install<T>,
{
    type Output = <crate::SubscriptionEngine<E, I, DB> as crate::Install<T>>::Output;
    type Error = <crate::SubscriptionEngine<E, I, DB> as crate::Install<T>>::Error;

    fn install(
        &mut self,
        subscription_id: SubscriptionId,
        input: T,
    ) -> Result<Self::Output, Self::Error> {
        crate::Install::install(&mut self.inner, subscription_id, input)
    }
}

impl<E, I, DB, X> crate::SubscriptionDispatch<I, E> for AutoResolvingEngine<E, I, DB, SyncMode<X>>
where
    E: CdcEvent + Send,
    E::Backend: SqlLiteralParse,
    <E::Backend as Backend>::Dialect: Send + Sync,
    I: IdTypes,
    DB: DatabaseLike + Send + 'static,
    X: Connector<Backend = E::Backend> + Send,
    X::AuthContext: Send,
{
    type Notifications = ReExecNotifications<I, E::Backend, E::Checkpoint>;
    type Error = ReExecError<X::Error>;

    fn consumers(&mut self, event: &E) -> Result<Self::Notifications, Self::Error> {
        Self::consumers(self, event)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::Postgres;
    use crate::backend::ScalarKind;
    use crate::testing::TestEvent;
    use crate::TableId;
    use crate::{DefaultIds, SubscriptionEngine};
    use core::cell::RefCell;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
        )
        .unwrap()
    }

    /// Records every call and serves a programmed value queue. Errors are
    /// modeled by leaving the queue empty when `panic_on_empty` is false.
    struct MockConnector {
        values: RefCell<alloc::vec::Vec<Value<Postgres>>>,
        calls: RefCell<alloc::vec::Vec<(String, BuiltinKind)>>,
    }

    impl MockConnector {
        fn new(values: alloc::vec::Vec<Value<Postgres>>) -> Self {
            Self {
                values: RefCell::new(values),
                calls: RefCell::new(alloc::vec::Vec::new()),
            }
        }
        fn call_count(&self) -> usize {
            self.calls.borrow().len()
        }
    }

    #[derive(Debug, PartialEq)]
    struct MockError(&'static str);

    impl core::fmt::Display for MockError {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl Connector for MockConnector {
        type AuthContext = ();
        type Error = MockError;
        type Checkpoint = crate::NoCheckpoint;
        type Backend = Postgres;

        fn execute_scalar(
            &self,
            sql: &str,
            column_kind: BuiltinKind,
            _auth: &(),
        ) -> Result<(Value<Postgres>, Option<Self::Checkpoint>), Self::Error> {
            self.calls
                .borrow_mut()
                .push((String::from(sql), column_kind));
            let value = self
                .values
                .borrow_mut()
                .pop()
                .ok_or(MockError("queue empty"))?;
            Ok((value, None))
        }

        fn read_page(
            &self,
            _sql: &str,
            _max_bytes: usize,
            _auth: &(),
        ) -> Result<
            super::super::connector::Snapshot<
                super::super::connector::RowPage<Postgres>,
                Self::Checkpoint,
            >,
            Self::Error,
        > {
            Err(MockError("read_page is not exercised by the scalar tests"))
        }
    }

    /// orders columns: id=0, price=1, quantity=2, status=3.
    fn row(id: i64, price: f64) -> Vec<Value<Postgres>> {
        alloc::vec![
            Value::Int(id),
            Value::Float(price),
            Value::Int(1),
            Value::String("paid".into()),
        ]
    }

    fn insert_event(table_id: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
        TestEvent::<Postgres>::insert(table_id, row(id, price)).with_pk_columns([0u16])
    }

    fn delete_event(table_id: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
        TestEvent::<Postgres>::delete(table_id, row(id, price)).with_pk_columns([0u16])
    }

    fn update_status_only(table_id: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
        TestEvent::<Postgres>::update(table_id, row(id, price), row(id, price))
            .with_pk_columns([0u16])
            .with_changed_columns([3u16])
    }

    fn engine_with_values(
        values: alloc::vec::Vec<Value<Postgres>>,
    ) -> (
        AutoResolvingEngine<TestEvent<Postgres>, DefaultIds, ParserDB, SyncMode<MockConnector>>,
        TableId,
    ) {
        let database = catalog();
        let orders_id =
            crate::catalog_helpers::table_id(&database, "orders").expect("orders table exists");
        let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
            database,
            PostgreSqlDialect {},
        );
        (
            AutoResolvingEngine::new(inner, SyncMode(MockConnector::new(values))),
            orders_id,
        )
    }

    /// Full path: register, bootstrap install, insert that does not displace
    /// the extreme (in-process scalar update, no connector call), delete of
    /// the current extreme (trigger -> connector -> ScalarUpdate). The
    /// returned notifications carry no triggers under AutoResolvingEngine.
    #[test]
    fn delete_of_extreme_resolves_via_connector() {
        // Connector returns 7.0 when re-run after the extreme is removed.
        let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(7.0)]);

        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec, got {other:?}"),
        };
        // Bootstrap: model = {1=>5.0}. Current MIN = 5.0.
        assert!(crate::Install::install(
            &mut e,
            qid,
            crate::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<crate::NoCheckpoint>
            }
        )
        .is_ok());

        // Insert price=9.0 (>5.0): in-process Unchanged, no scalar update, no trigger.
        let n = e.consumers(&insert_event(tid, 2, 9.0)).unwrap();
        assert!(n.scalar_updates.is_empty(), "insert above extreme");
        assert!(n.triggers.is_empty(), "auto-resolving never emits triggers");
        assert_eq!(e.connector().call_count(), 0, "no re-execution yet");

        // Delete id=1, price=5.0 (the current extreme): trigger -> connector -> 7.0.
        let n = e.consumers(&delete_event(tid, 1, 5.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].subscription_id, qid);
        assert_eq!(n.scalar_updates[0].value, Value::Float(7.0));
        assert!(
            n.triggers.is_empty(),
            "AutoResolvingEngine consumes triggers internally"
        );
        assert_eq!(e.connector().call_count(), 1);
        let (sql, kind) = e.connector().calls.borrow()[0].clone();
        assert!(sql.contains("MIN"));
        assert_eq!(kind, ScalarKind::Float);
    }

    #[test]
    fn unrelated_column_update_does_not_call_connector() {
        let (mut e, tid) = engine_with_values(alloc::vec![]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MAX(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec, got {other:?}"),
        };
        assert!(crate::Install::install(
            &mut e,
            qid,
            crate::ScalarInstall {
                value: Value::Float(10.0),
                checkpoint: None::<crate::NoCheckpoint>
            }
        )
        .is_ok());

        let n = e.consumers(&update_status_only(tid, 1, 10.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert!(n.triggers.is_empty());
        assert_eq!(e.connector().call_count(), 0);
    }

    #[test]
    fn connector_error_aborts_batch() {
        // Empty queue: the connector errors on first call.
        let (mut e, tid) = engine_with_values(alloc::vec![]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec, got {other:?}"),
        };
        assert!(crate::Install::install(
            &mut e,
            qid,
            crate::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<crate::NoCheckpoint>
            }
        )
        .is_ok());

        match e.consumers(&delete_event(tid, 1, 5.0)) {
            Ok(_) => panic!("expected Connector error, got Ok"),
            Err(ReExecError::Connector(MockError(msg))) => assert_eq!(msg, "queue empty"),
            Err(other) => panic!("expected Connector error, got {other:?}"),
        }
    }

    /// `snapshot(subscription_id)` reads through the connector and installs the
    /// value so subsequent dispatches see it as the current state.
    #[test]
    fn snapshot_installs_via_connector() {
        let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(12.5)]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec, got {other:?}"),
        };

        // No bootstrap install: snapshot does it.
        let snap = e.snapshot(qid).unwrap().expect("subscription_id exists");
        match snap {
            SnapshotResult::Scalar(value, checkpoint) => {
                assert_eq!(value, Value::Float(12.5));
                // MockConnector returns checkpoint = None.
                assert!(checkpoint.is_none());
            }
            other => panic!("a scalar capture snapshots as a scalar, got {other:?}"),
        }
        assert_eq!(e.connector().call_count(), 1);

        // After snapshot, the engine treats 12.5 as the current MIN.
        // An insert below it (e.g. 9.0) becomes the new in-process MIN.
        let n = e.consumers(&insert_event(tid, 2, 9.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].value, Value::Float(9.0));
        // No additional connector call: the insert was resolved in-process.
        assert_eq!(e.connector().call_count(), 1);
    }

    /// `snapshot(subscription_id)` on an unknown id returns `Ok(None)` rather
    /// than panicking so callers can race snapshot against unregister.
    #[test]
    fn snapshot_unknown_query_returns_none() {
        let (mut e, _tid) = engine_with_values(alloc::vec![]);
        assert!(e.snapshot(99999).unwrap().is_none());
        // Connector was never called.
        assert_eq!(e.connector().call_count(), 0);
    }

    /// T4.1 + T4.2: a batch of 3 events that displace the same captured
    /// query's extreme produces ONE connector call (dedup), and engine
    /// notifications come back in input order.
    #[test]
    fn consumers_batch_coalesces_repeated_triggers() {
        // Connector serves a single value, which is what we expect since
        // the trigger should be deduplicated to one call.
        let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(99.0)]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec, got {other:?}"),
        };
        // Bootstrap: extreme is 5.0.
        assert!(crate::Install::install(
            &mut e,
            qid,
            crate::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<crate::NoCheckpoint>
            }
        )
        .is_ok());

        // Three DELETEs of the current extreme. Each one in isolation
        // would emit a trigger, the batch should collapse them.
        let events = alloc::vec![
            delete_event(tid, 1, 5.0),
            delete_event(tid, 2, 5.0),
            delete_event(tid, 3, 5.0),
        ];

        let outcome = e.consumers_batch(&events).unwrap();
        assert_eq!(
            outcome.per_event.len(),
            3,
            "per_event must align positionally with input"
        );
        assert_eq!(
            e.connector().call_count(),
            1,
            "three displacing events must collapse to one connector call"
        );
        assert_eq!(outcome.scalar_updates.len(), 1);
        assert_eq!(outcome.scalar_updates[0].value, Value::Float(99.0));
        assert!(
            outcome.triggers.is_empty(),
            "auto-resolving drains triggers"
        );
    }

    /// T4.3: a connector failure mid-batch aborts the whole batch with
    /// `ReExecError::Connector` and the caller is expected to retry.
    #[test]
    fn consumers_batch_connector_error_aborts() {
        // Empty value queue: connector errors on first call.
        let (mut e, tid) = engine_with_values(alloc::vec![]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec, got {other:?}"),
        };
        assert!(crate::Install::install(
            &mut e,
            qid,
            crate::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<crate::NoCheckpoint>
            }
        )
        .is_ok());

        let events = alloc::vec![delete_event(tid, 1, 5.0)];

        match e.consumers_batch(&events) {
            Ok(_) => panic!("expected Connector error, got Ok"),
            Err(ReExecError::Connector(MockError(msg))) => assert_eq!(msg, "queue empty"),
            Err(other) => panic!("expected Connector error, got {other:?}"),
        }
    }

    /// Coalescing only collapses the **same** `subscription_id`. Distinct captured
    /// queries each trigger their own connector call.
    #[test]
    #[allow(clippy::similar_names)]
    fn consumers_batch_does_not_coalesce_distinct_queries() {
        // Two captured queries on the same table. Connector returns 11.0
        // (popped first) for one and 22.0 (popped second) for the other.
        // MockConnector pops from the back, so push values in reverse:
        // first pop = 22.0, second pop = 11.0.
        let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(22.0), Value::Float(11.0)]);
        let qid1 = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec for MIN, got {other:?}"),
        };
        let qid2 = match e
            .register(
                SubscriptionRequest::new(2u64, "SELECT MAX(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec for MAX, got {other:?}"),
        };
        // Bootstrap both at 7.0 so deleting price=7.0 displaces both.
        assert!(crate::Install::install(
            &mut e,
            qid1,
            crate::ScalarInstall {
                value: Value::Float(7.0),
                checkpoint: None::<crate::NoCheckpoint>
            }
        )
        .is_ok());
        assert!(crate::Install::install(
            &mut e,
            qid2,
            crate::ScalarInstall {
                value: Value::Float(7.0),
                checkpoint: None::<crate::NoCheckpoint>
            }
        )
        .is_ok());

        let events = alloc::vec![delete_event(tid, 1, 7.0)];
        let outcome = e.consumers_batch(&events).unwrap();
        assert_eq!(e.connector().call_count(), 2, "one call per distinct query");
        assert_eq!(outcome.scalar_updates.len(), 2);
        let qids: alloc::collections::BTreeSet<_> = outcome
            .scalar_updates
            .iter()
            .map(|u| u.subscription_id)
            .collect();
        assert!(qids.contains(&qid1));
        assert!(qids.contains(&qid2));
    }

    /// T6.1: a second displacing event within the debounce window is
    /// dropped: connector is not called and no ScalarUpdate is emitted.
    /// T6.2 in the same test: after the clock ticks past the window the
    /// next trigger fires normally.
    #[test]
    // The `.clone()` below needs the Arc<ManualClock> -> Arc<dyn Clock>
    // unsize coercion at the assignment site. `Arc::clone(&clock)` would
    // need an already-coerced source. Allow the clippy lint here.
    #[allow(clippy::clone_on_ref_ptr)]
    fn debounce_skips_within_window_and_fires_after() {
        let clock = alloc::sync::Arc::new(crate::ManualClock::new(0));
        let engine_clock: crate::ClockHandle = clock.clone();
        // Two values in the connector queue: one for the first re-exec,
        // one for the post-window re-exec. The "within window" re-exec
        // is debounced and never reaches the connector.
        let (e0, tid) = engine_with_values(alloc::vec![Value::Float(20.0), Value::Float(7.0)]);
        let mut e = e0
            .with_clock(engine_clock)
            .with_debounce_per_query(core::time::Duration::from_millis(100));

        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec, got {other:?}"),
        };
        assert!(crate::Install::install(
            &mut e,
            qid,
            crate::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<crate::NoCheckpoint>
            }
        )
        .is_ok());

        // First displacing event: re-exec proceeds (no prior stamp).
        let n = e.consumers(&delete_event(tid, 1, 5.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].value, Value::Float(7.0));
        assert_eq!(e.connector().call_count(), 1);

        // Second displacing event within 50ms (window is 100ms): skipped.
        clock.advance(core::time::Duration::from_millis(50));
        // The engine's MIN is currently 7.0 from the prior re-exec. To
        // force a second trigger we delete a row matching 7.0.
        let n = e.consumers(&delete_event(tid, 2, 7.0)).unwrap();
        assert!(
            n.scalar_updates.is_empty(),
            "debounced trigger must not emit a ScalarUpdate"
        );
        assert_eq!(
            e.connector().call_count(),
            1,
            "debounced trigger must not call the connector"
        );

        // Past the window now: 50ms + 100ms = 150ms total since first.
        clock.advance(core::time::Duration::from_millis(100));
        // Reinstall the value the engine thinks is current so the next
        // displacement is well-defined. The test exercises debounce,
        // not the state machine.
        assert!(crate::Install::install(
            &mut e,
            qid,
            crate::ScalarInstall {
                value: Value::Float(7.0),
                checkpoint: None::<crate::NoCheckpoint>
            }
        )
        .is_ok());
        let n = e.consumers(&delete_event(tid, 3, 7.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1, "post-window trigger must fire");
        assert_eq!(n.scalar_updates[0].value, Value::Float(20.0));
        assert_eq!(e.connector().call_count(), 2);
    }

    /// Without a configured clock, `with_debounce_per_query` is a no-op:
    /// triggers fire as normal.
    #[test]
    fn debounce_without_clock_is_a_noop() {
        let (e0, tid) = engine_with_values(alloc::vec![Value::Float(9.0), Value::Float(7.0)]);
        let mut e = e0.with_debounce_per_query(core::time::Duration::from_secs(3600));

        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec, got {other:?}"),
        };
        assert!(crate::Install::install(
            &mut e,
            qid,
            crate::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<crate::NoCheckpoint>
            }
        )
        .is_ok());

        assert!(!e
            .consumers(&delete_event(tid, 1, 5.0))
            .unwrap()
            .scalar_updates
            .is_empty());
        // The engine now has 7.0 installed. Deleting it again fires another
        // trigger. The debounce-without-clock case must NOT skip it.
        let n = e.consumers(&delete_event(tid, 2, 7.0)).unwrap();
        assert_eq!(n.scalar_updates.len(), 1, "no clock -> no debounce");
        assert_eq!(e.connector().call_count(), 2);
    }

    #[test]
    fn unregister_drops_auth_context() {
        let (mut e, _tid) = engine_with_values(alloc::vec![]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected ReExec, got {other:?}"),
        };
        assert_eq!(e.contexts.len(), 1);
        assert!(e.unregister_subscription(qid));
        assert_eq!(e.contexts.len(), 0);
        assert!(!e.unregister_subscription(qid), "second drop is a no-op");
    }
    #[test]
    fn grouped_batch_keeps_one_trigger_per_displaced_group() {
        let (mut engine, table) = engine_with_values(Vec::new());
        let subscription = engine
            .register(
                SubscriptionRequest::new(
                    1u64,
                    "SELECT status, MIN(price) FROM orders GROUP BY status",
                ),
                (),
            )
            .expect("grouped minimum registers")
            .subscription_id;
        crate::Install::install(
            &mut engine.inner,
            subscription,
            crate::GroupedScalarSeedInstall {
                rows: vec![
                    vec![
                        Value::String("paid".into()),
                        Value::Float(5.0),
                        Value::Int(2),
                    ],
                    vec![
                        Value::String("void".into()),
                        Value::Float(7.0),
                        Value::Int(2),
                    ],
                ],
                read_at: None::<crate::NoCheckpoint>,
            },
        )
        .expect("group map installs");
        let delete = |id, price, status: &str| {
            TestEvent::<Postgres>::delete(
                table,
                vec![
                    Value::Int(id),
                    Value::Float(price),
                    Value::Int(1),
                    Value::String(status.into()),
                ],
            )
            .with_pk_columns([0u16])
        };
        let output = engine
            .inner
            .reread_batch(&[delete(1, 5.0, "paid"), delete(2, 7.0, "void")])
            .expect("batch dispatches");
        assert_eq!(output.triggers.len(), 2);
        let mut groups: Vec<_> = output
            .triggers
            .iter()
            .filter_map(|trigger| trigger.read.group_key().map(<[u8]>::to_vec))
            .collect();
        groups.sort_unstable();
        groups.dedup();
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn grouped_debounce_is_scoped_by_group_key() {
        let clock = alloc::sync::Arc::new(crate::ManualClock::new(0));
        let engine_clock: crate::ClockHandle = clock;
        let (engine, _) = engine_with_values(Vec::new());
        let mut engine = engine
            .with_clock(engine_clock)
            .with_debounce_per_query(core::time::Duration::from_secs(1));
        let first = super::super::ReExecutionRead::GroupedScalar {
            group: vec![1],
            sql: String::new(),
            column_kinds: [ScalarKind::Int, ScalarKind::Int],
        };
        let second = super::super::ReExecutionRead::GroupedScalar {
            group: vec![2],
            sql: String::new(),
            column_kinds: [ScalarKind::Int, ScalarKind::Int],
        };
        engine.stamp_reexec(7, &first);
        assert!(engine.debounce_skip(7, &first));
        assert!(!engine.debounce_skip(7, &second));
    }

    #[test]
    fn unregister_subscription_resolves_either_registry() {
        let (mut e, _tid) = engine_with_values(alloc::vec![]);
        // Captured re-execution query: lands in the read registry with a context.
        let captured = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected Scalar, got {other:?}"),
        };
        // In-process row subscription: lands in the in-process registry, no context.
        let in_process = match e
            .register(
                SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE price > 100"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::InProcess(_),
                ..
            } => subscription_id,
            other => panic!("expected InProcess, got {other:?}"),
        };
        assert!(
            e.unregister_subscription(captured),
            "read registry id resolves"
        );
        assert!(
            e.unregister_subscription(in_process),
            "in-process registry id resolves"
        );
        assert!(
            !e.unregister_subscription(999u64),
            "an unknown id resolves to neither registry"
        );
    }

    #[test]
    fn unregister_subscription_drops_the_resolve_context() {
        let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(7.0)]);
        let captured = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected Scalar, got {other:?}"),
        };
        crate::Install::install(
            &mut e,
            captured,
            crate::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<crate::NoCheckpoint>,
            },
        )
        .unwrap();
        assert_eq!(e.contexts.len(), 1);
        assert!(e.unregister_subscription(captured));
        assert_eq!(e.contexts.len(), 0, "the resolve context is dropped");
        // A later delete of the former extreme must not reach the connector.
        let n = e.consumers(&delete_event(tid, 1, 5.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert_eq!(
            e.connector().call_count(),
            0,
            "no connector call after unregister"
        );
    }

    #[test]
    fn cursor_state_is_reachable_through_the_wrapper() {
        let (mut e, _tid) = engine_with_values(alloc::vec![]);
        let session = 1u64;
        let sub = 7u64;
        let cp = crate::OpaqueCheckpoint(alloc::vec![1, 2, 3]);
        assert_eq!(e.advance_cursor(session, sub, cp.clone()), Ok(None));
        assert_eq!(e.cursor_for(session, sub), Some(&cp));
        // force_set bypasses the monotonic rule and returns the previous value.
        let older = crate::OpaqueCheckpoint(alloc::vec![0]);
        assert_eq!(e.force_set_cursor(session, sub, older.clone()), Some(cp));
        assert_eq!(e.cursor_for(session, sub), Some(&older));
        let listed: Vec<_> = e
            .cursors_for_session(session)
            .map(|(s, c)| (s, c.clone()))
            .collect();
        assert_eq!(listed, alloc::vec![(sub, older.clone())]);
        assert_eq!(e.drop_cursor(session, sub), Some(older));
        assert_eq!(e.cursor_for(session, sub), None);
    }

    #[test]
    fn match_rows_replays_without_reading_or_folding() {
        // One value, for the single live re-execution below. match_rows reads
        // nothing, so it must never consume it.
        let (mut e, tid) = engine_with_values(alloc::vec![Value::Float(7.0)]);
        // Captured MIN: a delete of its extreme is a read on the live path, and
        // the connector call that read needs is the guard that match_rows stays
        // off the resolving path.
        let min_id = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::Scalar { .. },
                ..
            } => subscription_id,
            other => panic!("expected Scalar, got {other:?}"),
        };
        crate::Install::install(
            &mut e,
            min_id,
            crate::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<crate::NoCheckpoint>,
            },
        )
        .unwrap();
        // In-process row subscription: gives match_rows a non-empty verdict.
        e.register(
            SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE price < 100"),
            (),
        )
        .unwrap();

        // A delete of the current extreme, replayed against the seeded model
        // before any live dispatch has moved it, so it is still displacing.
        let ev = delete_event(tid, 1, 5.0);
        let replay = e.match_rows(&ev).unwrap();
        assert!(
            !replay.deleted().is_empty(),
            "match_rows matched the row subscription"
        );
        assert_eq!(
            e.connector().call_count(),
            0,
            "match_rows read nothing from the connector even for a displacing delete"
        );

        // The live dispatch of the same delete is still the first read, which
        // proves match_rows left the re-execution model untouched: it resolves
        // MIN once, to 7.0.
        let live = e.consumers(&ev).unwrap();
        assert_eq!(
            e.connector().call_count(),
            1,
            "the re-execution model was untouched, so the live read is the first"
        );
        assert_eq!(live.scalar_updates.len(), 1);
        assert_eq!(live.scalar_updates[0].value, Value::Float(7.0));
    }
}

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
use super::engine::{ReExecNotifications, RowDelta, RowsUpdate};
use crate::backend::{Backend, CdcEvent, ScalarFamily, Value};
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

mod install;
mod shared_reads;
mod state;
mod sync;
#[cfg(test)]
mod tests;

pub(super) use shared_reads::{
    absorb_keyed_page, decode_grouped_seed_rows, deltas_from, one_grouped_row,
    reconcile_checkpoint, KeyBatches, KeyedPage, SeenKeys,
};
pub(super) use state::{InProcessKind, ReadQueue, ResolveContext};

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
    pub(super) contexts: HashMap<SubscriptionId, ResolveContext<I, E::Backend, M::AuthContext>>,
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
    /// Reads discovered by [`apply`](Self::apply) and not yet delivered by
    /// `resolve`, deduplicated by subscription and group. A failed or
    /// abandoned resolve leaves them here, so retrying costs a read and
    /// never a second application of the event.
    pub(super) pending_reads: ReadQueue<I, E::Checkpoint, E::Backend>,
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
            pending_reads: ReadQueue::new(),
        }
    }

    /// Update connector-call metadata after the registry changes a subscription
    /// tier under the same identity.
    pub(super) fn apply_transitions(
        &mut self,
        transitions: &[crate::MaintenanceTransition<E::Backend>],
    ) {
        for transition in transitions {
            let Some(context) = self.contexts.get_mut(&transition.subscription_id) else {
                continue;
            };
            match &transition.to {
                Tier::Scalar { query, column_kind } => {
                    context.query = query.clone();
                    context.column_kind = *column_kind;
                    context.whole_result = false;
                    context.keyed = false;
                    context.grouped_bootstrap = None;
                }
                Tier::GroupedScalar { bootstrap } => {
                    context.query = bootstrap.query.clone();
                    context.column_kind = bootstrap
                        .kinds
                        .get(bootstrap.group_columns)
                        .copied()
                        .unwrap_or(ScalarFamily::String);
                    context.grouped_bootstrap = Some(bootstrap.clone());
                    context.whole_result = false;
                    context.keyed = false;
                }
                Tier::KeyedRows { query, .. } => {
                    context.query = query.clone();
                    context.column_kind = ScalarFamily::String;
                    context.whole_result = false;
                    context.keyed = true;
                    context.grouped_bootstrap = None;
                }
                Tier::WholeRows { query, .. } => {
                    context.query = query.clone();
                    context.column_kind = ScalarFamily::String;
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

    /// Rate-limit re-executions of one captured query, requiring
    /// [`with_clock`](Self::with_clock).
    ///
    /// A trigger inside the window is discarded rather than deferred and
    /// reported through
    /// [`Dispatched::debounced`](super::Dispatched::debounced). Nothing
    /// reschedules it, so the held value can be stale indefinitely.
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
        read: &super::ReExecutionRead<E::Backend>,
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
        read: &super::ReExecutionRead<E::Backend>,
    ) {
        if let Some(clock) = self.clock.as_ref() {
            self.last_reexec_at.insert(
                (subscription_id, read.group_key().map(<[u8]>::to_vec)),
                clock.now_micros(),
            );
        }
    }

    /// Queue one discovered read, replacing a queued read of the same
    /// subscription and group so a burst costs one read. A read inside its
    /// debounce window is dropped, exactly as the fused path dropped it.
    ///
    /// `false` when the debounce window discarded the trigger.
    pub(super) fn enqueue_read(
        &mut self,
        trigger: super::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
    ) -> bool {
        if self.debounce_skip(trigger.subscription_id, &trigger.read) {
            return false;
        }
        self.pending_reads.enqueue(trigger);
        true
    }

    /// Drop one queued read after its answer was installed and delivered.
    pub(super) fn dequeue_read(
        &mut self,
        trigger: &super::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
    ) {
        self.pending_reads
            .remove(trigger.subscription_id, trigger.read.group_key());
    }

    /// Reads waiting for the next `resolve`.
    #[must_use]
    pub fn pending_read_count(&self) -> usize {
        self.pending_reads.len()
    }

    /// Fold one CDC event into in-memory state, exactly once.
    ///
    /// Returns the notifications that state produces: row matches, in-process
    /// aggregate and scalar updates, and tier transitions. Reads the event
    /// makes necessary are queued, deduplicated by subscription and group,
    /// for `resolve` to execute. `apply` never touches the database, so its
    /// effects commit exactly once however the later reads fare, and
    /// retrying an applied event is never correct.
    ///
    /// # Errors
    ///
    /// [`crate::DispatchError`] when the event cannot be dispatched. Nothing
    /// is applied in that case.
    pub fn apply(
        &mut self,
        event: &E,
    ) -> Result<super::Dispatched<I, E::Backend, E::Checkpoint>, crate::DispatchError> {
        let ReExecNotifications {
            engine,
            aggregate_updates,
            scalar_updates,
            rows_updates,
            row_deltas,
            triggers,
            transitions,
        } = self.inner.reread_notifications(event)?;
        // Debug-time only. The invariant is the core's and is enforced
        // there by `the_core_delivers_no_read_answers`.
        debug_assert!(
            rows_updates.is_empty() && row_deltas.is_empty(),
            "the core has no connector, so it cannot deliver read answers"
        );
        self.apply_transitions(&transitions);
        let mut debounced = 0usize;
        for trigger in triggers {
            if !self.enqueue_read(trigger) {
                debounced += 1;
            }
        }
        debounced += self.enqueue_unanswered(&engine, event);
        Ok(super::Dispatched {
            engine,
            aggregate_updates,
            scalar_updates,
            transitions,
            outstanding: self.pending_read_count(),
            debounced,
        })
    }

    /// Queue a read for every subscription the event could not answer.
    ///
    /// The core reports these and stops there: it holds no connector, so a
    /// cell the stream did not carry leaves it with no answer to give. Here
    /// the query is retained and the connector is owned, so the report
    /// becomes the read that answers it, deduplicated and debounced like
    /// every other discovered read.
    ///
    /// Answers how many the debounce window discarded.
    fn enqueue_unanswered(
        &mut self,
        notifications: &crate::ConsumerNotifications<I, E::Checkpoint, E::Backend>,
        event: &E,
    ) -> usize {
        let mut dropped = 0usize;
        for entry in notifications.unanswered() {
            if !self.enqueue_read(super::ReExecutionTrigger {
                subscription_id: entry.subscription_id,
                consumer_id: entry.consumer_id,
                read: super::ReExecutionRead::Subscription,
                checkpoint: event.checkpoint(),
            }) {
                dropped += 1;
            }
        }
        dropped
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
    ) -> Result<Registered<E::Backend>, RegisterError>
    where
        R: crate::RegistrationRequest<I, E::Backend>,
    {
        let database_reads_per_consumer = R::DATABASE_READS_PER_CONSUMER;
        let spec = spec.into_request();
        let session = match spec.scope {
            SubscriptionScope::Session(s) => Some(s),
            SubscriptionScope::Durable => None,
        };
        // Retained before the spec is consumed, for the one case that needs
        // it: an in-process filter the stream later cannot answer, whose
        // report has to become a read. Nothing reads it per event.
        let source_query = crate::reexec::BoundQuery::new(spec.sql.clone(), spec.binds.clone());
        let result = self
            .inner
            .register_request(spec, database_reads_per_consumer)?;
        match &result.tier {
            Tier::Scalar { query, column_kind } => {
                self.contexts.insert(
                    result.subscription_id,
                    ResolveContext {
                        query: query.clone(),
                        column_kind: *column_kind,
                        grouped_bootstrap: None,
                        whole_result: false,
                        keyed: false,
                        in_process: None,
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
                        query: bootstrap.query.clone(),
                        column_kind: bootstrap
                            .kinds
                            .get(bootstrap.group_columns)
                            .copied()
                            .unwrap_or(ScalarFamily::String),
                        grouped_bootstrap: Some(bootstrap.clone()),
                        whole_result: false,
                        keyed: false,
                        in_process: None,
                        generation: 0,
                        session,
                        auth,
                    },
                );
            }
            Tier::KeyedRows { query, .. } | Tier::WholeRows { query, .. } => {
                self.contexts.insert(
                    result.subscription_id,
                    ResolveContext {
                        query: query.clone(),
                        // No single column to decode: the rows carry their own
                        // shape, which is why `RowPage` reports column names.
                        column_kind: ScalarFamily::String,
                        grouped_bootstrap: None,
                        // The tier decides which read serves a change, so it
                        // comes from the registration rather than a default.
                        // Defaulting here once made every keyed capture resolve
                        // as a whole re-read, which is correct output produced
                        // the expensive way, so nothing failed and nothing said
                        // so.
                        whole_result: matches!(result.tier, Tier::WholeRows { .. }),
                        keyed: matches!(result.tier, Tier::KeyedRows { .. }),
                        in_process: None,
                        generation: 0,
                        session,
                        auth,
                    },
                );
            }
            Tier::InProcess(served) => {
                // A still-folding in-process aggregate keeps only its auth and
                // session, so a later demotion to a whole re-read resolves with
                // the caller's own auth. The fold runs in the engine; nothing is
                // read here.
                //
                // A plain row filter keeps its own query as a whole read. It
                // is never executed while the stream can answer the filter,
                // and exists for the case where the stream cannot: a cell the
                // event did not carry has no answer in memory, and the read is
                // the only way to give the subscriber one.
                let (query, whole_result, kind) = served.aggregate_bootstrap.as_ref().map_or(
                    (source_query, true, InProcessKind::StreamServedFilter),
                    |bootstrap| {
                        (
                            bootstrap.query.clone(),
                            false,
                            InProcessKind::FoldingAggregate,
                        )
                    },
                );
                self.contexts.insert(
                    result.subscription_id,
                    ResolveContext {
                        query,
                        column_kind: ScalarFamily::String,
                        grouped_bootstrap: None,
                        whole_result,
                        keyed: false,
                        in_process: Some(kind),
                        generation: 0,
                        session,
                        auth,
                    },
                );
            }
        }
        Ok(result)
    }

    /// Describe the membership terms a registration would compile, without
    /// registering. The read-only sibling of [`register`](Self::register),
    /// delegating to
    /// [`SubscriptionEngine::describe_terms`](crate::SubscriptionEngine::describe_terms),
    /// so a caller can learn what a filter needs seeded before it registers,
    /// without reaching the inner engine.
    ///
    /// # Errors
    ///
    /// Whatever the same registration would answer short of the seed:
    /// [`RegisterError`] for a filter outside SubQL's shape.
    pub fn describe_terms(
        &self,
        spec: &crate::SubscriptionRequest<I, E::Backend>,
    ) -> Result<Vec<crate::term::TermDescription>, RegisterError> {
        self.inner.describe_terms(spec)
    }

    /// Drop every queued read whose subscription no longer holds a resolve
    /// context, which is what unregistration leaves behind. A queued read
    /// that outlived its subscription would send the next `resolve` to look
    /// up state that is gone.
    fn purge_unregistered_reads(&mut self) {
        let contexts = &self.contexts;
        self.pending_reads
            .retain(|trigger| contexts.contains_key(&trigger.subscription_id));
    }

    /// Unregister a session and drop every stored auth context and queued
    /// read that belonged to it.
    pub fn unregister_session(&mut self, session_id: I::SessionId) -> UnregisterReport {
        let engine = self.inner.unregister_session(session_id);
        self.contexts
            .retain(|_, ctx| ctx.session != Some(session_id));
        self.purge_unregistered_reads();
        engine
    }

    /// Unregister a subscription by id, resolving whichever registry holds
    /// it. Returns false if no such subscription existed.
    ///
    /// One id counter serves both registries (`next_subscription_id` lives
    /// only on the inner engine), so an id cannot be claimed by both and the
    /// order below is a resolution, not a precedence. The read registry is
    /// tried first, and when it claims the id the stored resolve context and
    /// any queued read are dropped with it.
    pub fn unregister_subscription(&mut self, subscription_id: SubscriptionId) -> bool {
        if self.inner.unregister_reread(subscription_id) {
            self.contexts.remove(&subscription_id);
            self.purge_unregistered_reads();
            return true;
        }
        let removed = self.inner.unregister_subscription(subscription_id);
        if removed {
            // Drop the stored context and any queued read: an in-process
            // aggregate's auth for a possible demotion, or a row filter's
            // retained query for a report the stream could not answer.
            self.contexts.remove(&subscription_id);
            self.purge_unregistered_reads();
        }
        removed
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

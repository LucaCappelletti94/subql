//! Async parallel of [`AutoResolvingEngine`](super::AutoResolvingEngine).
//!
//! Same surface (`register`, `install`, `snapshot`, `apply`,
//! `resolve_collect`, `unregister_*`), with the methods that touch the
//! connector returning `Send` futures. Pick this engine when the database
//! driver is async (sqlx, tokio-postgres, diesel-async). Pick the sync
//! engine when the driver is sync (diesel, rusqlite) or when you want the
//! simpler testing surface.

use super::async_connector::AsyncConnector;
use super::auto::{reconcile_checkpoint, AutoResolvingEngine, ResolverMode, SnapshotResult};
use super::connector::ReExecError;
use crate::backend::{Backend, CdcEvent, Value};
use crate::compiler::literals::SqlLiteralParse;
use crate::{IdTypes, SubscriptionId};
use alloc::sync::Arc;
use alloc::vec::Vec;
use async_lock::{Semaphore, SemaphoreGuardArc};
use core::sync::atomic::{AtomicUsize, Ordering};
use sql_traits::prelude::DatabaseLike;

mod jobs;
#[cfg(test)]
mod tests;
mod throttle;

use jobs::{
    KeyedJob, KeyedRows, PlannedJob, ReadOutcome, ReadOutcomes, ReadPage, ResolveJob, Resolved,
};
use throttle::{acquire_permit, ThrottleState};

/// Asynchronous [`AsyncConnector`] mode.
pub struct AsyncMode<X> {
    /// Connector called by asynchronous methods.
    pub connector: X,
    /// Persistent concurrency throttle.
    permits: Option<ThrottleState>,
}

impl<X> AsyncMode<X> {
    /// Wrap an asynchronous connector with no concurrency cap.
    pub const fn new(connector: X) -> Self {
        Self {
            connector,
            permits: None,
        }
    }
}

impl<B: Backend, X: AsyncConnector<Backend = B>> ResolverMode<B> for AsyncMode<X> {
    type AuthContext = X::AuthContext;
}

impl<E, I, DB, X> AutoResolvingEngine<E, I, DB, AsyncMode<X>>
where
    E: CdcEvent + Sync,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
    X: AsyncConnector<Backend = E::Backend>,
{
    /// Cap the number of trigger re-executions that may be in flight
    /// simultaneously across all [`apply`](Self::apply) and
    /// [`resolve_collect`](Self::resolve_collect) calls.
    ///
    /// The cap is enforced by a persistent semaphore on the engine: each
    /// `connector.execute_scalar(...)` acquires a permit before running
    /// and releases it on completion (including cancellation). With cap
    /// `N`, no more than `N` SQL queries are in flight at any moment,
    /// regardless of whether triggers arrive one per event or in batched
    /// bursts.
    ///
    /// Default `None`: every deduplicated trigger is dispatched
    /// concurrently. The connector's own pool may still throttle.
    ///
    /// A `cap` of 0 is treated as 1 to avoid a deadlock.
    #[must_use]
    pub fn with_max_concurrent_reexecutions(mut self, cap: usize) -> Self {
        let cap = cap.max(1);
        self.mode.permits = Some(ThrottleState {
            sem: Arc::new(Semaphore::new(cap)),
            inflight: Arc::new(AtomicUsize::new(0)),
            cap,
        });
        self
    }

    /// Number of re-execution permits currently held, i.e. concurrent
    /// connector calls in flight. Returns 0 when no cap is configured
    /// (unbounded mode does not track inflight). Useful for operator
    /// dashboards alerting on sustained `inflight ~= cap`.
    #[must_use]
    pub fn inflight(&self) -> usize {
        self.mode
            .permits
            .as_ref()
            .map_or(0, |state| state.inflight.load(Ordering::Acquire))
    }

    /// Configured concurrency cap, if any. `None` means unbounded.
    #[must_use]
    pub fn concurrency_cap(&self) -> Option<usize> {
        self.mode.permits.as_ref().map(|s| s.cap)
    }

    /// The connector this engine drives.
    pub const fn connector(&self) -> &X {
        &self.mode.connector
    }

    /// Bootstrap a captured query by reading its current answer through the
    /// async connector. Async analogue of
    /// [`AutoResolvingEngine::snapshot`](super::AutoResolvingEngine::snapshot),
    /// including the tier split: `Scalar` for a scalar capture, `Rows` for
    /// either row tier, and only the scalar is installed.
    ///
    /// # Errors
    ///
    /// Returns [`ReExecError::Connector`] if the connector fails, and
    /// [`ReExecError::Cursor`] if a row tier's read fails or the connector
    /// holds no cursors. Returns `Ok(None)` if `subscription_id` does not exist.
    #[allow(
        clippy::too_many_lines,
        reason = "snapshot handles each explicit read tier and always closes grouped cursors"
    )]
    pub async fn snapshot(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> Result<Option<SnapshotResult<E::Backend, X::Checkpoint, I>>, ReExecError<X::Error>> {
        let Some(context) = self.contexts.get(&subscription_id) else {
            return Ok(None);
        };
        // A subscription the stream maintains has nothing to prime. Ahead
        // of every branch below, because such a context carries
        // `whole_result` and would otherwise be read as one.
        if context.stream_answers_the_filter() {
            return Ok(None);
        }
        let grouped_bootstrap = context.grouped_bootstrap.clone();
        if let Some(bootstrap) = grouped_bootstrap {
            let (pages, checkpoint) = Self::read_whole_with(
                &self.mode.connector,
                subscription_id,
                &context.query,
                self.max_page_bytes,
                &context.auth,
            )
            .await?;
            let mut rows: Vec<_> = pages.into_iter().flat_map(|page| page.rows).collect();
            super::auto::decode_grouped_seed_rows::<E::Backend>(&mut rows, &bootstrap.kinds);
            let mut installed = crate::Install::install(
                &mut self.inner,
                subscription_id,
                crate::GroupedScalarSeedInstall {
                    rows,
                    read_at: reconcile_checkpoint(checkpoint.as_ref()),
                },
            )?;
            self.apply_transitions(&installed.transitions);
            let mut pending = core::mem::take(&mut installed.triggers);
            while let Some(trigger) = pending.pop() {
                match &trigger.read {
                    super::ReExecutionRead::GroupedScalar { group, query, .. } => {
                        let context = self
                            .contexts
                            .get(&subscription_id)
                            .expect("a grouped scalar read keeps its connector context");
                        let page = self
                            .mode
                            .connector
                            .read_page(&query.as_read_query(), self.max_page_bytes, &context.auth)
                            .await
                            .map_err(|error| ReExecError::Connector {
                                subscription: subscription_id,
                                error,
                            })?;
                        let row = super::auto::one_grouped_row(subscription_id, page.value)?;
                        let resolved = crate::Install::install(
                            &mut self.inner,
                            subscription_id,
                            crate::GroupedScalarInstall {
                                group: group.clone(),
                                row,
                                checkpoint: trigger.checkpoint.clone(),
                            },
                        )?;
                        self.apply_transitions(&resolved.transitions);
                        pending.extend(resolved.triggers);
                        installed.updates.extend(resolved.updates);
                        installed.transitions.extend(resolved.transitions);
                    }
                    super::ReExecutionRead::Subscription => {
                        let context = self
                            .contexts
                            .get(&subscription_id)
                            .expect("a transitioned read keeps its connector context");
                        let (pages, checkpoint) = Self::read_whole_with(
                            &self.mode.connector,
                            subscription_id,
                            &context.query,
                            self.max_page_bytes,
                            &context.auth,
                        )
                        .await?;
                        let mut columns = Vec::new();
                        let mut rows = Vec::new();
                        for page in pages {
                            if columns.is_empty() {
                                columns = page.columns;
                            }
                            rows.extend(page.rows);
                        }
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
            let query = context.query.clone();
            let (pages, checkpoint) = Self::read_whole_with(
                &self.mode.connector,
                subscription_id,
                &query,
                self.max_page_bytes,
                &context.auth,
            )
            .await?;
            let mut columns = Vec::new();
            let mut rows = Vec::new();
            for page in pages {
                if columns.is_empty() {
                    columns = page.columns;
                }
                rows.extend(page.rows);
            }
            return Ok(Some(SnapshotResult::Rows {
                columns,
                rows,
                checkpoint,
            }));
        }
        // A still-folding in-process aggregate is seeded through Install, not
        // read here. After a demotion the context is `whole_result` and handled
        // above, so this only fires before any demotion.
        if context.in_process == Some(super::auto::InProcessKind::FoldingAggregate) {
            return Ok(None);
        }
        let (value, checkpoint) = self
            .mode
            .connector
            .execute_scalar(
                &context.query.as_read_query(),
                context.column_kind,
                &context.auth,
            )
            .await
            .map_err(|error| ReExecError::Connector {
                subscription: subscription_id,
                error,
            })?;
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

    /// Execute every queued read through the connector, delivering each
    /// installed answer into `sink` as its read completes.
    ///
    /// The reads of one drain iteration run concurrently, at most
    /// [`with_max_concurrent_reexecutions`](Self::with_max_concurrent_reexecutions)
    /// in flight (unbounded when not configured). A read whose database
    /// call fails stays queued and the next `resolve` retries it. A read
    /// whose answer fails to install is already dequeued when the install
    /// runs, matching the sync engine: install failures are not retryable,
    /// per [`ReExecError::is_retryable`](super::ReExecError::is_retryable).
    /// Reads that succeeded in the same iteration are still installed and
    /// delivered before the failure is reported.
    ///
    /// Dropping the returned future loses nothing: engine state moves only
    /// between awaits, keys are copied rather than taken, and a read is
    /// dequeued only in the same poll that installs and delivers it, so
    /// undelivered reads stay queued for the next call.
    ///
    /// # Errors
    ///
    /// [`ReExecError::Connector`] and [`ReExecError::Cursor`] name the
    /// subscription whose read failed. Install errors mean the database
    /// answer does not match the subscription, and
    /// [`ReExecError::KeyedRowShape`] that a keyed row does not carry every
    /// key column the read named. Neither is retryable.
    pub async fn resolve<S>(&mut self, mut sink: S) -> Result<(), ReExecError<X::Error>>
    where
        S: FnMut(super::ReadDelivery<I, E::Backend, E::Checkpoint>) + Send,
    {
        use futures_util::stream::StreamExt;

        loop {
            if self.pending_reads.is_empty() {
                return Ok(());
            }
            let jobs = self.plan_pending_jobs();
            if jobs.is_empty() {
                continue;
            }

            // Phase two: shared borrows only, so the reads run concurrently.
            let connector = &self.mode.connector;
            let contexts = &self.contexts;
            let max_page_bytes = self.max_page_bytes;
            let throttle = self
                .mode
                .permits
                .as_ref()
                .map(|s| (Arc::clone(&s.sem), Arc::clone(&s.inflight)));
            let jobs_len = jobs.len();
            let resolved = {
                // Whole reads stream their pages from inside the concurrent
                // phase, so the sink is shared under an async lock for the
                // duration and handed back exclusively afterwards.
                let shared_sink = async_lock::Mutex::new(&mut sink);
                futures_util::stream::iter(jobs.into_iter().map(|(trigger, job)| {
                    let auth = &contexts
                        .get(&trigger.subscription_id)
                        .expect(
                            "every captured query stores its resolve context at register time, \
                             trigger.subscription_id must exist in `contexts`",
                        )
                        .auth;
                    Self::run_one(
                        connector,
                        &shared_sink,
                        trigger,
                        job,
                        max_page_bytes,
                        auth,
                        throttle.clone(),
                    )
                }))
                .buffer_unordered(jobs_len)
                .collect::<Vec<_>>()
                .await
            };

            self.apply_outcomes(resolved, &mut sink)?;
            // Grouped installs may have queued follow-up reads: loop drains
            // them with the same concurrency.
        }
    }

    /// Phase one of a resolve iteration, under `&mut self`: decide each
    /// queued read's job against a snapshot. Keys are copied, never taken,
    /// so a dropped future loses nothing. A read with nothing to ask is
    /// dequeued with its debounce stamp moved, as if it had been read.
    fn plan_pending_jobs(&mut self) -> Vec<PlannedJob<I, E::Checkpoint, E::Backend>> {
        let snapshot = self.pending_reads.snapshot();
        let mut jobs = Vec::with_capacity(snapshot.len());
        for trigger in snapshot {
            if let Some(job) = self.plan_job(&trigger) {
                jobs.push((trigger, job));
            } else {
                self.dequeue_read(&trigger);
                self.stamp_reexec(trigger.subscription_id, &trigger.read);
            }
        }
        jobs
    }

    /// One planned read, phase two: a whole read streams its pages into the
    /// shared sink, everything else resolves to an answer for phase three.
    /// Holds no exclusive borrow of the engine, so these run concurrently.
    async fn run_one<S>(
        connector: &X,
        shared_sink: &async_lock::Mutex<&mut S>,
        trigger: super::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
        job: ResolveJob<E::Backend>,
        max_page_bytes: usize,
        auth: &X::AuthContext,
        throttle: Option<(Arc<Semaphore>, Arc<AtomicUsize>)>,
    ) -> ReadOutcome<I, E::Checkpoint, E::Backend, X::Error>
    where
        S: FnMut(super::ReadDelivery<I, E::Backend, E::Checkpoint>) + Send,
    {
        let _guard = acquire_permit(throttle.as_ref(), trigger.subscription_id).await;
        let answer = match job {
            ResolveJob::Whole { query, generation } => {
                match Self::stream_whole(
                    connector,
                    shared_sink,
                    &trigger,
                    &query,
                    generation,
                    max_page_bytes,
                    auth,
                )
                .await
                {
                    Ok(()) => Resolved::WholeStreamed,
                    Err(error) => return Err((trigger, error)),
                }
            }
            other => {
                match Self::run_job(
                    connector,
                    other,
                    trigger.subscription_id,
                    max_page_bytes,
                    auth,
                )
                .await
                {
                    Ok(answer) => answer,
                    Err(error) => return Err((trigger, error)),
                }
            }
        };
        Ok((trigger, answer))
    }

    /// Phase three, between awaits so it cannot be interrupted: install and
    /// deliver the successes, keep the retryable failures queued, drop the
    /// non-retryable ones, and report the first failure.
    fn apply_outcomes<S>(
        &mut self,
        resolved: ReadOutcomes<I, E::Checkpoint, E::Backend, X::Error>,
        sink: &mut S,
    ) -> Result<(), ReExecError<X::Error>>
    where
        S: FnMut(super::ReadDelivery<I, E::Backend, E::Checkpoint>) + Send,
    {
        let mut first_error = None;
        for outcome in resolved {
            match outcome {
                Ok((trigger, answer)) => {
                    self.dequeue_read(&trigger);
                    self.apply_answer(&trigger, answer, sink)?;
                    self.stamp_reexec(trigger.subscription_id, &trigger.read);
                }
                Err((trigger, error)) => {
                    // The same read returns the same mismatched answer, so a
                    // non-retryable failure is dropped exactly as the sync
                    // engine drops it.
                    if !error.is_retryable() {
                        self.dequeue_read(&trigger);
                    }
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Drain every queued read, buffering deliveries by channel.
    ///
    /// The convenience shape over [`resolve`](Self::resolve) for callers
    /// that want the whole drain in hand rather than a delivery at a time.
    ///
    /// # Errors
    ///
    /// As [`resolve`](Self::resolve). Deliveries made before the failure
    /// are lost to the caller here, which is the buffering trade: use
    /// [`resolve`](Self::resolve) to keep them.
    pub async fn resolve_collect(
        &mut self,
    ) -> Result<super::ResolvedReads<I, E::Backend, E::Checkpoint>, ReExecError<X::Error>> {
        let mut collected = super::ResolvedReads::default();
        self.resolve(|delivery| collected.push(delivery)).await?;
        Ok(collected)
    }

    /// Install one answer, deliver what it produced, and queue any follow-up
    /// reads a grouped install displaced.
    fn apply_answer<S>(
        &mut self,
        trigger: &super::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
        answer: Resolved<E::Backend>,
        sink: &mut S,
    ) -> Result<(), ReExecError<X::Error>>
    where
        S: FnMut(super::ReadDelivery<I, E::Backend, E::Checkpoint>),
    {
        match answer {
            Resolved::Scalar(value) => {
                let update = crate::Install::install(
                    &mut self.inner,
                    trigger.subscription_id,
                    crate::ScalarInstall {
                        value,
                        checkpoint: trigger.checkpoint.clone(),
                    },
                )?;
                sink(super::ReadDelivery::Scalar(update));
            }
            Resolved::GroupedScalar { group, row } => {
                let installed = crate::Install::install(
                    &mut self.inner,
                    trigger.subscription_id,
                    crate::GroupedScalarInstall {
                        group,
                        row,
                        checkpoint: trigger.checkpoint.clone(),
                    },
                )?;
                self.apply_transitions(&installed.transitions);
                for update in installed.updates {
                    sink(super::ReadDelivery::Aggregate(update));
                }
                for transition in installed.transitions {
                    sink(super::ReadDelivery::Transition(transition));
                }
                for followup in installed.triggers {
                    self.enqueue_read(followup);
                }
            }
            Resolved::Keyed {
                keys,
                columns,
                present,
            } => {
                // The keys were a snapshot: only a delivered read drops them.
                self.inner
                    .remove_pending_keys(trigger.subscription_id, &keys);
                for delta in super::auto::deltas_from(
                    trigger.subscription_id,
                    trigger.consumer_id,
                    trigger.checkpoint.as_ref(),
                    &keys,
                    &present,
                    columns,
                ) {
                    sink(super::ReadDelivery::Delta(delta));
                }
            }
            // Pages already streamed from the concurrent phase.
            Resolved::WholeStreamed => {}
        }
        Ok(())
    }

    /// Decide what a triggered query has to ask the database, taking whatever
    /// engine state the read needs.
    ///
    /// `None` means there is nothing to ask: a keyed query whose changed keys
    /// were already drained, or a plan that cannot be scoped. The caller still
    /// stamps the query, because it was triggered.
    ///
    /// This is the only part of the resolve that needs `&mut self`, which is
    /// what lets the reads themselves run concurrently.
    fn plan_job(
        &mut self,
        trigger: &super::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
    ) -> Option<ResolveJob<E::Backend>> {
        let subscription_id = trigger.subscription_id;
        if let super::ReExecutionRead::GroupedScalar { group, query, .. } = &trigger.read {
            return Some(ResolveJob::GroupedScalar {
                group: group.clone(),
                query: query.clone(),
            });
        }
        let ctx = self.contexts.get(&subscription_id).expect(
            "every captured query stores its resolve context at register time, \
             subscription_id must exist in `contexts`",
        );
        if !ctx.keyed {
            if !ctx.whole_result {
                return Some(ResolveJob::Scalar {
                    query: ctx.query.clone(),
                    column_kind: ctx.column_kind,
                });
            }
            let ctx = self
                .contexts
                .get_mut(&subscription_id)
                .expect("just read above");
            let query = ctx.query.clone();
            // Bump first: a read that fails part way through must not let a
            // later one reuse the generation its partial pages carried.
            ctx.generation = ctx.generation.saturating_add(1);
            return Some(ResolveJob::Whole {
                query,
                generation: ctx.generation,
            });
        }

        let keys = self.inner.clone_pending_keys(subscription_id);
        if keys.is_empty() {
            return None;
        }
        let plan = Arc::clone(self.inner.keyed_plan(subscription_id)?);
        let query = ctx.query.clone();
        Some(ResolveJob::Keyed(alloc::boxed::Box::new(KeyedJob {
            plan,
            keys,
            query,
            max_keys: self.max_keys_per_read,
        })))
    }

    /// Run one planned read. Holds no borrow of the engine, so callers can run
    /// these concurrently.
    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive tier dispatch keeps async read semantics aligned"
    )]
    async fn run_job(
        connector: &X,
        job: ResolveJob<E::Backend>,
        subscription: SubscriptionId,
        max_page_bytes: usize,
        auth: &X::AuthContext,
    ) -> Result<Resolved<E::Backend>, ReExecError<X::Error>> {
        match job {
            ResolveJob::Scalar { query, column_kind } => {
                let (value, _db_checkpoint) = connector
                    .execute_scalar(&query.as_read_query(), column_kind, auth)
                    .await
                    .map_err(|error| ReExecError::Connector {
                        subscription,
                        error,
                    })?;
                Ok(Resolved::Scalar(value))
            }
            ResolveJob::GroupedScalar { group, query } => {
                let page = connector
                    .read_page(&query.as_read_query(), max_page_bytes, auth)
                    .await
                    .map_err(|error| ReExecError::Connector {
                        subscription,
                        error,
                    })?;
                let row = super::auto::one_grouped_row(subscription, page.value)?;
                Ok(Resolved::GroupedScalar { group, row })
            }
            ResolveJob::Keyed(job) => {
                let KeyedJob {
                    plan,
                    keys,
                    query,
                    max_keys,
                } = *job;
                let mut scoped = super::plan::ScopedRead::new(&plan).map_err(|e| {
                    ReExecError::Dispatch(crate::DispatchError::VmError(alloc::format!("{e}")))
                })?;
                let mut columns = Vec::new();
                let mut present: KeyedRows<E::Backend> = Vec::new();
                // Bounded batches, same reason as the sync engine: statement
                // duration tracks how many keys a statement names, and a
                // caller's statement timeout applies per statement, so one
                // unbounded request disables the only read ceiling it has.
                for batch in super::auto::KeyBatches::new(&keys, max_keys) {
                    let Some(sql) = scoped.render::<E::Backend>(batch).map_err(|e| {
                        ReExecError::Dispatch(crate::DispatchError::VmError(alloc::format!("{e}")))
                    })?
                    else {
                        continue;
                    };
                    let mut page_sql = sql;
                    let mut seen_in_batch: super::auto::SeenKeys<E::Backend> =
                        super::auto::SeenKeys::new();
                    loop {
                        let page = connector
                            .read_page(
                                &super::ReadQuery::borrowed(&page_sql, query.binds()),
                                max_page_bytes,
                                auth,
                            )
                            .await
                            .map_err(|error| ReExecError::Connector {
                                subscription,
                                error,
                            })?;
                        // Resuming inside the batch needs no cursor, which is
                        // what keeps this tier cancellation-safe with no
                        // server-side state to strand.
                        let remaining = match super::auto::absorb_keyed_page(
                            subscription,
                            page.value,
                            batch,
                            &plan.key_positions,
                            &mut columns,
                            &mut seen_in_batch,
                            &mut present,
                        )? {
                            super::auto::KeyedPage::Answered => break,
                            super::auto::KeyedPage::Resume(remaining) => remaining,
                        };
                        let Some(next) = scoped.render::<E::Backend>(&remaining).map_err(|e| {
                            ReExecError::Dispatch(crate::DispatchError::VmError(alloc::format!(
                                "{e}"
                            )))
                        })?
                        else {
                            break;
                        };
                        page_sql = next;
                    }
                }
                Ok(Resolved::Keyed {
                    keys,
                    columns,
                    present,
                })
            }
            // Whole reads never reach here: `resolve` streams their pages
            // from the concurrent phase instead.
            ResolveJob::Whole { .. } => {
                unreachable!("whole jobs stream pages in resolve")
            }
        }
    }

    /// Re-read a captured query in full, delivering each page into the
    /// shared sink as it is fetched, so retained memory tracks one page and
    /// never the whole answer.
    ///
    /// The read's own position is discarded on purpose: a re-read is
    /// delivered against the position of the event that triggered it, which
    /// is what a consumer reconciles by. A read that fails or is dropped
    /// part way leaves a generation with no final page. The next re-read
    /// delivers a higher generation, which is the consumer's signal to
    /// discard the partial one.
    async fn stream_whole<S>(
        connector: &X,
        sink: &async_lock::Mutex<&mut S>,
        trigger: &super::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
        query: &super::BoundQuery<E::Backend>,
        generation: u64,
        max_page_bytes: usize,
        auth: &X::AuthContext,
    ) -> Result<(), ReExecError<X::Error>>
    where
        S: FnMut(super::ReadDelivery<I, E::Backend, E::Checkpoint>) + Send,
    {
        let subscription = trigger.subscription_id;
        let cursor = connector
            .open_cursor(&query.as_read_query(), auth)
            .await
            .map_err(|error| ReExecError::Cursor {
                subscription,
                error,
            })?;
        let outcome = async {
            loop {
                let page = connector
                    .fetch_cursor(cursor, max_page_bytes)
                    .await
                    .map_err(|error| ReExecError::Cursor {
                        subscription,
                        error,
                    })?;
                let more = page.value.more;
                let delivery = super::ReadDelivery::Rows(super::engine::RowsUpdate {
                    subscription_id: subscription,
                    consumer_id: trigger.consumer_id,
                    generation,
                    columns: page.value.columns,
                    rows: page.value.rows,
                    more,
                    checkpoint: trigger.checkpoint.clone(),
                });
                (*sink.lock().await)(delivery);
                if !more {
                    return Ok::<(), ReExecError<X::Error>>(());
                }
            }
        }
        .await;
        // Close either way: a read error must not leave the cursor holding a
        // transaction and a connection. A read error outranks a close failure,
        // being the reason the caller asked.
        let closed = connector
            .close_cursor(cursor)
            .await
            .map_err(|error| ReExecError::Cursor {
                subscription,
                error,
            });
        outcome?;
        closed?;
        Ok(())
    }

    /// Page a whole result through a cursor, closing it on every path this
    /// function can take.
    ///
    /// Cancellation is the path it cannot cover: dropping this future runs no
    /// cleanup, because a destructor cannot await. What makes that safe is the
    /// connector opening the cursor's transaction through diesel's own
    /// transaction manager, so the pool sees a released connection is still
    /// inside a transaction and discards it instead of handing it to the next
    /// caller. A raw `BEGIN` leaves diesel's depth counter at zero and the pool
    /// blind, which was measured to hand an unrelated caller a transaction it
    /// never opened and silently lose its write.
    async fn read_whole_with(
        connector: &X,
        subscription: SubscriptionId,
        query: &super::BoundQuery<E::Backend>,
        max_page_bytes: usize,
        auth: &X::AuthContext,
    ) -> Result<(Vec<ReadPage<E::Backend>>, Option<X::Checkpoint>), ReExecError<X::Error>> {
        let cursor = connector
            .open_cursor(&query.as_read_query(), auth)
            .await
            .map_err(|error| ReExecError::Cursor {
                subscription,
                error,
            })?;

        let mut pages = Vec::new();
        let mut checkpoint = None;
        let outcome = async {
            loop {
                let page = connector
                    .fetch_cursor(cursor, max_page_bytes)
                    .await
                    .map_err(|error| ReExecError::Cursor {
                        subscription,
                        error,
                    })?;
                let more = page.value.more;
                checkpoint = page.checkpoint;
                pages.push(ReadPage {
                    columns: page.value.columns,
                    rows: page.value.rows,
                });
                if !more {
                    return Ok::<(), ReExecError<X::Error>>(());
                }
            }
        }
        .await;

        // Close either way: a read error must not leave the cursor holding a
        // transaction and a connection. A read error outranks a close failure,
        // being the reason the caller asked.
        let closed = connector
            .close_cursor(cursor)
            .await
            .map_err(|error| ReExecError::Cursor {
                subscription,
                error,
            });
        outcome?;
        closed?;
        Ok((pages, checkpoint))
    }
}

impl<E, I, DB, X> crate::AsyncSubscriptionDispatch<I, E>
    for AutoResolvingEngine<E, I, DB, AsyncMode<X>>
where
    E: CdcEvent + Send + Sync,
    E::Backend: SqlLiteralParse,
    <E::Backend as Backend>::Dialect: Send + Sync,
    E::Checkpoint: Send + Sync,
    I: IdTypes,
    I::ConsumerId: Send,
    I::SessionId: Send,
    DB: DatabaseLike + Send + Sync + 'static,
    X: AsyncConnector<Backend = E::Backend>,
    X::AuthContext: Send + Sync,
{
    type Notifications = super::Dispatched<I, E::Backend, E::Checkpoint>;
    type Error = crate::DispatchError;

    #[allow(clippy::manual_async_fn)]
    fn consumers(
        &mut self,
        event: &E,
    ) -> impl core::future::Future<Output = Result<Self::Notifications, Self::Error>> + Send {
        core::future::ready(self.apply(event))
    }
}

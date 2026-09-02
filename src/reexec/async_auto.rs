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
use super::engine::ReExecNotifications;
use crate::backend::{Backend, CdcEvent, Value};
use crate::compiler::literals::SqlLiteralParse;
use crate::{IdTypes, SubscriptionId};
use alloc::sync::Arc;
use alloc::vec::Vec;
use async_lock::{Semaphore, SemaphoreGuardArc};
use core::sync::atomic::{AtomicUsize, Ordering};
use sql_traits::prelude::DatabaseLike;

/// Internal state for the persistent re-execution concurrency cap.
///
/// The cap is enforced by an [`async_lock::Semaphore`]. The
/// [`AtomicUsize`] tracks how many permits are held so callers can read
/// [`AutoResolvingEngine::inflight`] without deriving it from the semaphore.
struct ThrottleState {
    sem: Arc<Semaphore>,
    inflight: Arc<AtomicUsize>,
    cap: usize,
}

/// RAII guard returned by the throttle's acquire path. Releases the
/// semaphore permit and decrements the inflight counter on drop, even
/// when dropped because the awaiting future was cancelled.
struct InflightGuard {
    inflight: Arc<AtomicUsize>,
    _permit: SemaphoreGuardArc,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::Release);
    }
}

/// Acquire a throttle permit if a cap is configured. Returns `None`
/// when no cap is set (the unbounded case), avoiding any
/// synchronisation overhead in that path. The returned guard releases
/// the permit on drop, which makes the call cancellation-safe: a
/// future that is dropped while awaiting `acquire_permit` (because the
/// outer `try_collect` short-circuited on a connector error or because
/// the caller cancelled) leaves the semaphore in a clean state.
async fn acquire_permit(
    throttle: Option<&(Arc<Semaphore>, Arc<AtomicUsize>)>,
    subscription_id: SubscriptionId,
) -> Option<InflightGuard> {
    let Some((sem, inflight)) = throttle else {
        let _ = subscription_id;
        return None;
    };
    let permit = Arc::clone(sem).acquire_arc().await;
    let now = inflight.fetch_add(1, Ordering::AcqRel) + 1;
    #[cfg(feature = "observability")]
    tracing::trace!(
        subscription_id,
        inflight = now,
        "subql reexec throttle: permit acquired",
    );
    #[cfg(not(feature = "observability"))]
    let _ = now;
    Some(InflightGuard {
        inflight: Arc::clone(inflight),
        _permit: permit,
    })
}

/// What one triggered query needs from the database, with every borrow of the
/// engine already resolved.
///
/// The async resolve runs in three phases: plan against a snapshot (needs
/// `&mut self`), read concurrently (needs only shared borrows), install and
/// deliver (needs `&mut self` again, between awaits). This type is what
/// crosses the first boundary, so anything the read needs from engine state
/// is owned by the time it is built. Pending keys are copied, never taken,
/// so a dropped or failed read loses nothing.
type KeyedRows<B> = Vec<(Vec<Value<B>>, Vec<Value<B>>)>;

enum ResolveJob<B: Backend> {
    /// A scalar the connector reads in one call.
    Scalar {
        query: super::BoundQuery<B>,
        column_kind: crate::backend::BuiltinKind,
    },
    /// One grouped extreme and its source-row count.
    GroupedScalar {
        group: Vec<u8>,
        query: super::BoundQuery<B>,
    },
    /// Rows for the keys that changed, read scoped to those keys. Boxed: this
    /// variant carries a parsed statement, and the others carry a string.
    Keyed(alloc::boxed::Box<KeyedJob<B>>),
    /// The whole result, paged. The generation is taken when the job is built,
    /// so a read that fails part way cannot let a later one reuse it.
    Whole {
        query: super::BoundQuery<B>,
        generation: u64,
    },
}

/// The keyed tier's read, as planned.
struct KeyedJob<B: Backend> {
    plan: super::plan::KeyedPlan,
    key_positions: Vec<usize>,
    keys: Vec<Vec<Value<B>>>,
    query: super::BoundQuery<B>,
    /// Keys one statement may name, carried so the read needs nothing from the
    /// engine once it is planned.
    max_keys: usize,
}

/// What the database answered, still owned, ready to install.
enum Resolved<B: Backend> {
    Scalar(Value<B>),
    GroupedScalar {
        group: Vec<u8>,
        row: Vec<Value<B>>,
    },
    Keyed {
        keys: Vec<Vec<Value<B>>>,
        columns: Vec<alloc::string::String>,
        present: KeyedRows<B>,
    },

    /// A whole re-read whose pages already streamed to the sink from the
    /// concurrent phase. Nothing is installed for it.
    WholeStreamed,
}
/// One page of a whole re-read buffered for a snapshot answer, which returns
/// the whole result by contract.
struct ReadPage<B: Backend> {
    columns: Vec<alloc::string::String>,
    rows: Vec<Vec<Value<B>>>,
}

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
                        if page.value.more || page.value.rows.len() != 1 {
                            return Err(crate::AggregateInstallError::RowCount {
                                subscription: subscription_id,
                                rows: page.value.rows.len(),
                            }
                            .into());
                        }
                        let resolved = crate::Install::install(
                            &mut self.inner,
                            subscription_id,
                            crate::GroupedScalarInstall {
                                group: group.clone(),
                                row: page
                                    .value
                                    .rows
                                    .into_iter()
                                    .next()
                                    .expect("the row count was checked"),
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
        if context.aggregate {
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
    /// in flight (unbounded when not configured). A read that fails stays
    /// queued and the next `resolve` retries it. Reads that succeeded in the
    /// same iteration are still installed and delivered before the failure
    /// is reported.
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
    /// answer does not match the subscription and are not retryable.
    #[allow(
        clippy::too_many_lines,
        reason = "the three phases share borrows that splitting would hand around"
    )]
    pub async fn resolve<S>(&mut self, mut sink: S) -> Result<(), ReExecError<X::Error>>
    where
        S: FnMut(super::ReadDelivery<I, E::Backend, E::Checkpoint>) + Send,
    {
        use futures_util::stream::StreamExt;

        loop {
            if self.pending_reads.is_empty() {
                return Ok(());
            }
            // Phase one, under `&mut self`: decide each read's job against a
            // snapshot. Keys are copied, never taken, so a dropped future
            // loses nothing.
            let snapshot = self.pending_reads.clone();
            let mut jobs = Vec::with_capacity(snapshot.len());
            for trigger in snapshot {
                if let Some(job) = self.plan_job(&trigger) {
                    jobs.push((trigger, job));
                } else {
                    // Nothing to ask, but the query was still triggered, so
                    // its debounce stamp moves as if it had been read.
                    self.dequeue_read(&trigger);
                    self.stamp_reexec(trigger.subscription_id, &trigger.read);
                }
            }
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
            #[allow(clippy::type_complexity)]
            let resolved: Vec<
                Result<
                    (
                        super::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
                        Resolved<E::Backend>,
                    ),
                    ReExecError<X::Error>,
                >,
            > = {
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
                    let throttle = throttle.clone();
                    let shared_sink = &shared_sink;
                    async move {
                        let _guard =
                            acquire_permit(throttle.as_ref(), trigger.subscription_id).await;
                        let answer = match job {
                            ResolveJob::Whole { query, generation } => {
                                Self::stream_whole(
                                    connector,
                                    shared_sink,
                                    &trigger,
                                    &query,
                                    generation,
                                    max_page_bytes,
                                    auth,
                                )
                                .await?;
                                Resolved::WholeStreamed
                            }
                            other => {
                                Self::run_job(
                                    connector,
                                    other,
                                    trigger.subscription_id,
                                    max_page_bytes,
                                    auth,
                                )
                                .await?
                            }
                        };
                        Ok::<_, ReExecError<X::Error>>((trigger, answer))
                    }
                }))
                .buffer_unordered(jobs_len)
                .collect::<Vec<_>>()
                .await
            };

            // Phase three, between awaits so it cannot be interrupted:
            // install and deliver the successes, keep the failures queued,
            // and report the first failure.
            let mut first_error = None;
            for outcome in resolved {
                match outcome {
                    Ok((trigger, answer)) => {
                        self.dequeue_read(&trigger);
                        self.apply_answer(&trigger, answer, &mut sink)?;
                        self.stamp_reexec(trigger.subscription_id, &trigger.read);
                    }
                    Err(error) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }
            if let Some(error) = first_error {
                return Err(error);
            }
            // Grouped installs may have queued follow-up reads: loop drains
            // them with the same concurrency.
        }
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
        let plan = self.inner.keyed_plan(subscription_id)?;
        let plan = plan.clone();
        let key_positions = plan.key_positions.clone();
        let query = ctx.query.clone();
        Some(ResolveJob::Keyed(alloc::boxed::Box::new(KeyedJob {
            plan,
            key_positions,
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
                if page.value.more || page.value.rows.len() != 1 {
                    return Err(crate::AggregateInstallError::RowCount {
                        subscription,
                        rows: page.value.rows.len(),
                    }
                    .into());
                }
                Ok(Resolved::GroupedScalar {
                    group,
                    row: page
                        .value
                        .rows
                        .into_iter()
                        .next()
                        .expect("the row count was checked"),
                })
            }
            ResolveJob::Keyed(job) => {
                let KeyedJob {
                    plan,
                    key_positions,
                    keys,
                    query,
                    max_keys,
                } = *job;
                let mut columns = Vec::new();
                let mut present: KeyedRows<E::Backend> = Vec::new();
                // Bounded batches, same reason as the sync engine: statement
                // duration tracks how many keys a statement names, and a
                // caller's statement timeout applies per statement, so one
                // unbounded request disables the only read ceiling it has.
                for batch in super::auto::KeyBatches::new(&keys, max_keys) {
                    let Some(sql) = super::plan::render_scoped_read::<E::Backend>(&plan, batch)
                        .map_err(|e| {
                            ReExecError::Dispatch(crate::DispatchError::VmError(alloc::format!(
                                "{e}"
                            )))
                        })?
                    else {
                        continue;
                    };
                    let mut page_sql = sql;
                    // Accumulated across pages, not per page. Resetting it
                    // would let a key answered on an earlier page back into the
                    // next statement, which delivers it twice and, with a
                    // stable row order, never terminates.
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
                        if columns.is_empty() {
                            columns.clone_from(&page.value.columns);
                        }
                        let before = seen_in_batch.recorded();
                        for row in page.value.rows {
                            let key: Vec<Value<E::Backend>> = key_positions
                                .iter()
                                .filter_map(|i| row.get(*i).cloned())
                                .collect();
                            seen_in_batch.record(&key);
                            present.push((key, row));
                        }
                        // A page with no rows ends the read whatever it claims
                        // about there being more. Our own reader cannot report
                        // that combination, but this trait has outside
                        // implementors, and without this a connector that did
                        // would loop here forever.
                        if !page.value.more || seen_in_batch.recorded() == before {
                            break;
                        }
                        // A batch whose rows do not fit one page resumes inside
                        // the batch, so the statement stays bounded. No cursor
                        // is needed, which is what keeps this tier
                        // cancellation-safe with no server-side state to strand.
                        let remaining: Vec<Vec<Value<E::Backend>>> = batch
                            .iter()
                            .filter(|k| !seen_in_batch.contains(k))
                            .cloned()
                            .collect();
                        if remaining.is_empty() {
                            break;
                        }
                        let Some(next) =
                            super::plan::render_scoped_read::<E::Backend>(&plan, &remaining)
                                .map_err(|e| {
                                    ReExecError::Dispatch(crate::DispatchError::VmError(
                                        alloc::format!("{e}"),
                                    ))
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
    type Notifications = ReExecNotifications<I, E::Backend, E::Checkpoint>;
    type Error = crate::DispatchError;

    #[allow(clippy::manual_async_fn)]
    fn consumers(
        &mut self,
        event: &E,
    ) -> impl core::future::Future<Output = Result<Self::Notifications, Self::Error>> + Send {
        core::future::ready(self.apply(event))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::connector::Snapshot;
    use super::*;
    use crate::backend::{BuiltinKind, Postgres};
    use crate::testing::TestEvent;
    use crate::{
        DefaultIds, NoCheckpoint, Registered, SubscriptionEngine, SubscriptionRequest, TableId,
        Tier,
    };
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll};
    use parking_lot::Mutex;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;
    use std::task::Wake;

    /// No-op `Wake` implementation: the `MockAsyncConnector` futures
    /// never park, so `wake` is never invoked. Built on the safe `Wake`
    /// trait so the workspace's `forbid(unsafe_code)` lint passes. The
    /// nightly-only `Waker::noop` would be the alternative, hence the
    /// `allow` below.
    struct NoopWake;
    #[allow(unknown_lints, clippy::manual_noop_waker)]
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    /// Tiny block-on for the test futures (which never park). Drives the
    /// future to completion by polling. If a real-world future returned
    /// Pending it would loop forever, but the `MockAsyncConnector`
    /// futures complete in one poll.
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

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
        )
        .unwrap()
    }

    /// `parking_lot::Mutex`-backed mock so the futures are `Send`.
    struct MockAsyncConnector {
        values: Mutex<Vec<Value<Postgres>>>,
        call_count: Mutex<usize>,
        scalar_queries: Mutex<Vec<super::super::ReadQuery<'static, Postgres>>>,
        page_queries: Mutex<Vec<super::super::ReadQuery<'static, Postgres>>>,
        cursor_queries: Mutex<Vec<super::super::ReadQuery<'static, Postgres>>>,
        /// When set, the next scalar read suspends once before answering, so
        /// a test can drop a resolve future mid-read.
        pend_next_read: Mutex<bool>,
        /// Pages a whole re-read serves, front first. Empty means the mock
        /// holds no cursors and `open_cursor` refuses.
        cursor_pages: Mutex<Vec<crate::reexec::RowPage<Postgres>>>,
        /// Fetch index that suspends once before serving, so a test can drop
        /// a resolve future between pages.
        pend_fetch_at: Mutex<Option<usize>>,
        /// Fetches served so far.
        fetch_count: Mutex<usize>,
        /// Interleaving log shared with the test's sink.
        log: Arc<Mutex<Vec<&'static str>>>,
    }

    impl MockAsyncConnector {
        fn new(values: Vec<Value<Postgres>>) -> Self {
            Self {
                values: Mutex::new(values),
                call_count: Mutex::new(0),
                scalar_queries: Mutex::new(Vec::new()),
                page_queries: Mutex::new(Vec::new()),
                cursor_queries: Mutex::new(Vec::new()),
                pend_next_read: Mutex::new(false),
                cursor_pages: Mutex::new(Vec::new()),
                pend_fetch_at: Mutex::new(None),
                fetch_count: Mutex::new(0),
                log: Arc::new(Mutex::new(Vec::new())),
            }
        }
        fn call_count(&self) -> usize {
            *self.call_count.lock()
        }
    }

    #[derive(Debug)]
    struct MockError(&'static str);

    impl core::fmt::Display for MockError {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    /// Pending on its first poll, ready on the second: the seam that lets a
    /// test observe a resolve suspended inside a connector read.
    struct YieldOnce(bool);

    impl Future for YieldOnce {
        type Output = ();
        fn poll(mut self: core::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    // The `+ Send` bound on the returned futures is the whole point of
    // the trait shape. `async fn in trait` cannot express it directly.
    #[allow(clippy::manual_async_fn)]
    impl AsyncConnector for MockAsyncConnector {
        type AuthContext = ();
        type Error = MockError;
        type Checkpoint = NoCheckpoint;
        type Backend = Postgres;

        fn execute_scalar(
            &self,
            query: &super::super::ReadQuery<'_, Postgres>,
            _kind: BuiltinKind,
            _auth: &(),
        ) -> impl Future<Output = Result<(Value<Postgres>, Option<Self::Checkpoint>), Self::Error>> + Send
        {
            async move {
                if core::mem::take(&mut *self.pend_next_read.lock()) {
                    YieldOnce(false).await;
                }
                *self.call_count.lock() += 1;
                self.scalar_queries.lock().push(query.clone().into_owned());
                let value = self.values.lock().pop().ok_or(MockError("queue empty"))?;
                Ok((value, None))
            }
        }

        fn read_page(
            &self,
            query: &super::super::ReadQuery<'_, Postgres>,
            _max_bytes: usize,
            _auth: &(),
        ) -> impl Future<
            Output = Result<
                Snapshot<crate::reexec::RowPage<Postgres>, Self::Checkpoint>,
                Self::Error,
            >,
        > + Send {
            async move {
                self.page_queries.lock().push(query.clone().into_owned());
                Err(MockError("read_page is not exercised by the scalar tests"))
            }
        }

        fn open_cursor(
            &self,
            query: &super::super::ReadQuery<'_, Postgres>,
            _auth: &(),
        ) -> impl Future<
            Output = Result<super::super::CursorId, super::super::CursorError<Self::Error>>,
        > + Send {
            self.cursor_queries.lock().push(query.clone().into_owned());
            if self.cursor_pages.lock().is_empty() {
                return core::future::ready(Err(super::super::CursorError::Unsupported));
            }
            self.log.lock().push("open");
            core::future::ready(Ok(super::super::CursorId(1)))
        }

        fn fetch_cursor(
            &self,
            _cursor: super::super::CursorId,
            _max_bytes: usize,
        ) -> impl Future<
            Output = Result<
                Snapshot<crate::reexec::RowPage<Postgres>, Self::Checkpoint>,
                super::super::CursorError<Self::Error>,
            >,
        > + Send {
            async move {
                let index = {
                    let mut count = self.fetch_count.lock();
                    let index = *count;
                    *count += 1;
                    index
                };
                if *self.pend_fetch_at.lock() == Some(index) {
                    YieldOnce(false).await;
                }
                self.log.lock().push("fetch");
                let page = self.cursor_pages.lock().remove(0);
                Ok(Snapshot {
                    value: page,
                    checkpoint: None,
                })
            }
        }

        fn close_cursor(
            &self,
            _cursor: super::super::CursorId,
        ) -> impl Future<Output = Result<(), super::super::CursorError<Self::Error>>> + Send
        {
            self.log.lock().push("close");
            core::future::ready(Ok(()))
        }
    }

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
        values: Vec<Value<Postgres>>,
    ) -> (
        AutoResolvingEngine<
            TestEvent<Postgres>,
            DefaultIds,
            ParserDB,
            AsyncMode<MockAsyncConnector>,
        >,
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
            AutoResolvingEngine::new(inner, AsyncMode::new(MockAsyncConnector::new(values))),
            orders_id,
        )
    }

    /// Full path through the async engine: register, snapshot (which
    /// installs), unrelated insert (no connector call), delete of the
    /// extreme (one connector call, ScalarUpdate emitted).
    #[test]
    fn async_engine_dispatch_round_trip() {
        // Two values for: snapshot bootstrap (5.0), delete re-execution (9.0).
        // Mock pops from the back so push in reverse order.
        let (mut e, tid) = engine_with_values(vec![Value::Float(9.0), Value::Float(5.0)]);

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

        // Snapshot bootstraps. Future is Send-bound and ready immediately.
        let snap = block_on(e.snapshot(qid))
            .unwrap()
            .expect("subscription_id exists");
        match snap {
            SnapshotResult::Scalar(Value::Float(v), None) => {
                assert!((v - 5.0).abs() < f64::EPSILON);
            }
            other => panic!("expected Scalar(5.0, None), got {other:?}"),
        }
        assert_eq!(e.connector().call_count(), 1);

        // Insert above the extreme: in-process Unchanged, no connector call.
        let n = e.apply(&insert_event(tid, 2, 9.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert_eq!(e.connector().call_count(), 1);

        // Delete the extreme: trigger -> connector -> ScalarUpdate.
        e.apply(&delete_event(tid, 1, 5.0)).unwrap();
        let n = block_on(e.resolve_collect()).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].subscription_id, qid);
        assert_eq!(n.scalar_updates[0].value, Value::Float(9.0));
        assert_eq!(e.connector().call_count(), 2);
    }

    #[test]
    fn async_scalar_initial_snapshot_forwards_registration_binds() {
        let (mut engine, _) = engine_with_values(vec![Value::Float(5.0)]);
        let subscription = engine
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders WHERE quantity > $1")
                    .binds(vec![Value::Int(2)]),
                (),
            )
            .expect("scalar registers")
            .subscription_id;

        block_on(engine.snapshot(subscription))
            .expect("snapshot succeeds")
            .expect("snapshot exists");
        let queries = engine.connector().scalar_queries.lock();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].binds(), &[Value::Int(2)]);
        drop(queries);
    }

    #[test]
    fn async_scalar_event_forwards_registration_binds() {
        let (mut engine, table) = engine_with_values(vec![Value::Float(9.0)]);
        let subscription = engine
            .register(
                SubscriptionRequest::new(1u64, "SELECT MIN(price) FROM orders WHERE quantity > $1")
                    .binds(vec![Value::Int(0)]),
                (),
            )
            .expect("scalar registers")
            .subscription_id;
        crate::Install::install(
            &mut engine,
            subscription,
            crate::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<crate::NoCheckpoint>,
            },
        )
        .expect("scalar installs");

        engine
            .apply(&delete_event(table, 1, 5.0))
            .expect("apply succeeds");
        let _ = block_on(engine.resolve_collect()).expect("resolve succeeds");
        let queries = engine.connector().scalar_queries.lock();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].binds(), &[Value::Int(0)]);
        drop(queries);
    }

    #[test]
    fn async_keyed_initial_snapshot_forwards_registration_binds() {
        let (mut engine, _) = engine_with_values(vec![]);
        let registered = engine
            .register(
                SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = $1")
                    .binds(vec![Value::String("paid".into())]),
                (),
            )
            .expect("keyed read registers");
        let Tier::KeyedRows { ref query, .. } = registered.tier else {
            panic!("expected keyed rows")
        };

        let _ = block_on(engine.snapshot(registered.subscription_id));
        let queries = engine.connector().cursor_queries.lock();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].sql(), query.sql());
        assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
        drop(queries);
    }

    #[test]
    fn async_keyed_event_scopes_registration_binds() {
        let (mut engine, table) = engine_with_values(vec![]);
        engine
            .register(
                SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = $1")
                    .binds(vec![Value::String("paid".into())]),
                (),
            )
            .expect("keyed read registers");

        engine.apply(&delete_event(table, 1, 5.0)).unwrap();
        let _ = block_on(engine.resolve_collect());
        let queries = engine.connector().page_queries.lock();
        assert_eq!(queries.len(), 1);
        assert_eq!(
            queries[0].sql(),
            "SELECT * FROM orders WHERE (lower(status) = $1) AND \"id\" IN (1)"
        );
        assert_eq!(queries[0].binds(), &[Value::String("paid".into())]);
        drop(queries);
    }

    #[test]
    fn async_grouped_bootstrap_forwards_registration_binds() {
        let (mut engine, _) = engine_with_values(vec![]);
        let registered = engine
            .register(
                SubscriptionRequest::new(
                    1u64,
                    "SELECT status, MIN(price) FROM orders WHERE quantity > $1 GROUP BY status",
                )
                .binds(vec![Value::Int(0)]),
                (),
            )
            .expect("grouped read registers");
        let Tier::GroupedScalar { ref bootstrap } = registered.tier else {
            panic!("expected grouped scalar")
        };

        let _ = block_on(engine.snapshot(registered.subscription_id));
        let queries = engine.connector().cursor_queries.lock();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].sql(), bootstrap.query.sql());
        assert_eq!(queries[0].binds(), &[Value::Int(0)]);
        drop(queries);
    }

    #[test]
    fn async_grouped_scoped_read_orders_registration_binds() {
        let (mut engine, table) = engine_with_values(vec![]);
        let subscription = engine
            .register(
                SubscriptionRequest::new(
                    1u64,
                    "SELECT status, MIN(price) FROM orders WHERE quantity > $1 GROUP BY status",
                )
                .binds(vec![Value::Int(0)]),
                (),
            )
            .expect("grouped read registers")
            .subscription_id;
        crate::Install::install(
            &mut engine.inner,
            subscription,
            crate::GroupedScalarSeedInstall {
                rows: vec![vec![
                    Value::String("paid".into()),
                    Value::Float(5.0),
                    Value::Int(2),
                ]],
                read_at: None::<crate::NoCheckpoint>,
            },
        )
        .expect("grouped seed installs");

        engine.apply(&delete_event(table, 1, 5.0)).unwrap();
        let _ = block_on(engine.resolve_collect());
        let queries = engine.connector().page_queries.lock();
        assert_eq!(queries.len(), 1);
        assert_eq!(
            queries[0].sql(),
            "SELECT MIN(\"price\") AS v, COUNT(*) AS c1 FROM orders WHERE (quantity > $1) AND \"status\" = $2"
        );
        assert_eq!(
            queries[0].binds(),
            &[Value::Int(0), Value::String("paid".into())]
        );
        drop(queries);
    }

    #[test]
    fn async_whole_snapshot_forwards_registration_binds() {
        let (mut e, _tid) = engine_with_values(vec![]);
        let qid = e
            .register(
                SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY id DESC LIMIT $1")
                    .binds(vec![Value::Int(3)]),
                (),
            )
            .unwrap()
            .subscription_id;

        let _ = block_on(e.snapshot(qid));

        let queries = e.connector().cursor_queries.lock();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].binds(), &[Value::Int(3)]);
        drop(queries);
    }

    #[test]
    fn async_whole_event_forwards_registration_binds() {
        let (mut engine, table) = engine_with_values(vec![]);
        engine
            .register(
                SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY id DESC LIMIT $1")
                    .binds(vec![Value::Int(3)]),
                (),
            )
            .expect("whole read registers");

        engine.apply(&insert_event(table, 2, 9.0)).unwrap();
        let _ = block_on(engine.resolve_collect());
        let queries = engine.connector().cursor_queries.lock();
        assert_eq!(queries.len(), 1);
        assert_eq!(
            queries[0].sql(),
            "SELECT * FROM orders ORDER BY id DESC LIMIT $1"
        );
        assert_eq!(queries[0].binds(), &[Value::Int(3)]);
        drop(queries);
    }

    /// `unrelated-column` UPDATE skip optimization still works under
    /// the async engine.
    #[test]
    fn async_engine_unrelated_column_update_skips_connector() {
        let (mut e, tid) = engine_with_values(vec![]);
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

        let event = update_status_only(tid, 1, 10.0);

        let n = e.apply(&event).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert!(n.triggers.is_empty());
        assert_eq!(e.connector().call_count(), 0);
    }

    /// `snapshot` on an unknown id returns `Ok(None)`.
    #[test]
    fn async_engine_snapshot_unknown_query_returns_none() {
        let (mut e, _tid) = engine_with_values(vec![]);
        assert!(block_on(e.snapshot(99999)).unwrap().is_none());
        assert_eq!(e.connector().call_count(), 0);
    }

    /// Connector failure aborts the batch with `ReExecError::Connector`.
    #[test]
    fn async_engine_connector_error_aborts_batch() {
        let (mut e, tid) = engine_with_values(vec![]);
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

        e.apply(&delete_event(tid, 1, 5.0)).unwrap();
        match block_on(e.resolve_collect()) {
            Ok(_) => panic!("expected Connector error, got Ok"),
            Err(ReExecError::Connector {
                error: MockError(msg),
                ..
            }) => assert_eq!(msg, "queue empty"),
            Err(other) => panic!("expected Connector error, got {other:?}"),
        }
    }

    /// Async batch coalesces repeated triggers for the same query into a
    /// single connector call. Mirrors the sync engine's T4.1 assertion.
    #[test]
    fn async_applied_burst_coalesces_repeated_triggers() {
        let (mut e, tid) = engine_with_values(vec![Value::Float(99.0)]);
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

        let events = vec![
            delete_event(tid, 1, 5.0),
            delete_event(tid, 2, 5.0),
            delete_event(tid, 3, 5.0),
        ];

        let per_event: Vec<_> = events.iter().map(|ev| e.apply(ev).unwrap()).collect();
        assert_eq!(per_event.len(), 3, "per_event positional alignment");
        let outcome = block_on(e.resolve_collect()).unwrap();
        assert_eq!(
            e.connector().call_count(),
            1,
            "three displacing events collapse to one connector call"
        );
        assert_eq!(outcome.scalar_updates.len(), 1);
        assert_eq!(outcome.scalar_updates[0].value, Value::Float(99.0));
    }

    /// `with_max_concurrent_reexecutions` does not change the result of
    /// one resolve of a burst. Correctness is preserved. The cap is a
    /// throughput / fairness knob, not a semantic one.
    #[test]
    #[allow(clippy::similar_names)]
    fn async_applied_burst_respects_max_concurrent_cap() {
        // Two distinct captured queries, each displaced once in the
        // batch. Both must resolve regardless of the cap.
        let (e0, tid) = engine_with_values(vec![Value::Float(22.0), Value::Float(11.0)]);
        let mut e = e0.with_max_concurrent_reexecutions(1);
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

        let events = vec![delete_event(tid, 1, 7.0)];
        for event in &events {
            e.apply(event).unwrap();
        }
        let outcome = block_on(e.resolve_collect()).unwrap();
        assert_eq!(e.connector().call_count(), 2);
        assert_eq!(outcome.scalar_updates.len(), 2);
        let qids: std::collections::BTreeSet<_> = outcome
            .scalar_updates
            .iter()
            .map(|u| u.subscription_id)
            .collect();
        assert!(qids.contains(&qid1));
        assert!(qids.contains(&qid2));
    }

    /// `unregister_subscription` drops the stored auth context.
    #[test]
    fn async_engine_unregister_drops_context() {
        let (mut e, _tid) = engine_with_values(vec![]);
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
    }

    // Re-execution concurrency throttle
    //
    // The `MockAsyncConnector` futures complete in one poll, so the
    // tests here cannot observe the *peak* inflight count during a
    // batch (that needs a real multi-tasking runtime, integration
    // tests exercise it). Unit tests validate the invariants that DO
    // hold under `block_on`: the accessors, the post-batch invariant
    // (`inflight == 0`), the zero-cap normalisation, and the
    // result-preservation contract.

    /// No cap by default: `inflight()` is 0, `concurrency_cap()` is `None`.
    #[test]
    fn throttle_disabled_by_default() {
        let (e, _tid) = engine_with_values(vec![]);
        assert_eq!(e.inflight(), 0);
        assert_eq!(e.concurrency_cap(), None);
    }

    /// `with_max_concurrent_reexecutions(n)` records `n` as the cap and
    /// starts with `inflight() == 0`.
    #[test]
    fn throttle_set_cap_observable_via_accessors() {
        let (e0, _tid) = engine_with_values(vec![]);
        let e = e0.with_max_concurrent_reexecutions(4);
        assert_eq!(e.concurrency_cap(), Some(4));
        assert_eq!(e.inflight(), 0);
    }

    /// `cap = 0` is normalised to 1 to prevent a deadlock on first
    /// `acquire`.
    #[test]
    fn throttle_zero_cap_normalised_to_one() {
        let (e0, _tid) = engine_with_values(vec![]);
        let e = e0.with_max_concurrent_reexecutions(0);
        assert_eq!(e.concurrency_cap(), Some(1));
    }

    /// Cleanup invariant: after a successful resolve of a burst the
    /// inflight counter is back to 0. Tests that the `InflightGuard`
    /// drop path actually fires when futures complete.
    #[test]
    fn throttle_inflight_returns_to_zero_after_batch() {
        let (e0, tid) = engine_with_values(vec![Value::Float(22.0), Value::Float(11.0)]);
        let mut e = e0.with_max_concurrent_reexecutions(1);
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
            other => panic!("expected ReExec, got {other:?}"),
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
            other => panic!("expected ReExec, got {other:?}"),
        };
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

        e.apply(&delete_event(tid, 1, 7.0)).unwrap();
        let _ = block_on(e.resolve_collect()).unwrap();
        assert_eq!(
            e.inflight(),
            0,
            "every InflightGuard must drop after batch completes"
        );
    }

    /// Cleanup invariant on the error path: when the connector fails
    /// mid-batch, every permit must still be released.
    #[test]
    fn throttle_inflight_returns_to_zero_after_connector_error() {
        // Two captured queries, only one value in the queue: the second
        // connector call hits "queue empty" and the batch aborts.
        let (e0, tid) = engine_with_values(vec![Value::Float(22.0)]);
        let mut e = e0.with_max_concurrent_reexecutions(2);
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
            other => panic!("expected ReExec, got {other:?}"),
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
            other => panic!("expected ReExec, got {other:?}"),
        };
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

        e.apply(&delete_event(tid, 1, 7.0)).unwrap();
        assert!(block_on(e.resolve_collect()).is_err());
        assert_eq!(
            e.inflight(),
            0,
            "InflightGuards must drop even when the batch aborts on connector error"
        );
    }

    /// The throttle preserves correctness: total connector call count
    /// equals the number of deduplicated triggers regardless of cap.
    #[test]
    fn throttle_total_call_count_unchanged_with_cap() {
        // Three queries, one trigger each, cap = 1.
        let values = vec![Value::Float(30.0), Value::Float(20.0), Value::Float(10.0)];
        let (e0, tid) = engine_with_values(values);
        let mut e = e0.with_max_concurrent_reexecutions(1);
        let qids: Vec<_> = (1u64..=3)
            .map(|c| {
                match e
                    .register(
                        SubscriptionRequest::new(
                            c,
                            "SELECT MIN(price) FROM orders WHERE quantity = 1",
                        ),
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
                }
            })
            .collect();
        for q in &qids {
            assert!(crate::Install::install(
                &mut e,
                *q,
                crate::ScalarInstall {
                    value: Value::Float(7.0),
                    checkpoint: None::<crate::NoCheckpoint>
                }
            )
            .is_ok());
        }
        e.apply(&delete_event(tid, 1, 7.0)).unwrap();
        let outcome = block_on(e.resolve_collect()).unwrap();
        assert_eq!(
            e.connector().call_count(),
            3,
            "three distinct queries each get one connector call regardless of cap"
        );
        assert_eq!(outcome.scalar_updates.len(), 3);
    }

    #[test]
    fn grouped_debounce_is_scoped_by_group_key_async() {
        let clock = alloc::sync::Arc::new(crate::ManualClock::new(0));
        let engine_clock: crate::ClockHandle = clock;
        let (engine, _) = engine_with_values(Vec::new());
        let mut engine = engine
            .with_clock(engine_clock)
            .with_debounce_per_query(core::time::Duration::from_secs(1));
        let first = super::super::ReExecutionRead::GroupedScalar {
            group: vec![1],
            query: super::super::BoundQuery::new(String::new(), Vec::new()),
            column_kinds: [BuiltinKind::Int, BuiltinKind::Int],
        };
        let second = super::super::ReExecutionRead::GroupedScalar {
            group: vec![2],
            query: super::super::BoundQuery::new(String::new(), Vec::new()),
            column_kinds: [BuiltinKind::Int, BuiltinKind::Int],
        };
        engine.stamp_reexec(7, &first);
        assert!(engine.debounce_skip(7, &first));
        assert!(!engine.debounce_skip(7, &second));
    }

    #[test]
    fn async_unregister_subscription_resolves_either_registry() {
        let (mut e, _tid) = engine_with_values(vec![]);
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
    fn async_unregister_subscription_drops_the_resolve_context() {
        let (mut e, tid) = engine_with_values(vec![Value::Float(7.0)]);
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
                checkpoint: None::<NoCheckpoint>,
            },
        )
        .unwrap();
        assert_eq!(e.contexts.len(), 1);
        assert!(e.unregister_subscription(captured));
        assert_eq!(e.contexts.len(), 0, "the resolve context is dropped");
        let n = e.apply(&delete_event(tid, 1, 5.0)).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert_eq!(
            e.connector().call_count(),
            0,
            "no connector call after unregister"
        );
    }

    #[test]
    fn async_cursor_state_is_reachable_through_the_wrapper() {
        let (mut e, _tid) = engine_with_values(vec![]);
        let session = 1u64;
        let sub = 7u64;
        let cp = crate::OpaqueCheckpoint(vec![1, 2, 3]);
        assert_eq!(e.advance_cursor(session, sub, cp.clone()), Ok(None));
        assert_eq!(e.cursor_for(session, sub), Some(&cp));
        let older = crate::OpaqueCheckpoint(vec![0]);
        assert_eq!(e.force_set_cursor(session, sub, older.clone()), Some(cp));
        assert_eq!(e.cursor_for(session, sub), Some(&older));
        let listed: Vec<_> = e
            .cursors_for_session(session)
            .map(|(s, c)| (s, c.clone()))
            .collect();
        assert_eq!(listed, vec![(sub, older.clone())]);
        assert_eq!(e.drop_cursor(session, sub), Some(older));
        assert_eq!(e.cursor_for(session, sub), None);
    }

    #[test]
    fn async_match_rows_replays_without_reading_or_folding() {
        // match_rows must be a plain sync call here: the inner match does no
        // I/O, so no block_on wraps it. One value, for the single live read.
        let (mut e, tid) = engine_with_values(vec![Value::Float(7.0)]);
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
                checkpoint: None::<NoCheckpoint>,
            },
        )
        .unwrap();
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

        // The live dispatch of the same delete is still the first read: proof
        // match_rows left the re-execution model untouched.
        e.apply(&ev).unwrap();
        let live = block_on(e.resolve_collect()).unwrap();
        assert_eq!(
            e.connector().call_count(),
            1,
            "the re-execution model was untouched, so the live read is the first"
        );
        assert_eq!(live.scalar_updates.len(), 1);
        assert_eq!(live.scalar_updates[0].value, Value::Float(7.0));
    }

    #[test]
    fn async_describe_terms_is_reachable_through_the_wrapper() {
        // Sync method even on the async wrapper: it only reads the engine's
        // compiler, no I/O, so no block_on.
        let (e, _tid) = engine_with_values(vec![]);
        let plain = e
            .describe_terms(&SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE price > 100",
            ))
            .expect("a plain filter is describable");
        assert!(plain.is_empty(), "a plain filter has no membership terms");
        let refused = e.describe_terms(&SubscriptionRequest::new(
            2u64,
            "SELECT * FROM orders WHERE nonexistent_column > 5",
        ));
        assert!(
            refused.is_err(),
            "an unknown-column filter is refused, got {refused:?}"
        );
    }

    #[test]
    fn async_connector_error_names_its_subscription() {
        let (mut e, tid) = engine_with_values(vec![]);
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
            other => panic!("expected Scalar, got {other:?}"),
        };
        crate::Install::install(
            &mut e,
            qid,
            crate::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<NoCheckpoint>,
            },
        )
        .unwrap();
        e.apply(&delete_event(tid, 1, 5.0)).unwrap();
        match block_on(e.resolve_collect()) {
            Ok(_) => panic!("expected the triggered read to fail"),
            Err(ReExecError::Connector {
                subscription,
                error: MockError(msg),
            }) => {
                assert_eq!(subscription, qid, "the failing subscription is named");
                assert_eq!(msg, "queue empty");
            }
            Err(other) => panic!("expected Connector naming its subscription, got {other:?}"),
        }
    }

    #[test]
    fn async_cursor_error_names_its_subscription() {
        let (mut e, _tid) = engine_with_values(vec![]);
        let qid = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
                (),
            )
            .unwrap()
        {
            Registered {
                subscription_id,
                tier: Tier::WholeRows { .. },
                ..
            } => subscription_id,
            other => panic!("expected WholeRows, got {other:?}"),
        };
        match block_on(e.snapshot(qid)) {
            Ok(_) => panic!("expected the cursorless read to fail"),
            Err(ReExecError::Cursor {
                subscription,
                error,
            }) => {
                assert_eq!(subscription, qid, "the failing subscription is named");
                assert!(matches!(error, super::super::CursorError::Unsupported));
            }
            Err(other) => panic!("expected Cursor naming its subscription, got {other:?}"),
        }
    }

    #[test]
    fn async_ungrouped_aggregate_folds_through_the_wrapper() {
        let (mut e, tid) = engine_with_values(vec![]);
        let count_id = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders"),
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
        crate::Install::install(
            &mut e,
            count_id,
            crate::AggregateSeedInstall {
                rows: vec![vec![Value::Int(5)]],
                read_at: None::<NoCheckpoint>,
            },
        )
        .unwrap();
        let n = e.apply(&insert_event(tid, 1, 5.0)).unwrap();
        assert_eq!(
            n.aggregate_updates.len(),
            1,
            "one aggregate update through the wrapper"
        );
        assert_eq!(
            n.aggregate_updates[0].folded_value(),
            Some(crate::AggValue::Count(6)),
            "the incremented total"
        );
        assert_eq!(
            e.connector().call_count(),
            0,
            "an in-process fold reads nothing"
        );
    }

    #[test]
    fn async_ungrouped_aggregate_folds_across_an_applied_burst() {
        let (mut e, tid) = engine_with_values(vec![]);
        let count_id = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders"),
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
        crate::Install::install(
            &mut e,
            count_id,
            crate::AggregateSeedInstall {
                rows: vec![vec![Value::Int(5)]],
                read_at: None::<NoCheckpoint>,
            },
        )
        .unwrap();
        let aggregate_updates: Vec<_> = [insert_event(tid, 1, 5.0), insert_event(tid, 2, 6.0)]
            .iter()
            .flat_map(|ev| e.apply(ev).unwrap().aggregate_updates)
            .collect();
        assert_eq!(aggregate_updates.len(), 2, "each insert folds");
        assert_eq!(
            aggregate_updates.last().unwrap().folded_value(),
            Some(crate::AggValue::Count(7)),
            "the running total after both inserts"
        );
    }

    #[test]
    fn async_ungrouped_aggregate_demotion_resolves_through_the_wrapper() {
        let (mut e, tid) = engine_with_values(vec![]);
        let count_id = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders WHERE status = 'paid'"),
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
        crate::Install::install(
            &mut e,
            count_id,
            crate::AggregateSeedInstall {
                rows: vec![vec![Value::Int(1)]],
                read_at: None::<NoCheckpoint>,
            },
        )
        .unwrap();
        let missing_old = TestEvent::<Postgres>::update(tid, vec![], row(1, 5.0))
            .with_pk_columns([0u16])
            .with_changed_columns([3u16]);
        e.apply(&missing_old).unwrap();
        match block_on(e.resolve_collect()) {
            Err(ReExecError::Cursor { subscription, .. }) => {
                assert_eq!(
                    subscription, count_id,
                    "the demoted aggregate attempts its whole read"
                );
            }
            Ok(_) => panic!("expected the demotion to attempt a whole read"),
            Err(other) => panic!("expected a Cursor error naming the aggregate, got {other:?}"),
        }
    }

    #[test]
    fn async_snapshot_of_a_folding_aggregate_is_none() {
        let (mut e, _tid) = engine_with_values(vec![]);
        let count_id = match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders"),
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
        crate::Install::install(
            &mut e,
            count_id,
            crate::AggregateSeedInstall {
                rows: vec![vec![Value::Int(5)]],
                read_at: None::<NoCheckpoint>,
            },
        )
        .unwrap();
        assert!(
            block_on(e.snapshot(count_id)).unwrap().is_none(),
            "no bootstrap for a fold"
        );
        assert_eq!(
            e.connector().call_count(),
            0,
            "snapshot reads nothing for a fold"
        );
    }

    #[test]
    fn async_a_seed_that_demotes_at_install_serves_the_whole_read() {
        let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
            catalog(),
            PostgreSqlDialect {},
        )
        .with_max_groups_per_aggregate(1);
        let mut e =
            AutoResolvingEngine::new(inner, AsyncMode::new(MockAsyncConnector::new(vec![])));
        let grouped = match e
            .register(
                SubscriptionRequest::new(
                    1u64,
                    "SELECT status, COUNT(*) FROM orders GROUP BY status",
                ),
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
        let seeded = crate::Install::install(
            &mut e,
            grouped,
            crate::AggregateSeedInstall {
                rows: vec![
                    vec![Value::String("open".into()), Value::Int(2), Value::Int(2)],
                    vec![Value::String("done".into()), Value::Int(1), Value::Int(1)],
                ],
                read_at: None::<NoCheckpoint>,
            },
        )
        .unwrap();
        assert!(
            !seeded.transitions.is_empty(),
            "the install-time demotion carries a transition"
        );
        match block_on(e.snapshot(grouped)) {
            Err(ReExecError::Cursor { subscription, .. }) => {
                assert_eq!(
                    subscription, grouped,
                    "the demoted subscription attempts its whole read"
                );
            }
            Ok(answer) => panic!("expected the whole read to be attempted, got {answer:?}"),
            Err(other) => panic!("expected a Cursor error naming the subscription, got {other:?}"),
        }
    }

    #[test]
    fn async_ordered_row_query_folds_in_process() {
        let (mut e, tid) = engine_with_values(vec![]);
        match e
            .register(
                SubscriptionRequest::new(1u64, "SELECT * FROM orders ORDER BY price"),
                (),
            )
            .unwrap()
        {
            Registered {
                tier: Tier::InProcess(_),
                ..
            } => {}
            other => panic!("expected InProcess for an ordered row query, got {other:?}"),
        }
        let n = e.apply(&insert_event(tid, 1, 5.0)).unwrap();
        assert!(
            n.engine.inserted().contains(&1),
            "the ordered row list is notified of the insert"
        );
        assert_eq!(
            e.connector().call_count(),
            0,
            "an ordered row list reads nothing"
        );
    }

    /// Dropping a resolve mid-read loses nothing: the read stays queued, a
    /// fresh resolve completes it, and the event is never reapplied.
    #[test]
    fn dropped_resolve_keeps_the_read_queued() {
        let (mut e, tid) = engine_with_values(vec![Value::Float(7.0)]);
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
            other => panic!("expected Scalar, got {other:?}"),
        };
        crate::Install::install(
            &mut e,
            qid,
            crate::ScalarInstall {
                value: Value::Float(5.0),
                checkpoint: None::<NoCheckpoint>,
            },
        )
        .unwrap();

        let applied = e.apply(&delete_event(tid, 1, 5.0)).unwrap();
        assert!(
            applied.scalar_updates.is_empty(),
            "the read is queued, not run"
        );
        assert_eq!(e.pending_read_count(), 1);

        *e.connector().pend_next_read.lock() = true;
        {
            let waker = Arc::new(NoopWake).into();
            let mut ctx = Context::from_waker(&waker);
            let mut sink =
                |_delivery: super::super::ReadDelivery<DefaultIds, Postgres, NoCheckpoint>| {};
            let fut = e.resolve(&mut sink);
            let mut pinned = pin!(fut);
            assert!(
                pinned.as_mut().poll(&mut ctx).is_pending(),
                "the resolve suspends inside the connector read"
            );
            // Dropped here, mid-read.
        }
        assert_eq!(e.pending_read_count(), 1, "the dropped read stayed queued");
        assert_eq!(e.connector().call_count(), 0, "the read never completed");

        let resolved = block_on(e.resolve_collect()).unwrap();
        assert_eq!(resolved.scalar_updates.len(), 1);
        assert_eq!(resolved.scalar_updates[0].value, Value::Float(7.0));
        assert_eq!(e.pending_read_count(), 0);
        assert_eq!(
            e.connector().call_count(),
            1,
            "one completed read, no redispatch"
        );
    }

    /// Each page reaches the sink before the next page is fetched, so
    /// retained memory tracks one page rather than the whole answer.
    #[test]
    fn async_pages_reach_the_sink_before_the_next_fetch() {
        let (mut e, tid) = engine_with_values(vec![]);
        e.register(
            SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
            (),
        )
        .expect("whole read registers");
        e.connector().cursor_pages.lock().extend([
            crate::reexec::RowPage {
                columns: vec![String::from("status")],
                rows: vec![vec![Value::String("paid".into())]],
                more: true,
            },
            crate::reexec::RowPage {
                columns: vec![String::from("status")],
                rows: vec![vec![Value::String("void".into())]],
                more: false,
            },
        ]);
        e.apply(&insert_event(tid, 1, 5.0)).unwrap();

        let log = Arc::clone(&e.connector().log);
        block_on(e.resolve(move |delivery| {
            if matches!(delivery, crate::reexec::ReadDelivery::Rows(_)) {
                log.lock().push("deliver");
            }
        }))
        .unwrap();
        assert_eq!(
            *e.connector().log.lock(),
            ["open", "fetch", "deliver", "fetch", "deliver", "close"],
            "a page is delivered before the next one is fetched"
        );
    }

    /// A resolve dropped between pages leaves the read queued, and the retry
    /// streams a complete answer under a higher generation, which is the
    /// consumer's signal to discard the partial one.
    #[test]
    fn dropped_stream_is_superseded_by_a_higher_generation() {
        let (mut e, tid) = engine_with_values(vec![]);
        e.register(
            SubscriptionRequest::new(1u64, "SELECT DISTINCT status FROM orders"),
            (),
        )
        .expect("whole read registers");
        e.connector().cursor_pages.lock().extend([
            crate::reexec::RowPage {
                columns: vec![String::from("status")],
                rows: vec![vec![Value::String("paid".into())]],
                more: true,
            },
            crate::reexec::RowPage {
                columns: vec![String::from("status")],
                rows: vec![vec![Value::String("void".into())]],
                more: false,
            },
        ]);
        e.apply(&insert_event(tid, 1, 5.0)).unwrap();

        // The second fetch suspends, and the future is dropped there: one
        // partial page was already delivered.
        *e.connector().pend_fetch_at.lock() = Some(1);
        let partial = Arc::new(Mutex::new(Vec::new()));
        {
            let partial = Arc::clone(&partial);
            let waker = Arc::new(NoopWake).into();
            let mut ctx = Context::from_waker(&waker);
            let fut = e.resolve(move |delivery| {
                if let crate::reexec::ReadDelivery::Rows(page) = delivery {
                    partial.lock().push(page.generation);
                }
            });
            let mut pinned = pin!(fut);
            assert!(
                pinned.as_mut().poll(&mut ctx).is_pending(),
                "the resolve suspends between pages"
            );
        }
        assert_eq!(partial.lock().len(), 1, "one partial page was delivered");
        assert_eq!(e.pending_read_count(), 1, "the dropped read stayed queued");

        // The retry streams a complete answer under a higher generation.
        e.connector()
            .cursor_pages
            .lock()
            .push(crate::reexec::RowPage {
                columns: vec![String::from("status")],
                rows: vec![vec![Value::String("paid".into())]],
                more: false,
            });
        let retried = block_on(e.resolve_collect()).unwrap();
        assert_eq!(e.pending_read_count(), 0);
        assert!(!retried.rows_updates.is_empty());
        let partial_generation = partial.lock()[0];
        assert!(
            retried
                .rows_updates
                .iter()
                .all(|page| page.generation > partial_generation),
            "the complete answer supersedes the partial generation"
        );
        assert!(
            !retried.rows_updates.last().unwrap().more,
            "the retry ends its generation"
        );
    }
}

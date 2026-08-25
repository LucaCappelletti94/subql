//! Async parallel of [`AutoResolvingEngine`](super::AutoResolvingEngine).
//!
//! Same surface (`register`, `install`, `snapshot`, `consumers`,
//! `unregister_*`), with the methods that touch the connector returning
//! `Send` futures. Pick this engine when the database driver is async
//! (sqlx, tokio-postgres, diesel-async). Pick the sync engine when the
//! driver is sync (diesel, rusqlite) or when you want the simpler
//! testing surface.

use super::async_connector::AsyncConnector;
use super::auto::{reconcile_checkpoint, AutoResolvingEngine, ResolverMode, SnapshotResult};
use super::connector::ReExecError;
use super::engine::{BatchOutcome, ReExecNotifications, ScalarUpdate};
use crate::backend::{Backend, CdcEvent, Value};
use crate::compiler::literals::SqlLiteralParse;
use crate::{IdTypes, RegisterError, SubscriptionId, UnregisterReport};
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
/// The async resolve runs in three phases: decide and take (needs `&mut self`),
/// read concurrently (needs only shared borrows), install and emit (needs
/// `&mut self` again). This type is what crosses the first boundary, so
/// anything the read needs from engine state is owned by the time it is built.
/// Pending keys in particular are *taken* in phase one, which is why they
/// cannot be re-read later.
type KeyedRows<B> = Vec<(Vec<Value<B>>, Vec<Value<B>>)>;

/// Keys a resolve took from the engine, per query, so a failure can give them
/// back.
type BorrowedKeys<B> = Vec<(SubscriptionId, Vec<Vec<Value<B>>>)>;

enum ResolveJob<B: Backend> {
    /// A scalar the connector reads in one call.
    Scalar {
        sql: alloc::string::String,
        column_kind: crate::backend::BuiltinKind,
    },
    /// One grouped extreme and its source-row count.
    GroupedScalar {
        group: Vec<u8>,
        sql: alloc::string::String,
    },
    /// Rows for the keys that changed, read scoped to those keys. Boxed: this
    /// variant carries a parsed statement, and the others carry a string.
    Keyed(alloc::boxed::Box<KeyedJob<B>>),
    /// The whole result, paged. The generation is taken when the job is built,
    /// so a read that fails part way cannot let a later one reuse it.
    Whole {
        sql: alloc::string::String,
        generation: u64,
    },
}

/// The keyed tier's read, as planned.
struct KeyedJob<B: Backend> {
    plan: super::plan::KeyedPlan,
    key_positions: Vec<usize>,
    keys: Vec<Vec<Value<B>>>,
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

    Whole {
        generation: u64,
        pages: Vec<ReadPage<B>>,
    },
}
struct ResolveOutputs<'a, I: IdTypes, B: Backend, C: crate::Checkpoint> {
    aggregate_updates: &'a mut Vec<crate::AggregateValueUpdate<I, B>>,
    scalar_updates: &'a mut Vec<ScalarUpdate<I, B, C>>,
    rows_updates: &'a mut Vec<super::engine::RowsUpdate<I, B, C>>,
    row_deltas: &'a mut Vec<super::engine::RowDelta<I, B, C>>,
    followup: &'a mut Vec<super::ReExecutionTrigger<I, C>>,
    transitions: &'a mut Vec<crate::MaintenanceTransition>,
}

/// One page of a whole re-read, as it will be delivered.
struct ReadPage<B: Backend> {
    columns: Vec<alloc::string::String>,
    rows: Vec<Vec<Value<B>>>,
    more: bool,
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
    /// simultaneously across all [`consumers`](Self::consumers) and
    /// [`consumers_batch`](Self::consumers_batch) calls.
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
                &bootstrap.sql,
                self.max_page_bytes,
                &context.auth,
            )
            .await?;
            let rows = pages.into_iter().flat_map(|page| page.rows).collect();
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
                    super::ReExecutionRead::GroupedScalar { group, sql, .. } => {
                        let context = self
                            .contexts
                            .get(&subscription_id)
                            .expect("a grouped scalar read keeps its connector context");
                        let page = self
                            .mode
                            .connector
                            .read_page(sql, self.max_page_bytes, &context.auth)
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
                            &context.sql,
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
            let sql = context.sql.clone();
            let (pages, checkpoint) = Self::read_whole_with(
                &self.mode.connector,
                subscription_id,
                &sql,
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
        let (value, checkpoint) = self
            .mode
            .connector
            .execute_scalar(&context.sql, context.column_kind, &context.auth)
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

    /// Dispatch a CDC event.
    ///
    /// For every [`ReExecutionTrigger`] the inner engine emits, this
    /// method looks up the auth context, awaits the connector's
    /// `execute_scalar`, installs the result, and replaces the trigger
    /// with a [`ScalarUpdate`]. The first connector failure aborts the
    /// rest of the batch and is surfaced as [`ReExecError::Connector`].
    ///
    /// [`ReExecutionTrigger`]: super::ReExecutionTrigger
    #[allow(clippy::too_many_lines)]
    pub async fn consumers(
        &mut self,
        event: &E,
    ) -> Result<ReExecNotifications<I, E::Backend, E::Checkpoint>, ReExecError<X::Error>> {
        use futures_util::stream::StreamExt;

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

        // Pre-filter debounced triggers.
        let actionable: Vec<_> = triggers
            .into_iter()
            .filter(|t| !self.debounce_skip(t.subscription_id, &t.read))
            .collect();

        if actionable.is_empty() {
            return Ok(ReExecNotifications {
                engine,
                aggregate_updates,
                scalar_updates,
                rows_updates,
                row_deltas,
                triggers: Vec::new(),
                transitions,
            });
        }

        // Phase one, under `&mut self`: decide each query's tier and take the
        // engine state its read needs. Pending keys are consumed here, so this
        // cannot be folded into the concurrent phase below.
        let (jobs, borrowed) = self.plan_jobs(actionable);
        if jobs.is_empty() {
            return Ok(ReExecNotifications {
                engine,
                aggregate_updates,
                scalar_updates,
                rows_updates,
                row_deltas,
                triggers: Vec::new(),
                transitions,
            });
        }

        // Phase two: shared borrows only, so the reads can run concurrently.
        // `inner` and `last_reexec_at` stay free for the mutation afterwards.
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
                    super::ReExecutionTrigger<I, E::Checkpoint>,
                    Resolved<E::Backend>,
                ),
                ReExecError<X::Error>,
            >,
        > = futures_util::stream::iter(jobs.into_iter().map(|(trigger, job)| {
            let auth = &contexts
                .get(&trigger.subscription_id)
                .expect(
                    "every captured query stores its resolve context at register time, \
                     trigger.subscription_id must exist in `contexts`",
                )
                .auth;
            let throttle = throttle.clone();
            async move {
                let _guard = acquire_permit(throttle.as_ref(), trigger.subscription_id).await;
                let answer = Self::run_job(
                    connector,
                    job,
                    trigger.subscription_id,
                    max_page_bytes,
                    auth,
                )
                .await?;
                Ok::<_, ReExecError<X::Error>>((trigger, answer))
            }
        }))
        .buffer_unordered(jobs_len)
        .collect::<Vec<_>>()
        .await;

        // One failure fails the call, so nothing is delivered and every key
        // taken goes back. Collected rather than short-circuited for exactly
        // that reason: `try_collect` drops the other jobs, and their keys with
        // them.
        let answers = match Self::first_failure(resolved) {
            Ok(answers) => answers,
            Err(e) => {
                self.restore_borrowed(borrowed);
                return Err(e);
            }
        };

        // Phase three: borrows released, apply what came back.
        let mut followup = Vec::new();
        for (trigger, answer) in answers {
            self.stamp_reexec(trigger.subscription_id, &trigger.read);
            self.apply_resolved(
                &trigger,
                answer,
                &mut ResolveOutputs {
                    aggregate_updates: &mut aggregate_updates,
                    scalar_updates: &mut scalar_updates,
                    rows_updates: &mut rows_updates,
                    row_deltas: &mut row_deltas,
                    followup: &mut followup,
                    transitions: &mut transitions,
                },
            )?;
        }
        self.drain_followup(
            &mut followup,
            &mut aggregate_updates,
            &mut scalar_updates,
            &mut rows_updates,
            &mut row_deltas,
            &mut transitions,
        )
        .await?;

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

    /// Decide every triggered query's read, and record what was taken.
    ///
    /// Phase one of the resolve, and the only part needing `&mut self`, which
    /// is what lets the reads themselves run concurrently. A failure here gives
    /// back whatever earlier queries in the same call already took.
    #[allow(clippy::type_complexity)]
    fn plan_jobs(
        &mut self,
        actionable: Vec<super::ReExecutionTrigger<I, E::Checkpoint>>,
    ) -> (
        Vec<(
            super::ReExecutionTrigger<I, E::Checkpoint>,
            ResolveJob<E::Backend>,
        )>,
        BorrowedKeys<E::Backend>,
    ) {
        let mut jobs = Vec::with_capacity(actionable.len());
        // Taking a key is a promise to ask the database about it, and an error
        // means nothing was delivered, so every key taken has to survive it.
        let mut borrowed: BorrowedKeys<E::Backend> = Vec::new();
        for trigger in actionable {
            if let Some(job) = self.plan_job(&trigger) {
                if let ResolveJob::Keyed(keyed) = &job {
                    borrowed.push((trigger.subscription_id, keyed.keys.clone()));
                }
                jobs.push((trigger, job));
            } else {
                // Nothing to ask about, but the query was still triggered, so
                // its debounce stamp moves as if it had been read.
                self.stamp_reexec(trigger.subscription_id, &trigger.read);
            }
        }
        (jobs, borrowed)
    }

    /// Split collected outcomes into every answer, or the first failure.
    ///
    /// Safe against dropped keys only because the caller materialises every
    /// outcome with `.collect()` before calling this: the `?`-style walk
    /// below does short-circuit, but by then every sibling job has already
    /// finished and returned its keys. Swapping that `collect` for a lazy
    /// try-collect would reintroduce the loss.
    #[allow(clippy::type_complexity)]
    fn first_failure(
        resolved: Vec<
            Result<
                (
                    super::ReExecutionTrigger<I, E::Checkpoint>,
                    Resolved<E::Backend>,
                ),
                ReExecError<X::Error>,
            >,
        >,
    ) -> Result<
        Vec<(
            super::ReExecutionTrigger<I, E::Checkpoint>,
            Resolved<E::Backend>,
        )>,
        ReExecError<X::Error>,
    > {
        let mut answers = Vec::with_capacity(resolved.len());
        for outcome in resolved {
            answers.push(outcome?);
        }
        Ok(answers)
    }

    /// Give back every key a failed call had taken.
    fn restore_borrowed(&mut self, borrowed: BorrowedKeys<E::Backend>) {
        for (subscription_id, keys) in borrowed {
            self.inner.restore_pending_keys(subscription_id, keys);
        }
    }

    /// Install one answer and record what the caller should be told.
    ///
    /// Shared by the single-event and batch paths so the two cannot drift on
    /// what a tier delivers.
    fn apply_resolved(
        &mut self,
        trigger: &super::ReExecutionTrigger<I, E::Checkpoint>,
        answer: Resolved<E::Backend>,
        outputs: &mut ResolveOutputs<'_, I, E::Backend, E::Checkpoint>,
    ) -> Result<(), ReExecError<X::Error>> {
        match answer {
            Resolved::Scalar(value) => {
                outputs.scalar_updates.push(crate::Install::install(
                    &mut self.inner,
                    trigger.subscription_id,
                    crate::ScalarInstall {
                        value,
                        checkpoint: trigger.checkpoint.clone(),
                    },
                )?);
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
                outputs.aggregate_updates.extend(installed.updates);
                outputs.followup.extend(installed.triggers);
                outputs.transitions.extend(installed.transitions);
            }
            Resolved::Keyed {
                keys,
                columns,
                present,
            } => outputs.row_deltas.extend(super::auto::deltas_from(
                trigger.subscription_id,
                trigger.consumer_id,
                trigger.checkpoint.as_ref(),
                &keys,
                &present,
                &columns,
            )),
            // One update per page, same as the sync engine: a re-read is
            // delivered in pages sharing a generation, not as one message.
            Resolved::Whole { generation, pages } => {
                outputs.rows_updates.extend(pages.into_iter().map(|page| {
                    super::engine::RowsUpdate {
                        subscription_id: trigger.subscription_id,
                        consumer_id: trigger.consumer_id,
                        generation,
                        columns: page.columns,
                        rows: page.rows,
                        more: page.more,
                        checkpoint: trigger.checkpoint.clone(),
                    }
                }));
            }
        }
        Ok(())
    }

    /// Serve the follow-up triggers a resolve produced (a grouped read can
    /// displace further groups), one at a time so each may push more.
    /// Shared by [`consumers`](Self::consumers) and
    /// [`consumers_batch`](Self::consumers_batch), which once carried
    /// diverging copies of this loop.
    async fn drain_followup(
        &mut self,
        followup: &mut Vec<super::ReExecutionTrigger<I, E::Checkpoint>>,
        aggregate_updates: &mut Vec<crate::AggregateValueUpdate<I, E::Backend>>,
        scalar_updates: &mut Vec<ScalarUpdate<I, E::Backend, E::Checkpoint>>,
        rows_updates: &mut Vec<super::engine::RowsUpdate<I, E::Backend, E::Checkpoint>>,
        row_deltas: &mut Vec<super::engine::RowDelta<I, E::Backend, E::Checkpoint>>,
        transitions: &mut Vec<crate::MaintenanceTransition>,
    ) -> Result<(), ReExecError<X::Error>> {
        while let Some(trigger) = followup.pop() {
            if self.debounce_skip(trigger.subscription_id, &trigger.read) {
                continue;
            }
            let Some(job) = self.plan_job(&trigger) else {
                continue;
            };
            let auth = &self
                .contexts
                .get(&trigger.subscription_id)
                .expect("a follow-up read stores its connector context")
                .auth;
            let answer = Self::run_job(
                &self.mode.connector,
                job,
                trigger.subscription_id,
                self.max_page_bytes,
                auth,
            )
            .await?;
            self.stamp_reexec(trigger.subscription_id, &trigger.read);
            self.apply_resolved(
                &trigger,
                answer,
                &mut ResolveOutputs {
                    aggregate_updates,
                    scalar_updates,
                    rows_updates,
                    row_deltas,
                    followup,
                    transitions,
                },
            )?;
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
        trigger: &super::ReExecutionTrigger<I, E::Checkpoint>,
    ) -> Option<ResolveJob<E::Backend>> {
        let subscription_id = trigger.subscription_id;
        if let super::ReExecutionRead::GroupedScalar { group, sql, .. } = &trigger.read {
            return Some(ResolveJob::GroupedScalar {
                group: group.clone(),
                sql: sql.clone(),
            });
        }
        let ctx = self.contexts.get(&subscription_id).expect(
            "every captured query stores its resolve context at register time, \
             subscription_id must exist in `contexts`",
        );
        if !ctx.keyed {
            if !ctx.whole_result {
                return Some(ResolveJob::Scalar {
                    sql: ctx.sql.clone(),
                    column_kind: ctx.column_kind,
                });
            }
            let ctx = self
                .contexts
                .get_mut(&subscription_id)
                .expect("just read above");
            let sql = ctx.sql.clone();
            // Bump first: a read that fails part way through must not let a
            // later one reuse the generation its partial pages carried.
            ctx.generation = ctx.generation.saturating_add(1);
            return Some(ResolveJob::Whole {
                sql,
                generation: ctx.generation,
            });
        }

        let keys = self.inner.take_pending_keys(subscription_id);
        if keys.is_empty() {
            return None;
        }
        let plan = self.inner.keyed_plan(subscription_id)?;
        let plan = plan.clone();
        let key_positions = plan.key_positions.clone();
        Some(ResolveJob::Keyed(alloc::boxed::Box::new(KeyedJob {
            plan,
            key_positions,
            keys,
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
            ResolveJob::Scalar { sql, column_kind } => {
                let (value, _db_checkpoint) = connector
                    .execute_scalar(&sql, column_kind, auth)
                    .await
                    .map_err(|error| ReExecError::Connector {
                        subscription,
                        error,
                    })?;
                Ok(Resolved::Scalar(value))
            }
            ResolveJob::GroupedScalar { group, sql } => {
                let page = connector
                    .read_page(&sql, max_page_bytes, auth)
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
                    let mut seen_in_batch: Vec<Vec<Value<E::Backend>>> = Vec::new();
                    loop {
                        let page = connector
                            .read_page(&page_sql, max_page_bytes, auth)
                            .await
                            .map_err(|error| ReExecError::Connector {
                                subscription,
                                error,
                            })?;
                        if columns.is_empty() {
                            columns.clone_from(&page.value.columns);
                        }
                        let before = seen_in_batch.len();
                        for row in page.value.rows {
                            let key: Vec<Value<E::Backend>> = key_positions
                                .iter()
                                .filter_map(|i| row.get(*i).cloned())
                                .collect();
                            seen_in_batch.push(key.clone());
                            present.push((key, row));
                        }
                        // A page with no rows ends the read whatever it claims
                        // about there being more. Our own reader cannot report
                        // that combination, but this trait has outside
                        // implementors, and without this a connector that did
                        // would loop here forever.
                        if !page.value.more || seen_in_batch.len() == before {
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
            ResolveJob::Whole { sql, generation } => {
                // The read's own position is discarded here on purpose: a
                // re-read is delivered against the position of the event that
                // triggered it, which is what a consumer reconciles by.
                let (pages, _) =
                    Self::read_whole_with(connector, subscription, &sql, max_page_bytes, auth)
                        .await?;
                Ok(Resolved::Whole { generation, pages })
            }
        }
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
        sql: &str,
        max_page_bytes: usize,
        auth: &X::AuthContext,
    ) -> Result<(Vec<ReadPage<E::Backend>>, Option<X::Checkpoint>), ReExecError<X::Error>> {
        let cursor =
            connector
                .open_cursor(sql, auth)
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
                    more,
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

    /// Async batch variant of [`consumers`](Self::consumers).
    ///
    /// Runs each event through the inner trigger-emitting engine in input
    /// order, then awaits the connector for each **deduplicated** trigger.
    /// Dispatches the deduplicated triggers concurrently, keeping at most
    /// [`with_max_concurrent_reexecutions`](Self::with_max_concurrent_reexecutions)
    /// in flight at any time (unbounded when not configured). Per-event
    /// engine notifications stay in input order. The returned
    /// [`BatchOutcome::triggers`] is always empty after resolution.
    ///
    /// The first connector failure aborts the whole batch (remaining
    /// in-flight futures are dropped). Partial notifications are
    /// discarded. The caller retries.
    #[allow(clippy::too_many_lines)]
    pub async fn consumers_batch(
        &mut self,
        events: &[E],
    ) -> Result<BatchOutcome<I, E::Backend, E::Checkpoint>, ReExecError<X::Error>> {
        use futures_util::stream::StreamExt;

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

        // Pre-filter debounced triggers.
        let actionable: alloc::vec::Vec<_> = triggers
            .into_iter()
            .filter(|t| !self.debounce_skip(t.subscription_id, &t.read))
            .collect();

        if actionable.is_empty() {
            return Ok(BatchOutcome {
                per_event,
                aggregate_updates,
                scalar_updates,
                rows_updates,
                row_deltas,
                triggers: Vec::new(),
                transitions,
            });
        }

        // Phase one, under `&mut self`: same tier split as the single-event
        // path. The batch already coalesced to one trigger per query, so a
        // keyed job here carries every key the batch changed and reads once.
        let (jobs, borrowed) = self.plan_jobs(actionable);
        if jobs.is_empty() {
            return Ok(BatchOutcome {
                per_event,
                aggregate_updates,
                scalar_updates,
                rows_updates,
                row_deltas,
                triggers: Vec::new(),
                transitions,
            });
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
        let resolved: alloc::vec::Vec<
            Result<
                (
                    super::ReExecutionTrigger<I, E::Checkpoint>,
                    Resolved<E::Backend>,
                ),
                ReExecError<X::Error>,
            >,
        > = futures_util::stream::iter(jobs.into_iter().map(|(trigger, job)| {
            let auth = &contexts
                .get(&trigger.subscription_id)
                .expect(
                    "every captured query stores its resolve context at register time, \
                     trigger.subscription_id must exist in `contexts`",
                )
                .auth;
            let throttle = throttle.clone();
            async move {
                let _guard = acquire_permit(throttle.as_ref(), trigger.subscription_id).await;
                let answer = Self::run_job(
                    connector,
                    job,
                    trigger.subscription_id,
                    max_page_bytes,
                    auth,
                )
                .await?;
                Ok::<_, ReExecError<X::Error>>((trigger, answer))
            }
        }))
        .buffer_unordered(jobs_len)
        .collect::<Vec<_>>()
        .await;

        // One failure fails the call, so nothing is delivered and every key
        // taken goes back. Collected rather than short-circuited for exactly
        // that reason: `try_collect` drops the other jobs, and their keys with
        // them.
        let answers = match Self::first_failure(resolved) {
            Ok(answers) => answers,
            Err(e) => {
                self.restore_borrowed(borrowed);
                return Err(e);
            }
        };

        // Phase three: borrows released, apply what came back.
        let mut followup = Vec::new();
        for (trigger, answer) in answers {
            self.stamp_reexec(trigger.subscription_id, &trigger.read);
            self.apply_resolved(
                &trigger,
                answer,
                &mut ResolveOutputs {
                    aggregate_updates: &mut aggregate_updates,
                    scalar_updates: &mut scalar_updates,
                    rows_updates: &mut rows_updates,
                    row_deltas: &mut row_deltas,
                    followup: &mut followup,
                    transitions: &mut transitions,
                },
            )?;
        }
        self.drain_followup(
            &mut followup,
            &mut aggregate_updates,
            &mut scalar_updates,
            &mut rows_updates,
            &mut row_deltas,
            &mut transitions,
        )
        .await?;

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
    /// One id counter serves both registries, so the read registry is tried
    /// first and, when it claims the id, the stored resolve context is dropped
    /// with it.
    pub fn unregister_subscription(&mut self, subscription_id: SubscriptionId) -> bool {
        if self.inner.unregister_reread(subscription_id) {
            self.contexts.remove(&subscription_id);
            return true;
        }
        self.inner.unregister_subscription(subscription_id)
    }

    /// Unregister an engine subscription by `(consumer_id, sql)`.
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
    /// Not async: it delegates straight to
    /// [`SubscriptionEngine::consumers`](crate::SubscriptionEngine::consumers),
    /// which does no I/O, so no connector call is made and no aggregate fold
    /// advances. The return carries more than its name suggests: alongside the
    /// row verdicts it reports term-membership narrowings, which a replay
    /// announces a second time. Re-applying them is a set union or difference
    /// that does not move the stored state, but a caller acting on the
    /// announcement itself must treat a replay's narrowings as possibly-stale
    /// repeats.
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
    type Error = ReExecError<X::Error>;

    #[allow(clippy::manual_async_fn)]
    fn consumers(
        &mut self,
        event: &E,
    ) -> impl core::future::Future<Output = Result<Self::Notifications, Self::Error>> + Send {
        async move { Self::consumers(self, event).await }
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
    }

    impl MockAsyncConnector {
        fn new(values: Vec<Value<Postgres>>) -> Self {
            Self {
                values: Mutex::new(values),
                call_count: Mutex::new(0),
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
            _sql: &str,
            _kind: BuiltinKind,
            _auth: &(),
        ) -> impl Future<Output = Result<(Value<Postgres>, Option<Self::Checkpoint>), Self::Error>> + Send
        {
            async move {
                *self.call_count.lock() += 1;
                let value = self.values.lock().pop().ok_or(MockError("queue empty"))?;
                Ok((value, None))
            }
        }

        fn read_page(
            &self,
            _sql: &str,
            _max_bytes: usize,
            _auth: &(),
        ) -> impl Future<
            Output = Result<
                Snapshot<crate::reexec::RowPage<Postgres>, Self::Checkpoint>,
                Self::Error,
            >,
        > + Send {
            async move { Err(MockError("read_page is not exercised by the scalar tests")) }
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
        let n = block_on(e.consumers(&insert_event(tid, 2, 9.0))).unwrap();
        assert!(n.scalar_updates.is_empty());
        assert_eq!(e.connector().call_count(), 1);

        // Delete the extreme: trigger -> connector -> ScalarUpdate.
        let n = block_on(e.consumers(&delete_event(tid, 1, 5.0))).unwrap();
        assert_eq!(n.scalar_updates.len(), 1);
        assert_eq!(n.scalar_updates[0].subscription_id, qid);
        assert_eq!(n.scalar_updates[0].value, Value::Float(9.0));
        assert!(n.triggers.is_empty(), "async engine drains triggers");
        assert_eq!(e.connector().call_count(), 2);
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

        let n = block_on(e.consumers(&event)).unwrap();
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

        match block_on(e.consumers(&delete_event(tid, 1, 5.0))) {
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
    fn async_engine_consumers_batch_coalesces_repeated_triggers() {
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

        let outcome = block_on(e.consumers_batch(&events)).unwrap();
        assert_eq!(outcome.per_event.len(), 3, "per_event positional alignment");
        assert_eq!(
            e.connector().call_count(),
            1,
            "three displacing events collapse to one connector call"
        );
        assert_eq!(outcome.scalar_updates.len(), 1);
        assert_eq!(outcome.scalar_updates[0].value, Value::Float(99.0));
        assert!(outcome.triggers.is_empty());
    }

    /// `with_max_concurrent_reexecutions` does not change the result of
    /// `consumers_batch`. Correctness is preserved. The cap is a
    /// throughput / fairness knob, not a semantic one.
    #[test]
    #[allow(clippy::similar_names)]
    fn async_engine_consumers_batch_respects_max_concurrent_cap() {
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
        let outcome = block_on(e.consumers_batch(&events)).unwrap();
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

    // ----------------------------------------------------------------
    // Re-execution concurrency throttle
    // ----------------------------------------------------------------
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

    /// Cleanup invariant: after a successful `consumers_batch` the
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

        let _ = block_on(e.consumers_batch(&[delete_event(tid, 1, 7.0)])).unwrap();
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

        assert!(block_on(e.consumers_batch(&[delete_event(tid, 1, 7.0)])).is_err());
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
        let outcome = block_on(e.consumers_batch(&[delete_event(tid, 1, 7.0)])).unwrap();
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
            sql: String::new(),
            column_kinds: [BuiltinKind::Int, BuiltinKind::Int],
        };
        let second = super::super::ReExecutionRead::GroupedScalar {
            group: vec![2],
            sql: String::new(),
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
        let n = block_on(e.consumers(&delete_event(tid, 1, 5.0))).unwrap();
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
        let live = block_on(e.consumers(&ev)).unwrap();
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
        match block_on(e.consumers(&delete_event(tid, 1, 5.0))) {
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
}

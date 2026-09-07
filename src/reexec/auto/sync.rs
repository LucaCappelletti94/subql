//! The synchronous half of [`AutoResolvingEngine`], for `M = SyncMode<X>`.

use super::{
    absorb_keyed_page, decode_grouped_seed_rows, deltas_from, one_grouped_row,
    reconcile_checkpoint, AutoResolvingEngine, CdcEvent, Connector, DatabaseLike, IdTypes,
    InProcessKind, KeyBatches, KeyedPage, ReExecError, RowDelta, RowsUpdate, ScalarFamily,
    SeenKeys, SnapshotResult, SqlLiteralParse, String, SubscriptionId, SyncMode, Value, Vec,
};

/// Closes a cursor if the read using it is abandoned by an unwinding panic.
///
/// The ordinary path closes explicitly and disarms this, which is what lets a
/// close failure be reported instead of swallowed. Unwinding runs nothing but
/// destructors, so without this a panic mid-read would strand the connector's
/// map entry, holding a pooled connection inside an open transaction forever.
struct CloseOnUnwind<'a, X: Connector> {
    connector: &'a X,
    cursor: crate::reexec::CursorId,
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
        // A subscription the stream maintains has nothing to prime.
        if context.stream_answers_the_filter() {
            return Ok(None);
        }
        let grouped_bootstrap = context.grouped_bootstrap.clone();
        if let Some(bootstrap) = grouped_bootstrap {
            let (_, mut rows, checkpoint) = self.read_whole(&context.query, subscription_id)?;
            decode_grouped_seed_rows::<E::Backend>(&mut rows, &bootstrap.kinds);
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
                    crate::reexec::ReExecutionRead::GroupedScalar {
                        group,
                        query,
                        column_kinds,
                    } => {
                        let resolved = self.resolve_grouped_scalar(
                            subscription_id,
                            group,
                            query,
                            *column_kinds,
                            trigger.checkpoint.clone(),
                        )?;
                        self.apply_transitions(&resolved.transitions);
                        pending.extend(resolved.triggers);
                        installed.updates.extend(resolved.updates);
                        installed.transitions.extend(resolved.transitions);
                    }
                    crate::reexec::ReExecutionRead::Subscription => {
                        let query = self
                            .contexts
                            .get(&subscription_id)
                            .expect("a transitioned read keeps its connector context")
                            .query
                            .clone();
                        let (columns, rows, checkpoint) =
                            self.read_whole(&query, subscription_id)?;
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
            let (columns, rows, checkpoint) = self.read_whole(&query, subscription_id)?;
            return Ok(Some(SnapshotResult::Rows {
                columns,
                rows,
                checkpoint,
            }));
        }
        // A still-folding in-process aggregate is seeded through Install, not
        // read here. After a demotion the context is `whole_result` and handled
        // above, so this only fires before any demotion.
        if context.in_process == Some(InProcessKind::FoldingAggregate) {
            return Ok(None);
        }
        let (value, checkpoint) = self
            .mode
            .0
            .execute_scalar(
                &context.query.as_read_query(),
                context.column_kind,
                &context.auth,
            )
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

    /// Read a captured query's whole answer from one cursor, concatenating its
    /// pages.
    ///
    /// One snapshot for the lot, which is what makes the concatenation mean
    /// something: pages from separate reads would describe no single instant.
    /// The page budget still bounds each round trip, so a large answer is read
    /// in bounded steps even though it is returned whole.
    fn read_whole(
        &self,
        query: &crate::reexec::BoundQuery<E::Backend>,
        subscription_id: SubscriptionId,
    ) -> Result<
        (
            Vec<String>,
            Vec<Vec<Value<E::Backend>>>,
            Option<X::Checkpoint>,
        ),
        ReExecError<X::Error>,
    > {
        let context = self
            .contexts
            .get(&subscription_id)
            .expect("the caller just read this context");
        let cursor = self
            .mode
            .0
            .open_cursor(&query.as_read_query(), &context.auth)
            .map_err(|error| ReExecError::Cursor {
                subscription: subscription_id,
                error,
            })?;

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
                    .map_err(|error| ReExecError::Cursor {
                        subscription: subscription_id,
                        error,
                    })?;
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
            .map_err(|error| ReExecError::Cursor {
                subscription: subscription_id,
                error,
            });
        guard.armed = false;
        outcome?;
        closed?;
        Ok((columns, rows, checkpoint))
    }

    /// Execute every queued read through the connector, delivering each
    /// installed answer into `sink` as it completes.
    ///
    /// A read that fails a retryable way (a connector or cursor failure)
    /// stays queued together with every read behind it, and the next
    /// `resolve` retries them. A read that fails a non-retryable way, per
    /// [`ReExecError::is_retryable`](crate::reexec::ReExecError::is_retryable), is
    /// dropped: the same read returns the same mismatched answer, and
    /// requeueing it would block every read behind it forever. Deliveries
    /// already made stand: their answers were installed, so retrying them
    /// would be a second read of a current value, not a repair.
    ///
    /// # Errors
    ///
    /// [`ReExecError::Connector`] and [`ReExecError::Cursor`] name the
    /// subscription whose read failed. Install errors mean the database
    /// answer does not match the subscription and are not retryable.
    pub fn resolve<S>(&mut self, mut sink: S) -> Result<(), ReExecError<X::Error>>
    where
        S: FnMut(crate::reexec::ReadDelivery<I, E::Backend, E::Checkpoint>),
    {
        while let Some(trigger) = self.pending_reads.pop_front() {
            match self.resolve_one(&trigger, &mut sink) {
                Ok(()) => self.stamp_reexec(trigger.subscription_id, &trigger.read),
                Err(error) => {
                    if error.is_retryable() {
                        self.pending_reads.push_front(trigger);
                    }
                    return Err(error);
                }
            }
        }
        Ok(())
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
    pub fn resolve_collect(
        &mut self,
    ) -> Result<crate::reexec::ResolvedReads<I, E::Backend, E::Checkpoint>, ReExecError<X::Error>>
    {
        let mut collected = crate::reexec::ResolvedReads::default();
        self.resolve(|delivery| collected.push(delivery))?;
        Ok(collected)
    }

    /// Resolve one queued read and deliver its answers.
    fn resolve_one<S>(
        &mut self,
        trigger: &crate::reexec::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
        sink: &mut S,
    ) -> Result<(), ReExecError<X::Error>>
    where
        S: FnMut(crate::reexec::ReadDelivery<I, E::Backend, E::Checkpoint>),
    {
        if let crate::reexec::ReExecutionRead::GroupedScalar {
            group,
            query,
            column_kinds,
        } = &trigger.read
        {
            let installed = self.resolve_grouped_scalar(
                trigger.subscription_id,
                group,
                query,
                *column_kinds,
                trigger.checkpoint.clone(),
            )?;
            self.apply_transitions(&installed.transitions);
            for update in installed.updates {
                sink(crate::reexec::ReadDelivery::Aggregate(update));
            }
            for transition in installed.transitions {
                sink(crate::reexec::ReadDelivery::Transition(transition));
            }
            for followup in installed.triggers {
                self.enqueue_read(followup);
            }
            return Ok(());
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
            for delta in deltas {
                sink(crate::reexec::ReadDelivery::Delta(delta));
            }
            return Ok(());
        }
        if ctx.whole_result {
            self.reread(
                trigger.subscription_id,
                trigger.consumer_id,
                trigger.checkpoint.as_ref(),
                sink,
            )?;
            return Ok(());
        }
        let (value, _db_checkpoint) = self
            .mode
            .0
            .execute_scalar(&ctx.query.as_read_query(), ctx.column_kind, &ctx.auth)
            .map_err(|error| ReExecError::Connector {
                subscription: trigger.subscription_id,
                error,
            })?;
        let update = crate::Install::install(
            &mut self.inner,
            trigger.subscription_id,
            crate::ScalarInstall {
                value,
                checkpoint: trigger.checkpoint.clone(),
            },
        )?;
        sink(crate::reexec::ReadDelivery::Scalar(update));
        Ok(())
    }

    fn resolve_grouped_scalar(
        &mut self,
        subscription_id: SubscriptionId,
        group: &[u8],
        query: &crate::reexec::BoundQuery<E::Backend>,
        _column_kinds: [ScalarFamily; 2],
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
            .read_page(&query.as_read_query(), self.max_page_bytes, &context.auth)
            .map_err(|error| ReExecError::Connector {
                subscription: subscription_id,
                error,
            })?;
        let row = one_grouped_row(subscription_id, snapshot.value)?;
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
        let keys = self.inner.clone_pending_keys(subscription_id);
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let Some(plan) = self.inner.keyed_plan(subscription_id) else {
            return Ok(Vec::new());
        };
        let mut columns = Vec::new();
        let mut present: Vec<(Vec<Value<E::Backend>>, Vec<Value<E::Backend>>)> = Vec::new();
        // Asked in bounded batches. Statement duration tracks how many keys a
        // statement names, and a caller's statement timeout applies per
        // statement, so one unbounded request disables the only read ceiling
        // the caller has. Each key appears in exactly one batch, so no key is
        // asked about twice however many rows come back. One renderer serves
        // every batch and page, so the plan's statement is copied once.
        //
        // The keys are a snapshot, removed only after every batch delivered.
        // A failure therefore loses nothing: the whole set stays recorded,
        // and the retry asks again, which is the cost of an all-or-nothing
        // result.
        let mut scoped = crate::reexec::plan::ScopedRead::new(plan).map_err(|e| {
            ReExecError::Dispatch(crate::DispatchError::VmError(alloc::format!("{e}")))
        })?;
        for batch in KeyBatches::new(&keys, self.max_keys_per_read) {
            self.read_one_batch(
                subscription_id,
                &mut scoped,
                &plan.key_positions,
                batch,
                &mut columns,
                &mut present,
            )?;
        }
        self.inner.remove_pending_keys(subscription_id, &keys);

        Ok(deltas_from(
            subscription_id,
            consumer_id,
            checkpoint,
            &keys,
            &present,
            columns,
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
        scoped: &mut crate::reexec::plan::ScopedRead,
        key_positions: &[usize],
        batch: &[Vec<Value<E::Backend>>],
        columns: &mut Vec<String>,
        present: &mut Vec<(Vec<Value<E::Backend>>, Vec<Value<E::Backend>>)>,
    ) -> Result<(), ReExecError<X::Error>> {
        let context = self
            .contexts
            .get(&subscription_id)
            .expect("a captured query stores its context at register time");
        let render = |scoped: &mut crate::reexec::plan::ScopedRead,
                      keys: &[Vec<Value<E::Backend>>]| {
            scoped.render::<E::Backend>(keys).map_err(|e| {
                ReExecError::Dispatch(crate::DispatchError::VmError(alloc::format!("{e}")))
            })
        };
        let Some(mut page_sql) = render(scoped, batch)? else {
            return Ok(());
        };
        let mut seen: SeenKeys<E::Backend> = SeenKeys::new();
        loop {
            let page = self
                .mode
                .0
                .read_page(
                    &crate::reexec::ReadQuery::borrowed(&page_sql, context.query.binds()),
                    self.max_page_bytes,
                    &context.auth,
                )
                .map_err(|error| ReExecError::Connector {
                    subscription: subscription_id,
                    error,
                })?;
            let remaining = match absorb_keyed_page(
                subscription_id,
                page.value,
                batch,
                key_positions,
                columns,
                &mut seen,
                present,
            )? {
                KeyedPage::Answered => return Ok(()),
                KeyedPage::Resume(remaining) => remaining,
            };
            match render(scoped, &remaining)? {
                Some(next) => page_sql = next,
                None => return Ok(()),
            }
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
    /// Re-read a captured query in full, delivering each page into `sink` as
    /// it is fetched, so retained memory tracks one page and never the whole
    /// answer.
    ///
    /// Every page comes from one cursor in one transaction, which is what makes
    /// the pages add up to a single instant. A read that fails or is abandoned
    /// part way leaves a generation with no final page. The next re-read
    /// delivers a higher generation, which is the consumer's signal to discard
    /// the partial one.
    fn reread<S>(
        &mut self,
        subscription_id: SubscriptionId,
        consumer_id: I::ConsumerId,
        checkpoint: Option<&E::Checkpoint>,
        sink: &mut S,
    ) -> Result<(), ReExecError<X::Error>>
    where
        S: FnMut(crate::reexec::ReadDelivery<I, E::Backend, E::Checkpoint>),
    {
        let ctx = self
            .contexts
            .get_mut(&subscription_id)
            .expect("every read tier stores its connector context at registration");
        let query = ctx.query.clone();
        ctx.generation = ctx.generation.saturating_add(1);
        let generation = ctx.generation;
        let cursor = self
            .mode
            .0
            .open_cursor(
                &query.as_read_query(),
                &self.contexts[&subscription_id].auth,
            )
            .map_err(|error| ReExecError::Cursor {
                subscription: subscription_id,
                error,
            })?;
        let mut guard = CloseOnUnwind {
            connector: &self.mode.0,
            cursor,
            armed: true,
        };
        let outcome = (|| -> Result<(), ReExecError<X::Error>> {
            loop {
                let page = self
                    .mode
                    .0
                    .fetch_cursor(cursor, self.max_page_bytes)
                    .map_err(|error| ReExecError::Cursor {
                        subscription: subscription_id,
                        error,
                    })?;
                let more = page.value.more;
                sink(crate::reexec::ReadDelivery::Rows(RowsUpdate {
                    subscription_id,
                    consumer_id,
                    generation,
                    columns: page.value.columns,
                    rows: page.value.rows,
                    more,
                    checkpoint: checkpoint.cloned(),
                }));
                if !more {
                    return Ok(());
                }
            }
        })();
        let closed = self
            .mode
            .0
            .close_cursor(cursor)
            .map_err(|error| ReExecError::Cursor {
                subscription: subscription_id,
                error,
            });
        guard.armed = false;
        outcome?;
        closed?;
        Ok(())
    }
}

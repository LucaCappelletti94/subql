//! Aggregate maintenance on the engine: tier transitions, event folds and
//! the reads that seed an aggregate after registration.
//!
//! Split from `engine.rs` for size only: everything here is inherent on
//! [`SubscriptionEngine`] and was moved verbatim.
use super::{
    catalog_helpers, AggregateRegistration, CdcEvent, DatabaseLike, DispatchError, EventKind,
    IdTypes, RereadRegistration, SqlLiteralParse, SubscriptionEngine, SubscriptionId,
    SubscriptionRequest, SubscriptionScope, TableId, ToString, Value, Vec,
};
use crate::backend::Backend;

impl<E: CdcEvent, I: IdTypes, DB: DatabaseLike + 'static> SubscriptionEngine<E, I, DB>
where
    E::Backend: SqlLiteralParse,
{
    /// Replace one in-process aggregate with a complete row read under the
    /// same subscription identity.
    pub(super) fn transition_aggregate_to_whole(
        &mut self,
        subscription_id: SubscriptionId,
        reason: crate::MaintenanceStopReason,
        checkpoint: Option<&E::Checkpoint>,
    ) -> Result<
        (
            crate::MaintenanceTransition<E::Backend>,
            crate::reexec::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
        ),
        DispatchError,
    > {
        let registration = self
            .aggregate_registrations
            .get(&subscription_id)
            .cloned()
            .ok_or_else(|| DispatchError::TierTransition {
                subscription: subscription_id,
                message: "aggregate registration metadata is missing".to_string(),
            })?;
        let plan = crate::reexec::plan::build_whole_rows_plan::<E::Backend, DB>(
            &registration.source_query,
            &self.dialect,
            &self.database,
        )
        .map_err(|error| DispatchError::TierTransition {
            subscription: subscription_id,
            message: error.to_string(),
        })?;
        let session = match registration.scope {
            SubscriptionScope::Durable => None,
            SubscriptionScope::Session(session) => Some(session),
        };
        let _ = self.unregister_subscription_internal(subscription_id);
        let reread = RereadRegistration {
            consumer: registration.consumer,
            session,
            source_query: &registration.source_query,
            database_reads_per_consumer: registration.database_reads_per_consumer,
        };
        let registered = self.capture_whole(subscription_id, plan, &reread);
        let transition = crate::MaintenanceTransition {
            subscription_id,
            from: crate::TierKind::InProcess,
            to: registered.tier,
            reason,
        };
        let trigger = crate::reexec::ReExecutionTrigger {
            subscription_id,
            consumer_id: registration.consumer,
            read: crate::reexec::ReExecutionRead::Subscription,
            checkpoint: checkpoint.cloned(),
        };
        Ok((transition, trigger))
    }

    fn push_aggregate_stop(
        &mut self,
        subscription: SubscriptionId,
        reason: crate::MaintenanceStopReason,
        checkpoint: Option<&E::Checkpoint>,
        output: &mut crate::AggregateMaintenanceOutput<I, E::Backend, E::Checkpoint>,
    ) -> Result<(), DispatchError> {
        let (transition, trigger) =
            self.transition_aggregate_to_whole(subscription, reason, checkpoint)?;
        output.transitions.push(transition);
        output.triggers.push(trigger);
        Ok(())
    }

    fn push_aggregate_stops(
        &mut self,
        table_id: TableId,
        missing_old: Vec<SubscriptionId>,
        group_key_failed: Vec<SubscriptionId>,
        mut group_limit: Vec<SubscriptionId>,
        checkpoint: Option<&E::Checkpoint>,
        output: &mut crate::AggregateMaintenanceOutput<I, E::Backend, E::Checkpoint>,
    ) -> Result<(), DispatchError> {
        for subscription in missing_old {
            self.push_aggregate_stop(
                subscription,
                crate::MaintenanceStopReason::MissingOldRow { table_id },
                checkpoint,
                output,
            )?;
        }
        for subscription in group_key_failed {
            self.push_aggregate_stop(
                subscription,
                crate::MaintenanceStopReason::GroupKeyUnencodable { table_id },
                checkpoint,
                output,
            )?;
        }
        group_limit.sort_unstable();
        group_limit.dedup();
        for subscription in group_limit {
            self.push_aggregate_stop(
                subscription,
                crate::MaintenanceStopReason::GroupLimit {
                    limit: self.max_groups_per_aggregate,
                },
                checkpoint,
                output,
            )?;
        }
        Ok(())
    }
    /// Report every aggregate subscription whose value moved on this event.
    ///
    /// One entry per subscription, naming its consumer alongside, because one
    /// consumer may hold several aggregate subscriptions and their totals are
    /// separate. A subscription whose value did not move is absent, and so is
    /// one that has not been given its starting numbers through
    /// [`Install::install`](crate::Install::install) with [`AggregateSeedInstall`](crate::AggregateSeedInstall): a total
    /// covering only the last few seconds is worse than silence.
    ///
    /// A `TRUNCATE` empties every held total for that table and reports the
    /// ones that moved. No re-read is needed, because an emptied table's
    /// components are all zero.
    ///
    /// # Errors
    ///
    /// [`DispatchError::UnknownTableId`] for a table absent from the catalog,
    /// [`DispatchError::AggregateUpdateRequiresOldRow`] when an UPDATE carries
    /// no old image, and [`DispatchError::Value`] when a cell cannot be
    /// decoded. An error leaves every total exactly as it was: the fold is
    /// computed whole before any of it is committed.
    ///
    /// # Examples
    /// ```
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     catalog_helpers, AggValue, DefaultIds, Install, SubscriptionEngine,
    ///     SubscriptionRequest,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT);",
    /// )
    /// .expect("the DDL parses");
    /// let orders_id = catalog_helpers::table_id(&database, "orders").expect("orders is cataloged");
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {});
    ///
    /// let registered = engine
    ///     .register(SubscriptionRequest::new(
    ///         99,
    ///         "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
    ///     ))
    ///     .expect("the query registers");
    ///
    /// // Nothing has changed yet, so the read cannot have raced anything and
    /// // needs no stream position.
    /// Install::install(
    ///     &mut engine,
    ///     registered.subscription_id,
    ///     subql::AggregateSeedInstall {
    ///         rows: vec![vec![Value::Int(4)]],
    ///         read_at: None,
    ///     },
    /// )
    /// .expect("the starting numbers land");
    ///
    /// let event = TestEvent::<Postgres>::insert(
    ///     orders_id,
    ///     vec![Value::Int(1), Value::String("paid".into())],
    /// )
    /// .with_pk_columns([0u16]);
    ///
    /// let updates = engine.aggregate_updates(&event).expect("the event folds");
    /// assert_eq!(updates.len(), 1);
    /// assert_eq!(updates[0].subscription, registered.subscription_id);
    /// assert_eq!(updates[0].consumer, 99);
    /// assert_eq!(
    ///     updates[0].change,
    ///     subql::AggregateValueChange::Set(subql::AggregateResultValue::Folded(
    ///         AggValue::Count(5),
    ///     ))
    /// );
    ///
    /// // Aggregate subscriptions are answered here, never by `consumers()`.
    /// let notifs = engine.consumers(&event).expect("the event matches");
    /// assert!(notifs.inserted().is_empty());
    /// ```
    pub fn aggregate_updates(
        &mut self,
        event: &E,
    ) -> Result<crate::AggregateMaintenanceOutput<I, E::Backend, E::Checkpoint>, DispatchError>
    {
        let table_id = event.table_id(&self.database);
        if !self.partitions.contains_key(&table_id) {
            return if self.table_in_catalog(table_id) {
                Ok(crate::AggregateMaintenanceOutput::empty())
            } else {
                Err(DispatchError::UnknownTableId(table_id))
            };
        }

        if event.kind() == EventKind::Truncate {
            let at = event.checkpoint();
            let mut output: crate::AggregateMaintenanceOutput<I, E::Backend, E::Checkpoint> =
                crate::AggregateMaintenanceOutput::empty();
            output.updates = self.empty_aggregate_totals(table_id, at.as_ref());
            return Ok(output);
        }

        let arity = catalog_helpers::table_arity(&self.database, table_id)?;
        let partition = self
            .partitions
            .get(&table_id)
            .ok_or(DispatchError::UnknownTableId(table_id))?;
        let computation = crate::runtime::dispatch::compute_agg_deltas(
            event,
            partition,
            &mut self.vm,
            arity,
            &self.database,
        )?;

        let cap = self.max_changes_during_aggregate_read;
        let at = event.checkpoint();
        let mut output: crate::AggregateMaintenanceOutput<I, E::Backend, E::Checkpoint> =
            crate::AggregateMaintenanceOutput::empty();
        let mut group_limit = Vec::new();
        for delta in computation.deltas {
            if let Some(group) = delta.group {
                let Some(total) = self.grouped_aggregates.get_mut(&delta.subscription) else {
                    continue;
                };
                match total.fold(
                    group,
                    delta.delta,
                    delta.rows,
                    at.as_ref(),
                    cap,
                    self.max_groups_per_aggregate,
                ) {
                    crate::runtime::aggregate::GroupedFoldOutcome::Unchanged => {}
                    crate::runtime::aggregate::GroupedFoldOutcome::Change(identity, change) => {
                        output.updates.push(crate::AggregateValueUpdate {
                            subscription: delta.subscription,
                            consumer: total.consumer(),
                            group: Some(identity),
                            change,
                        });
                    }
                    crate::runtime::aggregate::GroupedFoldOutcome::GroupLimit => {
                        group_limit.push(delta.subscription);
                    }
                }
                continue;
            }
            let (Some(total), Some(change)) =
                (self.aggregates.get_mut(&delta.subscription), delta.delta)
            else {
                continue;
            };
            if let Some(value) = total.fold(change, at.as_ref(), cap) {
                output.updates.push(crate::AggregateValueUpdate {
                    subscription: delta.subscription,
                    consumer: total.consumer(),
                    group: None,
                    change: crate::AggregateValueChange::Set(crate::AggregateResultValue::Folded(
                        value,
                    )),
                });
            }
        }

        self.push_aggregate_stops(
            table_id,
            computation.missing_old,
            computation.group_key_failed,
            group_limit,
            at.as_ref(),
            &mut output,
        )?;
        Ok(output)
    }

    /// Install one ungrouped aggregate row after its database read.
    #[allow(clippy::needless_pass_by_value)]
    pub(super) fn install_aggregate_rows_inner(
        &mut self,
        subscription: SubscriptionId,
        row: &[Value<E::Backend>],
        read_at: Option<E::Checkpoint>,
    ) -> Result<crate::AggValue, crate::AggregateInstallError> {
        let cap = self.max_changes_during_aggregate_read;
        let total = self
            .aggregates
            .get_mut(&subscription)
            .ok_or(crate::AggregateInstallError::UnknownAggregate(subscription))?;
        total.install(subscription, row, read_at.as_ref(), cap)
    }

    /// Empty an aggregate value and require another database read.
    pub fn reset_aggregate_value(&mut self, subscription: SubscriptionId) -> bool {
        if let Some(total) = self.aggregates.get_mut(&subscription) {
            total.reset();
            return true;
        }
        if let Some(total) = self.grouped_aggregates.get_mut(&subscription) {
            total.reset();
            return true;
        }
        false
    }

    /// Open unseeded state for a newly registered aggregate.
    pub(super) fn open_aggregate_total(
        &mut self,
        subscription: SubscriptionId,
        request: &SubscriptionRequest<I, E::Backend>,
        table_id: crate::TableId,
        projection: &crate::compiler::sql_shape::QueryProjection,
        database_reads_per_consumer: bool,
    ) {
        let consumer = request.consumer_id;
        let opened = match projection {
            crate::compiler::sql_shape::QueryProjection::Aggregate(spec) => {
                self.aggregates.insert(
                    subscription,
                    crate::runtime::aggregate::AggregateTotal::new(consumer, spec.clone()),
                );
                true
            }
            crate::compiler::sql_shape::QueryProjection::GroupedAggregate {
                groups,
                agg,
                having,
            } => {
                let columns = groups
                    .iter()
                    .map(|column| {
                        crate::catalog_helpers::group_key_column::<E::Backend, _>(
                            &self.database,
                            table_id,
                            *column,
                        )
                    })
                    .collect::<Option<Vec<_>>>()
                    .expect("the grouped planner resolved every key column");
                let group_key_encoder = E::Backend::group_key_encoder(columns)
                    .expect("the grouped planner selected a canonical key encoder");
                self.grouped_aggregates.insert(
                    subscription,
                    crate::runtime::aggregate::GroupedAggregateTotal::new(
                        consumer,
                        agg.clone(),
                        groups.len(),
                        having.as_ref(),
                        group_key_encoder,
                    ),
                );
                true
            }
            crate::compiler::sql_shape::QueryProjection::Rows => false,
        };
        if opened {
            self.aggregate_registrations.insert(
                subscription,
                AggregateRegistration {
                    consumer,
                    scope: request.scope,
                    source_query: crate::reexec::BoundQuery::new(
                        request.sql.clone(),
                        request.binds.clone(),
                    ),
                    database_reads_per_consumer,
                },
            );
        }
    }

    /// The value one ungrouped aggregate subscription holds right now.
    ///
    /// Grouped values arrive through `AggregateValueUpdate`, keyed by
    /// `group`. `None` here also covers an unseeded or unknown subscription.
    #[must_use]
    pub fn current_aggregate_value(&self, subscription: SubscriptionId) -> Option<crate::AggValue> {
        self.aggregates.get(&subscription)?.value()
    }

    /// Empty every held total on `table_id`, reporting the ones that moved.
    fn empty_aggregate_totals(
        &mut self,
        table_id: TableId,
        at: Option<&E::Checkpoint>,
    ) -> Vec<crate::AggregateValueUpdate<I, E::Backend>> {
        let cap = self.max_changes_during_aggregate_read;
        let mut updates = Vec::new();
        for (&subscription, total) in &mut self.aggregates {
            if self.subscription_to_table.get(&subscription) != Some(&table_id) {
                continue;
            }
            if let Some(value) = total.empty(at, cap) {
                updates.push(crate::AggregateValueUpdate {
                    subscription,
                    consumer: total.consumer(),
                    group: None,
                    change: crate::AggregateValueChange::Set(crate::AggregateResultValue::Folded(
                        value,
                    )),
                });
            }
        }
        for (&subscription, total) in &mut self.grouped_aggregates {
            if self.subscription_to_table.get(&subscription) != Some(&table_id) {
                continue;
            }
            for (group, change) in total.empty(at, cap) {
                updates.push(crate::AggregateValueUpdate {
                    subscription,
                    consumer: total.consumer(),
                    group: Some(group),
                    change,
                });
            }
        }
        updates
    }

    /// Unseeded aggregate subscriptions on this event's table, each requiring
    /// the bootstrap SQL returned at registration.
    pub(super) fn unseeded_aggregate_triggers(
        &self,
        event: &E,
    ) -> Vec<crate::reexec::ReExecutionTrigger<I, E::Checkpoint, E::Backend>> {
        let table_id = event.table_id(&self.database);
        let mut triggers: Vec<_> = self
            .aggregates
            .iter()
            .filter(|(subscription_id, total)| {
                total.value().is_none()
                    && self.subscription_to_table.get(*subscription_id) == Some(&table_id)
            })
            .map(
                |(subscription_id, total)| crate::reexec::ReExecutionTrigger {
                    subscription_id: *subscription_id,
                    consumer_id: total.consumer(),
                    read: crate::reexec::ReExecutionRead::Subscription,
                    checkpoint: event.checkpoint(),
                },
            )
            .collect();
        triggers.extend(
            self.grouped_aggregates
                .iter()
                .filter(|(subscription_id, total)| {
                    !total.is_seeded()
                        && self.subscription_to_table.get(*subscription_id) == Some(&table_id)
                })
                .map(
                    |(subscription_id, total)| crate::reexec::ReExecutionTrigger {
                        subscription_id: *subscription_id,
                        consumer_id: total.consumer(),
                        read: crate::reexec::ReExecutionRead::Subscription,
                        checkpoint: event.checkpoint(),
                    },
                ),
        );
        triggers
    }
}

//! [`crate::Install`] implementations: how each read tier's database result
//! enters the engine.
//!
//! Split from `engine.rs` for size only: every impl was moved verbatim.
use super::{
    CdcEvent, DatabaseLike, IdTypes, SqlLiteralParse, SubscriptionEngine, SubscriptionId, Vec,
};
use alloc::string::String;

impl<E, I, DB> SubscriptionEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
{
    /// Remember the fence an install brings as the engine's latest, and
    /// answer the stamp a read asked from this install on carries.
    fn record_fence(&mut self, fence: Option<&<E::Checkpoint as crate::Checkpoint>::Fence>) -> u64 {
        match fence {
            Some(fence) => self.latest_fence.record(fence),
            None => self.latest_fence.now(),
        }
    }
}

impl<E, I, DB> crate::Install<crate::FenceInstall<E::Checkpoint>> for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
{
    type Output = ();
    type Error = crate::InstallError;

    /// Adopt the database's current fence and drop from every re-read answer
    /// the unseen changes it holds, where no older read is outstanding.
    fn install(
        &mut self,
        _subscription_id: SubscriptionId,
        input: crate::FenceInstall<E::Checkpoint>,
    ) -> Result<(), Self::Error> {
        self.latest_fence.probing = false;
        let Some(fence) = input.fence else {
            return Ok(());
        };
        self.latest_fence.record(&fence);
        let cap = self.max_changes_during_aggregate_read;
        for entry in self.reexec.values_mut() {
            entry.runtime.forget_seen(&self.latest_fence, cap);
        }
        Ok(())
    }
}

impl<E, I, DB> crate::Install<crate::ScalarInstall<E::Backend, E::Checkpoint>>
    for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
{
    type Output = crate::reexec::ScalarInstalled<I, E::Backend, E::Checkpoint>;
    type Error = crate::InstallError;

    fn install(
        &mut self,
        subscription_id: SubscriptionId,
        input: crate::ScalarInstall<E::Backend, E::Checkpoint>,
    ) -> Result<Self::Output, Self::Error> {
        let tier = self
            .reexec
            .get(&subscription_id)
            .ok_or(crate::InstallError::UnknownSubscription(subscription_id))?
            .tier;
        if tier != crate::ReadTier::Scalar {
            return Err(crate::InstallError::WrongTier {
                subscription: subscription_id,
                input: "ScalarInstall",
            });
        }
        let asked = self.record_fence(input.fence.as_ref());
        let entry = self
            .reexec
            .get_mut(&subscription_id)
            .expect("looked up just above");
        Ok(
            match entry.runtime.install(input.value, input.fence, asked) {
                crate::reexec::maintain::ScalarInstallOutcome::Value(value) => {
                    crate::reexec::ScalarInstalled::Value(crate::reexec::ScalarUpdate {
                        subscription_id,
                        consumer_id: entry.consumer_id,
                        value,
                        checkpoint: input.checkpoint,
                    })
                }
                crate::reexec::maintain::ScalarInstallOutcome::ReadAgain => {
                    crate::reexec::ScalarInstalled::ReadAgain(crate::reexec::ReExecutionTrigger {
                        subscription_id,
                        consumer_id: entry.consumer_id,
                        read: crate::reexec::ReExecutionRead::Subscription,
                        checkpoint: input.checkpoint,
                    })
                }
            },
        )
    }
}
impl<E, I, DB> crate::Install<crate::GroupedScalarSeedInstall<E::Backend, E::Checkpoint>>
    for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
{
    type Output = crate::AggregateMaintenanceOutput<I, E::Backend, E::Checkpoint>;
    type Error = crate::AggregateInstallError;

    fn install(
        &mut self,
        subscription_id: SubscriptionId,
        input: crate::GroupedScalarSeedInstall<E::Backend, E::Checkpoint>,
    ) -> Result<Self::Output, Self::Error> {
        let group_limit = self.max_groups_per_aggregate;
        let pending_cap = self.max_changes_during_aggregate_read;
        self.record_fence(input.fence.as_ref());
        let (consumer, table_id, installed) = {
            let entry = self.reexec.get_mut(&subscription_id).ok_or(
                crate::AggregateInstallError::UnknownAggregate(subscription_id),
            )?;
            let crate::reexec::maintain::QueryRuntime::Grouped(query) = &mut entry.runtime else {
                return Err(crate::AggregateInstallError::UnknownAggregate(
                    subscription_id,
                ));
            };
            (
                entry.consumer_id,
                entry.tables[0],
                query.install_seed(
                    subscription_id,
                    &input.rows,
                    input.fence,
                    pending_cap,
                    group_limit,
                    &self.latest_fence,
                ),
            )
        };
        let grouped = match installed {
            Ok(grouped) => grouped,
            Err(error) => {
                return self.stopped_for_group_limit(
                    subscription_id,
                    super::GroupedStopTier::GroupedScalar,
                    error,
                    group_limit,
                    None,
                    Some(table_id),
                );
            }
        };
        let reason = if grouped.missing_group {
            Some(crate::MaintenanceStopReason::MissingOldRow { table_id })
        } else if grouped.group_limit {
            Some(crate::MaintenanceStopReason::GroupLimit { limit: group_limit })
        } else {
            None
        };
        if let Some(reason) = reason {
            return self.stopped_for_reason(
                subscription_id,
                super::GroupedStopTier::GroupedScalar,
                reason,
                None,
            );
        }
        Ok(crate::AggregateMaintenanceOutput {
            updates: grouped
                .changes
                .into_iter()
                .map(|(group, change)| crate::AggregateValueUpdate {
                    subscription: subscription_id,
                    consumer,
                    group: Some(group),
                    change,
                })
                .collect(),
            triggers: grouped
                .reads
                .into_iter()
                .map(|read| crate::reexec::ReExecutionTrigger {
                    subscription_id,
                    consumer_id: consumer,
                    read: crate::reexec::ReExecutionRead::GroupedScalar {
                        group: read.group,
                        query: read.query,
                        column_kinds: read.column_kinds,
                    },
                    checkpoint: read.checkpoint,
                })
                .collect(),
            transitions: Vec::new(),
            evaluation_failures: Vec::new(),
        })
    }
}

impl<E, I, DB> crate::Install<crate::GroupedScalarInstall<E::Backend, E::Checkpoint>>
    for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
{
    type Output = crate::AggregateMaintenanceOutput<I, E::Backend, E::Checkpoint>;
    type Error = crate::AggregateInstallError;

    fn install(
        &mut self,
        subscription_id: SubscriptionId,
        input: crate::GroupedScalarInstall<E::Backend, E::Checkpoint>,
    ) -> Result<Self::Output, Self::Error> {
        let group_limit = self.max_groups_per_aggregate;
        let asked = self.record_fence(input.fence.as_ref());
        let (consumer, installed) = {
            let entry = self.reexec.get_mut(&subscription_id).ok_or(
                crate::AggregateInstallError::UnknownAggregate(subscription_id),
            )?;
            let crate::reexec::maintain::QueryRuntime::Grouped(query) = &mut entry.runtime else {
                return Err(crate::AggregateInstallError::UnknownAggregate(
                    subscription_id,
                ));
            };
            (
                entry.consumer_id,
                query.install_group(
                    subscription_id,
                    &input.group,
                    &input.row,
                    input.fence,
                    input.checkpoint.as_ref(),
                    group_limit,
                    asked,
                ),
            )
        };
        let grouped = match installed {
            Ok(grouped) => grouped,
            Err(crate::AggregateInstallError::GroupLimit { .. }) => {
                return self.stopped_for_reason(
                    subscription_id,
                    super::GroupedStopTier::GroupedScalar,
                    crate::MaintenanceStopReason::GroupLimit { limit: group_limit },
                    input.checkpoint.as_ref(),
                );
            }
            Err(error) => return Err(error),
        };
        Ok(crate::AggregateMaintenanceOutput {
            updates: grouped
                .changes
                .into_iter()
                .map(|(group, change)| crate::AggregateValueUpdate {
                    subscription: subscription_id,
                    consumer,
                    group: Some(group),
                    change,
                })
                .collect(),
            triggers: grouped
                .reads
                .into_iter()
                .map(|read| crate::reexec::ReExecutionTrigger {
                    subscription_id,
                    consumer_id: consumer,
                    read: crate::reexec::ReExecutionRead::GroupedScalar {
                        group: read.group,
                        query: read.query,
                        column_kinds: read.column_kinds,
                    },
                    checkpoint: read.checkpoint,
                })
                .collect(),
            transitions: Vec::new(),
            evaluation_failures: Vec::new(),
        })
    }
}

impl<E, I, DB, C> crate::Install<crate::WholeRowsInstall<E::Backend, C>>
    for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    C: crate::Checkpoint,
    DB: DatabaseLike + 'static,
{
    type Output = Vec<crate::reexec::RowsUpdate<I, E::Backend, C>>;
    type Error = crate::InstallError;

    fn install(
        &mut self,
        subscription_id: SubscriptionId,
        input: crate::WholeRowsInstall<E::Backend, C>,
    ) -> Result<Self::Output, Self::Error> {
        let entry = self
            .reexec
            .get(&subscription_id)
            .ok_or(crate::InstallError::UnknownSubscription(subscription_id))?;
        if entry.tier != crate::ReadTier::WholeRows {
            return Err(crate::InstallError::WrongTier {
                subscription: subscription_id,
                input: "WholeRowsInstall",
            });
        }
        Ok(input
            .pages
            .into_iter()
            .map(|page| crate::reexec::RowsUpdate {
                subscription_id,
                consumer_id: entry.consumer_id,
                generation: input.generation,
                columns: page.columns,
                rows: page.rows,
                more: page.more,
                checkpoint: page.checkpoint,
            })
            .collect())
    }
}

impl<E, I, DB, C> crate::Install<crate::KeyedRowsInstall<E::Backend, C>>
    for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    C: crate::Checkpoint,
    DB: DatabaseLike + 'static,
{
    type Output = Vec<crate::reexec::RowDelta<I, E::Backend, C>>;
    type Error = crate::InstallError;

    fn install(
        &mut self,
        subscription_id: SubscriptionId,
        input: crate::KeyedRowsInstall<E::Backend, C>,
    ) -> Result<Self::Output, Self::Error> {
        let entry = self
            .reexec
            .get(&subscription_id)
            .ok_or(crate::InstallError::UnknownSubscription(subscription_id))?;
        if entry.tier != crate::ReadTier::KeyedRows {
            return Err(crate::InstallError::WrongTier {
                subscription: subscription_id,
                input: "KeyedRowsInstall",
            });
        }
        let crate::KeyedRowsInstall { columns, deltas } = input;
        // One shared allocation for every carried row, one for the removals.
        let columns: alloc::sync::Arc<[String]> = columns.into();
        let removed: alloc::sync::Arc<[String]> = alloc::sync::Arc::from(Vec::new());
        Ok(deltas
            .into_iter()
            .map(|delta| {
                let has_row = delta.row.is_some();
                crate::reexec::RowDelta {
                    subscription_id,
                    consumer_id: entry.consumer_id,
                    key: delta.key,
                    columns: if has_row {
                        alloc::sync::Arc::clone(&columns)
                    } else {
                        alloc::sync::Arc::clone(&removed)
                    },
                    row: delta.row,
                    checkpoint: delta.checkpoint,
                }
            })
            .collect())
    }
}

impl<E, I, DB> crate::Install<crate::AggregateSeedInstall<E::Backend, E::Checkpoint>>
    for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
{
    type Output = crate::AggregateMaintenanceOutput<I, E::Backend, E::Checkpoint>;
    type Error = crate::AggregateInstallError;

    fn install(
        &mut self,
        subscription_id: SubscriptionId,
        input: crate::AggregateSeedInstall<E::Backend, E::Checkpoint>,
    ) -> Result<Self::Output, Self::Error> {
        self.record_fence(input.fence.as_ref());
        if self.grouped_aggregates.contains_key(&subscription_id) {
            let installed = {
                let total = self
                    .grouped_aggregates
                    .get_mut(&subscription_id)
                    .expect("checked just above");
                let consumer = total.consumer();
                let result = total.install(
                    subscription_id,
                    total.group_columns(),
                    &input.rows,
                    input.fence,
                    self.max_changes_during_aggregate_read,
                    self.max_groups_per_aggregate,
                );
                (consumer, result)
            };
            let (consumer, opening) = match installed {
                (consumer, Ok(opening)) => (consumer, opening),
                (_, Err(error)) => {
                    let table_id = match &error {
                        crate::AggregateInstallError::GroupKeyUnencodable(_) => Some(
                            self.subscription_to_table
                                .get(&subscription_id)
                                .copied()
                                .expect("a grouped aggregate keeps its source table"),
                        ),
                        _ => None,
                    };
                    return self.stopped_for_group_limit(
                        subscription_id,
                        super::GroupedStopTier::Aggregate,
                        error,
                        self.max_groups_per_aggregate,
                        None,
                        table_id,
                    );
                }
            };
            return Ok(crate::AggregateMaintenanceOutput {
                updates: opening
                    .into_iter()
                    .map(|(group, change)| crate::AggregateValueUpdate {
                        subscription: subscription_id,
                        consumer,
                        group: Some(group),
                        change,
                    })
                    .collect(),
                triggers: Vec::new(),
                transitions: Vec::new(),
                evaluation_failures: Vec::new(),
            });
        }
        if input.rows.len() != 1 {
            return Err(crate::AggregateInstallError::RowCount {
                subscription: subscription_id,
                rows: input.rows.len(),
            });
        }
        let value =
            self.install_aggregate_rows_inner(subscription_id, &input.rows[0], input.fence)?;
        let consumer = self
            .aggregates
            .get(&subscription_id)
            .map(crate::runtime::aggregate::AggregateTotal::consumer)
            .ok_or(crate::AggregateInstallError::UnknownAggregate(
                subscription_id,
            ))?;
        Ok(crate::AggregateMaintenanceOutput {
            updates: vec![crate::AggregateValueUpdate {
                subscription: subscription_id,
                consumer,
                group: None,
                change: crate::AggregateValueChange::Set(crate::AggregateResultValue::Folded(
                    value,
                )),
            }],
            triggers: Vec::new(),
            transitions: Vec::new(),
            evaluation_failures: Vec::new(),
        })
    }
}

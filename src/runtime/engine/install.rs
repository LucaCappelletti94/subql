//! [`crate::Install`] implementations: how each read tier's database result
//! enters the engine.
//!
//! Split from `engine.rs` for size only: every impl was moved verbatim.
use super::{
    CdcEvent, DatabaseLike, IdTypes, SqlLiteralParse, SubscriptionEngine, SubscriptionId, Vec,
};
use alloc::string::String;

impl<E, I, DB, C> crate::Install<crate::ScalarInstall<E::Backend, C>>
    for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    C: crate::Checkpoint,
    DB: DatabaseLike + 'static,
{
    type Output = crate::reexec::ScalarUpdate<I, E::Backend, C>;
    type Error = crate::InstallError;

    fn install(
        &mut self,
        subscription_id: SubscriptionId,
        input: crate::ScalarInstall<E::Backend, C>,
    ) -> Result<Self::Output, Self::Error> {
        let entry = self
            .reexec
            .get_mut(&subscription_id)
            .ok_or(crate::InstallError::UnknownSubscription(subscription_id))?;
        if entry.tier != crate::ReadTier::Scalar {
            return Err(crate::InstallError::WrongTier {
                subscription: subscription_id,
                input: "ScalarInstall",
            });
        }
        entry.runtime.install(input.value.clone());
        Ok(crate::reexec::ScalarUpdate {
            subscription_id,
            consumer_id: entry.consumer_id,
            value: input.value,
            checkpoint: input.checkpoint,
        })
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
                    input.read_at.as_ref(),
                    pending_cap,
                    group_limit,
                ),
            )
        };
        let grouped = match installed {
            Ok(grouped) => grouped,
            Err(crate::AggregateInstallError::GroupLimit { .. }) => {
                return Self::stopped_output(
                    subscription_id,
                    self.transition_grouped_scalar_to_whole(
                        subscription_id,
                        crate::MaintenanceStopReason::GroupLimit { limit: group_limit },
                        input.read_at.as_ref(),
                    ),
                );
            }
            Err(crate::AggregateInstallError::GroupKeyUnencodable(_)) => {
                return Self::stopped_output(
                    subscription_id,
                    self.transition_grouped_scalar_to_whole(
                        subscription_id,
                        crate::MaintenanceStopReason::GroupKeyUnencodable { table_id },
                        input.read_at.as_ref(),
                    ),
                );
            }
            Err(error) => return Err(error),
        };
        let reason = if grouped.missing_group {
            Some(crate::MaintenanceStopReason::MissingOldRow { table_id })
        } else if grouped.group_limit {
            Some(crate::MaintenanceStopReason::GroupLimit { limit: group_limit })
        } else {
            None
        };
        if let Some(reason) = reason {
            return Self::stopped_output(
                subscription_id,
                self.transition_grouped_scalar_to_whole(
                    subscription_id,
                    reason,
                    input.read_at.as_ref(),
                ),
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
                query.install_group(subscription_id, &input.group, &input.row, group_limit),
            )
        };
        let change = match installed {
            Ok(change) => change,
            Err(crate::AggregateInstallError::GroupLimit { .. }) => {
                return Self::stopped_output(
                    subscription_id,
                    self.transition_grouped_scalar_to_whole(
                        subscription_id,
                        crate::MaintenanceStopReason::GroupLimit { limit: group_limit },
                        input.checkpoint.as_ref(),
                    ),
                );
            }
            Err(error) => return Err(error),
        };
        Ok(crate::AggregateMaintenanceOutput {
            updates: change
                .map(|(group, change)| crate::AggregateValueUpdate {
                    subscription: subscription_id,
                    consumer,
                    group: Some(group),
                    change,
                })
                .into_iter()
                .collect(),
            triggers: Vec::new(),
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
        if self.grouped_aggregates.contains_key(&subscription_id) {
            let read_at = input.read_at.clone();
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
                    input.read_at.as_ref(),
                    self.max_changes_during_aggregate_read,
                    self.max_groups_per_aggregate,
                );
                (consumer, result)
            };
            let (consumer, opening) = match installed {
                (consumer, Ok(opening)) => (consumer, opening),
                (_, Err(crate::AggregateInstallError::GroupLimit { .. })) => {
                    let limit = self.max_groups_per_aggregate;
                    return Self::stopped_output(
                        subscription_id,
                        self.transition_aggregate_to_whole(
                            subscription_id,
                            crate::MaintenanceStopReason::GroupLimit { limit },
                            read_at.as_ref(),
                        ),
                    );
                }
                (_, Err(crate::AggregateInstallError::GroupKeyUnencodable(_))) => {
                    let table_id = self
                        .subscription_to_table
                        .get(&subscription_id)
                        .copied()
                        .expect("a grouped aggregate keeps its source table");
                    return Self::stopped_output(
                        subscription_id,
                        self.transition_aggregate_to_whole(
                            subscription_id,
                            crate::MaintenanceStopReason::GroupKeyUnencodable { table_id },
                            read_at.as_ref(),
                        ),
                    );
                }
                (_, Err(error)) => return Err(error),
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
            self.install_aggregate_rows_inner(subscription_id, &input.rows[0], input.read_at)?;
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

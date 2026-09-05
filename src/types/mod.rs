//! Core type definitions for subql

mod domain_id_types;
mod generic_id_types;
mod subscription_types;
mod trait_definitions;

pub use domain_id_types::{ColumnId, EventKind, MergeJobId, ShardId, TableId};
pub use generic_id_types::{DefaultIds, Id, IdTypes, SubscriptionId, SubscriptionScope};
pub use subscription_types::{
    AggregateBootstrap, AggregateInstallError, AggregateSeedInstall, DropReason, DroppedRead,
    EvictionPolicy, GroupedScalarInstall, GroupedScalarSeedInstall, Install, InstallError,
    InstalledPage, InstalledRowDelta, KeyedRowsInstall, NotServed, PerConsumerDatabaseReads,
    ReadTier, Registered, RegistrationRequest, RestoredRead, RestoredReads, ScalarInstall, Served,
    StatedTermValues, SubscriptionMetadata, SubscriptionRequest, SubscriptionsView, Tier, TierKind,
    UnregisterReport, WholeRowsInstall,
};
#[cfg(feature = "std")]
pub use subscription_types::{DurabilityMode, MergeReport};
pub use trait_definitions::{
    AggValue, AggregateDispatch, AggregateMaintenanceOutput, AggregateResultValue,
    AggregateValueChange, AggregateValueUpdate, AsyncSubscriptionDispatch, ConsumerNotifications,
    ConsumerNotificationsIter, DispatchOutput, EvaluationFailure, GroupIdentity,
    MaintenanceStopReason, MaintenanceTransition, SubscriptionDispatch, SubscriptionRegistration,
    SubscriptionUnregistration, SumValue, TermNarrowing, UnansweredCell,
};
#[cfg(feature = "std")]
pub use trait_definitions::{DurableShardMerge, DurableShardStore};

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::generic_id_types::{DefaultIds, SubscriptionScope};
    use super::subscription_types::{SubscriptionMetadata, SubscriptionsView};

    #[test]
    fn subscription_scope_wire_compatible_with_option() {
        let none_bytes = crate::persistence::codec::serialize(&Option::<u64>::None).unwrap();
        let durable_bytes =
            crate::persistence::codec::serialize(&SubscriptionScope::<DefaultIds>::Durable)
                .unwrap();
        assert_eq!(none_bytes, durable_bytes);

        let some_bytes = crate::persistence::codec::serialize(&Some(42u64)).unwrap();
        let session_bytes =
            crate::persistence::codec::serialize(&SubscriptionScope::<DefaultIds>::Session(42))
                .unwrap();
        assert_eq!(some_bytes, session_bytes);
    }

    #[test]
    fn subscriptions_view_iterates_metadata_entries() {
        let entries: alloc::vec::Vec<SubscriptionMetadata<DefaultIds>> = alloc::vec![
            SubscriptionMetadata::new(1, 10, SubscriptionScope::Durable, None, 0),
            SubscriptionMetadata::new(2, 20, SubscriptionScope::Session(99), Some(1_000_000), 4),
        ];

        let view = SubscriptionsView::<DefaultIds>::new(&entries);
        assert_eq!(view.len(), 2);
        assert!(!view.is_empty());

        let collected: alloc::vec::Vec<u64> = view.iter().map(|m| m.subscription_id).collect();
        assert_eq!(collected, alloc::vec![1, 2]);

        let coldest = view
            .iter()
            .min_by_key(|m| m.dispatch_count)
            .map(|m| m.subscription_id);
        assert_eq!(coldest, Some(1));

        let least_active = view
            .iter()
            .min_by_key(|m| m.last_dispatch_at.unwrap_or(0))
            .map(|m| m.subscription_id);
        assert_eq!(least_active, Some(1));

        let session_count = view
            .iter()
            .filter(|m| matches!(m.scope, SubscriptionScope::Session(_)))
            .count();
        assert_eq!(session_count, 1);
    }
}

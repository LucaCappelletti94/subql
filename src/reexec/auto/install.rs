//! Install and dispatch plumbing shared by both modes.

use super::{
    AutoResolvingEngine, Backend, CdcEvent, DatabaseLike, IdTypes, ResolverMode, SqlLiteralParse,
    SubscriptionId,
};

/// Exposes the tier transitions an install produced, so the facade can apply
/// them to its own per-subscription context. Most install shapes carry none.
/// A grouped-aggregate seed that overflows the group budget demotes at install
/// time and carries the demotion here, which the facade must apply or a later
/// snapshot mistakes the demoted subscription for a still-folding aggregate.
pub trait InstallOutputTransitions<B: crate::backend::Backend> {
    fn transitions(&self) -> &[crate::MaintenanceTransition<B>];
}

impl<I: IdTypes, B: crate::backend::Backend, C: crate::Checkpoint> InstallOutputTransitions<B>
    for crate::AggregateMaintenanceOutput<I, B, C>
{
    fn transitions(&self) -> &[crate::MaintenanceTransition<B>] {
        &self.transitions
    }
}

impl<I: IdTypes, B: crate::backend::Backend, C: crate::Checkpoint> InstallOutputTransitions<B>
    for crate::reexec::ScalarUpdate<I, B, C>
{
    fn transitions(&self) -> &[crate::MaintenanceTransition<B>] {
        &[]
    }
}

impl<I: IdTypes, B: crate::backend::Backend, C: crate::Checkpoint> InstallOutputTransitions<B>
    for alloc::vec::Vec<crate::reexec::RowsUpdate<I, B, C>>
{
    fn transitions(&self) -> &[crate::MaintenanceTransition<B>] {
        &[]
    }
}

impl<I: IdTypes, B: crate::backend::Backend, C: crate::Checkpoint> InstallOutputTransitions<B>
    for alloc::vec::Vec<crate::reexec::RowDelta<I, B, C>>
{
    fn transitions(&self) -> &[crate::MaintenanceTransition<B>] {
        &[]
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
    <crate::SubscriptionEngine<E, I, DB> as crate::Install<T>>::Output:
        InstallOutputTransitions<E::Backend>,
{
    type Output = <crate::SubscriptionEngine<E, I, DB> as crate::Install<T>>::Output;
    type Error = <crate::SubscriptionEngine<E, I, DB> as crate::Install<T>>::Error;

    fn install(
        &mut self,
        subscription_id: SubscriptionId,
        input: T,
    ) -> Result<Self::Output, Self::Error> {
        let output = crate::Install::install(&mut self.inner, subscription_id, input)?;
        self.apply_transitions(output.transitions());
        Ok(output)
    }
}

impl<E, I, DB, M> crate::SubscriptionDispatch<I, E> for AutoResolvingEngine<E, I, DB, M>
where
    E: CdcEvent + Send,
    E::Backend: SqlLiteralParse,
    <E::Backend as Backend>::Dialect: Send + Sync,
    I: IdTypes,
    DB: DatabaseLike + Send + 'static,
    M: ResolverMode<E::Backend> + Send,
    M::AuthContext: Send,
{
    type Notifications = crate::reexec::Dispatched<I, E::Backend, E::Checkpoint>;
    type Error = crate::DispatchError;

    fn consumers(&mut self, event: &E) -> Result<Self::Notifications, Self::Error> {
        self.apply(event)
    }
}

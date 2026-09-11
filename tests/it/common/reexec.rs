//! Registration scaffold shared by the Docker-backed re-execution suites.

use sql_traits::structs::ParserDB;
use subql::backend::CdcEvent;
use subql::compiler::SqlLiteralParse;
use subql::reexec::{AutoResolvingEngine, ResolverMode};
use subql::{DefaultIds, Registered, SubscriptionId, SubscriptionRequest, Tier};

/// Register `sql` for `consumer` and assert it landed on the scalar
/// re-execution tier.
///
/// Mode-independent: `register` is synchronous in both modes and every
/// connector in these suites uses `AuthContext = ()`, so `M` is passive.
pub fn register_captured<E, M>(
    engine: &mut AutoResolvingEngine<E, DefaultIds, ParserDB, M>,
    consumer: u64,
    sql: &str,
) -> SubscriptionId
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse + core::fmt::Debug,
    M: ResolverMode<E::Backend, AuthContext = ()>,
{
    match engine
        .register(SubscriptionRequest::new(consumer, sql), ())
        .expect("captured registration")
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected the scalar re-execution tier for {sql}, got {other:?}"),
    }
}

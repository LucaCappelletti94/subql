//! Generic ID traits and the subscription scope type.

use core::fmt::Debug;
use core::hash::Hash;
use serde::{de::DeserializeOwned, Serialize};

/// Marker trait for ID types used in the subscription engine.
///
/// Any type satisfying these bounds can be used as a consumer, session, or
/// subscription identifier.
pub trait Id:
    Copy + Ord + Hash + Debug + Send + Sync + Serialize + DeserializeOwned + 'static
{
}

/// Blanket implementation: every type meeting the bounds is automatically an `Id`.
impl<T: Copy + Ord + Hash + Debug + Send + Sync + Serialize + DeserializeOwned + 'static> Id for T {}

/// Engine-assigned subscription identifier (always `u64`).
pub type SubscriptionId = u64;

/// Associated types that pin the consumer-facing ID representations.
///
/// `SubscriptionId` is always `u64` and auto-assigned by the engine.
pub trait IdTypes: 'static {
    /// Consumer identifier (globally unique)
    type ConsumerId: Id;
    /// Session identifier (per-connection)
    type SessionId: Id;
}

/// Default ID configuration using `u64` for consumer and session identifiers.
#[derive(Debug)]
pub struct DefaultIds;

impl IdTypes for DefaultIds {
    type ConsumerId = u64;
    type SessionId = u64;
}

/// Lifetime scope of a subscription.
///
/// Wire-compatible with `Option<SessionId>` under postcard's positional
/// encoding: `Durable` = variant 0 = `None`, `Session(id)` = variant 1 =
/// `Some(id)`.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound = "")]
pub enum SubscriptionScope<I: IdTypes> {
    /// Persists until explicitly unregistered.
    Durable,
    /// Bound to a session. Auto-removed when the session ends.
    Session(I::SessionId),
}

// Manual impls avoid derived bounds that would require `I: Copy/Clone/Debug/...`.
// Only `I::SessionId` (already `Copy + Debug + Hash + ...` via `Id`) is needed.
impl<I: IdTypes> Copy for SubscriptionScope<I> {}
impl<I: IdTypes> Clone for SubscriptionScope<I> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<I: IdTypes> core::fmt::Debug for SubscriptionScope<I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Durable => write!(f, "Durable"),
            Self::Session(id) => f.debug_tuple("Session").field(id).finish(),
        }
    }
}
impl<I: IdTypes> PartialEq for SubscriptionScope<I> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Durable, Self::Durable) => true,
            (Self::Session(a), Self::Session(b)) => a == b,
            _ => false,
        }
    }
}
impl<I: IdTypes> Eq for SubscriptionScope<I> {}
impl<I: IdTypes> core::hash::Hash for SubscriptionScope<I> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        if let Self::Session(id) = self {
            id.hash(state);
        }
    }
}

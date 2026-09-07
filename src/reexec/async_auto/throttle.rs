//! The persistent concurrency cap on in-flight reads.

use super::{Arc, AtomicUsize, Ordering, Semaphore, SemaphoreGuardArc, SubscriptionId};

/// Internal state for the persistent re-execution concurrency cap.
///
/// The cap is enforced by an [`async_lock::Semaphore`], which is what orders
/// the reads. The [`AtomicUsize`] beside it publishes no data and guards
/// nothing: it counts held permits so a caller can read
/// [`AutoResolvingEngine::inflight`] without deriving it from the semaphore,
/// which is why every access to it is `Relaxed`.
pub struct ThrottleState {
    pub sem: Arc<Semaphore>,
    pub inflight: Arc<AtomicUsize>,
    pub cap: usize,
}

/// RAII guard returned by the throttle's acquire path. Releases the
/// semaphore permit and decrements the inflight counter on drop, even
/// when dropped because the awaiting future was cancelled.
pub struct InflightGuard {
    pub inflight: Arc<AtomicUsize>,
    pub _permit: SemaphoreGuardArc,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Acquire a throttle permit if a cap is configured. Returns `None`
/// when no cap is set (the unbounded case), avoiding any
/// synchronisation overhead in that path. The returned guard releases
/// the permit on drop, which makes the call cancellation-safe: a
/// future that is dropped while awaiting `acquire_permit` (because the
/// outer `try_collect` short-circuited on a connector error or because
/// the caller cancelled) leaves the semaphore in a clean state.
pub async fn acquire_permit(
    throttle: Option<&(Arc<Semaphore>, Arc<AtomicUsize>)>,
    subscription_id: SubscriptionId,
) -> Option<InflightGuard> {
    let Some((sem, inflight)) = throttle else {
        let _ = subscription_id;
        return None;
    };
    let permit = Arc::clone(sem).acquire_arc().await;
    let now = inflight.fetch_add(1, Ordering::Relaxed) + 1;
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

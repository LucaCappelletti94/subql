//! Polling-based Postgres CDC source.
//!
//! # When to use polling vs. push
//!
//! The default and recommended CDC intake is the push-based
//! [`crate::PgStreamingCdcSource`], which delivers events at the
//! wire-RTT floor (~5 ms) the moment Postgres flushes them.
//!
//! [`PollingPgCdcSource`] is the polling alternative. It implements
//! the same [`crate::CdcSource`] trait so consumer code is
//! interchangeable. Use it deliberately when:
//!
//! - **Operational constraint** prevents a long-lived replication
//!   connection (edge environments, TCP-hostile networks, libpq-only
//!   downstreams that can't speak `START_REPLICATION`).
//! - **Equivalence ground truth**: the differential equivalence test
//!   in `tests/cdc_equivalence.rs` drains both transports against the
//!   same WAL stream and asserts they observe identical events.
//! - **Benchmark subject**: the workload-matrix examples measure both
//!   transports across regimes.
//!
//! # Latency characteristics
//!
//! Polling adds roughly `poll_interval / 2` average latency on top of
//! the wire RTT. At 100 ms polling cadence this is ~50 ms per event.
//!
//! # Ack semantics differ from push
//!
//! Polling uses `pg_logical_slot_get_binary_changes`, which
//! auto-advances the slot's `confirmed_flush_lsn` as a side effect of
//! the drain. Consequently, the `ack` method on the [`CdcSource`] impl
//! for [`PollingPgCdcSource`] is a **no-op**.
//!
//! [`CdcSource`]: crate::CdcSource

use alloc::format;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;

use pg_walstream::error::ReplicationError;
use pg_walstream::{parse_lsn, ChangeEvent, Lsn, PgOutputDecoder, PgReplicationConnection};
use sql_traits::prelude::DatabaseLike;

use crate::wal::into_engine_events;
use crate::PgLsn;

/// Configuration for a [`PollingPgCdcSource`].
///
/// The caller is responsible for creating the publication and the
/// replication slot before constructing the source. The slot must use
/// the `pgoutput` plugin (same as push). The polling source drains it
/// via `pg_logical_slot_get_binary_changes` rather than
/// `START_REPLICATION`.
///
/// `#[non_exhaustive]` so future fields can be added without breaking
/// downstream call sites. Construct via [`PollingPgCdcConfig::new`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PollingPgCdcConfig {
    /// libpq connection string. Polling uses the regular SQL transport,
    /// so `replication=database` is NOT required.
    pub url: String,
    /// Name of the pre-created pgoutput logical replication slot.
    pub slot_name: String,
    /// Name of the publication the slot follows.
    pub publication_name: String,
    /// Cadence at which the inner task issues
    /// `pg_logical_slot_get_binary_changes`. Default 100 ms.
    pub poll_interval: Duration,
    /// Bounded back-pressure ceiling for the internal event channel.
    pub buffer_capacity: usize,
}

impl PollingPgCdcConfig {
    /// Build a config with sensible defaults: `poll_interval = 100 ms`,
    /// `buffer_capacity = 1024`.
    #[must_use]
    pub fn new(
        url: impl Into<String>,
        slot_name: impl Into<String>,
        publication_name: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            slot_name: slot_name.into(),
            publication_name: publication_name.into(),
            poll_interval: Duration::from_millis(100),
            buffer_capacity: 1024,
        }
    }

    /// Override the polling cadence (default 100 ms).
    #[must_use]
    pub const fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Override the internal event-channel ceiling (default 1024).
    #[must_use]
    pub const fn buffer_capacity(mut self, buffer_capacity: usize) -> Self {
        self.buffer_capacity = buffer_capacity;
        self
    }
}

/// Errors surfaced by [`PollingPgCdcSource`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PollingPgCdcError {
    /// Underlying `pg_walstream` failure (transport, auth, server error).
    #[error("postgres error: {0}")]
    Postgres(#[from] ReplicationError),
    /// Unexpected protocol shape (e.g. `pg_logical_slot_get_binary_changes`
    /// returned an unparseable row).
    #[error("polling protocol error: {0}")]
    Protocol(String),
    /// The inner polling task has exited, so the source can no longer
    /// produce events.
    #[error("polling source shut down")]
    SourceClosed,
}

/// Polling-based Postgres CDC source. See the module-level docs for
/// when to choose polling over the default push source.
pub struct PollingPgCdcSource {
    config: PollingPgCdcConfig,
    event_rx: tokio::sync::mpsc::Receiver<Result<ChangeEvent, PollingPgCdcError>>,
    polls_issued: Arc<AtomicU64>,
    events_received: Arc<AtomicU64>,
    empty_polls_observed: Arc<AtomicU64>,
    total_drained_events: Arc<AtomicU64>,
    non_empty_drains: Arc<AtomicU64>,
    /// Cooperative shutdown signal. Polled at the top of each polling
    /// iteration. The drop guard also calls `abort()` on the handle as a
    /// backstop for the `JoinHandle` slot, although a blocking task's
    /// underlying OS thread is not actually killed by `abort()`.
    shutdown: Arc<AtomicBool>,
    /// Set to `true` by an in-task drop guard on any exit path.
    task_exited: Arc<AtomicBool>,
    /// Inner-task join handle.
    task: tokio::task::JoinHandle<()>,
}

impl PollingPgCdcSource {
    /// Open a regular SQL connection to Postgres, validate the slot
    /// exists, and spawn the inner task that drains the slot at the
    /// configured cadence.
    /// `catalog` is retained for API stability and is currently unused.
    /// The source yields raw `ChangeEvent`s that the engine resolves
    /// against its own catalog at dispatch time.
    #[allow(clippy::needless_pass_by_value)]
    pub async fn connect<DB: DatabaseLike + 'static>(
        config: PollingPgCdcConfig,
        _catalog: DB,
    ) -> Result<Self, PollingPgCdcError> {
        let slot_name_for_check = config.slot_name.clone();
        let url = config.url.clone();
        // Connect and slot-check are both synchronous on `pg_walstream`'s
        // libpq surface and would block the calling tokio worker, so
        // bounce both through the blocking pool in a single hop.
        // pg_walstream's `exec` has no bind-parameter support, so we
        // embed the slot name. PG validates slot names to `[a-z0-9_]`
        // and we quote-escape for defense in depth.
        let slot_check_sql = format!(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = {}",
            sql_string_literal(&slot_name_for_check),
        );
        let conn = tokio::task::spawn_blocking(move || -> Result<_, PollingPgCdcError> {
            let mut conn = PgReplicationConnection::connect(&url)?;
            let slot_check = conn.exec(&slot_check_sql)?;
            if slot_check.ntuples() == 0 {
                return Err(PollingPgCdcError::Protocol(format!(
                    "replication slot `{slot_name_for_check}` does not exist; the polling source \
                     will not auto-create slots"
                )));
            }
            Ok(conn)
        })
        .await
        .map_err(|e| PollingPgCdcError::Protocol(format!("connection task panicked: {e}")))??;

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(config.buffer_capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        let polls_issued = Arc::new(AtomicU64::new(0));
        let events_received = Arc::new(AtomicU64::new(0));
        let empty_polls_observed = Arc::new(AtomicU64::new(0));
        let total_drained_events = Arc::new(AtomicU64::new(0));
        let non_empty_drains = Arc::new(AtomicU64::new(0));
        let task_exited = Arc::new(AtomicBool::new(false));

        let task_slot = config.slot_name.clone();
        let task_publication = config.publication_name.clone();
        let task_interval = config.poll_interval;
        let task_shutdown = Arc::clone(&shutdown);
        let task_polls = Arc::clone(&polls_issued);
        let task_events = Arc::clone(&events_received);
        let task_empty = Arc::clone(&empty_polls_observed);
        let task_total = Arc::clone(&total_drained_events);
        let task_non_empty = Arc::clone(&non_empty_drains);
        let task_exited_for_task = Arc::clone(&task_exited);

        let task = tokio::task::spawn_blocking(move || {
            polling_loop(
                conn,
                task_slot,
                task_publication,
                task_interval,
                event_tx,
                task_polls,
                task_events,
                task_empty,
                task_total,
                task_non_empty,
                task_shutdown,
                task_exited_for_task,
            );
        });

        Ok(Self {
            config,
            event_rx,
            polls_issued,
            events_received,
            empty_polls_observed,
            total_drained_events,
            non_empty_drains,
            shutdown,
            task_exited,
            task,
        })
    }

    /// Borrow the configuration the source was built with.
    #[must_use]
    pub const fn config(&self) -> &PollingPgCdcConfig {
        &self.config
    }

    /// Cumulative number of `pg_logical_slot_get_binary_changes`
    /// queries the inner task has issued since `connect`.
    #[must_use]
    pub fn polls_issued(&self) -> u64 {
        self.polls_issued.load(Ordering::Relaxed)
    }

    /// Cumulative number of typed events the inner task has pushed onto
    /// the consumer channel.
    #[must_use]
    pub fn events_received(&self) -> u64 {
        self.events_received.load(Ordering::Relaxed)
    }

    /// Cumulative number of polls that returned zero events.
    #[must_use]
    pub fn empty_polls_observed(&self) -> u64 {
        self.empty_polls_observed.load(Ordering::Relaxed)
    }

    /// Average number of events returned per non-empty drain. `0.0` if
    /// no non-empty drains have happened yet.
    ///
    /// Never below `1.0` once any event has been delivered. Both counters
    /// move with the event that produces them, so a consumer holding an
    /// event cannot read an average that has not counted it yet.
    #[must_use]
    pub fn average_drain_batch_size(&self) -> f64 {
        let total = self.total_drained_events.load(Ordering::Relaxed);
        let drains = self.non_empty_drains.load(Ordering::Relaxed);
        if drains == 0 {
            0.0
        } else {
            #[allow(clippy::cast_precision_loss)]
            {
                total as f64 / drains as f64
            }
        }
    }

    /// `true` once the inner task has exited.
    #[must_use]
    pub fn task_exited(&self) -> bool {
        self.task_exited.load(Ordering::Relaxed)
    }

    /// Clone the `task_exited` flag so callers can observe the inner
    /// task's exit even after the source itself has been dropped.
    #[must_use]
    pub fn task_exited_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.task_exited)
    }
}

impl Drop for PollingPgCdcSource {
    fn drop(&mut self) {
        // Cooperative shutdown: the inner blocking loop polls this flag
        // between iterations. `abort()` does not interrupt a running
        // blocking thread, so the actual exit happens on the next
        // iteration boundary (at most one `poll_interval` away).
        self.shutdown.store(true, Ordering::Relaxed);
        self.task.abort();
    }
}

// ============================================================================
// Inner polling loop (runs on the tokio blocking pool)
// ============================================================================

// All `Arc` arguments are consumed by the loop's `Drop`-on-exit guard
// or held for the loop's lifetime, so `clippy::needless_pass_by_value`
// would only push us toward `Arc::clone` at every call site without a
// real readability win.
#[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
fn polling_loop(
    mut conn: PgReplicationConnection,
    slot_name: String,
    publication_name: String,
    poll_interval: Duration,
    event_tx: tokio::sync::mpsc::Sender<Result<ChangeEvent, PollingPgCdcError>>,
    polls_issued: Arc<AtomicU64>,
    events_received: Arc<AtomicU64>,
    empty_polls_observed: Arc<AtomicU64>,
    total_drained_events: Arc<AtomicU64>,
    non_empty_drains: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    task_exited: Arc<AtomicBool>,
) {
    struct ExitGuard(Arc<AtomicBool>);
    impl Drop for ExitGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
    let _exit_guard = ExitGuard(task_exited);

    let mut decoder = PgOutputDecoder::with_protocol_version(1);

    // pg_walstream's `exec` uses libpq's `PQexec` which returns all
    // columns in text format, so we cannot fetch raw BYTEA bytes
    // directly. Wrap the column in `encode(data, 'hex')` so the
    // returned text is lowercase hex without a leading `\x` prefix,
    // then hex-decode in Rust. A future `exec_with_params` upstream
    // would let us request binary result format and drop this round
    // trip entirely.
    let query = format!(
        "SELECT lsn::text, encode(data, 'hex') FROM pg_logical_slot_get_binary_changes(\
            {slot}, NULL, NULL, \
            'proto_version', '1', \
            'publication_names', {pub_name}\
        )",
        slot = sql_string_literal(&slot_name),
        pub_name = sql_string_literal(&publication_name),
    );

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        std::thread::sleep(poll_interval);
        if shutdown.load(Ordering::Relaxed) {
            return;
        }

        polls_issued.fetch_add(1, Ordering::Relaxed);
        let result = match conn.exec(&query) {
            Ok(r) => r,
            Err(e) => {
                let _ = event_tx.blocking_send(Err(PollingPgCdcError::Postgres(e)));
                return;
            }
        };

        if result.ntuples() == 0 {
            empty_polls_observed.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        // Counted before the send, so a consumer that has the event has the
        // counters that describe it. A drain is counted non-empty by its first
        // event rather than at the end, which keeps the average at or above one
        // for as long as any event has been delivered.
        let mut drain_counted = false;

        for row_idx in 0..result.ntuples() {
            let Some(lsn_text) = result.get_value(row_idx, 0) else {
                continue;
            };
            let Some(hex_text) = result.get_value(row_idx, 1) else {
                continue;
            };
            let bytes = match hex_decode(hex_text.as_bytes()) {
                Ok(b) => b,
                Err(e) => {
                    let _ = event_tx.blocking_send(Err(PollingPgCdcError::Protocol(e)));
                    return;
                }
            };
            let lsn = match parse_lsn(&lsn_text) {
                Ok(v) => Lsn::new(v),
                Err(e) => {
                    let _ = event_tx.blocking_send(Err(PollingPgCdcError::Postgres(e)));
                    return;
                }
            };
            let change = match decoder.decode_message(bytes, lsn) {
                Ok(Some(c)) => c,
                Ok(None) => continue,
                Err(e) => {
                    let _ = event_tx.blocking_send(Err(PollingPgCdcError::Postgres(e)));
                    return;
                }
            };
            for ev in into_engine_events(change) {
                events_received.fetch_add(1, Ordering::Relaxed);
                total_drained_events.fetch_add(1, Ordering::Relaxed);
                if !drain_counted {
                    drain_counted = true;
                    non_empty_drains.fetch_add(1, Ordering::Relaxed);
                }
                if event_tx.blocking_send(Ok(ev)).is_err() {
                    return;
                }
            }
        }
    }
}

impl crate::CdcSource for PollingPgCdcSource {
    type Event = ChangeEvent;
    type Error = PollingPgCdcError;

    #[allow(clippy::manual_async_fn)]
    fn next_event(
        &mut self,
    ) -> impl core::future::Future<Output = Result<Option<Self::Event>, Self::Error>> + Send {
        async move {
            match self.event_rx.recv().await {
                Some(Ok(ev)) => Ok(Some(ev)),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            }
        }
    }

    #[allow(clippy::manual_async_fn, clippy::unused_async)]
    fn ack(
        &mut self,
        _upto: PgLsn,
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send {
        // No-op: `pg_logical_slot_get_binary_changes` auto-advances the
        // slot's `confirmed_flush_lsn` as a side effect of the drain.
        async move { Ok(()) }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Escape a string for embedding as a single-quoted SQL literal.
fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Decode lowercase ASCII hex bytes into raw bytes. Rejects odd-length
/// or non-hex inputs with a structured error message.
fn hex_decode(bytes: &[u8]) -> Result<Vec<u8>, String> {
    if !bytes.len().is_multiple_of(2) {
        return Err(format!(
            "encode(data, 'hex') returned odd-length output ({} chars)",
            bytes.len()
        ));
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for i in 0..bytes.len() / 2 {
        let hi = hex_nibble(bytes[2 * i])?;
        let lo = hex_nibble(bytes[2 * i + 1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        other => Err(format!("invalid hex digit 0x{other:02X}")),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn hex_decode_round_trip() {
        let bytes = [0x00, 0xFF, 0x42, 0xc0];
        let mut hex = Vec::with_capacity(bytes.len() * 2);
        for b in bytes {
            hex.push(b"0123456789abcdef"[usize::from(b >> 4)]);
            hex.push(b"0123456789abcdef"[usize::from(b & 0x0F)]);
        }
        assert_eq!(hex_decode(&hex).unwrap(), bytes);
    }

    #[test]
    fn hex_decode_rejects_odd_length() {
        assert!(hex_decode(b"abc").is_err());
    }

    #[test]
    fn hex_decode_rejects_non_hex() {
        assert!(hex_decode(b"zz").is_err());
    }

    #[test]
    fn sql_string_literal_escapes_quotes() {
        assert_eq!(sql_string_literal("foo'bar"), "'foo''bar'");
        assert_eq!(sql_string_literal("ok_slot"), "'ok_slot'");
    }
}

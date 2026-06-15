//! Push-based Postgres CDC source driven by `tokio-postgres`.
//!
//! Owns a replication-mode `tokio_postgres::Client` and an attached
//! `START_REPLICATION` stream; surfaces typed [`crate::WalEvent<PgLsn>`]
//! values through the [`crate::CdcSource`] trait. Acks are sent inline
//! to the upstream server as `StandbyStatusUpdate` messages so the
//! slot's `confirmed_flush_lsn` tracks reality.
//!
//! Internal architecture is two pieces communicating via channels:
//!
//! - **Inner task** (spawned at `connect`): owns the
//!   [`tokio_postgres::Client`] and the [`tokio_postgres::CopyBothDuplex`]
//!   stream. Reads inbound `XLogData` / `PrimaryKeepalive` frames,
//!   parses pgoutput payloads with [`crate::PgOutputParser`], and
//!   pushes typed events into an `mpsc::channel`. Honors the
//!   `reply_requested` bit on inbound keepalives by emitting an
//!   inline `StandbyStatusUpdate`.
//! - **Source struct** (consumer-side): holds only the channel
//!   receiver. [`Self::next_event`] is `recv()` on the receiver;
//!   [`Self::ack`] is currently a no-op (Step 4 wires this to the
//!   inner task via a side channel).
//!
//! Step 3 implements the minimal viable streaming path: one event
//! through, latency wire-bound. Subsequent steps add the explicit ack
//! channel (Step 4), the periodic status-update pump (Step 5),
//! bounded back-pressure (Step 6), and graceful shutdown (Step 7).

use alloc::format;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use core::str::FromStr;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use sql_traits::prelude::DatabaseLike;
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::{Client, Config as PgClientConfig, CopyBothDuplex, NoTls, SimpleQueryMessage};

use super::pgoutput::PgOutputParser;
use super::{WalParseError, WalParser};
use crate::{PgLsn, WalEvent};

/// Configuration for a [`PgStreamingCdcSource`].
///
/// The caller is responsible for creating the publication and the
/// replication slot before constructing the source; `connect` does
/// not auto-create either.
///
/// `#[non_exhaustive]` so future fields can be added without breaking
/// downstream call sites. Construct via [`PgStreamingConfig::new`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PgStreamingConfig {
    /// libpq connection string (URL form or key=value form). The source
    /// puts the client into logical-replication mode programmatically;
    /// callers do NOT need to include `replication=database` (and
    /// `tokio-postgres`' URL parser does not accept it as a query
    /// param anyway).
    pub url: String,
    /// Name of the pre-created logical replication slot
    /// (`pg_create_logical_replication_slot(slot, 'pgoutput')`).
    pub slot_name: String,
    /// Name of the publication the slot should follow
    /// (`CREATE PUBLICATION pub FOR TABLE ...`).
    pub publication_name: String,
    /// Optional resume position. `None` starts from the slot's current
    /// `confirmed_flush_lsn`.
    pub start_lsn: Option<PgLsn>,
    /// Cadence at which the source sends `StandbyStatusUpdate` ack
    /// messages even without explicit consumer acks. Must be shorter
    /// than the server's `wal_sender_timeout` (default 60s).
    pub status_interval: Duration,
    /// Bounded back-pressure ceiling for the internal event channel.
    /// When the consumer is slow, the source stops reading from the
    /// underlying socket once this many events are queued.
    pub buffer_capacity: usize,
}

impl PgStreamingConfig {
    /// Build a config with sensible defaults for the optional fields:
    /// `start_lsn = None` (resume from the slot's current position),
    /// `status_interval = 10s`, `buffer_capacity = 1024`.
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
            start_lsn: None,
            status_interval: Duration::from_secs(10),
            buffer_capacity: 1024,
        }
    }

    /// Override the resume position. `None` means "start from the
    /// slot's current `confirmed_flush_lsn`".
    #[must_use]
    pub const fn start_lsn(mut self, start_lsn: Option<PgLsn>) -> Self {
        self.start_lsn = start_lsn;
        self
    }

    /// Override the periodic ack cadence (default 10s).
    #[must_use]
    pub const fn status_interval(mut self, status_interval: Duration) -> Self {
        self.status_interval = status_interval;
        self
    }

    /// Override the internal event-channel ceiling (default 1024).
    #[must_use]
    pub const fn buffer_capacity(mut self, buffer_capacity: usize) -> Self {
        self.buffer_capacity = buffer_capacity;
        self
    }
}

/// Errors surfaced by [`PgStreamingCdcSource`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PgStreamingError {
    /// Underlying `tokio-postgres` failure (transport, auth, server
    /// error, etc.).
    #[error("postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    /// The pgoutput payload could not be parsed.
    #[error("pgoutput parse error: {0}")]
    Parse(#[from] WalParseError),
    /// The server's response to a replication command did not match
    /// what the spec mandates (e.g. `IDENTIFY_SYSTEM` returned no row,
    /// truncated `XLogData` header, etc.).
    #[error("replication protocol error: {0}")]
    Protocol(String),
    /// The inner streaming task ended (channel closed). Either the
    /// upstream connection was lost or the source was dropped.
    #[error("streaming source shut down")]
    SourceClosed,
}

/// Push-based Postgres CDC source.
///
/// Construct with [`PgStreamingCdcSource::connect`]; once constructed,
/// the source implements [`crate::CdcSource`] and the consumer drives
/// the `next_event` / `ack` loop. See the trait docs for the lifecycle
/// contract.
pub struct PgStreamingCdcSource<DB: DatabaseLike> {
    config: PgStreamingConfig,
    catalog: Arc<DB>,
    event_rx: tokio::sync::mpsc::Receiver<Result<WalEvent<PgLsn>, PgStreamingError>>,
    /// Caller-side ack channel. `ack(upto)` pushes onto this; the inner
    /// task receives and surfaces an immediate `StandbyStatusUpdate` to
    /// the server.
    ack_tx: tokio::sync::mpsc::UnboundedSender<PgLsn>,
    /// Cumulative count of `StandbyStatusUpdate` messages the inner
    /// task has emitted since `connect`. Bumped on every emission:
    /// periodic-pump ticks, explicit `ack` calls, and keepalive
    /// auto-replies. Exposed via [`Self::status_updates_sent`] for
    /// production observability and to assert pump cadence in tests.
    status_updates_sent: Arc<AtomicU64>,
    /// Cumulative count of `WalEvent`s the inner task has pushed onto
    /// the consumer-facing channel since `connect`. Mirrors the
    /// equivalent counter on `PollingPgCdcSource`; consumed by
    /// generic benchmark code that drives any `CdcSource` impl.
    events_received: Arc<AtomicU64>,
    /// Cooperative shutdown signal. Held in an `Option` so the `Drop`
    /// impl can `take()` and explicitly drop the sender — that fires
    /// the corresponding `oneshot::Receiver` in the inner task, which
    /// is polled first in the task's `select!` and causes the task
    /// to break out cleanly within one iteration.
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    /// Set to `true` by an in-task drop guard when the inner task
    /// exits via any path (cooperative shutdown, abort, panic).
    /// Useful for production observability ("did the source stop?")
    /// and for graceful-shutdown tests.
    task_exited: Arc<AtomicBool>,
    /// Inner-task join handle. Kept so `Drop` can call `.abort()` as
    /// a backstop in case the cooperative signal does not propagate
    /// in time (e.g. the task is stuck inside a long await point).
    task: tokio::task::JoinHandle<()>,
}

impl<DB: DatabaseLike + 'static> PgStreamingCdcSource<DB> {
    /// Open a replication-mode connection to Postgres, issue
    /// `START_REPLICATION` against the configured slot, and spawn the
    /// inner task that streams typed events into the source's channel.
    ///
    /// # Pre-conditions
    ///
    /// - The publication `config.publication_name` must already exist
    ///   (`CREATE PUBLICATION ... FOR TABLE ...`).
    /// - The logical replication slot `config.slot_name` must already
    ///   exist with the `pgoutput` plugin
    ///   (`pg_create_logical_replication_slot(slot, 'pgoutput')`).
    /// - `config.url` is a libpq conninfo string. The source flips on
    ///   logical replication mode programmatically; the caller does
    ///   NOT need to include `replication=database` in the URL.
    pub async fn connect(
        config: PgStreamingConfig,
        catalog: Arc<DB>,
    ) -> Result<Self, PgStreamingError> {
        let mut pg_config = PgClientConfig::from_str(&config.url)?;
        pg_config.replication_mode(ReplicationMode::Logical);

        let (client, connection) = pg_config.connect(NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // IDENTIFY_SYSTEM validates the replication handshake before we
        // commit to START_REPLICATION. If it fails, the error path is
        // clean (no slot side effects).
        let messages = client.simple_query("IDENTIFY_SYSTEM").await?;
        if !messages
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::Row(_)))
        {
            return Err(PgStreamingError::Protocol(
                "IDENTIFY_SYSTEM returned no row; is the connection in \
                 `replication=database` mode?"
                    .to_string(),
            ));
        }

        // Issue START_REPLICATION. The slot is pre-created (per
        // pre-conditions); the LSN argument is `0/0` to mean "resume
        // from the slot's current confirmed_flush_lsn" unless the
        // caller provided an explicit start_lsn.
        let start_lsn = config.start_lsn.unwrap_or(PgLsn(0));
        let start_lsn_str = format_lsn(start_lsn);
        let start_cmd = format!(
            "START_REPLICATION SLOT \"{}\" LOGICAL {} (\"proto_version\" '1', \
             \"publication_names\" '{}')",
            config.slot_name, start_lsn_str, config.publication_name,
        );
        let copy_both: CopyBothDuplex<Bytes> = client.copy_both_simple(&start_cmd).await?;

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(config.buffer_capacity);
        let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let task_catalog = Arc::clone(&catalog);
        let status_updates_sent = Arc::new(AtomicU64::new(0));
        let task_status_counter = Arc::clone(&status_updates_sent);
        let events_received = Arc::new(AtomicU64::new(0));
        let task_events_counter = Arc::clone(&events_received);
        let task_exited = Arc::new(AtomicBool::new(false));
        let task_exited_for_task = Arc::clone(&task_exited);
        let status_interval = config.status_interval;
        let task = tokio::spawn(streaming_task(
            client,
            copy_both,
            task_catalog,
            event_tx,
            ack_rx,
            task_status_counter,
            task_events_counter,
            status_interval,
            shutdown_rx,
            task_exited_for_task,
        ));

        Ok(Self {
            config,
            catalog,
            event_rx,
            ack_tx,
            status_updates_sent,
            events_received,
            shutdown_tx: Some(shutdown_tx),
            task_exited,
            task,
        })
    }

    /// Cumulative number of `StandbyStatusUpdate` messages the inner
    /// task has sent to the upstream server since `connect`. Includes
    /// periodic-pump emissions, explicit `ack` calls, and keepalive
    /// auto-replies. Useful for production observability ("is my
    /// source actively talking to the server?") and to assert pump
    /// cadence in tests.
    #[must_use]
    pub fn status_updates_sent(&self) -> u64 {
        self.status_updates_sent.load(Ordering::Relaxed)
    }

    /// Cumulative number of `WalEvent`s the inner task has pushed onto
    /// the consumer-facing channel since `connect`. Symmetric with
    /// [`crate::polling::PollingPgCdcSource::events_received`]; generic
    /// benchmark code reads this on either source to count throughput.
    /// Bumped before the channel send, so the counter reflects
    /// transport-side work even when the consumer has not yet pulled.
    #[must_use]
    pub fn events_received(&self) -> u64 {
        self.events_received.load(Ordering::Relaxed)
    }

    /// `true` once the inner task has exited (cooperative shutdown,
    /// abort, or upstream-side close). Set by a drop guard in the
    /// task body, so it covers all exit paths.
    #[must_use]
    pub fn task_exited(&self) -> bool {
        self.task_exited.load(Ordering::Relaxed)
    }

    /// Clone the `task_exited` flag so callers can observe the inner
    /// task's exit even after the source itself has been dropped.
    /// Useful primarily in tests (e.g. asserting shutdown happens
    /// within a deadline after `drop(source)`).
    #[must_use]
    pub fn task_exited_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.task_exited)
    }

    /// Borrow the catalog the source resolves table/column metadata
    /// against. Exposed for tests and instrumentation.
    #[must_use]
    pub fn catalog(&self) -> &DB {
        &self.catalog
    }

    /// Borrow the configuration the source was built with.
    #[must_use]
    pub const fn config(&self) -> &PgStreamingConfig {
        &self.config
    }
}

impl<DB: DatabaseLike> Drop for PgStreamingCdcSource<DB> {
    fn drop(&mut self) {
        // Cooperative shutdown: drop the sender to fire the inner
        // task's shutdown arm immediately on the next select! poll.
        // Belt-and-braces: also abort, which forces the task to wake
        // at its next .await point even if cooperative signal can't
        // propagate (e.g. the task is suspended deep inside a
        // tokio_postgres call). Calling `abort` after the task has
        // already finished is a no-op.
        drop(self.shutdown_tx.take());
        self.task.abort();
    }
}

impl<DB: DatabaseLike + 'static> crate::CdcSource for PgStreamingCdcSource<DB> {
    type Checkpoint = PgLsn;
    type Error = PgStreamingError;

    // `manual_async_fn` would have us collapse these into `async fn`,
    // but native `async fn in trait` does not guarantee `Send` futures.
    // The trait's `impl Future + Send` shape requires us to mirror the
    // RPITIT here.
    #[allow(clippy::manual_async_fn)]
    fn next_event(
        &mut self,
    ) -> impl core::future::Future<Output = Result<Option<WalEvent<Self::Checkpoint>>, Self::Error>> + Send
    {
        async move {
            match self.event_rx.recv().await {
                Some(Ok(ev)) => Ok(Some(ev)),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            }
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn ack(
        &mut self,
        upto: Self::Checkpoint,
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send {
        let send_result = self.ack_tx.send(upto);
        async move {
            send_result.map_err(|_| PgStreamingError::SourceClosed)?;
            Ok(())
        }
    }
}

// ============================================================================
// Inner streaming task
// ============================================================================

/// Inner task that owns the replication socket. Reads `XLogData` frames,
/// parses pgoutput payloads, and forwards typed events through the
/// `event_tx` channel. Honors `reply_requested` keepalives by emitting
/// an inline `StandbyStatusUpdate` so the connection survives
/// `wal_sender_timeout` even before the periodic ack pump (Step 5) is in
/// place.
// The select! arms each carry a few lines of frame-parse / send code;
// hoisting any of them into a helper buys nothing (each is called
// exactly once and shares mutable state with the loop). Allow the
// length on this single function. The argument list is correspondingly
// long because the task closes over a lot of state — hand-rolling a
// state struct just to satisfy the lint is more obscure than the
// straight signature.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn streaming_task<DB: DatabaseLike>(
    _client: Client,
    copy_both: CopyBothDuplex<Bytes>,
    catalog: Arc<DB>,
    event_tx: tokio::sync::mpsc::Sender<Result<WalEvent<PgLsn>, PgStreamingError>>,
    mut ack_rx: tokio::sync::mpsc::UnboundedReceiver<PgLsn>,
    status_counter: Arc<AtomicU64>,
    events_counter: Arc<AtomicU64>,
    status_interval: Duration,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    task_exited: Arc<AtomicBool>,
) {
    // Drop guard sets `task_exited` regardless of which exit path the
    // task takes — normal return, early break, panic, abort. The flag
    // is observability metadata: production callers can see whether
    // the source's inner task is still healthy; tests use it to verify
    // graceful shutdown.
    struct ExitGuard(Arc<AtomicBool>);
    impl Drop for ExitGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
    let _exit_guard = ExitGuard(task_exited);

    // `CopyBothDuplex` is `!Unpin` (`PhantomPinned`); box-pin it so we
    // can call `Stream::next` / `Sink::send` directly on `&mut`.
    let mut copy_both = Box::pin(copy_both);
    // `oneshot::Receiver` is also `!Unpin`; pin it once and select on
    // `&mut shutdown_rx` so each iteration polls the same future.
    tokio::pin!(shutdown_rx);
    let parser = PgOutputParser::new();
    let mut latest_received_lsn: u64 = 0;
    // Highest LSN the caller has acked. Never regresses (slot's
    // confirmed_flush_lsn is `min(reported flush_lsn)` across recent
    // status updates, so reporting a lower value than before would pin
    // the slot at the earlier higher mark and effectively retain WAL
    // we promised to release).
    let mut latest_acked_lsn: u64 = 0;

    // Periodic status-update pump. `Delay` keeps the cadence bounded
    // when the task gets behind: a stalled task doesn't burst-emit
    // catch-up ticks when it resumes.
    let mut interval = tokio::time::interval(status_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Burn the first tick, which `tokio::time::interval` fires
    // immediately. The status emission machinery is ready, but we
    // don't want to send a redundant update right after START_REPLICATION
    // when the server already knows where we are.
    interval.tick().await;

    loop {
        tokio::select! {
            biased;
            // Cooperative shutdown is polled FIRST every iteration:
            // when the source is dropped, the sender is dropped, this
            // arm resolves immediately, and we break out before
            // touching the other (potentially slow) arms.
            _ = &mut shutdown_rx => break,
            // Prefer draining inbound frames so the consumer sees events
            // promptly. Subsequent arms are polled when no frame is ready.
            frame = copy_both.next() => {
                let Some(frame) = frame else {
                    // Stream closed cleanly (server hung up).
                    return;
                };
                let bytes = match frame {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = event_tx.send(Err(PgStreamingError::Postgres(e))).await;
                        return;
                    }
                };
                if bytes.is_empty() {
                    continue;
                }
                match bytes[0] {
                    b'w' => {
                        if bytes.len() < XLOG_DATA_HEADER_LEN {
                            let _ = event_tx
                                .send(Err(PgStreamingError::Protocol(format!(
                                    "truncated XLogData frame: {} bytes",
                                    bytes.len(),
                                ))))
                                .await;
                            return;
                        }
                        let start_lsn =
                            u64::from_be_bytes(bytes[1..9].try_into().unwrap_or([0; 8]));
                        let payload = &bytes[XLOG_DATA_HEADER_LEN..];
                        latest_received_lsn = latest_received_lsn
                            .max(start_lsn.saturating_add(payload.len() as u64));

                        match parser.parse_wal_message(payload, &*catalog) {
                            Ok(events) => {
                                for ev in events {
                                    let typed = ev.set_checkpoint(Some(PgLsn(start_lsn)));
                                    // Counter is bumped BEFORE the
                                    // channel send: reflects transport-
                                    // side work, not consumer-side
                                    // observations. A slow consumer
                                    // still produces accurate counter
                                    // values for benchmark / monitoring.
                                    events_counter.fetch_add(1, Ordering::Relaxed);
                                    // `.send().await` is the load-bearing
                                    // back-pressure point: yields when
                                    // the bounded channel is full, which
                                    // stops reading from `copy_both` and
                                    // propagates back-pressure all the
                                    // way to the upstream TCP socket.
                                    if event_tx.send(Ok(typed)).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = event_tx.send(Err(PgStreamingError::Parse(e))).await;
                                return;
                            }
                        }
                    }
                    b'k' => {
                        if bytes.len() < PRIMARY_KEEPALIVE_LEN {
                            let _ = event_tx
                                .send(Err(PgStreamingError::Protocol(format!(
                                    "truncated PrimaryKeepalive frame: {} bytes",
                                    bytes.len(),
                                ))))
                                .await;
                            return;
                        }
                        let reply_requested = bytes[PRIMARY_KEEPALIVE_LEN - 1] == 1;
                        if reply_requested {
                            // Reply with the LATEST acked LSN, not
                            // received. The server treats the reported
                            // flush_lsn as durable; reporting received
                            // would lie that we have already applied
                            // events we have only seen.
                            if send_status_update(
                                &mut copy_both,
                                &status_counter,
                                latest_acked_lsn,
                            )
                            .await
                            .is_err()
                            {
                                return;
                            }
                        }
                    }
                    _ => {
                        // Unknown / unsupported frame type; skip silently.
                    }
                }
            }
            ack = ack_rx.recv() => {
                let Some(upto) = ack else {
                    // Source dropped; ack channel closed. Keep
                    // streaming inbound frames until the upstream
                    // closes too. Setting `ack_rx` to a closed state
                    // means subsequent select polls return immediately;
                    // we just discard them.
                    continue;
                };
                // Never regress the reported flush_lsn. Slots track
                // `min(reported)` across recent status updates.
                latest_acked_lsn = latest_acked_lsn.max(upto.0);
                if send_status_update(
                    &mut copy_both,
                    &status_counter,
                    latest_acked_lsn,
                )
                .await
                .is_err()
                {
                    return;
                }
            }
            _ = interval.tick() => {
                // Periodic pump: tells the server "still here" even
                // when the consumer is idle and the server is not
                // requesting a reply. Without this, `wal_sender_timeout`
                // (server default 60s) would tear down the connection
                // during quiet periods.
                if send_status_update(
                    &mut copy_both,
                    &status_counter,
                    latest_acked_lsn,
                )
                .await
                .is_err()
                {
                    return;
                }
            }
        }
    }
}

/// Shared emission helper for the three status-update sites in the
/// inner task (keepalive auto-reply, explicit ack, periodic pump).
/// Bumps the observability counter and forwards to the `CopyBothDuplex`
/// sink. Returns `Err(())` only when the sink has closed; the caller
/// uses that to terminate the task cleanly.
async fn send_status_update(
    copy_both: &mut core::pin::Pin<alloc::boxed::Box<CopyBothDuplex<Bytes>>>,
    status_counter: &Arc<AtomicU64>,
    lsn: u64,
) -> Result<(), ()> {
    let status = build_standby_status_update(lsn, lsn, lsn, false);
    copy_both.send(status).await.map_err(|_| ())?;
    status_counter.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

// ============================================================================
// Wire-format helpers
// ============================================================================

const XLOG_DATA_HEADER_LEN: usize = 1 + 8 + 8 + 8; // 'w' + start + end + clock
const PRIMARY_KEEPALIVE_LEN: usize = 1 + 8 + 8 + 1; // 'k' + end + clock + reply

/// Format a `PgLsn` as the Postgres wire-text form `XXXX/YYYY` (two
/// big-endian halves in upper-case hex). Used in
/// `START_REPLICATION SLOT ... LOGICAL <lsn>`.
fn format_lsn(lsn: PgLsn) -> String {
    let hi = (lsn.0 >> 32) as u32;
    let lo = (lsn.0 & 0xFFFF_FFFF) as u32;
    format!("{hi:X}/{lo:X}")
}

/// Serialize a `StandbyStatusUpdate` (`'r'`) message body for the
/// `CopyBothDuplex` sink. Returns 34 bytes: tag + write_lsn + flush_lsn
/// + apply_lsn + client_clock + reply_requested.
///
/// The `client_clock` field is the microseconds since 2000-01-01 UTC;
/// the upstream server uses it for log-shipping latency tracking but
/// does not require correctness for the connection to stay alive. We
/// send `0` for now and revisit if Step 5's periodic pump needs to
/// supply a real value.
fn build_standby_status_update(
    write_lsn: u64,
    flush_lsn: u64,
    apply_lsn: u64,
    reply_requested: bool,
) -> Bytes {
    let mut buf = BytesMut::with_capacity(1 + 8 + 8 + 8 + 8 + 1);
    buf.put_u8(b'r');
    buf.put_u64(write_lsn);
    buf.put_u64(flush_lsn);
    buf.put_u64(apply_lsn);
    buf.put_i64(0); // client_clock; PG accepts 0 for non-monitoring clients
    buf.put_u8(u8::from(reply_requested));
    buf.freeze()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn lsn_format_matches_pg_wire_text() {
        assert_eq!(format_lsn(PgLsn(0)), "0/0");
        assert_eq!(format_lsn(PgLsn(0x1234_5678)), "0/12345678");
        assert_eq!(
            format_lsn(PgLsn(0x0000_0001_FFFF_FFFF)),
            "1/FFFFFFFF",
            "high bit of low half must NOT bleed into the high half"
        );
        assert_eq!(
            format_lsn(PgLsn(0xDEAD_BEEF_CAFE_BABE)),
            "DEADBEEF/CAFEBABE"
        );
    }

    #[test]
    fn standby_status_update_layout_matches_wire_spec() {
        // 'r' + 8 + 8 + 8 + 8 + 1 = 34 bytes
        let buf = build_standby_status_update(0x1111_2222_3333_4444, 0x5555, 0x66, false);
        assert_eq!(buf.len(), 34);
        assert_eq!(buf[0], b'r');
        assert_eq!(
            u64::from_be_bytes(buf[1..9].try_into().unwrap()),
            0x1111_2222_3333_4444
        );
        assert_eq!(u64::from_be_bytes(buf[9..17].try_into().unwrap()), 0x5555);
        assert_eq!(u64::from_be_bytes(buf[17..25].try_into().unwrap()), 0x66);
        // client_clock is whatever we put (currently 0); reply_requested last byte.
        assert_eq!(buf[33], 0);

        let with_reply = build_standby_status_update(0, 0, 0, true);
        assert_eq!(with_reply[33], 1);
    }

    #[test]
    fn xlog_data_header_constants_match_spec() {
        // tag 'w' + 8 start_lsn + 8 current_end_lsn + 8 server_clock = 25.
        assert_eq!(XLOG_DATA_HEADER_LEN, 25);
        // tag 'k' + 8 end_lsn + 8 server_clock + 1 reply_requested = 18.
        assert_eq!(PRIMARY_KEEPALIVE_LEN, 18);
    }
}

//! Push-based Postgres CDC source driven by `pg_walstream`'s native backend.
//!
//! Owns a `pg_walstream::PgReplicationConnection` opened in replication
//! mode plus an attached `START_REPLICATION` stream; surfaces typed
//! [`crate::WalEvent<PgLsn>`] values through the [`crate::CdcSource`]
//! trait. Acks flow back to the server as `StandbyStatusUpdate` messages
//! so the slot's `confirmed_flush_lsn` tracks reality.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;

use pg_walstream::error::ReplicationError;
use pg_walstream::PgReplicationConnection;
use sql_traits::prelude::DatabaseLike;
use tokio_util::sync::CancellationToken;

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
    /// appends `?replication=database` if the caller did not.
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
    /// Underlying `pg_walstream` failure (transport, auth, server error).
    #[error("postgres error: {0}")]
    Postgres(#[from] ReplicationError),
    /// The pgoutput payload could not be parsed.
    #[error("pgoutput parse error: {0}")]
    Parse(#[from] WalParseError),
    /// The server's response to a replication command did not match
    /// what the spec mandates (e.g. truncated `XLogData` header).
    #[error("replication protocol error: {0}")]
    Protocol(String),
    /// The inner streaming task ended (channel closed). Either the
    /// upstream connection was lost or the source was dropped.
    #[error("streaming source shut down")]
    SourceClosed,
}

/// Push-based Postgres CDC source. See the [`crate::CdcSource`] trait for
/// the lifecycle contract.
pub struct PgStreamingCdcSource<DB: DatabaseLike> {
    config: PgStreamingConfig,
    catalog: Arc<DB>,
    event_rx: tokio::sync::mpsc::Receiver<Result<WalEvent<PgLsn>, PgStreamingError>>,
    ack_tx: tokio::sync::mpsc::UnboundedSender<PgLsn>,
    status_updates_sent: Arc<AtomicU64>,
    events_received: Arc<AtomicU64>,
    /// Cancellation handle shared with the inner task's
    /// `get_copy_data_async` arm so the task wakes promptly on shutdown.
    shutdown_token: CancellationToken,
    /// Set to `true` by an in-task drop guard on any exit path.
    task_exited: Arc<AtomicBool>,
    /// Inner-task join handle, kept so `Drop` can call `.abort()` as a
    /// backstop in case the cooperative signal does not propagate in
    /// time.
    task: tokio::task::JoinHandle<()>,
}

impl<DB: DatabaseLike + 'static> PgStreamingCdcSource<DB> {
    /// Open a replication-mode connection to Postgres, issue
    /// `START_REPLICATION` against the configured slot, and spawn the
    /// inner task that streams typed events into the source's channel.
    ///
    /// # Pre-conditions
    ///
    /// - The publication `config.publication_name` must already exist.
    /// - The logical replication slot `config.slot_name` must already
    ///   exist with the `pgoutput` plugin.
    /// - `config.url` is a libpq conninfo string. The source appends
    ///   `replication=database` if the caller did not.
    pub async fn connect(
        config: PgStreamingConfig,
        catalog: Arc<DB>,
    ) -> Result<Self, PgStreamingError> {
        let conninfo = ensure_replication_param(&config.url);
        let slot_name = config.slot_name.clone();
        let publication_names = config.publication_name.clone();
        let start_lsn = config.start_lsn.unwrap_or(PgLsn(0)).0;

        // libpq connect + IDENTIFY_SYSTEM + START_REPLICATION are all
        // synchronous calls that block on socket I/O. Bounce through the
        // blocking pool so we do not stall the runtime worker thread.
        let conn = tokio::task::spawn_blocking(move || -> Result<_, PgStreamingError> {
            let mut conn = PgReplicationConnection::connect(&conninfo)?;
            let ident = conn.identify_system()?;
            if ident.ntuples() == 0 {
                return Err(PgStreamingError::Protocol(
                    "IDENTIFY_SYSTEM returned no row; is the connection in \
                     replication=database mode?"
                        .to_string(),
                ));
            }
            let options = [
                ("proto_version", "1"),
                ("publication_names", publication_names.as_str()),
            ];
            conn.start_replication(&slot_name, start_lsn, &options)?;
            Ok(conn)
        })
        .await
        .map_err(|e| PgStreamingError::Protocol(format!("connection task panicked: {e}")))??;

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(config.buffer_capacity);
        let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();
        let shutdown_token = CancellationToken::new();
        let task_token = shutdown_token.clone();

        let task_catalog = Arc::clone(&catalog);
        let status_updates_sent = Arc::new(AtomicU64::new(0));
        let task_status_counter = Arc::clone(&status_updates_sent);
        let events_received = Arc::new(AtomicU64::new(0));
        let task_events_counter = Arc::clone(&events_received);
        let task_exited = Arc::new(AtomicBool::new(false));
        let task_exited_for_task = Arc::clone(&task_exited);
        let status_interval = config.status_interval;

        let task = tokio::spawn(streaming_task(
            conn,
            task_catalog,
            event_tx,
            ack_rx,
            task_status_counter,
            task_events_counter,
            status_interval,
            task_token,
            task_exited_for_task,
        ));

        Ok(Self {
            config,
            catalog,
            event_rx,
            ack_tx,
            status_updates_sent,
            events_received,
            shutdown_token,
            task_exited,
            task,
        })
    }

    /// Cumulative number of `StandbyStatusUpdate` messages the inner
    /// task has sent to the upstream server since `connect`. Includes
    /// periodic-pump emissions, explicit `ack` calls, and keepalive
    /// auto-replies.
    #[must_use]
    pub fn status_updates_sent(&self) -> u64 {
        self.status_updates_sent.load(Ordering::Relaxed)
    }

    /// Cumulative number of `WalEvent`s the inner task has pushed onto
    /// the consumer-facing channel since `connect`. Symmetric with
    /// [`crate::polling::PollingPgCdcSource::events_received`].
    #[must_use]
    pub fn events_received(&self) -> u64 {
        self.events_received.load(Ordering::Relaxed)
    }

    /// `true` once the inner task has exited (cooperative shutdown,
    /// abort, or upstream-side close).
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

    /// Borrow the catalog the source resolves table/column metadata against.
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
        // Cooperative shutdown: cancel the token so the inner task's
        // `get_copy_data_async` arm wakes immediately; belt-and-braces
        // abort in case it is stuck inside a sync libpq call.
        self.shutdown_token.cancel();
        self.task.abort();
    }
}

impl<DB: DatabaseLike + 'static> crate::CdcSource for PgStreamingCdcSource<DB> {
    type Checkpoint = PgLsn;
    type Error = PgStreamingError;

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

    // The body is sync (unbounded channel send is sync), but the trait
    // requires `impl Future + Send`. `unused_async` would flag the
    // wrapper if it were `async fn`, and `manual_async_fn` flags this
    // pattern too; both are intentional.
    #[allow(clippy::manual_async_fn, clippy::unused_async)]
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

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn streaming_task<DB: DatabaseLike>(
    mut conn: PgReplicationConnection,
    catalog: Arc<DB>,
    event_tx: tokio::sync::mpsc::Sender<Result<WalEvent<PgLsn>, PgStreamingError>>,
    mut ack_rx: tokio::sync::mpsc::UnboundedReceiver<PgLsn>,
    status_counter: Arc<AtomicU64>,
    events_counter: Arc<AtomicU64>,
    status_interval: Duration,
    shutdown_token: CancellationToken,
    task_exited: Arc<AtomicBool>,
) {
    struct ExitGuard(Arc<AtomicBool>);
    impl Drop for ExitGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
    let _exit_guard = ExitGuard(task_exited);

    let parser = PgOutputParser::new();
    let mut latest_received_lsn: u64 = 0;
    // Never regress the reported flush_lsn; slots track `min(reported)`.
    let mut latest_acked_lsn: u64 = 0;

    let mut interval = tokio::time::interval(status_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;

    loop {
        tokio::select! {
            biased;
            // Cooperative shutdown wins every iteration.
            () = shutdown_token.cancelled() => break,
            // Inbound frame; takes &mut conn for the duration of the
            // await but the borrow releases as soon as another arm
            // wins or this arm completes.
            frame = conn.get_copy_data_async(&shutdown_token) => {
                let bytes = match frame {
                    Ok(b) => b,
                    Err(ReplicationError::Cancelled(_)) => return,
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
                        // The XLogData header is `'w'` + `start_lsn` (u64
                        // big-endian) + `wal_end` (u64 big-endian) + send time.
                        // `wal_end` is the server's WAL end position at send
                        // time, which is what `StandbyStatusUpdate` expects in
                        // `received_lsn`. The payload byte count is NOT a
                        // WAL-space distance: pgoutput payloads are
                        // protocol-encoded, not raw WAL.
                        let start_lsn = u64::from_be_bytes(
                            bytes[1..9].try_into().expect("slice is exactly 8 bytes"),
                        );
                        let wal_end = u64::from_be_bytes(
                            bytes[9..17].try_into().expect("slice is exactly 8 bytes"),
                        );
                        let payload = &bytes[XLOG_DATA_HEADER_LEN..];
                        latest_received_lsn = latest_received_lsn.max(wal_end);

                        match parser.parse_wal_message(payload, &*catalog) {
                            Ok(events) => {
                                for ev in events {
                                    let typed = ev.set_checkpoint(Some(PgLsn(start_lsn)));
                                    events_counter.fetch_add(1, Ordering::Relaxed);
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
                        if reply_requested
                            && send_status_update(
                                &mut conn,
                                &status_counter,
                                latest_received_lsn,
                                latest_acked_lsn,
                            )
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    _ => {
                        // Unknown frame type; skip silently.
                    }
                }
            }
            ack = ack_rx.recv() => {
                let Some(upto) = ack else { continue; };
                latest_acked_lsn = latest_acked_lsn.max(upto.0);
                if send_status_update(
                    &mut conn,
                    &status_counter,
                    latest_received_lsn,
                    latest_acked_lsn,
                )
                .await
                .is_err()
                {
                    return;
                }
            }
            _ = interval.tick() => {
                if send_status_update(
                    &mut conn,
                    &status_counter,
                    latest_received_lsn,
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

async fn send_status_update(
    conn: &mut PgReplicationConnection,
    status_counter: &Arc<AtomicU64>,
    received_lsn: u64,
    flushed_lsn: u64,
) -> Result<(), ()> {
    conn.send_standby_status_update(received_lsn, flushed_lsn, flushed_lsn, false)
        .await
        .map_err(|_| ())?;
    status_counter.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

// ============================================================================
// Wire-format helpers and conninfo handling
// ============================================================================

const XLOG_DATA_HEADER_LEN: usize = 1 + 8 + 8 + 8; // 'w' + start + end + clock
const PRIMARY_KEEPALIVE_LEN: usize = 1 + 8 + 8 + 1; // 'k' + end + clock + reply

/// Append `replication=database` to a libpq conninfo string if the
/// caller did not already include it. Accepts both URL-style
/// (`postgresql://...`) and key=value forms; the latter just needs the
/// param appended.
fn ensure_replication_param(url: &str) -> String {
    if url.contains("replication=") {
        return url.to_string();
    }
    if url.contains("://") {
        if url.contains('?') {
            format!("{url}&replication=database")
        } else {
            format!("{url}?replication=database")
        }
    } else {
        format!("{url} replication=database")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn xlog_data_header_constants_match_spec() {
        assert_eq!(XLOG_DATA_HEADER_LEN, 25);
        assert_eq!(PRIMARY_KEEPALIVE_LEN, 18);
    }

    #[test]
    fn ensure_replication_param_url_no_query() {
        assert_eq!(
            ensure_replication_param("postgresql://u:p@h:5432/db"),
            "postgresql://u:p@h:5432/db?replication=database"
        );
    }

    #[test]
    fn ensure_replication_param_url_with_query() {
        assert_eq!(
            ensure_replication_param("postgresql://u:p@h:5432/db?sslmode=require"),
            "postgresql://u:p@h:5432/db?sslmode=require&replication=database"
        );
    }

    #[test]
    fn ensure_replication_param_keyvalue_form() {
        assert_eq!(
            ensure_replication_param("host=h port=5432 dbname=db"),
            "host=h port=5432 dbname=db replication=database"
        );
    }

    #[test]
    fn ensure_replication_param_already_present_is_idempotent() {
        let s = "postgresql://u:p@h:5432/db?replication=database";
        assert_eq!(ensure_replication_param(s), s);
    }
}

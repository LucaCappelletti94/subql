//! Push-based Postgres CDC source driven by `pg_walstream`'s native backend.
//!
//! Owns a `pg_walstream::PgReplicationConnection` opened in replication
//! mode plus an attached `START_REPLICATION` stream. Surfaces each row
//! change as a [`crate::PgChangeEvent`] through the [`crate::CdcSource`]
//! trait, placed in commit order by its transaction's commit position and
//! its ordinal in that transaction, and then the transaction's
//! [`crate::PgCommit`]. Acks flow back so the slot's `confirmed_flush_lsn`
//! moves to the end of each commit the consumer has acknowledged, and no
//! further.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::ops::ControlFlow;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;

use pg_walstream::error::ReplicationError;
use pg_walstream::{Lsn, PgOutputDecoder, PgReplicationConnection};
use sql_traits::prelude::DatabaseLike;
use tokio_util::sync::CancellationToken;

use super::{
    PgChangeEvent, PgCommit, PgOutputOrder, PgSourceItem, ReleaseQueue, SourceItem,
    TransactionOrderError,
};
use crate::{PgCommitPosition, PgLsn};

mod wire_format_helpers;
use wire_format_helpers::{
    ensure_replication_param, parse_timeline_history, PRIMARY_KEEPALIVE_LEN, XLOG_DATA_HEADER_LEN,
};

/// Configuration for a [`PgStreamingCdcSource`].
///
/// The caller is responsible for creating the publication and the
/// replication slot before constructing the source. `connect` does
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
    /// Optional resume position. The source delivers only the items after
    /// it. `None` starts from the slot's current `confirmed_flush_lsn`.
    ///
    /// Any checkpoint a source delivered resumes exactly after that item,
    /// including a row in the middle of a transaction. Resuming after a
    /// transaction's rows but before its commit delivers the commit again.
    pub start: Option<PgCommitPosition>,
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
    /// `start = None` (resume from the slot's current position),
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
            start: None,
            status_interval: Duration::from_secs(10),
            buffer_capacity: 1024,
        }
    }

    /// Override the resume position. `None` means "start from the
    /// slot's current `confirmed_flush_lsn`".
    #[must_use]
    pub const fn start(mut self, start: Option<PgCommitPosition>) -> Self {
        self.start = start;
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
    /// The server's response to a replication command did not match
    /// what the spec mandates (e.g. truncated `XLogData` header).
    #[error("replication protocol error: {0}")]
    Protocol(String),
    /// The inner streaming task ended (channel closed). Either the
    /// upstream connection was lost or the source was dropped.
    #[error("streaming source shut down")]
    SourceClosed,
    /// The stream's transaction frames were out of place, so its rows could
    /// not be placed in commit order.
    #[error("replication stream out of order: {0}")]
    TransactionOrder(#[from] TransactionOrderError),
}

/// One point where a later timeline branched from `timeline`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimelineSwitch {
    /// The ancestor timeline, the history line's first field.
    pub timeline: u32,
    /// Position where the branch left the ancestor.
    pub switch_lsn: PgLsn,
}

/// The history a [`PgStreamingCdcSource`] streams from, which a WAL
/// position is only meaningful within.
///
/// A promoted point-in-time restore keeps `system_id` and moves to a new
/// `timeline` branching below earlier positions, while a dump restored
/// into a fresh cluster changes `system_id`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterIdentity {
    /// `IDENTIFY_SYSTEM`'s `systemid`.
    pub system_id: u64,
    /// `IDENTIFY_SYSTEM`'s `timeline`, the one the server is writing.
    pub timeline: u32,
    /// One entry per ancestor timeline in ascending order, empty on timeline 1.
    pub history: Vec<TimelineSwitch>,
}

/// Push-based Postgres CDC source. See the [`crate::CdcSource`] trait for
/// the lifecycle contract.
pub struct PgStreamingCdcSource {
    config: PgStreamingConfig,
    event_rx: tokio::sync::mpsc::Receiver<Result<PgSourceItem, PgStreamingError>>,
    ack_tx: tokio::sync::mpsc::UnboundedSender<PgCommitPosition>,
    status_updates_sent: Arc<AtomicU64>,
    events_received: Arc<AtomicU64>,
    /// The furthest position seen on a frame, and the position last
    /// reported back as flushed.
    received_lsn: Arc<AtomicU64>,
    acked_lsn: Arc<AtomicU64>,
    /// Whether a caller has acknowledged anything, which the position alone
    /// cannot say now that it starts at the slot's own.
    acked_seen: Arc<AtomicBool>,
    /// Cancellation handle shared with the inner task's
    /// `get_copy_data_async` arm so the task wakes promptly on shutdown.
    shutdown_token: CancellationToken,
    /// Set to `true` by an in-task drop guard on any exit path.
    task_exited: Arc<AtomicBool>,
    /// Inner-task join handle, kept so `Drop` can call `.abort()` as a
    /// backstop in case the cooperative signal does not propagate in
    /// time.
    task: tokio::task::JoinHandle<()>,
    /// The cluster identity read on the replication connection during
    /// `connect`.
    cluster_identity: ClusterIdentity,
}

impl PgStreamingCdcSource {
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
    ///
    /// `catalog` is retained for API stability and is currently unused.
    /// The engine resolves the events against its own catalog at dispatch
    /// time.
    #[allow(clippy::needless_pass_by_value)]
    pub async fn connect<DB: DatabaseLike + 'static>(
        config: PgStreamingConfig,
        _catalog: DB,
    ) -> Result<Self, PgStreamingError> {
        let conninfo = ensure_replication_param(&config.url);
        let slot_name = config.slot_name.clone();
        let publication_names = config.publication_name.clone();
        let start = config.start;
        let replication_start = start.map_or(0, |position| position.commit_lsn().0);

        // libpq connect + IDENTIFY_SYSTEM + START_REPLICATION are all
        // synchronous calls that block on socket I/O. Bounce through the
        // blocking pool so we do not stall the runtime worker thread.
        let (conn, base_lsn, cluster_identity) =
            tokio::task::spawn_blocking(move || -> Result<_, PgStreamingError> {
                let mut conn = PgReplicationConnection::connect(&conninfo)?;
                let cluster_identity = read_cluster_identity(&mut conn)?;
                let options = [
                    ("proto_version", "1"),
                    ("publication_names", publication_names.as_str()),
                ];
                // The slot's own position, so the retention gauges start at the
                // distance the slot is already holding rather than at an absolute
                // LSN that would read as gigabytes.
                let slot_row = conn.exec(&format!(
                    "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
                 WHERE slot_name = '{}'",
                    slot_name.replace('\'', "''")
                ))?;
                let base = slot_row
                    .get_value(0, 0)
                    .and_then(|text| pg_walstream::parse_lsn(&text).ok())
                    .unwrap_or(0);
                conn.start_replication(&slot_name, replication_start, &options)?;
                Ok((conn, base, cluster_identity))
            })
            .await
            .map_err(|e| PgStreamingError::Protocol(format!("connection task panicked: {e}")))??;

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(config.buffer_capacity);
        let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();
        let shutdown_token = CancellationToken::new();
        let task_token = shutdown_token.clone();

        let status_updates_sent = Arc::new(AtomicU64::new(0));
        let task_status_counter = Arc::clone(&status_updates_sent);
        let events_received = Arc::new(AtomicU64::new(0));
        let task_events_counter = Arc::clone(&events_received);
        let received_lsn = Arc::new(AtomicU64::new(base_lsn));
        let task_received_lsn = Arc::clone(&received_lsn);
        let acked_lsn = Arc::new(AtomicU64::new(base_lsn));
        let task_acked_lsn = Arc::clone(&acked_lsn);
        let acked_seen = Arc::new(AtomicBool::new(false));
        let task_acked_seen = Arc::clone(&acked_seen);
        let task_exited = Arc::new(AtomicBool::new(false));
        let task_exited_for_task = Arc::clone(&task_exited);
        let status_interval = config.status_interval;

        let task = tokio::spawn(streaming_task(
            conn,
            base_lsn,
            start,
            event_tx,
            ack_rx,
            task_status_counter,
            task_events_counter,
            task_received_lsn,
            task_acked_lsn,
            task_acked_seen,
            status_interval,
            task_token,
            task_exited_for_task,
        ));

        Ok(Self {
            config,
            event_rx,
            ack_tx,
            status_updates_sent,
            events_received,
            received_lsn,
            acked_lsn,
            acked_seen,
            shutdown_token,
            task_exited,
            task,
            cluster_identity,
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

    /// Cumulative number of [`PgChangeEvent`]s the inner task has
    /// pushed onto the consumer-facing channel since `connect`.
    /// Symmetric with
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

    /// Latest position this source has reported to the server as flushed.
    ///
    /// `None` until the first [`CdcSource::ack`](crate::CdcSource::ack). It
    /// advances to the [`PgCommit::end_lsn`] of the last commit acknowledged,
    /// and the task publishes it once the server has it, so a read right
    /// after `ack` can still report the previous position.
    #[must_use]
    pub fn acknowledged_position(&self) -> Option<PgLsn> {
        self.acked_seen
            .load(Ordering::Relaxed)
            .then(|| PgLsn(self.acked_lsn.load(Ordering::Relaxed)))
    }

    /// Distance between the furthest position this source has seen and the
    /// one it last reported as flushed.
    ///
    /// A keepalive carries the server's own WAL end, and a data frame
    /// carries the position of the record it holds, so the figure counts
    /// WAL this publication never carries and rises on activity elsewhere
    /// in the cluster. A consumer that never acknowledges sees it grow
    /// without bound, and so does the server's WAL volume, until it fills,
    /// so a sustained rise is what catches that before the disk answers
    /// for it.
    ///
    /// Close to what the slot retains without being it. The server retains
    /// to the slot's `restart_lsn`, which lags the confirmed flush
    /// position, and the end here is only as fresh as the last frame
    /// received. Read `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`
    /// from `pg_replication_slots` for the size itself.
    #[must_use]
    pub fn unacknowledged_bytes(&self) -> u64 {
        self.received_lsn
            .load(Ordering::Relaxed)
            .saturating_sub(self.acked_lsn.load(Ordering::Relaxed))
    }

    /// Borrow the configuration the source was built with.
    #[must_use]
    pub const fn config(&self) -> &PgStreamingConfig {
        &self.config
    }

    /// The cluster identity read at `connect`, which a fresh `connect` re-reads.
    #[must_use]
    pub const fn cluster_identity(&self) -> &ClusterIdentity {
        &self.cluster_identity
    }
}

impl Drop for PgStreamingCdcSource {
    fn drop(&mut self) {
        // Cooperative shutdown: cancel the token so the inner task's
        // `get_copy_data_async` arm wakes immediately; belt-and-braces
        // abort in case it is stuck inside a sync libpq call.
        self.shutdown_token.cancel();
        self.task.abort();
    }
}

impl crate::CdcSource for PgStreamingCdcSource {
    type Event = PgChangeEvent;
    type Commit = PgCommit;
    type Error = PgStreamingError;

    #[allow(clippy::manual_async_fn)]
    fn next_item(
        &mut self,
    ) -> impl core::future::Future<Output = Result<Option<PgSourceItem>, Self::Error>> + Send {
        super::shared_helpers::recv_source_item(&mut self.event_rx)
    }

    // The body is sync (unbounded channel send is sync), but the trait
    // requires `impl Future + Send`. `unused_async` would flag the
    // wrapper if it were `async fn`, and `manual_async_fn` flags this
    // pattern too; both are intentional.
    #[allow(clippy::manual_async_fn, clippy::unused_async)]
    fn ack(
        &mut self,
        upto: PgCommitPosition,
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send {
        let send_result = self.ack_tx.send(upto);
        async move {
            send_result.map_err(|_| PgStreamingError::SourceClosed)?;
            Ok(())
        }
    }
}

// Inner streaming task

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn streaming_task(
    mut conn: PgReplicationConnection,
    base_lsn: u64,
    start: Option<PgCommitPosition>,
    event_tx: tokio::sync::mpsc::Sender<Result<PgSourceItem, PgStreamingError>>,
    mut ack_rx: tokio::sync::mpsc::UnboundedReceiver<PgCommitPosition>,
    status_counter: Arc<AtomicU64>,
    events_counter: Arc<AtomicU64>,
    received_gauge: Arc<AtomicU64>,
    acked_gauge: Arc<AtomicU64>,
    acked_seen: Arc<AtomicBool>,
    status_interval: Duration,
    shutdown_token: CancellationToken,
    task_exited: Arc<AtomicBool>,
) {
    let _exit_guard = crate::wal::ExitFlagGuard(task_exited);

    let mut decoder = PgOutputDecoder::with_protocol_version(1);
    let mut delivery = Delivery::new(start);
    // Both start where the slot already is, which is what the gauges
    // published at connect. Starting at zero reports a client that has
    // received nothing and lets the first frame store a position below the
    // seed, since a frame can carry a zero WAL end.
    let mut latest_received_lsn: u64 = base_lsn;
    // Never regress the reported flush_lsn; slots track `min(reported)`.
    let mut latest_acked_lsn: u64 = base_lsn;

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
                        // Under logical decoding `wal_end` is the end of the
                        // record in this message, measured equal to
                        // `start_lsn` on every frame of the suite, and a
                        // keepalive is what carries the server's own WAL end.
                        // The payload byte count is NOT a WAL-space distance,
                        // since pgoutput payloads are protocol-encoded rather
                        // than raw WAL.
                        let start_lsn = u64::from_be_bytes(
                            bytes[1..9].try_into().expect("slice is exactly 8 bytes"),
                        );
                        let wal_end = u64::from_be_bytes(
                            bytes[9..17].try_into().expect("slice is exactly 8 bytes"),
                        );
                        let payload = bytes.slice(XLOG_DATA_HEADER_LEN..);
                        latest_received_lsn = latest_received_lsn.max(wal_end);
                        received_gauge.store(latest_received_lsn, Ordering::Relaxed);

                        let change = match decoder.decode_message(payload, Lsn::new(start_lsn)) {
                            Ok(Some(change)) => change,
                            Ok(None) => continue,
                            Err(e) => {
                                let _ = event_tx.send(Err(PgStreamingError::Postgres(e))).await;
                                return;
                            }
                        };
                        let end = match delivery.deliver(change, &event_tx, &events_counter).await {
                            ControlFlow::Continue(Some(end)) => end,
                            ControlFlow::Continue(None) => continue,
                            ControlFlow::Break(()) => return,
                        };
                        latest_acked_lsn = latest_acked_lsn.max(end.0);
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
                        acked_gauge.store(latest_acked_lsn, Ordering::Relaxed);
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
                        // A keepalive carries the server's WAL end, which is
                        // how an idle publication still reports a slot that is
                        // holding more and more.
                        let wal_end = u64::from_be_bytes(
                            bytes[1..9].try_into().expect("slice is exactly 8 bytes"),
                        );
                        latest_received_lsn = latest_received_lsn.max(wal_end);
                        received_gauge.store(latest_received_lsn, Ordering::Relaxed);
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
                if let Some(end) = delivery.acknowledge(upto) {
                    latest_acked_lsn = latest_acked_lsn.max(end.0);
                }
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
                // Published only once the server has the position, so a failed
                // feedback cannot understate what the slot is still holding.
                acked_gauge.store(latest_acked_lsn, Ordering::Relaxed);
                acked_seen.store(true, Ordering::Relaxed);
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

/// Row delivery for the streaming task: the transaction frame, the resume
/// filter, and the commits the slot still holds.
struct Delivery {
    order: PgOutputOrder,
    rows: Vec<PgChangeEvent>,
    releases: ReleaseQueue,
    start: Option<PgCommitPosition>,
}

impl Delivery {
    const fn new(start: Option<PgCommitPosition>) -> Self {
        Self {
            order: PgOutputOrder::new(),
            rows: Vec::new(),
            releases: ReleaseQueue::new(),
            start,
        }
    }

    /// Send the rows and the commit `change` carries past `start`, and
    /// continue with the flush position that releases, if any. Breaks once
    /// the task must end.
    async fn deliver(
        &mut self,
        change: pg_walstream::ChangeEvent,
        event_tx: &tokio::sync::mpsc::Sender<Result<PgSourceItem, PgStreamingError>>,
        events_counter: &AtomicU64,
    ) -> ControlFlow<(), Option<PgLsn>> {
        let committed = match self.order.apply(change, &mut self.rows) {
            Ok(committed) => committed,
            Err(e) => {
                let _ = event_tx.send(Err(e.into())).await;
                return ControlFlow::Break(());
            }
        };
        let start = self.start;
        let unseen = |position: PgCommitPosition| start.is_none_or(|start| position > start);
        for ev in self.rows.drain(..) {
            if !unseen(ev.position()) {
                continue;
            }
            events_counter.fetch_add(1, Ordering::Relaxed);
            if event_tx.send(Ok(SourceItem::Event(ev))).await.is_err() {
                return ControlFlow::Break(());
            }
        }
        let Some(commit) = committed.filter(|commit| unseen(commit.position())) else {
            return ControlFlow::Continue(None);
        };
        self.releases.committed(commit);
        if event_tx.send(Ok(SourceItem::Commit(commit))).await.is_err() {
            return ControlFlow::Break(());
        }
        // The consumer may have acknowledged past this commit before it arrived.
        ControlFlow::Continue(self.releases.release())
    }

    /// Record the consumer's acknowledgement, returning the flush position it
    /// releases, if any.
    fn acknowledge(&mut self, upto: PgCommitPosition) -> Option<PgLsn> {
        self.releases.acknowledge(upto);
        self.releases.release()
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

/// Read `IDENTIFY_SYSTEM` and, above timeline 1, the current timeline's history.
fn read_cluster_identity(
    conn: &mut PgReplicationConnection,
) -> Result<ClusterIdentity, PgStreamingError> {
    let ident = conn.identify_system()?;
    if ident.ntuples() == 0 {
        return Err(PgStreamingError::Protocol(
            "IDENTIFY_SYSTEM returned no row; is the connection in \
             replication=database mode?"
                .to_string(),
        ));
    }
    let system_id = ident
        .get_value(0, 0)
        .and_then(|text| text.trim().parse::<u64>().ok())
        .ok_or_else(|| {
            PgStreamingError::Protocol(format!(
                "IDENTIFY_SYSTEM returned an unreadable systemid: {:?}",
                ident.get_value(0, 0)
            ))
        })?;
    let timeline = ident
        .get_value(0, 1)
        .and_then(|text| text.trim().parse::<u32>().ok())
        .ok_or_else(|| {
            PgStreamingError::Protocol(format!(
                "IDENTIFY_SYSTEM returned an unreadable timeline: {:?}",
                ident.get_value(0, 1)
            ))
        })?;
    // The server has no history file for timeline 1 and rejects the command there.
    let history = if timeline > 1 {
        let reply = conn.exec(&format!("TIMELINE_HISTORY {timeline}"))?;
        let content = (reply.ntuples() == 1)
            .then(|| reply.get_value(0, 1))
            .flatten()
            .ok_or_else(|| {
                PgStreamingError::Protocol(format!(
                    "TIMELINE_HISTORY {timeline} returned {} rows, expected one file",
                    reply.ntuples()
                ))
            })?;
        parse_timeline_history(&content)?
    } else {
        Vec::new()
    };
    Ok(ClusterIdentity {
        system_id,
        timeline,
        history,
    })
}

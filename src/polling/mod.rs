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
//!   same WAL stream and asserts they observe identical events. This
//!   catches push-side parser bugs, `tokio-postgres` `CopyBothDuplex`
//!   framing regressions, and Materialize-fork drift.
//! - **Benchmark subject**: the workload-matrix examples in
//!   `examples/phase{1..5}_*.rs` measure both transports across
//!   regimes. Polling is required to be a first-class trait impl so
//!   the benchmark harness is generic over `S: CdcSource`.
//!
//! # Latency characteristics
//!
//! Polling adds roughly `poll_interval / 2` average latency on top of
//! the wire RTT (events arrive at PG's WAL and must wait for the next
//! polling cycle to be observed). At 100 ms polling cadence this is
//! ~50 ms per event; at 1 s it is ~500 ms. See
//! `docs/pg-streaming-design.md` § "Empirical polling-vs-push latency"
//! for measured numbers.
//!
//! # Ack semantics differ from push
//!
//! Polling uses `pg_logical_slot_get_binary_changes`, which
//! auto-advances the slot's `confirmed_flush_lsn` as a side effect of
//! the drain. Consequently, [`PollingPgCdcSource::ack`] is a **no-op**
//! — by the time the consumer has the event, the slot has already
//! advanced past it. Consumer code that wants at-least-once semantics
//! on a polling source must persist its own progress out-of-band
//! before processing each event.

#![cfg(feature = "pg-streaming")]

use alloc::format;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::str::FromStr;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;

use sql_traits::prelude::DatabaseLike;
use tokio_postgres::{Config as PgClientConfig, NoTls};

use crate::wal::PgOutputParser;
use crate::{PgLsn, WalEvent, WalParser};

/// Configuration for a [`PollingPgCdcSource`].
///
/// The caller is responsible for creating the publication and the
/// replication slot before constructing the source. The slot must use
/// the `pgoutput` plugin (same as push); the polling source drains it
/// via `pg_logical_slot_get_binary_changes` rather than
/// `START_REPLICATION`.
///
/// `#[non_exhaustive]` so future fields can be added without breaking
/// downstream call sites. Construct via [`PollingPgCdcConfig::new`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PollingPgCdcConfig {
    /// libpq connection string. Unlike the push source's config, this
    /// does **not** need to enable replication mode — polling uses the
    /// regular SQL transport to call `pg_logical_slot_get_binary_changes`.
    pub url: String,
    /// Name of the pre-created pgoutput logical replication slot.
    pub slot_name: String,
    /// Name of the publication the slot follows.
    pub publication_name: String,
    /// Cadence at which the inner task issues
    /// `pg_logical_slot_get_binary_changes`. Smaller intervals reduce
    /// observed latency but raise query overhead on the server. Default
    /// 100 ms (a common polling-client compromise).
    pub poll_interval: Duration,
    /// Bounded back-pressure ceiling for the internal event channel.
    /// When the consumer is slow, the source stops draining once this
    /// many events are queued.
    pub buffer_capacity: usize,
}

impl PollingPgCdcConfig {
    /// Build a config with sensible defaults for the optional fields:
    /// `poll_interval = 100 ms`, `buffer_capacity = 1024`.
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
    /// Underlying `tokio-postgres` failure (transport, auth, server
    /// error, etc.).
    #[error("postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    /// The pgoutput payload could not be parsed.
    #[error("pgoutput parse error: {0}")]
    Parse(#[from] crate::WalParseError),
    /// Unexpected protocol shape (e.g. `pg_logical_slot_get_binary_changes`
    /// returned an unparseable row).
    #[error("polling protocol error: {0}")]
    Protocol(String),
    /// The inner polling task has exited; the source can no longer
    /// produce events.
    #[error("polling source shut down")]
    SourceClosed,
}

/// Polling-based Postgres CDC source.
///
/// Construct with [`PollingPgCdcSource::connect`]; once constructed,
/// implements [`crate::CdcSource`] and the consumer drives the
/// `next_event` / `ack` loop. See the module-level docs for when to
/// choose polling over the default push source.
pub struct PollingPgCdcSource<DB: DatabaseLike> {
    config: PollingPgCdcConfig,
    catalog: Arc<DB>,
    /// Consumer-facing event channel.
    event_rx: tokio::sync::mpsc::Receiver<Result<WalEvent<PgLsn>, PollingPgCdcError>>,
    /// Cumulative count of `pg_logical_slot_get_binary_changes` queries
    /// the inner task has issued since `connect`.
    polls_issued: Arc<AtomicU64>,
    /// Cumulative count of events the inner task has pushed onto the
    /// consumer channel since `connect`. Symmetric with
    /// [`crate::PgStreamingCdcSource::events_received`].
    events_received: Arc<AtomicU64>,
    /// Cumulative count of polls that returned zero events. A high
    /// value at idle is the operational cost of polling — every empty
    /// poll is a wasted round-trip.
    empty_polls_observed: Arc<AtomicU64>,
    /// Running sum of events per non-empty drain. Combined with
    /// `polls_issued - empty_polls_observed` to compute the average
    /// drain batch size. Exposed by
    /// [`PollingPgCdcSource::average_drain_batch_size`].
    total_drained_events: Arc<AtomicU64>,
    /// Cooperative shutdown signal, mirroring the push source.
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    /// Set to `true` by an in-task drop guard when the polling task
    /// exits via any path. Mirrors the push source.
    task_exited: Arc<AtomicBool>,
    /// Inner-task join handle. Kept so `Drop` can call `.abort()` as
    /// a backstop.
    task: tokio::task::JoinHandle<()>,
}

impl<DB: DatabaseLike + 'static> PollingPgCdcSource<DB> {
    /// Open a regular SQL connection to Postgres, validate the slot
    /// exists, and spawn the inner task that drains the slot at the
    /// configured cadence.
    ///
    /// # Pre-conditions
    ///
    /// - The publication `config.publication_name` must already exist.
    /// - The slot `config.slot_name` must already exist with the
    ///   `pgoutput` plugin.
    /// - `config.url` is a libpq conninfo string. Unlike the push
    ///   source, replication mode is NOT required — polling uses the
    ///   regular SQL transport.
    pub async fn connect(
        config: PollingPgCdcConfig,
        catalog: Arc<DB>,
    ) -> Result<Self, PollingPgCdcError> {
        // Regular SQL transport. Replication mode is intentionally NOT
        // set: the polling source uses `pg_logical_slot_get_binary_changes`
        // which is a regular function call, callable from any role
        // that has the `pg_read_server_files` (or sufficient) privileges.
        let pg_config = PgClientConfig::from_str(&config.url)?;
        let (client, connection) = pg_config.connect(NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Sanity-check the slot exists before spawning the polling
        // task. Polling a non-existent slot would silently return zero
        // rows forever; failing here gives the caller a clear error.
        let slot_check = client
            .query(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
                &[&config.slot_name],
            )
            .await?;
        if slot_check.is_empty() {
            return Err(PollingPgCdcError::Protocol(format!(
                "replication slot `{}` does not exist; the polling source \
                 will not auto-create slots",
                config.slot_name
            )));
        }

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(config.buffer_capacity);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let polls_issued = Arc::new(AtomicU64::new(0));
        let events_received = Arc::new(AtomicU64::new(0));
        let empty_polls_observed = Arc::new(AtomicU64::new(0));
        let total_drained_events = Arc::new(AtomicU64::new(0));
        let task_exited = Arc::new(AtomicBool::new(false));

        let task_polls = Arc::clone(&polls_issued);
        let task_events = Arc::clone(&events_received);
        let task_empty = Arc::clone(&empty_polls_observed);
        let task_total = Arc::clone(&total_drained_events);
        let task_exited_for_task = Arc::clone(&task_exited);
        let task_catalog = Arc::clone(&catalog);
        let task_slot = config.slot_name.clone();
        let task_publication = config.publication_name.clone();
        let task_interval = config.poll_interval;

        let task = tokio::spawn(polling_task(
            client,
            task_catalog,
            task_slot,
            task_publication,
            task_interval,
            event_tx,
            task_polls,
            task_events,
            task_empty,
            task_total,
            shutdown_rx,
            task_exited_for_task,
        ));

        Ok(Self {
            config,
            catalog,
            event_rx,
            polls_issued,
            events_received,
            empty_polls_observed,
            total_drained_events,
            shutdown_tx: Some(shutdown_tx),
            task_exited,
            task,
        })
    }

    /// Borrow the catalog the source resolves table/column metadata
    /// against.
    #[must_use]
    pub fn catalog(&self) -> &DB {
        &self.catalog
    }

    /// Borrow the configuration the source was built with.
    #[must_use]
    pub const fn config(&self) -> &PollingPgCdcConfig {
        &self.config
    }

    /// Cumulative number of `pg_logical_slot_get_binary_changes`
    /// queries the inner task has issued since `connect`. Includes
    /// polls that returned zero events. Useful for observing the
    /// per-event server-side cost of polling vs. push (push issues no
    /// SQL queries during steady-state streaming).
    #[must_use]
    pub fn polls_issued(&self) -> u64 {
        self.polls_issued.load(Ordering::Relaxed)
    }

    /// Cumulative number of `WalEvent`s the inner task has pushed onto
    /// the consumer channel. Symmetric with
    /// [`crate::PgStreamingCdcSource::events_received`].
    #[must_use]
    pub fn events_received(&self) -> u64 {
        self.events_received.load(Ordering::Relaxed)
    }

    /// Cumulative number of polls that returned zero events. A high
    /// value at idle is the operational cost of polling: every empty
    /// poll is a wasted server round-trip. The push transport has no
    /// analogue because it doesn't issue queries at idle.
    #[must_use]
    pub fn empty_polls_observed(&self) -> u64 {
        self.empty_polls_observed.load(Ordering::Relaxed)
    }

    /// Average number of events returned per non-empty drain. `0.0` if
    /// no non-empty drains have happened yet. Larger averages mean
    /// each polling round-trip amortizes more events; very small
    /// averages signal that polling is paying its overhead repeatedly
    /// for thin batches.
    #[must_use]
    pub fn average_drain_batch_size(&self) -> f64 {
        let total = self.total_drained_events.load(Ordering::Relaxed);
        let polls = self.polls_issued.load(Ordering::Relaxed);
        let empty = self.empty_polls_observed.load(Ordering::Relaxed);
        let non_empty = polls.saturating_sub(empty);
        if non_empty == 0 {
            0.0
        } else {
            #[allow(clippy::cast_precision_loss)]
            {
                total as f64 / non_empty as f64
            }
        }
    }

    /// `true` once the inner task has exited (cooperative shutdown,
    /// abort, or upstream-side close). Symmetric with the push source.
    #[must_use]
    pub fn task_exited(&self) -> bool {
        self.task_exited.load(Ordering::Relaxed)
    }

    /// Clone the `task_exited` flag so callers can observe the inner
    /// task's exit even after the source itself has been dropped.
    /// Symmetric with the push source.
    #[must_use]
    pub fn task_exited_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.task_exited)
    }
}

impl<DB: DatabaseLike> Drop for PollingPgCdcSource<DB> {
    fn drop(&mut self) {
        drop(self.shutdown_tx.take());
        self.task.abort();
    }
}

// The polling task closes over a lot of state. As with the streaming
// task in `crate::wal::pg_streaming`, the argument list is honest
// about what it owns; a state struct just to satisfy the lint is more
// obscure than the straight signature.
#[allow(clippy::too_many_arguments)]
async fn polling_task<DB: DatabaseLike>(
    client: tokio_postgres::Client,
    catalog: Arc<DB>,
    slot_name: String,
    publication_name: String,
    poll_interval: Duration,
    event_tx: tokio::sync::mpsc::Sender<Result<WalEvent<PgLsn>, PollingPgCdcError>>,
    polls_issued: Arc<AtomicU64>,
    events_received: Arc<AtomicU64>,
    empty_polls_observed: Arc<AtomicU64>,
    total_drained_events: Arc<AtomicU64>,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    task_exited: Arc<AtomicBool>,
) {
    // Drop guard. Same shape as the streaming task: sets `task_exited`
    // regardless of exit path (normal, panic, abort).
    struct ExitGuard(Arc<AtomicBool>);
    impl Drop for ExitGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
    let _exit_guard = ExitGuard(task_exited);

    let parser = PgOutputParser::new();

    // Prepared statement for the polling query. The arguments
    // (`proto_version`, `publication_names`) are pgoutput plugin
    // options and must match the slot's expectations.
    let query = format!(
        "SELECT data FROM pg_logical_slot_get_binary_changes(\
            '{slot_name}', NULL, NULL, \
            'proto_version', '1', \
            'publication_names', '{publication_name}'\
        )"
    );

    let mut ticker = tokio::time::interval(poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Burn the first tick (fires immediately by default).
    ticker.tick().await;

    tokio::pin!(shutdown_rx);

    loop {
        tokio::select! {
            biased;
            // Cooperative shutdown polled first.
            _ = &mut shutdown_rx => break,
            _ = ticker.tick() => {
                polls_issued.fetch_add(1, Ordering::Relaxed);
                let rows = match client.query(query.as_str(), &[]).await {
                    Ok(rows) => rows,
                    Err(e) => {
                        let _ = event_tx
                            .send(Err(PollingPgCdcError::Postgres(e)))
                            .await;
                        return;
                    }
                };

                if rows.is_empty() {
                    empty_polls_observed.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // Track non-empty drain size for `average_drain_batch_size`.
                // Counted as number of EVENTS produced, not number of
                // pgoutput messages — a Relation message yields 0
                // events, an Insert yields 1, etc.
                let mut events_in_this_drain: u64 = 0;

                for row in rows {
                    let bytes: Vec<u8> = row.get(0);
                    // pgoutput XLogData wrappers are NOT present here:
                    // `pg_logical_slot_get_binary_changes` returns the
                    // plugin payload directly (the bytes that would
                    // sit inside an XLogData frame on a streaming
                    // connection). Polling has no per-message LSN
                    // available — the slot advances atomically on
                    // drain, so individual events get `None` for
                    // their checkpoint. Consumers needing per-event
                    // LSN should use the push source.
                    let events = match parser.parse_wal_message(&bytes, &*catalog) {
                        Ok(events) => events,
                        Err(e) => {
                            let _ = event_tx
                                .send(Err(PollingPgCdcError::Parse(e)))
                                .await;
                            return;
                        }
                    };
                    for ev in events {
                        let typed = ev.set_checkpoint(None::<PgLsn>);
                        events_received.fetch_add(1, Ordering::Relaxed);
                        events_in_this_drain += 1;
                        if event_tx.send(Ok(typed)).await.is_err() {
                            return;
                        }
                    }
                }

                total_drained_events.fetch_add(
                    events_in_this_drain,
                    Ordering::Relaxed,
                );
            }
        }
    }
}

impl<DB: DatabaseLike + 'static> crate::CdcSource for PollingPgCdcSource<DB> {
    type Checkpoint = PgLsn;
    type Error = PollingPgCdcError;

    // `manual_async_fn` would have us collapse these into `async fn`,
    // but native `async fn in trait` does not guarantee `Send` futures.
    // Same constraint as the push source.
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
        _upto: Self::Checkpoint,
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send {
        // No-op: `pg_logical_slot_get_binary_changes` auto-advances
        // the slot's `confirmed_flush_lsn` as a side effect of the
        // drain. By the time the consumer has the event, the slot has
        // already advanced past it. See module-level docs.
        async move { Ok(()) }
    }
}

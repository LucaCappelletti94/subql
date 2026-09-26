// All `Arc` arguments are consumed by the loop's `Drop`-on-exit guard
// or held for the loop's lifetime, so `clippy::needless_pass_by_value`
// would only push us toward `Arc::clone` at every call site without a
// real readability win.

use alloc::format;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;

use pg_walstream::{parse_lsn, Lsn, PgOutputDecoder, PgReplicationConnection};

use crate::wal::{PgChangeEvent, PgOutputOrder, PgSourceItem, ReleaseQueue, SourceItem};
use crate::PgCommitPosition;

use super::helpers::{hex_decode, render_lsn, sql_string_literal};
use super::PollingPgCdcError;

/// Highest position the consumer has acknowledged since the last look, if
/// it acknowledged anything.
fn acknowledged(ack_rx: &std::sync::mpsc::Receiver<PgCommitPosition>) -> Option<PgCommitPosition> {
    let mut upto = None;
    while let Ok(position) = ack_rx.try_recv() {
        upto = upto.max(Some(position));
    }
    upto
}

#[allow(
    clippy::too_many_arguments,
    clippy::needless_pass_by_value,
    clippy::too_many_lines
)]
pub(super) fn polling_loop(
    mut conn: PgReplicationConnection,
    slot_name: String,
    publication_name: String,
    poll_interval: Duration,
    event_tx: tokio::sync::mpsc::Sender<Result<PgSourceItem, PollingPgCdcError>>,
    ack_rx: std::sync::mpsc::Receiver<PgCommitPosition>,
    polls_issued: Arc<AtomicU64>,
    events_received: Arc<AtomicU64>,
    empty_polls_observed: Arc<AtomicU64>,
    total_drained_events: Arc<AtomicU64>,
    non_empty_drains: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    task_exited: Arc<AtomicBool>,
) {
    let _exit_guard = crate::wal::ExitFlagGuard(task_exited);

    let mut decoder = PgOutputDecoder::with_protocol_version(1);
    let mut order = PgOutputOrder::new();
    let mut rows: Vec<PgChangeEvent> = Vec::new();
    let mut releases = ReleaseQueue::new();
    // A peek re-reads every unacknowledged transaction at the same positions.
    let mut last_delivered: Option<PgCommitPosition> = None;
    let unseen = |last: Option<PgCommitPosition>, position| last.is_none_or(|last| position > last);
    let slot = sql_string_literal(&slot_name);
    let publication = sql_string_literal(&publication_name);

    // Peeking leaves the slot where it is, so it advances on an ack and
    // nowhere else, which is what `CdcSource::ack` promises. A consumer that
    // dies with events buffered then loses a read rather than the events.
    //
    // pg_walstream's `exec` uses libpq's `PQexec`, which returns every column
    // in text format, so the payload goes through `encode(data, 'hex')` and is
    // decoded here.
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        std::thread::sleep(poll_interval);
        if shutdown.load(Ordering::Relaxed) {
            return;
        }

        if let Some(upto) = acknowledged(&ack_rx) {
            releases.acknowledge(upto);
        }
        if let Some(release) = releases.release() {
            // `get` with an upper bound consumes whole transactions and
            // stops at the commit, so the server does the boundary
            // arithmetic that `pg_replication_slot_advance` would not.
            let consume = format!(
                "SELECT 1 FROM pg_logical_slot_get_binary_changes(\
                    {slot}, '{}'::pg_lsn, NULL, \
                    'proto_version', '1', \
                    'publication_names', {publication}\
                )",
                render_lsn(release.0)
            );
            if let Err(e) = conn.exec(&consume) {
                let _ = event_tx.blocking_send(Err(PollingPgCdcError::Postgres(e)));
                return;
            }
        }

        let query = format!(
            "SELECT lsn::text, encode(data, 'hex') FROM pg_logical_slot_peek_binary_changes(\
                {slot}, NULL, NULL, \
                'proto_version', '1', \
                'publication_names', {publication}\
            )"
        );

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
            let raw_lsn = match parse_lsn(&lsn_text) {
                Ok(v) => v,
                Err(e) => {
                    let _ = event_tx.blocking_send(Err(PollingPgCdcError::Postgres(e)));
                    return;
                }
            };
            let change = match decoder.decode_message(bytes, Lsn::new(raw_lsn)) {
                Ok(Some(change)) => change,
                Ok(None) => continue,
                Err(e) => {
                    let _ = event_tx.blocking_send(Err(PollingPgCdcError::Postgres(e)));
                    return;
                }
            };
            let committed = match order.apply(change, &mut rows) {
                Ok(committed) => committed,
                Err(e) => {
                    let _ = event_tx.blocking_send(Err(e.into()));
                    return;
                }
            };
            #[expect(
                clippy::iter_with_drain,
                reason = "the buffer is reused for every message"
            )]
            for ev in rows.drain(..) {
                if !unseen(last_delivered, ev.position()) {
                    continue;
                }
                last_delivered = Some(ev.position());
                events_received.fetch_add(1, Ordering::Relaxed);
                total_drained_events.fetch_add(1, Ordering::Relaxed);
                if !drain_counted {
                    drain_counted = true;
                    non_empty_drains.fetch_add(1, Ordering::Relaxed);
                }
                if event_tx.blocking_send(Ok(SourceItem::Event(ev))).is_err() {
                    return;
                }
            }
            if let Some(commit) =
                committed.filter(|commit| unseen(last_delivered, commit.position()))
            {
                last_delivered = Some(commit.position());
                releases.committed(commit);
                if event_tx
                    .blocking_send(Ok(SourceItem::Commit(commit)))
                    .is_err()
                {
                    return;
                }
            }
        }
    }
}

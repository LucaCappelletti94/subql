// All `Arc` arguments are consumed by the loop's `Drop`-on-exit guard
// or held for the loop's lifetime, so `clippy::needless_pass_by_value`
// would only push us toward `Arc::clone` at every call site without a
// real readability win.

use alloc::format;
use alloc::string::String;
use alloc::sync::Arc;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;

use pg_walstream::{parse_lsn, ChangeEvent, Lsn, PgOutputDecoder, PgReplicationConnection};

use crate::wal::into_engine_events;

use super::helpers::{hex_decode, render_lsn, sql_string_literal};
use super::PollingPgCdcError;

/// Highest position the consumer has acknowledged since the last look, if
/// it acknowledged anything.
fn acknowledged(ack_rx: &std::sync::mpsc::Receiver<u64>) -> Option<u64> {
    let mut upto = None;
    while let Ok(lsn) = ack_rx.try_recv() {
        upto = Some(upto.map_or(lsn, |held: u64| held.max(lsn)));
    }
    upto
}

/// Where a transaction acknowledged at `upto` ends, and what is left
/// outstanding once it is released.
///
/// Postgres reports every row of a transaction at the transaction's own
/// position, so an acknowledgement names a transaction and never a record,
/// and the position that releases it is the `end_lsn` its commit states.
fn release_for(
    pending: &mut alloc::vec::Vec<(u64, u64)>,
    delivered: &mut alloc::collections::BTreeSet<u64>,
    upto: u64,
) -> Option<u64> {
    let last = pending.iter().rposition(|(txn, _)| *txn <= upto)?;
    let release = pending[last].1;
    for (txn, _) in pending.drain(..=last) {
        delivered.remove(&txn);
    }
    Some(release)
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
    event_tx: tokio::sync::mpsc::Sender<Result<ChangeEvent, PollingPgCdcError>>,
    ack_rx: std::sync::mpsc::Receiver<u64>,
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
    let slot = sql_string_literal(&slot_name);
    let publication = sql_string_literal(&publication_name);

    // Peeking leaves the slot where it is, so it advances on an ack and
    // nowhere else, which is what `CdcSource::ack` promises. A consumer that
    // dies with events buffered then loses a read rather than the events.
    //
    // pg_walstream's `exec` uses libpq's `PQexec`, which returns every column
    // in text format, so the payload goes through `encode(data, 'hex')` and is
    // decoded here.
    // Delivered and unacknowledged transactions, each with the position that
    // releases it. A transaction whose end was never observed simply stays
    // here, which costs a re-read and never a loss.
    let mut pending: alloc::vec::Vec<(u64, u64)> = alloc::vec::Vec::new();
    // Positions of those transactions, so a re-peek of what is still
    // unacknowledged delivers nothing a second time.
    let mut delivered: alloc::collections::BTreeSet<u64> = alloc::collections::BTreeSet::new();
    // Held across polls, since a transaction's rows and its commit could
    // fall either side of a batch boundary.
    let mut open_txn: Option<u64> = None;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        std::thread::sleep(poll_interval);
        if shutdown.load(Ordering::Relaxed) {
            return;
        }

        if let Some(upto) = acknowledged(&ack_rx) {
            if let Some(release) = release_for(&mut pending, &mut delivered, upto) {
                // `get` with an upper bound consumes whole transactions and
                // stops at the commit, so the server does the boundary
                // arithmetic that `pg_replication_slot_advance` would not.
                let consume = format!(
                    "SELECT 1 FROM pg_logical_slot_get_binary_changes(\
                        {slot}, '{}'::pg_lsn, NULL, \
                        'proto_version', '1', \
                        'publication_names', {publication}\
                    )",
                    render_lsn(release)
                );
                if let Err(e) = conn.exec(&consume) {
                    let _ = event_tx.blocking_send(Err(PollingPgCdcError::Postgres(e)));
                    return;
                }
            }
        }

        // No position filter. A transaction's rows all carry its own
        // position and a commit carries the position the next transaction
        // begins at, so filtering on `lsn >` would drop that next
        // transaction for good. What has already been delivered is tracked
        // by transaction instead.
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
            if delivered.contains(&raw_lsn) {
                continue;
            }
            let message = match decoder.decode_message(bytes, Lsn::new(raw_lsn)) {
                Ok(message) => message,
                Err(e) => {
                    let _ = event_tx.blocking_send(Err(PollingPgCdcError::Postgres(e)));
                    return;
                }
            };
            let Some(change) = message else {
                continue;
            };
            if let pg_walstream::EventType::Commit { end_lsn, .. } = change.event_type {
                // The commit states where the transaction ends, so the
                // release point is read rather than inferred.
                if let Some(txn) = open_txn.take() {
                    pending.push((txn, end_lsn.value()));
                }
                continue;
            }
            let events = into_engine_events(change);
            if events.is_empty() {
                continue;
            }
            open_txn = Some(raw_lsn);
            delivered.insert(raw_lsn);
            for ev in events {
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

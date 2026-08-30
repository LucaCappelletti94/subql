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

use super::helpers::{hex_decode, sql_string_literal};
use super::PollingPgCdcError;

#[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
pub(super) fn polling_loop(
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

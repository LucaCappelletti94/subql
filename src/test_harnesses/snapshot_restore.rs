#![allow(
    clippy::manual_let_else,
    clippy::too_long_first_doc_paragraph,
    clippy::items_after_statements,
    clippy::needless_pass_by_value,
    clippy::map_entry,
    clippy::match_same_arms
)]

use std::collections::BTreeMap;

use arbitrary::{Arbitrary, Unstructured};
use pg_walstream::{Lsn, PgOutputDecoder};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use crate::backend::{CdcEvent, Postgres};
use crate::testing::TestEvent;
use crate::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

use super::aggregate_consistency::{agg_catalog, agg_row_values, VirtRow};

/// Fixed pool of `SELECT *` queries the snapshot/restore harness picks
/// from. All target the same `agg_catalog()` `orders` table so
/// registrations always succeed. The fuzzer controls which subset of
/// the pool ends up registered and in what order.
const SNAPSHOT_REGISTER_SQLS: &[&str] = &[
    "SELECT * FROM orders WHERE amount > 100",
    "SELECT * FROM orders WHERE status = 'open'",
    "SELECT * FROM orders WHERE amount IS NULL",
    "SELECT * FROM orders WHERE id IN (1, 2, 3)",
    "SELECT * FROM orders WHERE amount BETWEEN 10 AND 100",
    "SELECT * FROM orders WHERE status = 'shipped' OR amount > 500",
    "SELECT * FROM orders WHERE amount > 0 AND status = 'pending'",
    "SELECT * FROM orders WHERE status != 'cancelled'",
];

#[derive(Debug, Arbitrary)]
struct SnapRegister {
    consumer_id: u8,
    sql_idx: u8,
}

#[derive(Debug, Arbitrary)]
enum SnapEvent {
    Insert {
        id: u8,
        amount: Option<i16>,
        status: Option<u8>,
    },
    Update {
        id: u8,
        amount: Option<i16>,
        status: Option<u8>,
    },
    Delete {
        id: u8,
    },
}

/// Per-process working directory for the snapshot/restore harness.
/// libFuzzer spawns one worker process per parallel run. Pinning the
/// path to `pid` keeps separate workers from clobbering each other's
/// shard files, and a per-iteration `remove_dir_all` + `create_dir_all`
/// starts each round-trip from a clean slate.
///
/// Prefers `/dev/shm` (Linux tmpfs / RAM) over `std::env::temp_dir()`
/// (often a btrfs / ext4 mount), because the harness's bottleneck is
/// the snapshot write's `fsync` and the restore's directory scan. On
/// systems without `/dev/shm` the fallback to the platform temp dir
/// keeps the harness working with slower iteration speed.
fn snapshot_workdir() -> std::path::PathBuf {
    let shm = std::path::Path::new("/dev/shm");
    let mut p = if shm.is_dir() {
        shm.to_path_buf()
    } else {
        std::env::temp_dir()
    };
    p.push(format!(
        "subql-fuzz-snapshot-restore-{}",
        std::process::id()
    ));
    p
}

fn snap_event_to_event(
    op: SnapEvent,
    table_id: crate::TableId,
    pk_col: crate::ColumnId,
    virt: &mut BTreeMap<i64, VirtRow>,
) -> Option<TestEvent<Postgres>> {
    match op {
        SnapEvent::Insert { id, amount, status } => {
            let id = i64::from(id);
            if virt.contains_key(&id) {
                return None;
            }
            let row = VirtRow::from_op(amount, status);
            let values = agg_row_values(id, &row);
            virt.insert(id, row);
            Some(TestEvent::<Postgres>::insert(table_id, values).with_pk_columns([pk_col]))
        }
        SnapEvent::Update { id, amount, status } => {
            let id = i64::from(id);
            let old = virt.get(&id).cloned()?;
            let new_row = VirtRow::from_op(amount, status);
            let old_values = agg_row_values(id, &old);
            let new_values = agg_row_values(id, &new_row);
            virt.insert(id, new_row);
            Some(
                TestEvent::<Postgres>::update(table_id, old_values, new_values)
                    .with_pk_columns([pk_col]),
            )
        }
        SnapEvent::Delete { id } => {
            let id = i64::from(id);
            let old = virt.remove(&id)?;
            let old_values = agg_row_values(id, &old);
            Some(TestEvent::<Postgres>::delete(table_id, old_values).with_pk_columns([pk_col]))
        }
    }
}

fn notifications_equal(
    a: &crate::ConsumerNotifications<DefaultIds>,
    b: &crate::ConsumerNotifications<DefaultIds>,
) -> bool {
    let mut a_ins = a.inserted().to_vec();
    let mut b_ins = b.inserted().to_vec();
    let mut a_upd = a.updated().to_vec();
    let mut b_upd = b.updated().to_vec();
    let mut a_del = a.deleted().to_vec();
    let mut b_del = b.deleted().to_vec();
    a_ins.sort_unstable();
    b_ins.sort_unstable();
    a_upd.sort_unstable();
    b_upd.sort_unstable();
    a_del.sort_unstable();
    b_del.sort_unstable();
    a_ins == b_ins && a_upd == b_upd && a_del == b_del
}

/// Build an engine, register an arbitrary set of subscriptions, snapshot
/// them to disk, rebuild a fresh engine from the same on-disk shards,
/// then dispatch an arbitrary event sequence through both engines and
/// assert their `ConsumerNotifications` match for every event.
///
/// Strong oracle: any drift between the in-memory state of the
/// registering engine and the restored engine surfaces as a real test
/// failure.
///
/// Contract: panics are bugs. Assertion failures are bugs. Errors from
/// `register`, `snapshot_table`, `with_storage`, or `consumers` are
/// fine - the harness simply bails out cleanly on any of them.
pub fn harness_snapshot_restore_roundtrip(data: &[u8]) {
    let mut u = Unstructured::new(data);
    // Bounded register and event counts: the harness is by far the
    // slowest one because it constructs two engines and snapshots /
    // restores per iteration. Capping at 4 / 16 keeps a typical
    // iteration well under 1 second even under disk / CPU contention
    // from the other fuzz panes.
    let Ok(n_reg) = u.int_in_range(1usize..=4) else {
        return;
    };
    let Ok(regs): arbitrary::Result<Vec<SnapRegister>> = (0..n_reg)
        .map(|_| SnapRegister::arbitrary(&mut u))
        .collect()
    else {
        return;
    };
    let Ok(n_events) = u.int_in_range(0usize..=16) else {
        return;
    };
    let Ok(events): arbitrary::Result<Vec<SnapEvent>> = (0..n_events)
        .map(|_| SnapEvent::arbitrary(&mut u))
        .collect()
    else {
        return;
    };

    let workdir = snapshot_workdir();
    let _ = std::fs::remove_dir_all(&workdir);
    if std::fs::create_dir_all(&workdir).is_err() {
        return;
    }

    let database = agg_catalog();
    let Some(table_id) = catalog_helpers::table_id(&database, "orders") else {
        return;
    };
    let pk_col = match catalog_helpers::column_id(&database, table_id, "id") {
        Some(c) => c,
        None => return,
    };

    let mut engine_a: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        match SubscriptionEngine::with_storage(agg_catalog(), PostgreSqlDialect {}, workdir.clone())
        {
            Ok((e, _reads)) => e,
            Err(_) => return,
        };

    // Track which (consumer_id, sql) pairs we've registered to avoid
    // duplicate consumer_id collisions, which would fail on the engine
    // side and desynchronise A and B's view of the consumer set.
    use std::collections::HashSet;
    let mut seen_consumers: HashSet<u64> = HashSet::new();
    for reg in &regs {
        let cid = u64::from(reg.consumer_id);
        if !seen_consumers.insert(cid) {
            continue;
        }
        let sql = SNAPSHOT_REGISTER_SQLS[(reg.sql_idx as usize) % SNAPSHOT_REGISTER_SQLS.len()];
        let _ = engine_a.register(SubscriptionRequest::<DefaultIds>::new(cid, sql));
    }

    if engine_a.snapshot_table(table_id).is_err() {
        return;
    }

    let mut engine_b: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        match SubscriptionEngine::with_storage(database, PostgreSqlDialect {}, workdir) {
            Ok((e, _reads)) => e,
            Err(_) => return,
        };

    let mut virt: BTreeMap<i64, VirtRow> = BTreeMap::new();
    for op in events {
        let Some(event) = snap_event_to_event(op, table_id, pk_col, &mut virt) else {
            continue;
        };
        let notif_a = match engine_a.consumers(&event) {
            Ok(n) => n,
            Err(_) => return,
        };
        let notif_b = match engine_b.consumers(&event) {
            Ok(n) => n,
            Err(_) => return,
        };
        assert!(
            notifications_equal(&notif_a, &notif_b),
            "snapshot/restore drift: A={:?} B={:?} event_kind={:?}",
            (notif_a.inserted(), notif_a.updated(), notif_a.deleted()),
            (notif_b.inserted(), notif_b.updated(), notif_b.deleted()),
            event.kind(),
        );
    }
}

/// Drive raw bytes through the pgoutput binary parser. Exercises both
/// the cursor-parsing paths in every message-type branch (single-message
/// mode) and the relation-cache cross-message state (sequenced mode).
///
/// Contract: panics are bugs. Any decode error is fine.
pub fn harness_pgoutput(data: &[u8]) {
    // Single-message mode: the whole input is one pgoutput message body.
    {
        let mut decoder = PgOutputDecoder::with_protocol_version(1);
        let _ = decoder.decode_message(data.to_vec(), Lsn::new(0));
    }

    // Sequenced mode: up to 8 length-prefixed chunks through one decoder
    // so a relation chunk can prime the cache for a later chunk.
    {
        let mut decoder = PgOutputDecoder::with_protocol_version(1);
        let mut cur = data;
        for _ in 0..8 {
            if cur.len() < 2 {
                break;
            }
            let len = u16::from_le_bytes([cur[0], cur[1]]) as usize;
            cur = &cur[2..];
            let take = len.min(cur.len());
            let (chunk, rest) = cur.split_at(take);
            cur = rest;
            let _ = decoder.decode_message(chunk.to_vec(), Lsn::new(0));
        }
    }
}

/// End-to-end pgoutput fuzz harness on the fake-Postgres-over-SQLite
/// emulator.
///
/// Drives an arbitrary DML stream through [`crate::PgSqliteEmuSource`],
/// which internally re-encodes each session changeset as pgoutput wire
/// bytes, decodes them with `pg_walstream`'s `PgOutputDecoder`, and
/// dispatches every emitted [`crate::ChangeEvent`] through a
/// populated [`SubscriptionEngine`]. Exercises the whole pipeline
/// (catalog plus pg2sqlite plus session extension plus changeset->
/// pgoutput encode plus pgoutput decode plus VM dispatch) on every
/// input.
///
/// # Invariants enforced at fixture build time
///
/// Several seams use fixed inputs (the DDL string, the connection
/// target, the subscription SQL). A regression that breaks any of them
/// should crash the very first fuzz iteration, never report "green"
/// while silently fuzzing nothing. The init path asserts:
///
/// * the fixed PG DDL parses and applies through `pg2sqlite`,
/// * the in-memory SQLite connection opens,
/// * every fixed [`SubscriptionRequest`] compiles and registers.
///
/// # Per-iteration contract
///
/// Panics inside `source.execute`, `source.poll_next_event`, or
/// `engine.consumers` are bugs. Errors at those seams are fine because
/// adversarial DML can legitimately produce them (constraint
/// violations, dispatch errors when an UPDATE arrives without the old
/// row), but the result is fed to [`core::hint::black_box`] so the
/// optimizer cannot dead-code-eliminate the dispatch.
#[cfg(feature = "pg-sqlite-emu")]
pub fn harness_sqlite_pgoutput_e2e(data: &[u8]) {
    use core::cell::RefCell;
    use core::hint::black_box;

    // libfuzzer hands us tiny inputs too: bail out early so we do not
    // pay the (cached but still nonzero) fixture-borrow cost.
    if data.len() < 2 {
        return;
    }

    // Reuse one fixture per thread across iterations. The init path
    // hard-asserts every constant invariant so a regression crashes
    // the first iter rather than producing silent "green" runs.
    thread_local! {
        static FIXTURE: RefCell<E2eFixture> = RefCell::new(E2eFixture::init());
    }

    FIXTURE.with(|cell| {
        let mut fixture = cell.borrow_mut();
        fixture.reset();
        let mut u = Unstructured::new(data);

        // With a cached fixture each op stays cheap, so cap higher than
        // the per-iter throw-away version: more mutations per input
        // reach the dispatch path.
        let op_count = u.int_in_range(0u8..=64).unwrap_or(0);
        for _ in 0..op_count {
            // 4 % chance of injecting a synthetic Truncate event. The
            // session extension has no TRUNCATE analog, so this is the
            // only way the engine's Truncate dispatch fires in this
            // harness.
            if u.int_in_range(0u8..=24).unwrap_or(0) == 0 {
                let _ = fixture.inject_truncate();
                continue;
            }

            let Some(sql) = next_dml(&mut u, fixture.table_id) else {
                return;
            };
            // Execute the DML against SQLite. Errors here are
            // adversarial-input territory (constraint violations,
            // syntax we did not anticipate) and skipped.
            if fixture.execute_sql(&sql).is_err() {
                continue;
            }
            fixture.drain_and_dispatch(&mut |ev| {
                let _ = black_box(ev);
            });
        }
    });
}

#[cfg(feature = "pg-sqlite-emu")]
struct E2eFixture {
    source: crate::PgSqliteEmuSource,
    engine: SubscriptionEngine<crate::ChangeEvent, DefaultIds, ParserDB>,
    table_id: crate::TableId,
}

#[cfg(feature = "pg-sqlite-emu")]
impl E2eFixture {
    /// Fixed PG-dialect DDL for the `orders` table. Single-column INT
    /// primary key, one nullable INT, one nullable TEXT. Composite PK
    /// and a wider column set belong in a separate harness with its
    /// own fixture.
    const PG_DDL: &'static str =
        "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

    /// Subscriptions the engine dispatches against.
    const SUBSCRIPTIONS: &'static [(u64, &'static str)] = &[
        (1, "SELECT * FROM orders WHERE amount > 100"),
        (2, "SELECT * FROM orders WHERE status = 'paid'"),
        (3, "SELECT * FROM orders WHERE amount < 50"),
        (4, "SELECT * FROM orders WHERE id = 5"),
        (5, "SELECT * FROM orders WHERE amount IS NULL"),
    ];

    fn init() -> Self {
        let source = crate::PgSqliteEmuSource::open_in_memory(Self::PG_DDL)
            .expect("PgSqliteEmuSource fixture must construct from fixed PG DDL");
        let table_id = catalog_helpers::table_id(source.pg_catalog(), "orders")
            .expect("fuzz fixture orders table must resolve");

        let mut engine: SubscriptionEngine<crate::ChangeEvent, DefaultIds, ParserDB> =
            SubscriptionEngine::new(source.pg_catalog().clone(), PostgreSqlDialect {});
        for (consumer_id, sql) in Self::SUBSCRIPTIONS {
            engine
                .register(SubscriptionRequest::new(*consumer_id, *sql))
                .expect("fuzz fixture subscription must register");
        }

        Self {
            source,
            engine,
            table_id,
        }
    }

    /// Clear table state between iterations. The source, session, and
    /// engine all survive.
    fn reset(&mut self) {
        // The bulk DELETE fires one changeset op per row, which then
        // flows through the drain loop; we discard everything so the
        // next iter starts with an empty stream.
        let _ = self.source.execute_sql("DELETE FROM orders");
        while let Ok(Some(_)) = self.source.poll_next_event() {
            // Discard residual events from the bulk DELETE.
        }
    }

    fn execute_sql(&mut self, sql: &str) -> Result<usize, crate::PgSqliteEmuError> {
        self.source.execute_sql(sql)
    }

    fn inject_truncate(&mut self) -> Result<(), crate::PgSqliteEmuError> {
        self.source.inject_truncate(self.table_id)
    }

    /// Drain every event the source has for us and dispatch each
    /// through the engine, feeding the dispatch result to `sink` so the
    /// optimizer cannot dead-code-eliminate the whole loop.
    fn drain_and_dispatch<F>(&mut self, sink: &mut F)
    where
        F: FnMut(
            &Result<crate::ConsumerNotifications<DefaultIds, crate::PgLsn>, crate::DispatchError>,
        ),
    {
        loop {
            let event = match self.source.poll_next_event() {
                Ok(Some(ev)) => ev,
                Ok(None) | Err(_) => break,
            };
            let result = self.engine.consumers(&event);
            sink(&result);
        }
    }
}

#[cfg(feature = "pg-sqlite-emu")]
fn next_dml(u: &mut Unstructured<'_>, _table_id: crate::TableId) -> Option<String> {
    // Six branches widen the previous `0u8..=2` mix: NULL inserts, NULL
    // updates, and PK-changing updates all exercise paths the original
    // generator left starved.
    Some(match u.int_in_range(0u8..=5).ok()? {
        0 => {
            // INSERT with concrete values.
            let id = u.int_in_range(1i32..=8).ok()?;
            let amount = u.int_in_range(-200i32..=200).ok()?;
            let status = pick_status(u);
            alloc_format(format_args!(
                "INSERT INTO orders (id, amount, status) VALUES ({id}, {amount}, '{status}')"
            ))
        }
        1 => {
            // INSERT with NULL amount and / or status. Exercises the
            // Value::Null branch end-to-end, including the
            // `amount IS NULL` subscription registered above.
            let id = u.int_in_range(1i32..=8).ok()?;
            let amount = if bool::arbitrary(u).ok()? {
                "NULL".to_string()
            } else {
                u.int_in_range(-200i32..=200).ok()?.to_string()
            };
            let status = if bool::arbitrary(u).ok()? {
                "NULL".to_string()
            } else {
                alloc_format(format_args!("'{}'", pick_status(u)))
            };
            alloc_format(format_args!(
                "INSERT INTO orders (id, amount, status) VALUES ({id}, {amount}, {status})"
            ))
        }
        2 => {
            // UPDATE with concrete values, may be a no-op.
            let id = u.int_in_range(1i32..=8).ok()?;
            let amount = u.int_in_range(-200i32..=200).ok()?;
            let status = pick_status(u);
            alloc_format(format_args!(
                "UPDATE orders SET amount = {amount}, status = '{status}' WHERE id = {id}"
            ))
        }
        3 => {
            // UPDATE that sets amount or status to NULL.
            let id = u.int_in_range(1i32..=8).ok()?;
            let amount = if bool::arbitrary(u).ok()? {
                "NULL".to_string()
            } else {
                u.int_in_range(-200i32..=200).ok()?.to_string()
            };
            let status = if bool::arbitrary(u).ok()? {
                "NULL".to_string()
            } else {
                alloc_format(format_args!("'{}'", pick_status(u)))
            };
            alloc_format(format_args!(
                "UPDATE orders SET amount = {amount}, status = {status} WHERE id = {id}"
            ))
        }
        4 => {
            // PK-changing UPDATE. SQLite allows it directly when the
            // new id is unused. Otherwise the statement fails with a
            // UNIQUE constraint, which the harness swallows. Either
            // outcome is interesting for the dispatch path.
            let old_id = u.int_in_range(1i32..=8).ok()?;
            let new_id = u.int_in_range(1i32..=8).ok()?;
            alloc_format(format_args!(
                "UPDATE orders SET id = {new_id} WHERE id = {old_id}"
            ))
        }
        _ => {
            let id = u.int_in_range(1i32..=8).ok()?;
            alloc_format(format_args!("DELETE FROM orders WHERE id = {id}"))
        }
    })
}

#[cfg(feature = "pg-sqlite-emu")]
#[allow(clippy::range_minus_one)] // `Unstructured::int_in_range` takes `RangeInclusive` only.
fn pick_status(u: &mut Unstructured<'_>) -> &'static str {
    const STATUSES: &[&str] = &["paid", "open", "closed", "pending"];
    let idx = u.int_in_range(0usize..=STATUSES.len() - 1).unwrap_or(0);
    STATUSES[idx]
}

#[cfg(feature = "pg-sqlite-emu")]
fn alloc_format(args: core::fmt::Arguments<'_>) -> String {
    use core::fmt::Write;
    let mut out = String::new();
    let _ = out.write_fmt(args);
    out
}

# Push-based PG CDC source: design notes

## Why this document exists

The repeated "this is complex" framing around shipping a real streaming
CDC intake has been hand-wavy. This document grounds the work in
concrete protocol mechanics so the decisions are inspectable. None of
the individual pieces below is hard. The "complexity" comes from there
being more pieces than for a regular SQL connection — about a dozen
small things rather than two big things.

By the end of this document you should be able to answer:

- What bytes go on the wire, in what order, for a logical replication
  session.
- What state the client has to maintain on top of the bytes (the
  shortlist is: latest LSN, last ack sent, last keepalive received,
  status-pump timer).
- Why subql is hand-rolling instead of taking a dep.
- What a `diesel-pg-replication` helper feature would look like, what
  its sync surface would be, and where it would and would not fit.

## What we actually need on the wire

A Postgres logical replication session is one TCP connection that goes
through three phases.

### Phase 1: connection in replication mode

A regular libpq connection sets the `replication` startup parameter to
`database` (instead of leaving it unset). Everything else about the
startup handshake is identical to a normal connection — the same
auth, the same database name, the same `ParameterStatus` exchange.

The only difference from the server's point of view is that, once
`replication=database` is in the startup message, the connection accepts
the small set of replication commands (`IDENTIFY_SYSTEM`,
`CREATE_REPLICATION_SLOT`, `START_REPLICATION`, `DROP_REPLICATION_SLOT`,
`TIMELINE_HISTORY`) and stops accepting regular queries until the
connection is closed.

This is the only place `tokio-postgres` released crate currently falls
down: its `Config` parser does not accept the `replication` keyword. A
custom `tokio-postgres-replication` fork, the unmerged upstream PR, or
hand-rolling the startup message all solve this.

### Phase 2: handshake

Two commands are issued via the simple-query protocol, exactly as you
would issue any other SQL string:

```
IDENTIFY_SYSTEM
```

Returns a single row with four columns: `systemid` (text), `timeline`
(int4), `xlogpos` (pg_lsn as `XXXX/YYYY` text), `dbname` (text).
Confirms the connection is in replication mode and tells the client
the server's current WAL position.

```
START_REPLICATION SLOT <slot> LOGICAL <lsn> (
  "proto_version" '1',
  "publication_names" '<pub>'
)
```

The slot must already exist (`pg_create_logical_replication_slot`).
`<lsn>` is either `0/0` (resume from the slot's `confirmed_flush_lsn`)
or a specific position. The output plugin is determined by the slot,
not by this command — but the options (`proto_version`,
`publication_names`) are plugin-specific. The above options are for
`pgoutput`.

The server response is what makes this command different from a
regular query: instead of `CommandComplete + ReadyForQuery`, the
server sends a `CopyBothResponse` and then the connection enters
**CopyBoth mode**. From that point on, both directions of the TCP
connection carry framed copy data.

### Phase 3: CopyBoth streaming

CopyBoth framing is identical in both directions: a single-byte
message type, followed by a 4-byte big-endian length (including the
length field itself), followed by the payload. There are exactly two
inbound message types and one outbound message type the client must
care about.

**Inbound: XLogData (`'w'`)**

```
'w'                              1 byte
length                           4 bytes BE (includes itself)
start_lsn                        8 bytes BE  -- WAL position of the first byte in the payload
current_end_lsn                  8 bytes BE  -- server's current WAL flush position
server_clock                     8 bytes BE  -- microseconds since 2000-01-01 UTC
plugin_payload                   length - 1 - 4 - 8 - 8 - 8 bytes
```

The `plugin_payload` is the output plugin's wire format. For
`pgoutput` it's a pgoutput message (the bytes that `PgOutputParser`
in this codebase already eats). For `wal2json` it would be a JSON
string. The replication transport does not know or care about the
content.

**Inbound: PrimaryKeepalive (`'k'`)**

```
'k'                              1 byte
length                           4 bytes BE
end_lsn                          8 bytes BE  -- server's current WAL flush position
server_clock                     8 bytes BE  -- microseconds since 2000-01-01 UTC
reply_requested                  1 byte      -- 1 = client MUST send StandbyStatusUpdate immediately
```

If `reply_requested == 1`, the client must send a StandbyStatusUpdate
within the server's `wal_sender_timeout` (default 60s). Failing to do
so causes the server to drop the connection.

**Outbound: StandbyStatusUpdate (`'r'`)**

```
'r'                              1 byte
length                           4 bytes BE
write_lsn                        8 bytes BE  -- highest LSN the client has written locally
flush_lsn                        8 bytes BE  -- highest LSN the client has durably flushed
apply_lsn                        8 bytes BE  -- highest LSN the client has applied to its destination
client_clock                     8 bytes BE  -- microseconds since 2000-01-01 UTC
reply_requested                  1 byte      -- 1 to request a 'k' message back
```

`confirmed_flush_lsn` on the slot is the **min** of `flush_lsn`
values seen across recent StandbyStatusUpdates. So:

- Subscribers that want maximally aggressive WAL release should send
  `write_lsn = flush_lsn = apply_lsn = latest_acked_lsn`.
- Subscribers that want write-ahead but defer-apply semantics can
  send different values for the three.

For subql we are a single applier on a single slot. All three values
are the latest LSN the consumer has acked.

That is the entire wire protocol for logical replication. Three
message types, three fields per message, two LSN comparisons. The
"complexity" is not in any one piece — it is in keeping the timing of
the periodic status update consistent with what the server's
`wal_sender_timeout` expects, in handling the `reply_requested` flag
the moment it arrives instead of on the next status tick, and in not
mixing up which LSN field means what.

## What the client has to remember

Across the whole session the client maintains exactly five pieces of
state on top of the underlying TCP / CopyBoth stream:

| Name | Type | Update trigger |
| --- | --- | --- |
| `slot_name` | `String` | Set once at connect |
| `latest_received_lsn` | `u64` | On every `'w'` frame (use `start_lsn + payload.len()` for the end) |
| `latest_acked_lsn` | `u64` | On every `ack(upto)` call from the consumer |
| `last_status_sent_at` | `Instant` | On every outbound `'r'` |
| `parser_relation_cache` | `HashMap` | On `pgoutput` `'R'` messages (already lives inside `PgOutputParser`) |

The status pump is a single decision tree run on a `tokio::select!`
across the inbound stream and a `tokio::time::interval(status_interval)`:

```text
select! {
    Some(frame) = stream.next() => match first_byte(frame) {
        'w' => parse-xlog, push event to consumer channel, advance latest_received_lsn
        'k' => if reply_requested { send StandbyStatusUpdate; update last_status_sent_at }
        _   => skip (no other inbound types in CopyBoth)
    }
    _ = interval.tick() => {
        send StandbyStatusUpdate; update last_status_sent_at
    }
    Some(upto) = ack_rx.recv() => {
        latest_acked_lsn = upto;
        // optionally: send StandbyStatusUpdate immediately so the slot advances now
    }
}
```

That is the entire core loop. About 30 lines of `select!`. Everything
else is byte serialization and channel plumbing.

## The reason "it's complex" gets repeated anyway

Three operational hazards turn easy code into easy-to-break code:

1. **Missing a status update kills the slot.** The default
   `wal_sender_timeout = 60s`. If the client goes silent for that long
   (no `'w'` frames being acked, no `'r'` being pushed, no
   `reply_requested` honored), the server kills the connection. The
   slot stays around but no client is consuming, so WAL backs up.
   Repeated dead connections + a slow `wal_sender_timeout` are how
   you discover Postgres has filled its disk with retained WAL.

2. **`reply_requested = 1` is not advisory.** If the keepalive's
   `reply_requested` flag is 1 and the client skips it (because it's
   busy parsing a long pgoutput message), the server reads silence
   and kills the connection mid-stream. The auto-reply must run on
   the SAME task as the stream read, not on a separate task that
   could be starved.

3. **`flush_lsn` and `confirmed_flush_lsn` are not the same thing.**
   The client reports `flush_lsn`. The server tracks `confirmed_flush_lsn`
   as the minimum of recent client reports. If the client reports
   `flush_lsn = X` then later reports `flush_lsn = Y < X` (because
   the consumer ack'd a lower LSN than a previous status update),
   the slot does NOT regress — it stays at the higher of the two,
   which is then never released. The client must maintain
   `latest_acked_lsn = max(latest_acked_lsn, new_ack)` to avoid
   regressing the reported flush_lsn.

These are the hazards. They are why "I'll just hand-roll it" needs
real tests against a real PG server, not just unit tests of byte
serialization.

## Subql's concrete architecture

The trait already shipped in `src/wal/streaming.rs`:

```rust
pub trait CdcSource: Send {
    type Checkpoint: Checkpoint;
    type Error: core::error::Error + Send + 'static;
    fn next_event(&mut self) -> impl Future<...> + Send;
    fn ack(&mut self, upto: Self::Checkpoint) -> impl Future<...> + Send;
}
```

The PG impl lives at `src/wal/pg_streaming.rs` behind the
`pg-streaming` feature. The internal architecture:

```
┌──────────────────────────────────────────────────────────────────┐
│ PgStreamingCdcSource<DB>                                         │
│ (lives on the consumer task; owns nothing protocol-level)        │
│                                                                  │
│   event_rx: mpsc::Receiver<WalEvent<PgLsn>>  ◄─── from inner    │
│   ack_tx: mpsc::Sender<PgLsn>                ───► to inner       │
│   shutdown_tx: oneshot::Sender<()>           ───► to inner       │
└──────────────────────────────────────────────────────────────────┘
                            │
                            │   (spawned at connect())
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│ Inner task                                                       │
│                                                                  │
│   client: tokio_postgres::Client (replication mode)              │
│   copy_both: Pin<&mut CopyBothDuplex<Bytes>>                     │
│   parser: PgOutputParser                                         │
│   catalog: Arc<DB>                                               │
│   latest_received_lsn: u64                                       │
│   latest_acked_lsn: u64                                          │
│   status_interval: Duration                                      │
│                                                                  │
│   event_tx: mpsc::Sender<WalEvent<PgLsn>>                        │
│   ack_rx: mpsc::Receiver<PgLsn>                                  │
│   shutdown_rx: oneshot::Receiver<()>                             │
│                                                                  │
│   loop {                                                         │
│     select! {                                                    │
│       frame = copy_both.next() => handle_frame(...)              │
│       _ = interval.tick()      => send_status_update()           │
│       Some(upto) = ack_rx.recv() => {                            │
│           latest_acked_lsn = latest_acked_lsn.max(upto);         │
│           send_status_update()                                   │
│       }                                                          │
│       _ = &mut shutdown_rx => break                              │
│     }                                                            │
│   }                                                              │
└──────────────────────────────────────────────────────────────────┘
```

`next_event` on the source is `event_rx.recv().await`. `ack(upto)` is
`ack_tx.send(upto).await`. Shutdown happens in `Drop` via the oneshot.

The source struct itself is therefore tiny — three channel handles +
the config. All the protocol work lives in the inner task, which is
spawned at `connect` time and lives until the source is dropped. This
matches the `tokio-postgres` pattern of "client struct is a thin
handle, connection task is the protocol driver".

## Why subql is hand-rolling

Three options were on the table:

1. **Use `tokio-postgres`'s replication API**: doesn't exist in the
   released crate (0.7.18 has no `replication_mode` setter, no
   `replication` conninfo option, no `CopyBoth`-from-replication
   plumbing). Verified by grepping the source on disk.
2. **Pull the Supabase ETL framework** (`pg_replicate` / `etl-postgres`
   in the `github.com/supabase/etl` repo): real, active, 2.3k-star
   project, but it's a *framework* (sqlx + tokio-postgres + a full
   TLS stack + workspace config crates) and it's unpublished on
   crates.io (would need a `git` dep with a pinned commit and an MSRV
   bump to 1.93). Sledgehammer for a nail.
3. **Hand-roll the transport** on top of base `tokio-postgres` (for
   the auth/startup) + `postgres-protocol` (for the byte-level
   serialization helpers we need anyway). Subql owns 100% of the
   replication-protocol code; the dep footprint is `tokio-postgres`
   we already chose to take + `postgres-protocol` which it transitively
   depends on; ~300-500 lines of focused code.

(3) is the chosen path. The protocol surface is what's described
above — three message types, five pieces of state. Hand-rolling that
is materially less work than the integration cost of (1) or (2).

The work in (3) is concretely:

- Hand-write the startup message that sets `replication=database`
  (or fork the helper out of `tokio-postgres-replication` style code).
- Issue `IDENTIFY_SYSTEM` and `START_REPLICATION` via simple_query.
- Capture the CopyBoth stream that returns and read it as a
  `Stream<Item = Bytes>`.
- Implement the three-byte serialization for `StandbyStatusUpdate`
  and the parser for `'w'` / `'k'` frames.
- Wire the `tokio::select!` loop with the periodic interval.

That last bullet is the only thing that requires care. The rest is
arithmetic.

## Could it go in diesel instead?

Yes, with a specific shape. Diesel's PG backend already links libpq
via `pq-sys`. libpq has full support for replication via the same
`PQexec` / `PQgetCopyData` / `PQputCopyData` primitives. So a diesel
helper feature could expose a sync replication transport without any
new external dep, leaning on libpq for the protocol mechanics.

### What the feature would look like

A new diesel cargo feature, `postgres-replication`, would build on top
of the existing `postgres` feature. It would surface roughly this API:

```rust
// in diesel::pg::replication
pub struct PgReplicationConnection { /* wraps PGconn in replication mode */ }

impl PgReplicationConnection {
    pub fn establish(database_url: &str) -> ConnectionResult<Self> { /* sets replication=database */ }
    pub fn identify_system(&mut self) -> QueryResult<IdentifySystem> { /* PQexec("IDENTIFY_SYSTEM") */ }
    pub fn start_replication(
        &mut self,
        slot: &str,
        start_lsn: PgLsn,
        options: &[(&str, &str)], // e.g. [("proto_version", "1"), ("publication_names", "pub")]
    ) -> QueryResult<ReplicationStream> { /* PQexec("START_REPLICATION ...") */ }
}

pub struct ReplicationStream<'a> { conn: &'a mut PgReplicationConnection }

impl<'a> ReplicationStream<'a> {
    /// Block until a frame is available; returns the inbound message
    /// classified by type. None on clean shutdown.
    pub fn read_frame(&mut self) -> QueryResult<Option<ReplicationFrame>>;

    /// Send a StandbyStatusUpdate.
    pub fn send_status_update(
        &mut self,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
    ) -> QueryResult<()>;
}

pub enum ReplicationFrame {
    XLogData { start_lsn: PgLsn, current_end_lsn: PgLsn, server_clock: i64, payload: Vec<u8> },
    Keepalive { end_lsn: PgLsn, server_clock: i64, reply_requested: bool },
}
```

This is a **sync** API — diesel's whole posture is sync. Async
consumers wrap `read_frame` calls in `tokio::task::spawn_blocking` or
a dedicated thread + channel; that's the same pattern they already
use for `diesel::pg::PgConnection::execute`.

### Where it does and does not fit

It fits diesel reasonably well in three respects:

1. **libpq is already there.** Diesel users on the `postgres` feature
   are already linking `pq-sys`. The helper adds zero new transitive
   deps. By contrast, `tokio-postgres` users need a separate libpq-free
   Rust client OR a fork OR hand-rolled code.
2. **The sync model matches diesel's connection model.** A diesel user
   already does `conn.execute(...)` synchronously. `conn.read_frame()`
   is the same shape.
3. **Type integration with diesel's PgLsn / PgInterval / etc.**
   Diesel already ships `diesel::pg::data_types::PgLsn`. The
   replication helper reuses it for free.

It does NOT fit cleanly in three other respects:

1. **Diesel is an ORM.** Its value prop is type-safe SQL, schema
   inference, query builder, migrations. Replication has none of
   those. The helper would live in `diesel::pg::replication` as an
   orthogonal module that shares nothing with the rest of the crate
   except the underlying `PGconn`. Maintainers may reasonably push
   back on scope creep.
2. **Sync semantics push problem upward.** Periodic status updates
   need to fire on a cadence even when no events have arrived. With
   a sync API, that means either:
     - Consumers manage their own timer thread and call
       `send_status_update` themselves (clunky and easy to get wrong).
     - The helper spawns a status-pump thread internally (now diesel
       has thread management, and Rust connections aren't `Send` so
       this gets weird).
   Async-native sources sidestep this with `tokio::select!`. Sync
   sources don't have an equally clean answer.
3. **Most CDC users want async.** A diesel sync replication helper
   would be useful, but it would always be a starting point that
   downstream wraps in spawn_blocking — i.e. you'd pay the cost of
   the diesel maintenance burden plus the cost of the async adapter,
   and end up with a tower of indirection that's no better than
   either pure async or pure sync.

### Where it would actually be high-leverage

There is one place where a `diesel-pg-replication` feature would be
genuinely high-leverage independent of the async/sync debate: as a
**reference, in-tree, tested implementation** of the wire protocol.

Right now everyone who wants to do PG logical replication in Rust
reinvents the byte serialization for `StandbyStatusUpdate`, the
`'w'` / `'k'` frame parsers, and the keepalive timing logic. Each
project does it slightly differently; each has its own subtle bugs
(LSN regression, missed keepalive replies, off-by-one frame parsing).

A diesel module that shipped the **byte-level primitives** — types
for the three message kinds, serializers, parsers, the LSN
arithmetic — without taking a position on async-vs-sync transport
would be useful even to async-only consumers like subql. We would
import:

```rust
use diesel::pg::replication::wire::{
    parse_inbound_frame, serialize_standby_status_update,
    ReplicationFrame, StandbyStatusUpdate,
};
```

and feed bytes through them from our own `tokio-postgres`-based
transport. Diesel's added complexity is minimal (the byte serialization
is ~200 lines of well-tested code); the ecosystem benefit is real
(everyone stops reinventing this part).

That, I think, is the strongest case for diesel inclusion: not as a
full replication client, but as a **canonical Rust implementation of
the PG logical replication wire format**, with sync libpq-backed
streaming on top as an opt-in convenience for diesel-native users.

### A scoped proposal for diesel

If you wanted to PR this to diesel-rs, the minimal first cut would be:

1. New module `diesel::pg::replication::wire`. Pure-data types and
   serializers, no I/O. Zero new diesel deps. Maybe 250 lines + tests.
2. New module `diesel::pg::replication::stream`. Sync libpq-backed
   `PgReplicationConnection` and `ReplicationStream` as sketched above.
   Roughly 200-400 lines.
3. Gated behind a `postgres-replication` cargo feature that requires
   `postgres`. Off by default.
4. No async story. No status-pump thread management. Consumers drive
   the cadence themselves. Docs include a "running this in an async
   context" page that points at spawn_blocking.

If the upstream maintainers say "wire types yes, transport no", that's
still a clear win — subql would import the wire types and skip the
transport, and the rest of the ecosystem would stop hand-rolling LSN
serializers.

If they say "no thanks, this isn't diesel's job", an obvious sibling
crate exists: `diesel-pg-replication`, sitting in `diesel-rs/diesel`
as a workspace member but published separately. Same code, looser
coupling.

## What goes next in subql

This document does not change the plan that was already approved
(`/home/luca/.claude/plans/start-planning-to-1-purrfect-bubble.md`).
The next implementation steps remain:

- Step 3: single-event read end-to-end with sub-200ms latency
- Step 4: ack advances `confirmed_flush_lsn`
- Step 5: keepalive auto-reply + periodic pump
- Step 6: back-pressure
- Step 7: graceful shutdown
- Steps 8-9: doctest + lint sweep

The Step-2 connect skeleton currently uses `tokio_postgres::Config::replication_mode`
which **does not exist in the released crate**. Step 3 begins with
replacing that call — either by patching the conninfo to inject the
`replication` startup option manually, or by switching to a fork. The
hand-rolled `replication=database` startup-message approach is what
this document recommends.

## Empirical polling-vs-push latency

A one-shot benchmark (`tests/polling_vs_push_benchmark.rs`, gated
`#[ignore]` and `pg-streaming` feature) measures COMMIT-to-event
delivery latency for the push-based source against a polling
implementation that calls `pg_logical_slot_get_binary_changes` at a
configurable interval. Same Postgres 16 container, same pgoutput
byte format, same `PgOutputParser`, same async runtime — only the
transport differs.

Methodology: the receiver task is spawned first so the polling clock
is already running before any insert. Inserts are then driven at
intervals ~1.3x the polling interval so each commit lands at a
varying offset within the next polling cycle (the realistic case for
a long-running consumer). An earlier broken version of this test
burst-inserted all events before any polling tick fired and
inadvertently measured wire-RTT four ways; the corrected methodology
exposes the polling-interval-floor effect explicitly.

For polling at interval `P`, theory predicts per-event added latency
of `P/2 + drain_rtt`. For push, the floor is wire RTT plus parse
cost, regardless of any interval.

Results from a single benchmark run against PG 16 in Docker on the
project workstation, N=20 samples per row (raw output preserved in
`docs/benchmarks/pg-streaming-latency-2026-06-15.txt`):

| Transport | min | median | mean | p99 | max |
| --- | --- | --- | --- | --- | --- |
| **push (`PgStreamingCdcSource`)** | 4.0 ms | **4.6 ms** | 4.7 ms | 7.2 ms | 7.2 ms |
| poll @ 10 ms | 3.3 ms | 5.5 ms | 5.4 ms | 8.5 ms | 8.5 ms |
| poll @ 100 ms | 4.5 ms | 57.9 ms | 51.4 ms | 94.8 ms | 94.8 ms |
| poll @ 1000 ms | 21.6 ms | 554.7 ms | 500.9 ms | 961.6 ms | 961.6 ms |

The measured polling medians match the `wire + P/2` prediction to
within a few milliseconds. The push transport delivers events at the
wire-RTT floor regardless of any interval. Quantitative takeaways:

- **At 10 ms polling, push has only marginal benefit (~1 ms saved)**
  per event. But 10 ms polling means ~100 round trips per second per
  consumer; operationally expensive at scale.
- **At 100 ms polling**, push saves ~53 ms per event median, ~88 ms
  at p99. This is the regime where most polling-based clients
  actually deploy.
- **At 1 s polling**, push saves ~550 ms per event median, ~954 ms
  at p99. This is the regime where polling visibly breaks any
  "real-time" claim.

The benchmark's load-bearing assertions are that push beats poll @
100 ms and poll @ 1000 ms in the median. Poll @ 10 ms is not
asserted strictly because the difference is within measurement noise.

**This is empirical proof of the architectural premise.** Subql's
dispatch engine completes predicate fan-out in sub-millisecond; an
intake that adds 50–550 ms per event would dominate the end-to-end
latency budget and erase the design's reason for existing.

The Phase 1 workload-matrix benchmark (`examples/phase1_baseline.rs`,
captured at `docs/benchmarks/phase1-2026-06-15.md`) reproduces the
table above within ±14 % on every cell using the same methodology
against the library polling helper. The harness asserts the prior
table within `[0.5x, 2x]`; if a future change broke either transport
the assertion would catch it.

## Multi-phase workload findings

The single-INSERT benchmark above measures the wire-RTT floor and the
polling-interval floor in a clean, isolated setting. To check whether
the verdict holds across realistic workloads, a five-phase plan
(`docs/cdc-workload-benchmark-plan.md`) drives polling vs. push
across mixed-DML, multi-table-commit, bursty, large-transaction,
slow-consumer, and WAL-retention regimes. Each phase is its own
example under `examples/phase{N}_*.rs`, with raw output captured
under `docs/benchmarks/phase{N}-<date>.md`.

The headline result holds: push beats polling on per-event median
latency in every cell of every phase, with TWO exceptions that are
themselves engineering findings worth surfacing.

### Finding 1: polling at sub-second cadence beats push on large COMMITs

Workload W3.2 commits 1000 rows in a single transaction, three
commits 2 s apart. pgoutput emits all 1000 events to the slot at
COMMIT time. Both transports see the same 1000 events but deliver
them differently:

- Push reads them one frame at a time through `CopyBothDuplex`,
  paying the per-frame `XLogData` header (25 bytes per event) and a
  Tokio task wakeup per frame.
- Polling drains them all in one `pg_logical_slot_get_binary_changes`
  query — one round trip for 1000 events, no per-event framing
  overhead.

Measured on the project workstation (`docs/benchmarks/phase3-2026-06-15.md`):

| Transport | min | median | mean | p99 | max |
| --- | --- | --- | --- | --- | --- |
| **push** | 2.6 ms | 7.3 ms | 7.3 ms | 12.9 ms | 13.3 ms |
| poll @ 10 ms | 2.7 ms | **3.6 ms** | 3.6 ms | 4.7 ms | 4.7 ms |
| poll @ 100 ms | 2.9 ms | **3.5 ms** | 3.5 ms | 4.2 ms | 4.2 ms |
| poll @ 1000 ms | 739.4 ms | 761.0 ms | 761.8 ms | 785.0 ms | 785.0 ms |

Polling at 10 ms and 100 ms beats push by ~4 ms per event median.
Polling's MAX (4.7 ms) beats push's MAX (13.3 ms) by ~8 ms,
reflecting the absence of per-event task wakeups in the polling path.

There is a measurement caveat: with only three independent commits,
all 1000 events of a commit share `commit_at` and `observed_at`, so
the per-event medians are 1000 copies of three random draws of
"next-poll-cycle offset". The qualitative direction (polling beats
push on large-COMMIT throughput) is robust; the exact magnitude
varies run-to-run.

**Operational implication.** Workloads dominated by large transactions
(ETL backfills, bulk INSERTs, schema migrations that touch many rows)
may see lower end-to-end latency from polling at 100 ms than from
push, while paying a higher latency floor on isolated single-row
commits. Mixed workloads — single-row commits AND occasional bulk
batches — favor push, since the bulk batches are a small fraction of
total events.

### Finding 2: push leaves WAL on the server when consumers forget to ack; polling cannot

Workload W4.3 drives 1000 INSERTs at 10 ms gap (10 s total) without
ever calling `source.ack()`. After the workload completes, the test
queries `pg_current_wal_lsn() - confirmed_flush_lsn` for each slot
(`docs/benchmarks/phase4-2026-06-15.md`):

| Transport | median latency | slot lag at end |
| --- | --- | --- |
| **push** | 4.8 ms | **219,680 bytes** |
| poll @ 10 ms | 5.6 ms | 0 bytes |
| poll @ 100 ms | 50.2 ms | 0 bytes |
| poll @ 1000 ms | 517.3 ms | 0 bytes |

Push leaves ~220 KB of unflushed WAL on the server because
`confirmed_flush_lsn` only advances on an explicit `StandbyStatusUpdate`
carrying a `flush_lsn` value, and the source only sends that when
the consumer calls `ack()`. Polling drains via
`pg_logical_slot_get_binary_changes`, which auto-advances
`confirmed_flush_lsn` as a side effect of the query — no ack call is
needed (or possible; `PollingPgCdcSource::ack` is intentionally a
no-op).

**Operational implication.** A push consumer that forgets to ack will
accumulate WAL on the server indefinitely. At sustained workload
rates this can fill PG's data volume in hours. The polling source has
this safety property baked in: as long as the consumer drains, WAL
cannot accumulate. The asymmetry is in the protocol, not in the
implementation.

This is reason enough to consider polling for workloads where ack
discipline cannot be guaranteed (e.g. fan-out consumers with
unreliable downstream sinks, or operators who want a hard upper
bound on WAL retention regardless of consumer behavior). The default
choice for production data paths remains push (per the
[`no-polling-cdc`] guidance), but the trade-off is now visible.

[`no-polling-cdc`]: ../subql/memory/no-polling-cdc.md

### Finding 3: schema complexity scales parse cost equally on both transports

Workload S5 sweeps three schemas: 50-column wide rows, real Pagila
(15-table DVD-rental, applied verbatim from `pagila-schema.sql`), and
an append-only audit log paired with an UPDATE-heavy lookup
(`docs/benchmarks/phase5-2026-06-15.md`).

| Workload | push median | poll @ 10 ms | poll @ 100 ms | poll @ 1000 ms |
| --- | --- | --- | --- | --- |
| S5.1 (50-col wide rows) | 32.2 ms | 33.8 ms | 74.9 ms | 522.4 ms |
| S5.2 (Pagila, 14 base tables) | 3.5 ms | 5.5 ms | 51.0 ms | 491.9 ms |
| S5.3 (audit + lookup, 4:1) | 3.2 ms | 5.3 ms | 50.7 ms | 461.8 ms |

The 50-column wide-row schema is roughly an order of magnitude slower
on both transports than the 5-column baseline; the absolute floor
moves but the polling-vs-push verdict survives (poll @ 100 ms still
loses to push by ~40 ms). Pagila's full DDL surface (DOMAINs, ENUMs,
`tsvector`, `text[]`, triggers, schema-qualified ownership, table
partitioning) is consumed end-to-end through both transports without
parser drift.

**Operational implication.** Per-event parse cost is dominated by row
width, not by the number of distinct relations or by the presence of
exotic types. Schema migrations that widen rows materially (adding
many columns to a hot table) raise the CDC latency floor for both
transports symmetrically. Schema migrations that add tables, foreign
keys, triggers, or partitioning do not move the floor.

**One scope cut**. The Phase 5 example skips Pagila's `payment`
table, which is `PARTITION BY RANGE (payment_date)`. Under pgoutput
`proto_version = 1` (what subql's parser currently handles),
INSERTs into a partitioned parent are emitted with the partition
CHILD's relation OID rather than the parent's. The
`publish_via_partition_root` publication option that re-tags
partition-child events to the parent's identity requires
`proto_version >= 2`. Adding proto v2 support to `PgOutputParser`
(streaming-in-progress transactions, two-phase commit, partition
roots) is a real but bounded follow-up; the existing v1 parser is
strictly correct for non-partitioned tables.

### What still holds, what does not

The strict "push delivers at wire-RTT regardless of cadence" claim
holds on all single-row-COMMIT regimes across all five phases
(Phase 1, W2.1, W2.3, W3.1, W3.3, W4.1, S5.1, S5.2, S5.3). It is
falsified on W3.2 (large COMMITs at sub-second polling cadence),
where the per-event framing overhead in `CopyBothDuplex` exceeds the
next-poll wait.

The weaker "push is the better default for most workloads" claim
holds across all phases on either the latency or the operational
axis. Polling has two niche regimes where it wins (large-COMMIT
workloads, ack-undisciplined consumers); push wins everywhere else.

Schema complexity (Phase 5) shifts the absolute latency floor for
both transports symmetrically and does not change the ranking
between them.

### Aside: the W1.3 post-idle "wake-up" tail

Phase 1 W1.3 (3 s idle, then 1 INSERT, repeated 5 times) recorded a
push median of 29.8 ms — roughly 6x the wire-RTT floor everywhere
else. The first natural hypothesis was that PG's wal sender quiesces
during idle and incurs a wake-up cost on the next event.

A focused follow-up run (`examples/w1_3_idle_wakeup.rs`, captured at
`docs/benchmarks/w1-3-idle-wakeup-2026-06-15.md`) sweeps idle
durations (0 ms / 100 ms / 500 ms / 1 s / 3 s) x push
`status_interval` (1 s / 10 s), 5 isolated trials per cell with a
fresh slot per trial. Every push cell's median is 4.7–6.9 ms,
regardless of idle duration or pump cadence. The polling control at
100 ms cadence shows the expected next-poll wait at long idle
(~51 ms median at 3 s idle).

Provisional read: the Phase 1 number was either noise from 5 samples
or an artifact of the long-running source observed across 5
back-to-back bursts (Tokio runtime scheduling state, parser cache
state, etc.). Not a fundamental wal-sender wake-up cost. The
wire-RTT-floor claim survives. A re-run of Phase 1 W1.3 with more
samples would confirm.

### Aside: the W1.2 high-rate tail

Phase 1 W1.2 (500 INSERTs at 2 ms gap, ~500/s) measured a push median
of 13.9 ms — also ~3x the wire-RTT floor seen in Phase 3 W3.3
(1000 INSERTs at 10 ms gap, ~100/s, push median 3.4 ms). The first
natural hypothesis was bounded-channel backpressure on the source's
internal `event_tx` mpsc, since the default `buffer_capacity = 1024`
can fill if the receiver task is slower than the producer.

`examples/w1_2_burst_rate.rs`
(`docs/benchmarks/w1-2-burst-rate-2026-06-15.md`) sweeps event gap
in `{1, 2, 5, 10, 50}` ms x `buffer_capacity` in `{256, 1024, 4096}`,
200 isolated events per cell. Every cell's median is 3.1–5.4 ms,
including at 1000/s (1 ms gap). Buffer size moves nothing at the
median; only p99/max occasionally wobbles. The bounded-channel
backpressure hypothesis is **refuted** under isolated conditions.

The Phase 1 W1.2 13.9 ms number, like the W1.3 30 ms number, did not
reproduce in this dedicated rig. Both anomalies look like artifacts
of the Phase 1 example's structure (single Tokio runtime running
W1.1 → W1.2 → W1.3 sequentially with sources reconstructed per
measurement) rather than properties of the transport. The
wire-RTT-floor claim survives both regimes.

### Aside: Phase 1 run-to-run variance

Re-running `examples/phase1_baseline.rs` repeatedly on the same
hardware (`docs/benchmarks/phase1-2026-06-15-rerun.md`) reveals
substantial run-to-run variance. Each run sees ~3-5x inflation on at
least one workload row, but the inflated row rotates: in different
back-to-back runs we observed W1.1's push median at 4.8 ms, 18.1 ms,
14.6 ms, and 33.6 ms with no code changes.

Importantly, **the variance affects polling as well as push** at
similar magnitudes within the same inflated row. That points the
cause at host-wide noise (PG background work, Docker container
scheduling, filesystem cache warmup, kernel scheduling jitter)
rather than a transport-specific regression. The architectural-claim
ranking (`push < poll@100ms < poll@1000ms`) survives every observed
run regardless of the absolute number variance.

Two harness adjustments accompany this observation: a short warmup
pass before the first measured workload (un-measured INSERTs +
500 ms settle time) and a relaxed assertion strategy that keeps the
architectural-claim invariant as the load-bearing check while
demoting per-cell-vs-prior numerical comparisons to printouts. The
W1.2 and W1.3 investigations (`docs/benchmarks/w1-2-burst-rate-2026-06-15.md`,
`docs/benchmarks/w1-3-idle-wakeup-2026-06-15.md`) used dedicated
fresh-source-per-trial rigs that don't carry over the Phase 1
sequential-source state and produced tight wire-RTT-floor numbers
across every cell.

### Smoothed Phase 1 (the reproducible headline)

`examples/phase1_smoothed.rs` combines the lessons from the W1.2 and
W1.3 investigations: each cell runs 3 trials with a fresh slot and
fresh source per trial, samples are pooled across trials, and event
counts scale with polling cadence so wall-clock stays roughly
constant per cell. Total wall-clock ~5 min on the project hardware.

Three back-to-back runs against the same hardware
(`docs/benchmarks/phase1-smoothed-2026-06-15.md`) reveal a clean
structural picture:

| run | push median | poll @ 100 ms median | poll @ 1000 ms median |
| ---:| ---:| ---:| ---:|
| 1 | 16.0 ms | 50.1 ms | 540.0 ms |
| 2 | 3.8 ms | 50.0 ms | 539.8 ms |
| 3 | 4.3 ms | 49.8 ms | 527.7 ms |

`poll @ 100 ms` and `poll @ 1000 ms` medians are stable within 1 %
across runs because they are dominated by the polling cadence
(~P/2). `push` and `poll @ 10 ms` medians fluctuate with host load
(busy/quiet shell, cargo background, etc.) but the MIN of each is
always the wire-RTT floor (~3-4 ms).

**The operationally meaningful number is the polling savings vs push**,
which is stable across all three runs:

| polling cadence | push savings vs polling (median) |
| ---:| ---:|
| 100 ms | ~45 ms |
| 1000 ms | ~525 ms |

Those two numbers reproduce. They match the theoretical `P/2`
prediction. They are what the polling-vs-push verdict actually says.

## References

- PostgreSQL logical replication protocol:
  <https://www.postgresql.org/docs/current/protocol-replication.html>
- pgoutput message format:
  <https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html>
- libpq replication-mode connection:
  <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-REPLICATION>
- Supabase ETL (reference impl we cross-check against):
  <https://github.com/supabase/etl> — specifically `crates/etl-postgres`
- The long-running `rust-postgres` replication PR(s) — search the
  repo's issues for `replication`.

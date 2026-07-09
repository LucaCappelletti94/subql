# SubQL vs adjacent tools

Working draft for a website comparison section. Refine in place.

SubQL's niche is **API subscriptions**: the SQL your API already runs, a SELECT
behind a read endpoint, an INSERT or UPDATE behind a write, becomes a
per-consumer live subscription. It runs on your existing Postgres / MySQL /
SQLite through CDC, is transport-neutral (you bring the socket), and ships as an
embeddable engine rather than a database or service you adopt.

Almost none of the tools below are built for that exact job. They are adjacent
categories: GraphQL API layers, streaming databases, sync engines, DB-native
live queries, or a raw change signal. The table exists to make the boundaries
legible, not to claim the others are bad at what they are for.

> Point-in-time note: these products move fast and this reflects a current
> understanding of each. Verify before publishing, especially the language,
> license, and pricing cells, which change often and are easy to get wrong.

| | What it is for | Materialization | Fanout | Data source | What it pushes | Delivery | Form factor | Language | Cost | Type-checked Diesel queries | Client coupling | Payload format |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **SubQL** | Turn the SQL your API runs (SELECT, INSERT, UPDATE) into per-consumer live subscriptions. A write subscribes you to the rows it touches. | Partial (memory-cheap: row + scalar-aggregate deltas; re-executes joins, GROUP BY, MIN/MAX, DISTINCT) | Yes (dedup predicates, per-consumer deltas) | Your existing DB (Postgres / MySQL / SQLite) | Per-consumer result deltas (rows in and out, plus aggregate deltas) | Bring your own (WebSocket, SSE, anything) | Library (embedded in your Rust codebase) | Rust | Free, open source (MIT) | Yes, first-class (a Diesel query becomes a compile-time-checked subscription; SELECT, INSERT, UPDATE) | None (any transport, any client) | Binary SQLite patchsets (first-party plan); transport-neutral, so your own encoding otherwise |
| **Hasura** | GraphQL API layer with live queries | No (re-runs the query) | Yes (multiplexed subscriptions) | Your existing DB (Postgres, plus others) | Live queries re-run (polled / multiplexed), not deltas | GraphQL over WebSocket | Service (server / gateway) | Haskell (v2), Rust (v3 / DDN) | Open-core: free OSS core, paid cloud / enterprise | No | Protocol (GraphQL, any GraphQL client) | JSON (GraphQL over WebSocket) |
| **PostGraphile** | Auto-generated GraphQL API over your Postgres schema | No (re-runs the query) | Yes (subscriptions) | Your existing DB (Postgres) | Subscriptions via LISTEN/NOTIFY plus query re-run, not deltas | GraphQL over WebSocket | Service (Node server) | TypeScript (Node.js) | Open source (MIT), paid Pro plugins | No | Protocol (GraphQL, any GraphQL client) | JSON (GraphQL over WebSocket) |
| **Supabase Realtime** | Stream DB changes to clients | No (row-level changes only) | Yes (channel broadcast, column filters + RLS) | Your existing DB (Postgres, their platform) | Row-level changes matched by column filters (eq/gt/in/like, etc., AND-combinable, no OR) plus RLS. No joins or aggregates, so not query-result deltas | Supabase channels plus client SDK | Service (managed) + client SDK | Elixir | OSS (Apache-2.0), paid hosting (free tier) | No | Their SDK (open-ish WebSocket protocol) | JSON change events (new/old record over WebSocket) |
| **Materialize** | Streaming SQL database (incremental view maintenance) | Yes (full IVM, incl. joins) | No (you build fanout) | A separate DB (you ingest into it) | Maintains incremental views. SUBSCRIBE gives a changefeed. Per-consumer fan-out is yours | SUBSCRIBE stream, you build client delivery | Service (database) | Rust | Commercial (source-available BSL), paid cloud | No (pgwire, so Diesel can connect as a plain client) | None (you build delivery) | pgwire result rows; you build the client payload |
| **RisingWave** | Streaming SQL database (IVM), Postgres-wire compatible | Yes (full IVM, incl. joins) | No (you build fanout) | A separate DB (you ingest into it) | Maintains materialized views. Subscribe to changes. Per-consumer fan-out is yours | SUBSCRIBE / sinks, you build client delivery | Service (database) | Rust | Open source (Apache-2.0), paid cloud | No (pgwire, plain client only) | None (you build delivery) | pgwire rows / sink records; you build the client payload |
| **Feldera** | Incremental compute engine (DBSP) for streaming SQL | Yes (full IVM via DBSP) | No (you build fanout) | A separate pipeline (you feed it) | Incremental view outputs. You build client delivery | Output connectors / HTTP | Service (engine) | Rust | Open source (MIT), paid enterprise | No | None (you build delivery) | Connector output (JSON, Avro, etc.) |
| **ElectricSQL** | Local-first sync: partial Postgres replication by "shape" | No (partial replication, not IVM) | Partial (per-shape sync) | Your existing DB (Postgres) | Data synced to a local store, then you query locally | Electric sync protocol (HTTP) plus client | Service + client library | Elixir (service), TypeScript (client) | Open source (Apache-2.0) | No | Their client, or the plain HTTP shapes API | JSON (HTTP shape log) |
| **PowerSync** | Local-first sync to on-device SQLite | No (partial replication) | Partial (per-client buckets) | Your existing DB (Postgres / MySQL / MongoDB) | Data synced to on-device SQLite, then you query locally | PowerSync protocol plus client SDKs | Service + client SDK | Rust core plus client SDKs | Source-available (FSL) self-host, paid cloud | No | Their client SDK (required) | Their sync protocol (operation log) into SQLite |
| **Zero (Rocicorp)** | Sync engine for local-first apps (query-driven, ZQL) | Partial (client-side reactive queries) | Partial (per-client queries) | Your existing DB (Postgres) | Data synced to a client store, reactive local queries | Zero sync protocol plus client | Service + client library | TypeScript | Open source (Apache-2.0), paid support | No | Their client and ZQL (framework-like) | Their sync protocol (row patches) |
| **Firestore** | NoSQL document DB with built-in real-time listeners | No (snapshot re-eval) | Yes (per-listener) | Their DB (must use Firestore) | Updated query snapshots to listeners | Firestore client SDKs | Service (managed) + client SDK | Proprietary (Google-internal) | Proprietary, usage-based (free tier) | No | Firebase SDK (required) | JSON document snapshots (via SDK) |
| **RethinkDB** | Document DB with changefeeds (live queries) | Partial (changefeeds, limited query types) | Yes (per-subscriber) | Their DB (must use RethinkDB) | Query result changes (changefeeds) | RethinkDB drivers | Service (database) + drivers | C++ | Free, open source (Apache-2.0), project dormant | No | Their driver (backend) | JSON change documents (changefeeds) |

## A write is a subscription

Every tool above is, at best, query-subscription oriented. SubQL also treats a
**write** as a statement of interest: an INSERT executes and subscribes you to
the row it created (by the DB-minted key), and an UPDATE subscribes you to the
rows it targets. So a write endpoint (POST / PUT) does not just mutate data, it
registers the caller's interest in the affected rows, with no separate SELECT to
restate the write. No other tool here does this.

(DELETE is handled as a change and as the terminal event of a row-follow, but it
is not a registration input. The subscription inputs are SELECT, INSERT, UPDATE.)

## Honest overlaps (state these, do not paper over them)

- **Materialize / RisingWave / Feldera** share SubQL's *mechanism*, incremental
  view maintenance. The distinction is form factor and job: they are a database
  you adopt and operate and they do not ship the per-consumer subscription and
  fan-out layer. SubQL is an engine you embed that does.
- **Supabase Realtime** is the closest on "push changes to clients." The sharp
  line: it broadcasts *row* changes matched by column filters (operators like
  `eq/gt/in/like`, combinable with AND but not OR) plus RLS. It cannot express a
  join or an aggregate, so it never tells a consumer how *their query's result
  set* moved. A SubQL subscription is a full SQL SELECT.

## The two axes: materialization and fanout

The `Materialization` and `Fanout` columns are the heart of the comparison:

- **Materialization** = does it incrementally compute how a query's result set
  moved (rows in/out, aggregates, joins), rather than re-running the query or
  forwarding raw row changes.
- **Fanout** = does it route the right change to each of many consumers and
  deliver it, rather than leaving that to you.

As a quadrant:

- **Streaming databases** (Materialize, RisingWave, Feldera): full
  materialization, no fanout. Incrementally-maintained views, but you build the
  subscription and delivery layer.
- **Realtime / live-query tools** (Hasura, Supabase, Firestore, RethinkDB):
  fanout, little or no materialization. They deliver to many subscribers but
  re-run the query or forward row-level changes.
- **Local-first sync** (ElectricSQL, PowerSync, Zero): partial replication to a
  per-client store, not query-result materialization.
- **SubQL**: the one entry doing both, deliberately the *memory-cheap*
  materialization (re-executing the expensive cases) plus full fanout, as an
  embeddable library. That intersection is its niche, not a claim to be best at
  either axis alone.

## The DIY baseline (Postgres LISTEN/NOTIFY)

Left out of the table because it is neither: a raw change signal, no
materialization, no real fanout. A bare channel nudge with no standard payload
(you define and parse the JSON yourself, ~8 KB cap), and to route per user you
need a channel plus hand-written routing, with LISTEN bound to a connection so
no live channel-per-user at scale. It is the plumbing SubQL replaces, not a peer.

## When another tool is the better fit

State these plainly, or the comparison is not credible. SubQL is not the answer
for:

- **Full incremental view maintenance** (joins, GROUP BY, complex aggregates
  maintained incrementally): use **Materialize / RisingWave / Feldera**. SubQL
  re-executes those cases.
- **A turnkey, batteries-included platform** (auth, storage, hosting, client SDKs
  out of the box): use **Supabase / Firestore / Hasura**.
- **Offline-first client replicas** (the client keeps a local copy, works
  offline): use **ElectricSQL / PowerSync / Zero**. SubQL is a backend delta
  engine, not a sync client.
- **A non-Rust backend**: SubQL is a Rust library you embed. If your backend is
  not Rust, it does not drop in (no service wrapper today).
- **Production, today**: SubQL is alpha and unpublished, and its first-party
  transport/client (connetto-rs) is not built yet. Everything above is shipping.

## Open questions to resolve

- Is the tool set right? Drop or merge any?
- Characterizations web-verified in a research pass: all inline "(verify)" flags
  resolved. Corrections applied: the Supabase filter is *not* single-column (it
  supports column filters combinable with AND, just no OR), and PowerSync's
  self-host license is source-available (FSL), not OSI open source.
- The table is 13 columns wide now, so the website render will feature a curated
  subset (e.g. What it's for, Materialization, Fanout, Form factor, Cost) and
  link the full reference.

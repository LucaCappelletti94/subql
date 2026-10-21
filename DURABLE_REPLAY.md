# Durable replay

`DurableEngine` runs one `ReplayDetector` per instance and publishes no state until the caller's storage commits the receipt. The query surface, the bootstrap input contract, and the operator checkpoint semantics are the operator's, in [REPLAY_STATE.md](REPLAY_STATE.md).

## Construction and recovery

`DurableEngine::new(compiler, context, at)` opens an empty registry. `compiler` must hold no registered subscriptions or re-reads, `context` is a `DurableContext` with non-empty `source` and `authority` byte identities, and `at` is a transaction boundary, one whose `transaction_boundary()` is itself.

`DurableEngine::restore(compiler, context, revision, at, bytes, authorize)` rebuilds an engine from a stored receipt. The envelope version, backend, checkpoint type, context, revision, and boundary must match the arguments. Every declaration requires fresh authorization and operator verification against the configured compiler and catalog. Healthy instances must fit their quotas, and failed instances must carry consistent failure and last-valid boundaries. Envelope or receipt mismatches are `DurableError::State`, while declaration and operator errors retain their typed causes.

The `postcard` envelope carries the context, revision, boundary, and instance records. Each instance holds its declaration and operator state as opaque `CBOR` bytes beside its quota, `unread`, `last_valid`, and failure record.

## Admissions

`prepare_register(instance, spec, at, tables, quota, authorize)` admits one instance at the engine's published boundary. `spec` must carry `SubscriptionScope::Durable`, `at` must equal that boundary, `authorize` approves the declaration once before anything is retained, and `tables` follow the operator bootstrap contract, which for a live source is a live exported snapshot or a quiescent slot or read equivalent at that boundary. `quota` is a non-zero byte budget for the instance's complete retained encoding, declaration, operator state, and metadata, and `state_bytes(instance)` reports the retained charge the admission compares against.

## Source transactions

`prepare_transaction(source, after, at, events)` stages one complete source commit. `source` must name the context's source identity, a commit at or behind the published boundary is a duplicate that answers `Ok(None)` with no receipt, and `after` must be the published boundary with `at` beyond it or the gap is a `DurableError::HistoryGap`. `at` and every event's checkpoint must normalize to the same complete transaction boundary through `Checkpoint::transaction_boundary`, and a `PgCommitPosition` row ordinal normalizes to its transaction's commit position.

Each healthy instance whose dependencies the events touch applies the transaction under its quota. A changed selected result records `DurableChange { instance, consumer, at }` and advances `unread` to `at`. Every successful instance advances `last_valid` to `at`, while an unchanged result, including transaction-local cancellation, retains its prior `unread`.

Quota exhaustion ends that instance's wake guarantee and records `DurableFailure { instance, at, last_valid, quota }`. The instance retains its last valid operator state and unread evidence, while unrelated instances advance in the same receipt. The terminal failure ledger is outside the healthy quota and permits no continued evaluation.

## Acknowledgements and removal

`unread(instance)` is the newest selected-result change not covered by an acknowledgement, and `last_valid(instance)` is the newest boundary whose selected result the instance maintains exactly. `prepare_acknowledge(instance, through, authorize)` requires a fresh authorization and a `through` no further than the published boundary, and clears the `unread` only when `through` covers it. The receipt's operation names the kind of commit it records. `Source` for a transaction, `Register` for an admission, `Acknowledge` carrying the residual `unread` so a ledger records the coverage as the engine settled it, or `Remove`. `prepare_remove(instance, authorize)` removes an instance without moving source progress.

## Persistence contract

`PreparedDurable` holds exclusive engine access until persistence or drop. `commit()` exposes the `DurableCommit` receipt. `persist` and `persist_async` publish staged state only after the callback returns `Ok`. The callback must atomically compare the stored revision with `expected_revision`, replace it with `revision`, and persist `checkpoint`, `operation`, `changed`, `failures`, and the source progress from `previous` to `at`. The caller owns this storage transaction and must return `Ok` only after every field commits.

`DurablePersistError::Callback` returns the storage error and unchanged preparation, allowing a confirmed rollback to retry without recapturing bootstrap inputs or refolding events. Dropping a preparation, panicking, or cancelling asynchronous persistence preserves published state.

An unknown commit outcome requires dropping the preparation, reloading the actual committed receipt, and restoring its revision. The storage compare-and-swap rejects stale retries, and a restored duplicate boundary returns `Ok(None)`.

## Source retention

The caller's source must keep serving every transaction it has delivered from the published `at` onward. A gap between the engine's boundary and the next delivered transaction is a `HistoryGap` the engine cannot advance over, and the persisted receipts, each naming its `previous` and its `at`, are what recovery reloads to find the resume point.

## Example

```rust
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, DurableContext, DurableEngine, PgLsn, ReplayTable,
    SubscriptionEngine, SubscriptionRequest,
};

type Event = TestEvent<Postgres, PgLsn>;
type Engine = DurableEngine<Event, DefaultIds, ParserDB>;

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE items (id INT PRIMARY KEY, cost INT);",
    )
    .unwrap()
}

let database = catalog();
let table = catalog_helpers::table_id::<Postgres, _>(&database, "items").unwrap();
let compiler = SubscriptionEngine::new(database, PostgreSqlDialect {});

let context = DurableContext {
    source: b"source-generation-1".to_vec(),
    authority: b"policy-1".to_vec(),
};
let mut engine: Engine = DurableEngine::new(compiler, context.clone(), PgLsn(1)).unwrap();

engine
    .prepare_register(
        42,
        SubscriptionRequest::new(7, "SELECT COUNT(*) FROM items"),
        PgLsn(1),
        vec![ReplayTable {
            table_id: table,
            rows: vec![vec![Value::Int(1), Value::Int(5)]],
        }],
        1_000_000,
        |_| Ok(()),
    )
    .unwrap()
    .persist(|_| Ok::<_, ()>(()))
    .unwrap();

let insert = Event::insert(table, vec![Value::Int(2), Value::Int(4)]).with_checkpoint(PgLsn(2));
engine
    .prepare_transaction(b"source-generation-1", &PgLsn(1), PgLsn(2), &[insert])
    .unwrap()
    .unwrap()
    .persist(|_| Ok::<_, ()>(()))
    .unwrap();
assert_eq!(engine.unread(42), Some(&PgLsn(2)));

let bytes = engine.checkpoint().unwrap();
let revision = engine.revision();
let compiler = SubscriptionEngine::new(catalog(), PostgreSqlDialect {});
let restored: Engine =
    DurableEngine::restore(compiler, context, revision, PgLsn(2), &bytes, |_| Ok(())).unwrap();
assert_eq!(restored.unread(42), Some(&PgLsn(2)));
```

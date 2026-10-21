# Replay state

`ReplayDetector` maintains the selected result of one single-table subscription exactly across complete source transactions and restarts, with no database read.

## Query surface

`SubscriptionEngine::replay_detector(spec, tables)` admits the single-table forms below and answers them from retained state. Admission requires the backend to serve every numeric `HAVING` coercion, even on empty input.

- Row subscriptions. `SELECT *` or a projection of the table's own columns, with the `WHERE` the in-process tier serves and typed binds.
- Scalar folds. `COUNT(*)`, `COUNT(col)`, and `SUM(col)` over the table.
- Grouped folds. The same three with `GROUP BY` and the `HAVING` the compiler admits.
- Extremes. `MIN(col)` and `MAX(col)`, scalar or grouped with the admitted `HAVING`.
- Caller membership. The existing admitted membership terms, seeded from `SubscriptionRequest::subscriber` and `SubscriptionRequest::subjects`, whose membership tables are dependencies.

Joins, `ORDER BY`, `LIMIT`, `AVG`, variance, standard deviation, `COUNT(DISTINCT ...)`, windows, set operations, general subqueries, and other database-reread plans are outside this surface. Text extremes require backend-reproducible ordering, equality, and admitted `HAVING` comparisons. Row-level security requires the read rule folded into the predicate. Extremes over membership terms or row-level-secure tables are refused.

Grouped numeric `HAVING` follows backend literal typing and widening. Integer and decimal boundaries remain exact unless the backend requires floating coercion. Single-precision sums and double-precision means retain independent histories.

## Bootstrap inputs

`tables` names one `ReplayTable` per table in `dependencies()`, exactly once, covering the queried table and every membership table it names. The rows are the source's rows at the exact boundary the change stream resumes from, a live exported snapshot or a quiescent slot or read equivalent, and an expired exported snapshot id is insufficient. Each row carries every column in catalog order and no `Value::Missing` cell, a duplicate captured primary key is `ReplayError::InputConflict`, and a table without a primary key keeps its duplicate rows as multiplicities. Rows that do not match the predicate are included, because the bags retain the whole table, so an update or delete verifies against the retained row and hydrates the cells it did not send, and a row that matches only after a later update is present when it does.

## Transaction application

`apply_transaction` and `apply_transaction_limited` apply one complete source transaction and answer whether the selected result moved, and they are atomic, on any error the detector is unchanged. An insert carries a complete new image. An update or a delete locates its row by primary key among the retained rows, a supplied old cell that contradicts the retained row is `ReplayError::InputConflict`, and a missing old cell, as an unchanged TOASTed column, hydrates from the retained row. A table without a primary key locates the row by its complete old image, so one delete removes one copy of a duplicate. `TRUNCATE` clears the table's retained rows and, on the queried table, every retained fold, group, and extreme. Events on tables outside `dependencies()` are ignored. `apply_transaction_limited` takes a byte budget for the checkpoint encoding and reports `ReplayError::Capacity` when the encoded state crosses it, with the detector unchanged.

## Checkpoint and restore

`checkpoint()` encodes the complete detector as one versioned `CBOR` document. It carries the backend, dependent catalog fingerprints, compiled-plan signature, input bags, and incremental operator state. `restore_replay_detector(spec, operator)` recompiles the statement and verifies this state before exposing it. Incompatible bytes, catalogs, or plans fail explicitly. `answer()` borrows the selected result at the last applied boundary. Projected rows are unordered distinct values with multiplicities, grouped results follow canonical group-key order, and an empty extreme is `Value::Null`.

## Example

```rust
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, DefaultIds, ReplayAnswer, ReplayDetector, ReplayTable,
    ReplayValue, SubscriptionEngine, SubscriptionRequest,
};

type Event = TestEvent<Postgres>;

let catalog = ParserDB::parse::<PostgreSqlDialect>(
    "CREATE TABLE items (id INT PRIMARY KEY, cost INT);",
)
.unwrap();
let table = catalog_helpers::table_id::<Postgres, _>(&catalog, "items").unwrap();
let engine: SubscriptionEngine<Event, DefaultIds, ParserDB> =
    SubscriptionEngine::new(catalog.clone(), PostgreSqlDialect {});

let spec = || SubscriptionRequest::new(1, "SELECT COUNT(*) FROM items WHERE cost > 100");
let mut detector: ReplayDetector<Postgres> = engine
    .replay_detector(
        &spec(),
        vec![ReplayTable {
            table_id: table,
            rows: vec![vec![Value::Int(1), Value::Int(150)]],
        }],
    )
    .unwrap();
assert!(matches!(
    detector.answer(),
    ReplayAnswer::Scalar(ReplayValue::Folded(AggValue::CountStar(1)))
));

let insert = Event::insert(table, vec![Value::Int(2), Value::Int(400)]).with_pk_columns([0u16]);
assert!(detector.apply_transaction(&[insert], &catalog).unwrap());

let bytes = detector.checkpoint().unwrap();
let restored: ReplayDetector<Postgres> =
    engine.restore_replay_detector(&spec(), &bytes).unwrap();
assert_eq!(restored.answer(), detector.answer());
```

# subql API ergonomics backlog

The `subql-demo` website is the real client driving subql's public API. Friction
the demo hits becomes a candidate cleanup. The catalog `Arc` was the first one
found (removed in PR #3). This document tracks the rest so they are not lost.

Each item shows the current call-site code and a proposed better shape. Line
references are into `subql-demo/src/` at the time of writing.

Status legend: `[ ]` open, `[~]` in progress, `[x]` done.

## [x] E1 - unified dispatch

Dispatching one event takes two calls plus a hand-rolled merge of the notified
consumer set.

Now (`state.rs:298-334`):
```rust
let notifications = self.engine.consumers(event)?;
for &cid in notifications.inserted() { /* counters.inserted += 1 */ }
for &cid in notifications.deleted() { /* ... */ }
for &cid in notifications.updated() { /* ... */ }
let agg_deltas = self.engine.aggregate_deltas(event)?;   // separate second call
for (cid, delta) in &agg_deltas { /* agg.apply(delta) */ }
// then merge "who was notified" by hand:
let mut notified = notifications.inserted().iter()
    .chain(notifications.updated()).chain(notifications.deleted()).copied().collect::<Vec<_>>();
notified.extend(agg_deltas.iter().map(|(cid, _)| *cid));
notified.sort_unstable(); notified.dedup();
```

Better:
```rust
let out = self.engine.dispatch(event)?;
for &cid in out.notifications().inserted() { /* ... */ }
for (cid, delta) in out.aggregate_deltas() { /* ... */ }
let notified = out.notified();   // deduped union, computed in the library
```

## [x] E2 - aggregate accumulator

The engine returns signed `AggDelta`s and makes every caller hold the running
value. The demo hand-writes ~50 lines of accumulator (`state.rs:54-106`).

Now (`state.rs:54-106`):
```rust
enum AggState { None, Count(i64), Sum(f64), Avg { sum: f64, count: i64 } }
fn apply(&mut self, delta: &AggDelta) { match (self, delta) {
    (Self::Count(c), AggDelta::Count(d)) => *c += d,
    (Self::Sum(s), AggDelta::Sum(d)) => *s += d,
    (Self::Avg { sum, count }, AggDelta::Avg { sum_delta, count_delta }) => {
        *sum += sum_delta; *count += count_delta; }
    _ => {} } }
// plus for_spec(&AggSpec) and a display()
```

Better:
```rust
let mut acc = AggAccumulator::from_spec(spec);   // seeded from the RegisterResult
acc.apply(&delta);
acc.value();        // typed AggValue
format!("{acc}");   // Display
```

Note: `AggDelta` already covers Count/Sum/Avg/Stats (var/stddev). The library has
the math, the caller should not re-derive it.

## [x] E3 - table resolver

Resolving a table's id, columns, and primary key is 18 lines of stitched
`catalog_helpers` calls.

Now (`state.rs:156-173`):
```rust
let table_id = catalog_helpers::table_id(&catalog, name).ok_or(..)?;
let column_ids = preset.columns.iter()
    .map(|c| catalog_helpers::column_id(&catalog, table_id, c).ok_or(..))
    .collect::<Result<Vec<_>, _>>()?;
let pk_column = catalog_helpers::primary_key_columns(&catalog, table_id)
    .and_then(|pks| pks.first().copied()).unwrap_or(column_ids[0]);
let pk_idx = column_ids.iter().position(|&c| c == pk_column).unwrap_or(0);
```

Better:
```rust
let t = catalog_helpers::resolve_table(&catalog, name)?;
// t.table_id, t.column_ids, t.primary_key, t.column_id("status")
```

## [x] E4 - registration turbofish + projection sniffing

Registration needs a turbofish, and detecting an aggregate query means matching
the `QueryProjection` enum.

Now (`state.rs:229-235`):
```rust
let req = SubscriptionRequest::<DefaultIds>::new(consumer_id, sql.clone());
let result = self.engine.register(req)?;
let agg = match &result.projection {
    QueryProjection::Aggregate(spec) => AggState::for_spec(spec),
    _ => AggState::None,
};
```

Better:
```rust
let result = self.engine.register_select(consumer_id, sql)?;  // engine knows its I
if let Some(spec) = result.aggregate_spec() { /* ... */ }
```

## [x] E5 - RowImage cell access

Callers reach into the `pub cells: Arc<[Cell]>` field and cast `col as usize`.

Now (`state.rs:360`, `sim/mod.rs:105`, `components/schema_view.rs:57`):
```rust
row.cells.get(col as usize).cloned()   // hand-rolled cell_at
for cell in image.cells.iter() { /* ... */ }
```

Better: `RowImage::get(ColumnId) -> Option<&Cell>` already exists. Add the owned
and iterator forms so nobody touches `.cells`:
```rust
row.cell(col)   // Option<Cell>, by ColumnId, no cast
row.iter()      // impl Iterator<Item = &Cell>
```

## [ ] E6 - two catalogs from one DDL (demo wiring only)

The engine parses one `ParserDB` and `SqliteCdcSource::with_pg_ddl` parses another
from the same DDL; a comment asserts they "line up deterministically".

NOT a subql API gap and NOT a sql-traits gap. `ParserDB` IS `Clone` (verified by
compiling - the old handoff's "not Clone" claim was wrong). Both
`SqliteCdcSource::new` and `SubscriptionEngine::new` already take an owned
catalog. So the fix is just demo wiring: parse once, clone for the second owner.

```rust
let catalog = ParserDB::parse::<PostgreSqlDialect>(pg_ddl)?;
let source  = SqliteCdcSource::new(conn, catalog.clone(), cfg)?; // clone, not re-parse
let engine  = SubscriptionEngine::new(catalog, dialect);         // move
```

One parse, one cheap clone of the same parsed schema - the two catalogs cannot
drift. Lands on the demo (the `SqliteCdcSource` path lives on `feat/website`), not
in subql's API.

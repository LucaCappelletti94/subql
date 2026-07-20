# Uphill: a source-independent semantic type key for `sqlite-diff-rs` `digest`

## Purpose

This document specifies an upstream change to `sqlite-diff-rs` (the `wire` module and the `digest` entry point) so that a catalog carrying semantic column types can drive `DiffSetBuilder::digest` for every wire source without translating to a source-native type key. It is the "real destination" from section 7 of `docs/handoff-cdc-ecosystem-convergence.md`. subql maintains `sqlite-diff-rs`, so this lands as a fork change the maintainer owns. This file is the actionable brief for that work. It does not change any subql code and it does not edit the fork.

Decision context: subql chose option B (semantic key upstreamed) as the destination and the wal2json vehicle for the near-term Phase 4 and Phase 5. The wal2json vehicle is fork-free and reintroduces no OIDs, so Phase 4 and Phase 5 proceed against `sqlite-diff-rs` 0.3.0 in parallel with this upstream work. Once the change below ships (as 0.4.0), subql deletes its type-name shim and re-keys the same `WireSchema` to the semantic key with no structural change.

## Verified facts about 0.3.0 that motivate the change

Read against the crate source under the cargo registry (`sqlite-diff-rs-0.3.0`).

1. `digest` binds the schema, the adapter, and the table type to the event's source through `E::Src`:

   ```rust
   // builders/change.rs
   pub fn digest<E, Sch, A>(self, event: &E, schema: &Sch, adapter: &A) -> Result<Self, E::Error>
   where
       E: crate::wire::Digestable<F, T, S, B>,
       Sch: crate::wire::WireSchema<E::Src, Table = T>,
       A: crate::wire::WireAdapter<E::Src, S, B>,
       T: crate::wire::WireColumnTypes<E::Src>,
   ```

2. The schema is authoritative for column types. Every `build_*` helper in every source reads the decoder key from the schema and ignores the type tag the wire event itself carries. The three call sites:

   ```rust
   // pg_walstream.rs build_insert_from_pg
   oid: table.column_type_key(col_idx),
   // wal2json.rs build_insert_from_v2
   pg_type_name: table.column_type_key(col_idx).as_ref(),
   // maxwell.rs build_insert_from_maxwell
   mysql_type: Some(table.column_type_key(col_idx).as_ref()),
   ```

3. `TypeMap<Src, S, B>` is keyed by `Src::TypeKey`, and `TypeMap::decode` re-derives that key from the payload:

   ```rust
   // wire/type_map.rs
   fn decode(&self, payload: Src::Payload<'_>) -> Result<Value<S, B>, DecodeError> {
       let key = Src::type_key(&payload);
       // HashMap<Src::TypeKey, Arc<dyn Decoder<Src, S, B>>>::get(&key) ...
   }
   ```

4. `Src::TypeKey` differs per source: `Oid` for `PgWalstream`, `Arc<str>` (Postgres type name) for `Wal2Json`, `Arc<str>` (MySQL type name) for `Maxwell`. So any catalog that is not already keyed on these must maintain one translation table per source.

5. `WireColumnTypes<Src>` and `WireSchema<Src>` are public and not sealed. Only `WireSource` is sealed. So downstream can implement the schema side, but only against the per-source native key.

Net: because dispatch is schema-driven, the source-native `TypeKey` is really "the schema's declared type identity for a column." Making that identity a single crate-wide semantic enum removes the per-source translation entirely and loses no information the decoders need (see the defaults tables, which already collapse many native names onto one decoder).

## The problem

subql's catalog carries `ScalarKind` (a backend-neutral semantic type). To feed `digest` today, subql must translate `ScalarKind` into `Oid` for the pg source, into a Postgres type-name string for wal2json, and into a MySQL type-name string for maxwell. That is three hand-maintained tables that duplicate mappings the crate already encodes in its `TypeMapDefaults`, and the pg table reintroduces the OIDs this branch deliberately removed.

## Proposed change

Replace the per-source `TypeKey` with a single crate-wide semantic type on the schema side, keep decoders source-specific, and key the `TypeMap` on the semantic type.

### New enum

```rust
/// Semantic column type used to select a decoder, independent of any
/// wire source's native type identity. The schema side declares one of
/// these per column and `TypeMap` dispatches on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WireType {
    Bool,
    Int,
    Real,
    Text,
    Bytes,
    Uuid,
    Decimal,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    Interval,
    Json,
    Jsonb,
}
```

The set covers every decoder the crate ships and every subql `ScalarKind`. `Json` and `Jsonb` stay distinct so a downstream that treats them differently can register different decoders. Decoder policy variants (for example `UuidText36Decoder` versus `UuidBlob16Decoder`, or `JsonVerbatimDecoder` versus `JsonCanonicalDecoder`) are chosen at registration under the one semantic key, not by adding more keys.

### Trait changes (before and after)

`WireColumnTypes` becomes source-independent:

```rust
// before
pub trait WireColumnTypes<Src: WireSource> {
    fn column_type_key(&self, column_index: usize) -> Src::TypeKey;
}
// after
pub trait WireColumnTypes {
    fn column_type(&self, column_index: usize) -> WireType;
}
```

`WireSchema` becomes source-independent:

```rust
// before
pub trait WireSchema<Src: WireSource> {
    type Table: NamedColumns + WireColumnTypes<Src>;
    fn get(&self, table_name: &str) -> Option<&Self::Table>;
}
// after
pub trait WireSchema {
    type Table: NamedColumns + WireColumnTypes;
    fn get(&self, table_name: &str) -> Option<&Self::Table>;
}
```

`WireSource` drops the associated `TypeKey` and returns `WireType`:

```rust
// before
pub trait WireSource: Sealed {
    type Payload<'a>;
    type TypeKey: Hash + Eq + Clone;
    fn type_key(payload: &Self::Payload<'_>) -> Self::TypeKey;
    fn column_name<'a>(payload: &'a Self::Payload<'_>) -> &'a str;
}
// after
pub trait WireSource: Sealed {
    type Payload<'a>;
    fn wire_type(payload: &Self::Payload<'_>) -> WireType;
    fn column_name<'a>(payload: &'a Self::Payload<'_>) -> &'a str;
}
```

`Digestable` and `digest` drop `Src` from the schema and column-type bounds:

```rust
// after
pub trait Digestable<F, T, S, B>
where
    F: Format<S, B>,
    T: NamedColumns + WireColumnTypes,
{
    type Src: WireSource;
    type Error;
    fn digest_into<Sch, A>(&self, builder, schema, adapter) -> Result<..., Self::Error>
    where
        Sch: WireSchema<Table = T>,
        A: WireAdapter<Self::Src, S, B>;
}

pub fn digest<E, Sch, A>(self, event: &E, schema: &Sch, adapter: &A) -> Result<Self, E::Error>
where
    E: Digestable<F, T, S, B>,
    Sch: WireSchema<Table = T>,
    A: WireAdapter<E::Src, S, B>,
    T: WireColumnTypes,
```

`WireAdapter` and `Decoder` keep their `Src` parameter. Payload decoding stays source-specific (binary bytes for pg, JSON for wal2json and maxwell). Only the lookup key becomes semantic.

`TypeMap` keys on `WireType`:

```rust
// after
pub struct TypeMap<Src: WireSource, S, B> {
    entries: HashMap<WireType, Arc<dyn Decoder<Src, S, B> + Send + Sync>>,
}
// decode: let key = Src::wire_type(&payload);
```

### Payload struct changes

Each per-column payload replaces its native key field with `wire_type: WireType`. Keep the raw value field and any modifier a decoder actually consumes.

- `PgWalstreamColumn`: replace `oid: Oid` with `wire_type: WireType`. Keep `data: &ColumnValue`. Drop `type_modifier` if unused by decoders (the digest path sets it to `-1` today), otherwise keep it.
- `Wal2JsonColumn`: replace `pg_type_name: &str` with `wire_type: WireType`. Keep `value: &serde_json::Value`.
- `MaxwellColumn`: replace `mysql_type: Option<&str>` with `wire_type: WireType`. Keep `value: &serde_json::Value`.

The `build_*` helpers change from `let key = table.column_type_key(col_idx)` to `let wire_type = table.column_type(col_idx)` and set `payload.wire_type` from it. The current key normalizations disappear: wal2json's paren-stripping (`numeric(10,2)` to `numeric`) and maxwell's `tinyint(1)` and `bigint unsigned` special casing are no longer needed, because the schema already declares `WireType::Decimal`, `WireType::Bool`, or `WireType::Int` directly.

### `TypeMapDefaults` simplification

`defaults()` shrinks from a per-name table to one entry per `WireType`. The wal2json defaults collapse from about thirty name entries to fourteen semantic entries, and the current missing `uuid` mapping (0.3.0 `Wal2Json::defaults()` registers no `uuid` key, so UUID columns fail with `NoDecoderForType`) is fixed for free. Example for wal2json:

```rust
TypeMap::new()
    .with(WireType::Bool, BoolDecoder)
    .with(WireType::Int, IntDecoder)
    .with(WireType::Real, RealDecoder)
    .with(WireType::Text, TextDecoder)
    .with(WireType::Bytes, PgByteaTextModeDecoder)
    .with(WireType::Uuid, UuidText36Decoder)
    .with(WireType::Decimal, DecimalTextDecoder)
    .with(WireType::Timestamp, TimestampVerbatimDecoder)
    .with(WireType::TimestampTz, TimestampTzVerbatimDecoder)
    .with(WireType::Date, DateVerbatimDecoder)
    .with(WireType::Time, TimeVerbatimDecoder)
    .with(WireType::Interval, IntervalVerbatimDecoder)
    .with(WireType::Json, JsonVerbatimDecoder)
    .with(WireType::Jsonb, JsonVerbatimDecoder)
```

The pg and maxwell `defaults()` collapse the same way.

## Scope and non-goals

- Scope is the forward `digest` path: `wire/{source, adapter, type_map, impls_*}`, the `Digestable` build helpers in `pg_walstream.rs`, `wal2json.rs`, `maxwell.rs`, and the three payload structs.
- Non-goal: the reverse path (`pg_walstream_reverse`) and any consumer that needs the true wire OID for encoding. `PgWalstreamColumn.oid` is public and may be read elsewhere. Verify those before removing the OID field. If a native OID is still needed for encoding, keep it there and only remove its role as the digest dispatch key.
- Non-goal: the pg_walstream 0.7 versus 0.8 alignment. The pg `Digestable` is implemented on `pg_walstream` 0.7 `EventType` behind the `pg-walstream` feature, while subql's own `CdcEvent` is on the 0.8 fork `ChangeEvent`. That version wall is independent of this semantic-key change and is tracked separately. The wal2json vehicle avoids it entirely.

## Breaking change and versioning

This changes the public `wire` API surface (`WireSource`, `WireColumnTypes`, `WireSchema`, `TypeMap`, the payload structs, and `Digestable`). Cut it as `sqlite-diff-rs` 0.4.0. Downstreams that currently key on OIDs or type-name strings migrate by implementing the source-independent `WireColumnTypes` and `WireSchema` and by registering decoders under `WireType`. subql pins to 0.4.0 and removes its type-name shim at that point.

## subql integration after this lands

subql implements the schema side once, source-independent, over `ParserDB`.

- A catalog-table view over `(ParserDB, TableId)` implements `DynTable + SchemaWithPK + NamedColumns + WireColumnTypes`, where `column_type(col_idx)` maps the column's `ScalarKind` to `WireType`:

  | subql `ScalarKind` | `WireType`    |
  | ------------------ | ------------- |
  | `Bool`             | `Bool`        |
  | `Int`              | `Int`         |
  | `Float`            | `Real`        |
  | `String`           | `Text`        |
  | `Bytes`            | `Bytes`       |
  | `Uuid`             | `Uuid`        |
  | `Timestamp`        | `Timestamp`   |
  | `TimestampTz`      | `TimestampTz` |
  | `Date`             | `Date`        |
  | `Time`             | `Time`        |
  | `Decimal`          | `Decimal`     |
  | `Json`             | `Json`        |
  | `Jsonb`            | `Jsonb`       |

  subql has no `Interval` scalar, so `WireType::Interval` is never produced by subql.

- A `WireSchema` over `ParserDB` resolves a table name to that view via the existing `catalog_helpers::table_id` and `column_scalar_kind`.
- One adapter builder per source (`TypeMap::<Src, String, Vec<u8>>::defaults()`) works uniformly, because the key is now `WireType` and the same schema drives pg, wal2json, and maxwell.

## Implementation checklist for the fork

1. Add the `WireType` enum to `wire` and re-export it from the crate root.
2. Drop `WireSource::TypeKey`, rename `type_key` to `wire_type` returning `WireType`.
3. Make `WireColumnTypes` and `WireSchema` source-independent.
4. Replace the native key field in each payload struct with `wire_type: WireType`.
5. Re-key `TypeMap` on `WireType`, update `decode`.
6. Rewrite the three `TypeMapDefaults::defaults()` to register by `WireType`, including the wal2json `uuid` entry.
7. Update the `build_*` helpers to call `table.column_type(col_idx)` and drop the paren and `tinyint(1)` normalizations.
8. Relax `digest` and `Digestable` bounds to drop `Src` from the schema and column-type positions.
9. Review the reverse path and any public reader of `PgWalstreamColumn.oid` before deleting the OID field.
10. Bump to 0.4.0. Update the crate's own tests and `wire_scaffold.rs`.

# Uphill: handle `DataType::Enum` and `DataType::Set` in `sql_traits` normalization

## Purpose

`sql_traits::utils::normalize_sqlparser_type` panics on a MySQL inline `ENUM(...)` (and `SET(...)`) column, so subql cannot parse a MySQL table that has one. This blocks the MySQL angle of the CDC round trip, where an inline `ENUM` is the custom-type analog (MySQL has no `CREATE TYPE` enum and no `DOMAIN`). The fix is a two-line addition to `sql_traits`, which subql maintains. This document is the actionable brief. It does not change subql or edit the fork.

## The problem, verified

`normalize_sqlparser_type` matches `sqlparser::ast::DataType` variant by variant and ends with:

```rust
_ => {
    unimplemented!(
        "Normalization for SQLParser data type `{sqlparser_type:?}` is not yet implemented `{}`",
        sqlparser_type.to_string()
    )
}
```

MySQL's inline `ENUM('happy','sad','neutral')` parses (with `MySqlDialect`) to `DataType::Enum(Vec<EnumMember>, Option<u8>)`, and `SET('a','b')` to `DataType::Set(Vec<String>)`. Neither has a match arm, so both hit the `unimplemented!()` and panic.

Empirical probe against subql's pinned `sql_traits` (`ParserDB::parse::<MySqlDialect>`):

- `id BINARY(16)` resolves to `ScalarKind::Bytes` (normalized to `BINARY`).
- `active BOOLEAN` resolves to `ScalarKind::Bool`.
- `b TINYINT(1)` resolves to `ScalarKind::Int` (normalized to `TINYINT`, the `(1)` is dropped, so the MySQL boolean convention is invisible: declare bool columns `BOOLEAN`).
- `v VARCHAR(255)` resolves to `ScalarKind::String`.
- `feeling ENUM('happy','sad')` panics at `normalize_sqlparser_type.rs` (the `unimplemented!()` arm).

So the only blocker for the MySQL round trip is the enum (and, for completeness, set).

## The fix

Add two arms to the `match sqlparser_type` in `normalize_sqlparser_type`, alongside the other family arms:

```rust
// MySQL enumerations: the member list is not part of the type family
// for value decoding, just as a length is not (`VARCHAR(255)` -> "VARCHAR").
DataType::Enum(..) => "ENUM",
DataType::Set(..) => "SET",
```

`normalize_sqlparser_type` returns `&'static str`, and these arms fit that contract because they drop the member list, exactly as `DataType::Varchar(_) => "VARCHAR"` and `DataType::Binary(_) => "BINARY"` drop their length. Add a doctest line mirroring the existing ones, for example:

```rust
use sqlparser::ast::{DataType, EnumMember};
assert_eq!(
    normalize_sqlparser_type(&DataType::Enum(vec![EnumMember::Name("a".into())], None)),
    "ENUM",
);
assert_eq!(normalize_sqlparser_type(&DataType::Set(vec!["a".to_string()])), "SET");
```

## Why "ENUM" (dropping members) is the right token

subql treats the enum value as text end to end, so it only needs the column classified as a non-scalar that falls back to text. With the token `"ENUM"`, subql's `canonical_type_token("ENUM")` yields `OTHER:enum`, `scalar_kind_from_raw` returns `None`, and the emit path's `build_wire_table` maps `None` to `WireType::Text`. Maxwell emits the enum label as a JSON string, which the `TextDecoder` turns into `Value::Text`, so the label round-trips as text into SQLite and back into the MySQL `ENUM` column (MySQL accepts a string literal for an enum with no cast). This is the same path the Postgres domain and enum already take.

## Design note for the maintainer

Dropping the enum member list means two enums with different members normalize to the same token, so a schema fingerprint would not distinguish `ENUM('a','b')` from `ENUM('a','b','c')`. That matches how the normalizer already drops `VARCHAR` length and `DECIMAL` precision, so it is consistent, but adding or removing an enum value is arguably more semantically significant than a length change. If member-level fingerprint identity is wanted later, a distinct representation (for example a normalized `ENUM(a,b,c)` string) could be introduced without affecting subql's value-decoding path, which only cares that the token is not a known scalar family. For now the static `"ENUM"`/`"SET"` tokens are the minimal, consistent fix.

## subql integration after this lands

Point subql's `sql_traits` dependency at the fixed branch (the same git-fork pattern subql already uses for diesel, sqlparser, and pg_walstream), then the MySQL/Maxwell round trip can include an inline `ENUM` column. No further subql change is needed: the enum flows as text through the existing `WireType::Text` fallback and the `MysqlAdapter` binds it through `DefaultBinder`, which MySQL accepts for an enum column.

## Scope and non-goals

- Scope is the two match arms in `normalize_sqlparser_type` plus a doctest. It is a strict addition: those variants previously panicked.
- Non-goal: any change to `canonical_type_token` or subql. The enum intentionally remains an unknown scalar family in subql and takes the text fallback.
- Non-goal: MySQL `TINYINT(1)` boolean detection. It normalizes to `TINYINT` and reads as `Int`. Declaring bool columns `BOOLEAN` is the supported path, so no change is proposed here.

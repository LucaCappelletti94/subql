# pg_sqlite_emu integration test restoration TODO

Every priority-1 and priority-2 integration test that Phase 8.1
deleted is now restored under `tests/pg_sqlite_emu_*.rs` and
`tests/proptest_pg_sqlite_emu_*.rs`. Live coverage:

- `tests/pg_sqlite_emu_smoke.rs`
- `tests/pg_sqlite_emu_dml.rs` (single PK, composite PK, partial-UPDATE row-lookup fallback)
- `tests/proptest_pg_sqlite_emu_dispatch.rs`
- `tests/proptest_pg_sqlite_emu_cdc.rs`
- `tests/follow_pg_sqlite_emu.rs`

See `docs/emulator-coverage-restoration.md` for the full mapping and
rationale.

Only remaining gap, owned upstream by `sqlite-diff-rs`:

| Planned test | Replaces | Coverage |
|---|---|---|
| `sqlite-diff-rs` test #12 in `docs/upstream-sqlite-diff-pgoutput-reverse.md` | `tests/proptest_pgoutput_bridge.rs` | `parse(encode_message(op_to_message(op))) == op` (semantic) for arbitrary Insert / Update / Delete `ChangesetOp`s over a fixed relation schema. Fires when `sqlite-diff-rs 0.1.2` is cut. |

Delete this file when the upstream row lands.

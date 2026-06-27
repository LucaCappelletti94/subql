# Splitting `rust-postgres` PR #778 for minimum-friction review

## What this document is

[`rust-postgres/rust-postgres#778`](https://github.com/rust-postgres/rust-postgres/pull/778) (*Support CopyBoth queries and replication mode in config* by `petrosagg`) has been open since 2021-05-25 and rebased multiple times. The reviewer (jeff-davis) approved it in mid-2021. Steven Fackler (sfackler), the original maintainer, has not engaged with the PR in 4+ years. Multiple downstreams (Materialize, Supabase ETL, individual CDC users) depend on Materialize's fork as a consequence.

The repo itself, however, **is actively maintained**. The `rust-postgres` GitHub org is owned and run by **Paolo Barbolini** (@paolobarbolini), with multiple other contributors holding merge rights (`exedealer`, `alejandrocgf`, `pczarn`, etc.). Paolo shipped releases of `postgres`, `tokio-postgres`, `postgres-types`, `postgres-derive`, and `postgres-protocol` as recently as 2026-06-12. The repo is not abandoned; #778 specifically is stuck because it's a big feature PR with non-trivial review weight, not because there's no maintainer.

That changes the strategy. The goal of splitting is not to break a 4-year silence — it's to factor the PR into chunks small enough that an actively-maintaining-on-evenings reviewer can land them one at a time. The trivial-review extractions (PR #2 in this plan, +34 lines) are in the same shape as the security-hardening commits Paolo has been merging this month: additive, spec-driven, no design surface.

The PR is one large monolith: 11 files, +639 / −4 across `postgres-protocol` and `tokio-postgres`. Reading the diff shows it has clean internal seams — it can be decomposed into smaller PRs that each have their own merge case. This document describes those seams in enough detail that someone (you) can submit them as a coordinated sequence of stacked PRs.

This is not a replacement for #778 — it is the same content factored into landable units. Coordinate with `petrosagg` (the original author) before submitting; co-authorship via `Co-Authored-By:` in commit trailers preserves credit. Coordinate with Paolo (the active maintainer) before submitting the first split; an "I asked first" PR has materially different review optics than a cold-PR.

## The splits at a glance

| # | Unit | Files | Lines (approx) | Standalone value | Mergeable alone? |
|---|------|-------|----------------|------------------|-------------------|
| 1 | Test infra: enable `wal_level=logical` in CI image | `docker/sql_setup.sh` | +2 | Prepares CI for replication tests | Marginal — needs justification |
| 2 | `postgres-protocol`: parse `CopyBothResponse` | `postgres-protocol/src/message/backend.rs` | +34 | Protocol parser becomes spec-complete | **Yes — trivial** |
| 3 | `tokio-postgres`: `ReplicationMode` + conninfo + startup | `config.rs`, `connect_raw.rs` | +52 | Connect in replication mode; management commands work via `simple_query` | **Yes** |
| 4 | `tokio-postgres`: `copy_both` module scaffolding | `lib.rs`, `simple_query.rs`, `copy_both.rs` (skeleton) | ~+50 | None — pure scaffolding | No — fold into #5 |
| 5 | `tokio-postgres`: `CopyBoth` driver + connection-driver integration | `copy_both.rs` (state machine), `connection.rs` | ~+200 | Internal driver, no user API | Possible with unit tests; better folded into #6 |
| 6 | `tokio-postgres`: `Client::copy_both_simple()` public API + integration tests | `copy_both.rs` (`CopyBothDuplex`), `client.rs`, `tests/test/copy_both.rs`, `tests/test/main.rs` | ~+285 | Full feature available | Yes (depends on #2, #4, #5) |

**Three realistic landing sequences:**

- **Minimum** (3 PRs): #2 → #3 → (#1 + #4 + #5 + #6 folded into one "the rest" PR)
- **Recommended** (4 PRs): #2 → #3 → #4+#5 combined → #6 (with #1 folded into #6)
- **Aggressive** (6 PRs): all of #1–#6 as separate stacked PRs

The minimum is the smallest number of PRs that each have standalone merge value. The recommended balances reviewability and per-PR review burden. The aggressive maximizes per-PR review chunk size at the cost of extra PR ceremony.

## Dependency graph

```
                  ┌─────────────────────────────────────────┐
                  │ #2  postgres-protocol: CopyBothResponse │
                  │     (+34)                                │
                  └──────────────────┬──────────────────────┘
                                     │
              ┌──────────────────────┼────────────────────────┐
              │                      │                        │
              ▼                      │                        ▼
┌──────────────────────────┐         │           ┌─────────────────────────────────┐
│ #3  Config + handshake   │         │           │ #4  CopyBoth scaffolding        │
│     (+52)                │         │           │     (module + visibility)       │
│  Independent — no deps   │         │           │     (~+50)                      │
└──────────────────────────┘         │           └──────────────┬──────────────────┘
                                     │                          │
                                     │                          ▼
                                     │           ┌─────────────────────────────────┐
                                     └──────────►│ #5  CopyBoth driver             │
                                                 │     (state machine + conn glue) │
                                                 │     (~+200)                     │
                                                 └──────────────┬──────────────────┘
                                                                │
                                                                ▼
                                                 ┌─────────────────────────────────┐
                                                 │ #6  Public API + integration    │
                                                 │     tests (+ docker infra #1)   │
                                                 │     (~+285 + 125 tests + 2)     │
                                                 └─────────────────────────────────┘
```

Independent root nodes (no in-PR dependencies): #2, #3, #1.
Strict dependency chain: #2 → #4 → #5 → #6.
#3 is fully independent of the streaming stack, though its end-to-end usefulness benefits from #2 landing first (so `START_REPLICATION` errors gracefully rather than tripping the parser).

## PR-by-PR detail

Each entry below is a draft you can adapt. Lines are approximate; recheck against `git diff` when rebasing.

---

### PR #1 — `ci: enable wal_level=logical and replication slots in the test image`

**Files**

- `docker/sql_setup.sh` (+2 lines)

**What it does**

Sets `wal_level = logical`, `max_wal_senders > 0`, `max_replication_slots > 0` in the docker setup script used by the CI integration tests. No production code changes.

**Standalone value**

Low. Prepares CI infrastructure for future replication tests, but doesn't enable any new functionality on its own.

**Merge case**

Marginal as a standalone PR. The honest pitch is "this is a prerequisite for the replication PR series; landing it first means future PRs don't need infra+feature in the same change." If maintainers prefer infra-with-feature, fold this into PR #6.

**Suggested PR title**

```
ci: enable wal_level=logical and replication slots in test docker image
```

**Suggested PR body**

> Prerequisite for the [CopyBoth / replication mode PR
> series](#PR_LINK_HERE). Adds `wal_level = logical`,
> `max_wal_senders = 4`, `max_replication_slots = 4` to the docker
> setup script so future tests can exercise logical replication
> without further infra changes.
>
> No library code changes. No effect on existing tests.

**Reviewer focus**

- Confirm the values are sane defaults.
- Confirm no other tests rely on `wal_level = replica` (the previous default).

**Risks**

- Other tests on the same docker image expecting different settings: low risk; `wal_level=logical` is a superset of `replica`.

---

### PR #2 — `postgres-protocol: recognize CopyBothResponse ('W') message`

**Files**

- `postgres-protocol/src/message/backend.rs` (+34 lines)

**What it does**

Adds the `COPY_BOTH_RESPONSE_TAG = b'W'` constant, the `Message::CopyBothResponse(CopyBothResponseBody)` enum variant, the parser arm in `Message::parse`, and the `CopyBothResponseBody` struct with `format`, `len`, `storage` fields plus accessor methods. All additive; no existing API changed.

**Standalone value**

**High.** The Postgres frontend/backend protocol defines `'W'` as `CopyBothResponse`, sent when the server enters CopyBoth mode (e.g. in response to `START_REPLICATION` or in physical streaming). The released `postgres-protocol` parser does not recognize this tag — it falls into the catch-all error path. Fixing this is a pure spec-compliance addition: `postgres-protocol` becomes able to decode one more message type that the server already sends.

The win extends beyond replication users: anyone hand-rolling a Postgres client on top of `postgres-protocol` (Materialize, Supabase, embedded clients) gains the ability to parse this message without forking. Subql itself, even in its hand-rolled streaming path, benefits — we can match `Message::CopyBothResponse` instead of pattern-matching on raw bytes.

**Merge case**

Trivial review. No design questions. The byte tag and message body are dictated by the Postgres protocol specification ([§ Frontend/Backend Protocol § Message Formats](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYBOTHRESPONSE)). There is nothing to disagree on. Even the most bandwidth-constrained maintainer can approve this in 10 minutes.

**Suggested PR title**

```
postgres-protocol: recognize CopyBothResponse ('W') message
```

**Suggested PR body**

> The Postgres frontend/backend protocol defines message tag `'W'`
> as `CopyBothResponse`, sent when the server enters CopyBoth mode
> (e.g. in response to `START_REPLICATION` or in physical streaming
> mode). The current `postgres-protocol` `Message::parse` falls into
> the catch-all error path for this tag, breaking any client that
> issues a query the server responds to with CopyBoth.
>
> This PR adds the constant, enum variant, body struct, and parser
> arm. Purely additive; no existing API changes.
>
> Reference: [PostgreSQL § Message Formats §
> CopyBothResponse](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYBOTHRESPONSE)
>
> Extracted from [#778 by
> @petrosagg](https://github.com/rust-postgres/rust-postgres/pull/778),
> which has been waiting on review since 2021. Splitting out the
> protocol-parser piece because it is self-contained, spec-mandated,
> and useful to every consumer of `postgres-protocol` regardless of
> whether the rest of #778 lands.
>
> Co-Authored-By: Petros Angelatos <petrosagg@gmail.com>

**Reviewer focus**

- Verify the body parse against the protocol spec (`format: i8`, `len: i16`, then `len` × `i16` column formats per the spec).
- Verify the new variant is wired into `Message::parse` correctly.
- Spot-check the accessor methods.

**Risks**

- The protocol spec wasn't read correctly: very low; the spec is short and tests cover it.
- Backwards-incompat for users matching exhaustively on `Message`: existing variants are not `#[non_exhaustive]` — adding a variant is technically a breaking change for users who `match` on the enum without a wildcard arm. In practice, `Message` is `#[non_exhaustive]` in some crate versions. Confirm before merging; if not, this PR also flips that bit.

---

### PR #3 — `tokio-postgres: support replication=database|true conninfo option`

**Files**

- `tokio-postgres/src/config.rs` (+45 lines)
- `tokio-postgres/src/connect_raw.rs` (+7 lines)

**What it does**

Adds:

- `ReplicationMode` enum (`Physical`, `Logical`) in `config.rs`.
- `replication_mode: Option<ReplicationMode>` field on `Config` (defaults to `None`).
- `Config::replication_mode()` builder setter and `Config::get_replication_mode()` getter.
- Conninfo string parser arm for `"replication" = "off" | "true" | "database"`.
- Wiring in `connect_raw.rs` startup message construction: if `replication_mode` is set, push `("replication", "true" | "database")` into the startup parameters.

**Standalone value**

**High.** With this PR landed alone (no other changes), users can:

- Connect with `tokio_postgres::Config::new().replication_mode(ReplicationMode::Logical).connect(...)`, OR with a conninfo URL containing `replication=database`.
- Issue `IDENTIFY_SYSTEM` via `simple_query` and get back the systemid, timeline, xlogpos, dbname (a regular tuple response).
- Issue `CREATE_REPLICATION_SLOT slot_name LOGICAL pgoutput` via `simple_query`.
- Issue `DROP_REPLICATION_SLOT slot_name` via `simple_query`.

All of these work because none of them enter CopyBoth mode — they're regular query-response exchanges that the existing protocol driver already handles. **75% of replication slot management becomes available immediately.**

The remaining 25% — actually consuming `START_REPLICATION` — is unlocked by PRs #2 + #5 + #6.

**Merge case**

Additive API surface; no behavior changes for users not on the new feature. Field defaults to `None`. Setter is the only way to enable. Conninfo parser arm rejects unknown values cleanly.

Mergeable alone but END-TO-END usefulness gates on PR #2 (otherwise `START_REPLICATION` calls fail at the parser layer). Reviewer should understand this is a partial step.

**Suggested PR title**

```
tokio-postgres: support `replication=database|true` conninfo option
```

**Suggested PR body**

> Adds `ReplicationMode` enum, `Config::replication_mode()` setter,
> and conninfo parsing for the `replication` keyword as defined by
> libpq ([§ Replication
> Parameters](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-REPLICATION)).
>
> After this PR, users can connect in replication mode and issue
> `IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT`, `DROP_REPLICATION_SLOT`,
> and other management commands via the existing `simple_query` API.
> Streaming `START_REPLICATION` data is unlocked by follow-up PRs in
> the series.
>
> No changes for users not setting `replication_mode`. The field
> defaults to `None`.
>
> Extracted from [#778 by
> @petrosagg](https://github.com/rust-postgres/rust-postgres/pull/778).
>
> Co-Authored-By: Petros Angelatos <petrosagg@gmail.com>

**Reviewer focus**

- Confirm the conninfo parser correctly rejects invalid values.
- Confirm `connect_raw.rs` correctly emits the startup parameter only when set.
- Confirm field default of `None` preserves existing behavior.

**Risks**

- libpq accepts `replication = on | off | true | database`. The PR maps `off → None`, `true → Some(Physical)`, `database → Some(Logical)`. Confirm `on` is also handled or explicitly rejected.
- The `#[non_exhaustive]` attribute on `Config` may or may not be present; adding a public field via a setter is safe regardless, but verify the field itself is `pub(crate)` to avoid accidentally exposing internal state.

---

### PR #4 — `tokio-postgres: scaffold copy_both module` *(optional; recommend folding into PR #5)*

**Files**

- `tokio-postgres/src/lib.rs` (+2 lines): `pub mod copy_both;`
- `tokio-postgres/src/simple_query.rs` (+1 line): make `encode()` visible to other modules in the crate (`pub(crate)`)
- `tokio-postgres/src/copy_both.rs` (new, ~50 lines): just the `CopyBothState` enum and struct declarations for `CopyBothReceiver` and `CopyBothDuplex<T>` — with `todo!()` bodies on impls.

**What it does**

Pure scaffolding. After this PR lands, the public API of the crate gains references to `copy_both::CopyBothDuplex` (visible in docs) but no behavior. Methods either don't exist or `todo!()`.

**Standalone value**

**None.** Dead code from the user perspective. Pure preparatory shape.

**Merge case**

Hard. Maintainers reasonably reject "scaffolding-only" PRs. Recommend folding this into PR #5 (driver implementation) so the scaffolding + driver land together.

If you DO submit it separately, the pitch is "establishes the public API surface for review independent of the state-machine details." Some maintainers value this; most don't.

**Recommendation:** skip this as a standalone PR; merge the file additions with PR #5.

---

### PR #5 — `tokio-postgres: implement CopyBoth subprotocol driver`

**Files**

- `tokio-postgres/src/copy_both.rs` (~+220 lines): `CopyBothState` enum, `CopyBothReceiver` struct, state machine in `poll_backend`, `Stream for CopyBothReceiver` impl.
- `tokio-postgres/src/connection.rs` (+20 lines): `RequestMessages::CopyBoth(CopyBothReceiver)` variant + matching arm in `poll_write`.
- `tokio-postgres/src/simple_query.rs` (+1 line): `encode()` → `pub(crate)`.
- `tokio-postgres/src/lib.rs` (+2 lines): module declaration.
- Unit tests of the state machine (new, ~50–100 lines): synthetic `Message` sequences driving `CopyBothReceiver` through state transitions, asserting expected outputs and state changes.

**Depends on**

PR #2 (the `Message::CopyBothResponse` variant the state machine matches on).

**What it does**

Adds the internal driver for the CopyBoth subprotocol. The connection task's `poll_write` learns to dispatch `RequestMessages::CopyBoth(receiver)` by polling the receiver's Stream impl. The receiver internally drives the 7-state machine (Setup → CopyBoth → CopyOut/CopyIn → CopyNone → CopyComplete → CommandComplete) and forwards messages bidirectionally between the connection and two `mpsc::channel`s.

**Standalone value**

Internal-only. No user API exposed. The driver is functional and unit-testable, but no `Client` method exists to instantiate it.

**Merge case**

The trickiest review of the series, because this is where the actual design choices live: state machine correctness, error handling in CopyBoth mode (does an `ErrorResponse` mid-stream cleanly terminate? how does `CopyDone` from either side cascade?), backpressure semantics on the internal channels.

Submitting it with **unit tests of the state machine** is essential. The test suite should construct synthetic `Message` sequences (including error sequences) and assert state transitions + emitted messages. This lets the reviewer scrutinize the state machine in isolation, which is THE thing that needs scrutiny.

**Suggested PR title**

```
tokio-postgres: implement CopyBoth subprotocol driver
```

**Suggested PR body**

> Adds the internal driver for the Postgres CopyBoth subprotocol
> (used by physical streaming, logical replication, and any
> output-plugin that wants bidirectional control flow). No public
> API yet — the next PR in the series exposes `Client::copy_both_simple()`.
>
> The driver is a 7-state machine in `CopyBothReceiver::poll_backend`:
>
> ```text
>      CopyBoth
>       /   \
>      v     v
>  CopyOut  CopyIn
>       \   /
>        v v
>      CopyNone -> CopyComplete -> CommandComplete
> ```
>
> Bidirectional flow termination is handled per the spec: either
> side can send `CopyDone` to half-close; both halves complete
> independently; final `CommandComplete` + `ReadyForQuery` returns
> the connection to normal query mode.
>
> Unit tests in this PR cover state transitions for clean shutdown,
> error mid-stream, unexpected message responses, and pipelined
> requests after streaming has stopped (see PG bug fix in 14.0 /
> 13.2 / 12.6 / 11.11 / 10.16 / 9.6.21 / 9.5.25 — earlier patch
> levels have a known issue here).
>
> Integration tests against a live Postgres are deferred to the
> follow-up PR that exposes the public API.
>
> Depends on #PR_2_LINK_HERE.
>
> Extracted from [#778 by
> @petrosagg](https://github.com/rust-postgres/rust-postgres/pull/778).
>
> Co-Authored-By: Petros Angelatos <petrosagg@gmail.com>

**Reviewer focus**

- State machine correctness: every documented transition in the diagram, plus error paths.
- Unexpected-message handling: every state has a fallback that closes channels and sets the receiver to a terminal state.
- Channel buffer sizing: `mpsc::channel(16)` — is this the right size?
- The interaction with `Responses::poll_next` (which is what `CopyBothReceiver` polls upstream).

**Risks**

- The state machine handles a less-common state (Setup → CopyOut directly when the server responds with `CopyOutResponse` instead of `CopyBothResponse`, for plain `COPY ... TO STDOUT` queries that get routed through this code path). Verify with the PG docs.
- `ErrorResponse` recovery: there's a known disagreement in the original PR thread about whether the protocol can de-sync on certain error-during-CopyBoth scenarios. Re-read the [PG message in the discussion](https://www.postgresql.org/message-id/c71317d9a1f486de00943cebc3ad1b27cf28d075.camel%40j-davis.com) before merging.

---

### PR #6 — `tokio-postgres: expose Client::copy_both_simple() public API + integration tests`

**Files**

- `tokio-postgres/src/copy_both.rs` (~+138 lines): `CopyBothDuplex<T>` struct, `Stream for CopyBothDuplex` impl, `Sink for CopyBothDuplex` impl, `copy_both_simple()` function.
- `tokio-postgres/src/client.rs` (+44 lines): `CopyBothHandles` struct, `InnerClient::start_copy_both()` method, `Client::copy_both_simple()` public method.
- `tokio-postgres/tests/test/copy_both.rs` (+125 lines, new): integration tests exercising `copy_both_simple()` against a live Postgres in replication mode.
- `tokio-postgres/tests/test/main.rs` (+1 line): module declaration for the new test file.
- *(if PR #1 was folded in here)* `docker/sql_setup.sh` (+2 lines).

**Depends on**

PR #2 (`CopyBothResponse`), PR #5 (driver). Useful in combination with PR #3 (replication mode) since the integration tests exercise `START_REPLICATION`.

**What it does**

Adds the user-facing API: `client.copy_both_simple::<&[u8]>("START_REPLICATION SLOT s LOGICAL 0/0 (...)").await?` returns a `CopyBothDuplex<&[u8]>` that implements `Stream<Item = Result<Bytes, Error>>` (for inbound CopyData) and `Sink<&[u8]>` (for outbound CopyData). Inbound parses `CopyData` automatically; outbound wraps the user's bytes in the CopyData frame.

Integration tests in `tests/test/copy_both.rs` connect to the live PG container, configure replication mode, create a slot, issue `START_REPLICATION`, drive the resulting `CopyBothDuplex`, and assert that DML inserted in a side connection appears in the stream.

**Standalone value**

**High.** This is the user-visible feature. After this PR lands, `tokio-postgres` users can implement logical replication clients without forking.

**Merge case**

The natural endpoint of the series. Ergonomics review (Stream+Sink shape, error types, lifetime/borrow story of `CopyBothDuplex`) lands here.

**Suggested PR title**

```
tokio-postgres: expose Client::copy_both_simple() public API
```

**Suggested PR body**

> Final PR in the [CopyBoth /
> replication](PR_LINK_OR_LABEL_HERE) series. Adds the
> user-facing API on top of the internal driver from #PR_5.
>
> Public surface:
>
> ```rust
> impl Client {
>     pub async fn copy_both_simple<T>(&self, query: &str)
>         -> Result<CopyBothDuplex<T>, Error>
>     where
>         T: Buf + 'static + Send;
> }
>
> pub struct CopyBothDuplex<T> { ... }
> impl<T> Stream for CopyBothDuplex<T> { type Item = Result<Bytes, Error>; }
> impl<T: Buf> Sink<T> for CopyBothDuplex<T> { type Error = Error; }
> ```
>
> Integration tests cover the full `START_REPLICATION` round-trip:
> set up a logical replication slot, issue `START_REPLICATION`, drive
> the duplex, send `StandbyStatusUpdate` ack messages, observe DML
> from a side connection.
>
> Depends on #PR_2_LINK, #PR_3_LINK, #PR_5_LINK.
>
> Extracted from [#778 by
> @petrosagg](https://github.com/rust-postgres/rust-postgres/pull/778).
>
> Co-Authored-By: Petros Angelatos <petrosagg@gmail.com>

**Reviewer focus**

- The API surface: is `Stream + Sink` the right abstraction? Most other Postgres clients expose two separate methods or an explicit poll/send loop. Justify the choice.
- The lifetime of `CopyBothDuplex` vs the parent `Client`: the doc-comment warns "users should ensure that CopyBothDuplex is dropped before attempting to await on a new query." This is a footgun. Is there a safer API that consumes `&mut Client` for the duration?
- The generic parameter `T: Buf` on `CopyBothDuplex<T>`: necessary or could it be `Bytes`?
- Integration test coverage: does it exercise enough of the state machine? (Insert + delete + commit-boundary + slot-advance.)

**Risks**

- The `&Client` + footgun API: this has come up multiple times in the original PR thread; if you have a cleaner alternative, propose it here. The original PR author has indicated openness to alternative shapes.
- Integration test stability: depends on PG container startup time. Use the existing fixtures from the docker setup.

---

## Recommended landing sequence

```
PR #2  ──►  PR #3  ──►  PR #5 (fold #1, #4 into it)  ──►  PR #6
```

Or, if you want maximum trivia-mergeability per PR (more PR ceremony, less per-PR review effort):

```
PR #2  ──►  PR #3  ──►  PR #4  ──►  PR #5  ──►  PR #6 (fold #1 into it)
```

**Start with PR #2.** Its argument is the strongest, its review burden is the smallest, and its merge would break the 4-year silence on this protocol surface. After it lands, the others have an established "we are doing this in pieces" context.

## Coordination with maintainers and the original author

Two people to talk to before submitting anything:

### Paolo Barbolini (`@paolobarbolini`) — active maintainer who almost certainly does not know #778 exists

Paolo owns the `rust-postgres` GitHub org and merges things this week. He's CTO at M4SS-Code, also maintains `lettre`, `deps-rs`, and other Rust projects on the side. His public-facing rhythm in `rust-postgres` is: security fixes, dep upgrades, releases, smaller PRs from contributors.

**Crucial fact**: Paolo has zero touchpoints with PR #778. No comments, no review, no timeline events, no mentions by anyone over 4+ years. His first contribution to `rust-postgres` was April 2024 (`#1130 — Make license metadata SPDX compliant`), so by the time he became an active maintainer the PR was already 3 years buried in the backlog. Nobody surfaced it to him when sfackler de facto handed over maintenance.

The strategy therefore doesn't start with "convince Paolo to engage" — it starts with **make Paolo aware**. Approach him directly (NOT a `@`-mention buried in a 4-year-old PR thread; that's the channel that already failed for 4 years). The Italian / European Rust community is small enough that you likely have shared channels — Mastodon, Discord, Telegram, in-person meetups, email. Use one of those.

Draft message:

> "Paolo, ciao. Quick heads-up about [PR #778 on rust-postgres](https://github.com/rust-postgres/rust-postgres/pull/778) — `CopyBoth` + replication mode support, originally by Petros at Materialize. It's been open since 2021, was approved by jeff-davis the same year, but never merged. Multiple downstreams (Materialize, Supabase ETL, individual CDC users) currently rely on Materialize's fork because of this gap, and `tokio-postgres` is more or less the only blocker for the Rust ecosystem to do logical replication without forking.
>
> Looking at the diff, it can be cleanly split into ~3-6 smaller PRs, with the first one being a 34-line protocol-parser fix (no design surface, pure spec compliance). I'm willing to do the splitting work and shepherd them through, co-authored with Petros. Would you be open to looking at the smaller PRs as they come in? I just want to know if this is something you'd consider merging in pieces, or if there's a reason it shouldn't go in (sfackler veto, scope concern, API shape disagreement) that I should know about before sinking time into the splits."

This message does several useful things at once:

1. **Tells him the PR exists** (most likely the actual unblock).
2. **Provides the context he'd otherwise have to dig out** (it was approved, downstreams depend on it, the fork situation).
3. **Asks an answerable question** ("would you look at smaller PRs?") rather than a vague "what do you think."
4. **Surfaces possible objections proactively** ("is there a reason it shouldn't go in?") so a "no" gives you information, not just rejection.
5. **Frames you as low-cost-to-him** ("I'll do the work, just need to know you're not opposed in principle").

Why personal channel and not GitHub: the GitHub notification stream is the channel that already failed for 4 years. Petros tagged sfackler half a dozen times. Random users tagged sfackler. Benesch (Materialize CTO) tagged sfackler. None of those moved anything. Paolo was never in that thread. A direct out-of-band message — short, friendly, just asking — has materially higher response probability.

### Petros Angelatos (`@petrosagg`) — original author

`petrosagg` is from Materialize. He's been carrying the patch on a fork in production since 2021 and has rebased it on master at least four times. Likely positions:

- **Co-author the splits.** Use `Co-Authored-By: Petros Angelatos <petrosagg@gmail.com>` in commit trailers; that preserves attribution in the GitHub UI and credits him in the contributor stats.
- **Close PR #778 when the series lands.** Coordinate so #778 isn't withdrawn prematurely. Suggest he leaves it open until at least PR #5 of the series lands, so that downstreams still depending on the monolithic patch have a way to track progress.
- **Materialize fork.** They've explicitly said they want to retire it. Once the series lands, they can.

A short GitHub comment on #778 itself is the right venue here (he checks it):

> "Hey @petrosagg — I'd like to extract PR #778 into a stack of smaller PRs to try to bring it over the finish line. @paolobarbolini has signaled he's open to reviewing the splits one by one. Are you OK with co-authorship on each split (Co-Authored-By trailer), and would you be willing to engage with the reviews as they come in? Plan is to start with the 34-line `postgres-protocol` chunk."

Note: send this comment AFTER Paolo has said yes. The line "Paolo has signaled he's open" is load-bearing for Petros — he's been pushing this rock for years and won't engage on another speculative attempt without seeing a maintainer signal.

## Rebase strategy

If `petrosagg` rebases #778 against master while you have the stack open, your splits will need corresponding rebases. Mitigate by:

- Branching each split from the latest commit of the previous one (proper stacked workflow).
- Using `git rebase --onto` to retarget when master moves.
- Tools: `spr` (`getcord/spr`), `graphite-cli`, `Sapling` — any of them works on `rust-postgres/rust-postgres` without any GitHub-side feature flag.
- If you don't want tooling: just maintain N branches manually, named `pr-778-split/1-protocol`, `pr-778-split/2-config`, etc.

GitHub's first-party stacked PRs feature is in private preview and almost certainly not available on the `rust-postgres` org. Don't gate the strategy on getting access.

## When and how to introduce money

Money is a legitimate lever for landing upstream work, but it's a follow-up move, not an opener. For PR #778 specifically the diagnosis is "Paolo doesn't know it exists" — no funding mechanism fixes that. The 30-second personal-channel message remains the highest-EV first action. Reach for money only after the message has revealed what kind of blocker (if any) actually exists.

### What's actually available, ranked by applicability to this case

**GitHub Sponsors** — Paolo specifically does NOT have a sponsors listing enabled. Verified via the GraphQL `hasSponsorsListing` field on his user. So even if you wanted to send him a one-off or recurring stipend through GitHub's native mechanism, the pipe doesn't exist. Asking him to set one up is possible but it's a non-trivial ask of him — most maintainers who haven't enabled it have made that choice deliberately.

**Polar.sh / Algora** — bounty platforms that let sponsors put money on specific issues / PRs, paid on merge. Both require the repo's maintainer (or org) to connect the platform. Almost certainly not enabled for `rust-postgres` since GitHub Sponsors isn't. Same opt-in barrier as Sponsors.

**Tidelift** — enterprise-side: companies subscribe and pay a small share to "lifters" (maintainers) of the packages they depend on, in exchange for SLA-style security and maintenance commitments. Crate-level, retainer-shaped. Not a fit for "land this specific PR" but a real long-term mechanism for keeping maintainers paid in proportion to downstream usage. Would require both Paolo enrolling rust-postgres and corporate sponsors signing up.

**Open Collective** — project-level fiscal host. `rust-postgres` doesn't have a collective; setting one up is Paolo's call, not a downstream contributor's. Long-term option if rust-postgres ever decides to formalize funding.

**Direct consulting via M4SS-Code** — Paolo is CTO of M4SS-Code Srl, an Italian software company. Companies hire other companies for scoped open-source maintenance work routinely; this is a normal commercial arrangement that doesn't require any GitHub-side tooling. If subql (or a downstream with real budget — Materialize, Supabase, anyone shipping a CDC product) wanted to fund a focused engagement, this is the cleanest legal/financial route. Scope something concrete: "N hours/month to keep the replication surface moving" or "land the PR #778 series + initial review of subsequent replication-related PRs." Commercial contract, normal invoicing, no awkwardness.

**Maintainer-for-hire firms** — Mainmatter (Rust consultancy with upstreaming experience), Ferrous Systems (Rust core contributors), and similar specialty shops will take scoped upstream-contribution engagements. They'd not be reviewing on Paolo's behalf, but they'd do the rebase / splitting / argument-marshaling work to maximize the probability the PR lands. Enterprise pricing. Real option if a commercial backer materializes.

### The indirect lever: pay Petros, not Paolo

If the diagnosis after talking to Paolo turns out to be "I'd merge smaller PRs but I don't have time to split them up myself," then the funding pressure point shifts. Paying *Petros* (or his team at Materialize) to do the splitting work is a different lever:

- Petros wrote the original code; splitting it is mostly mechanical for him.
- Materialize has commercial interest in PR #778 landing (they'd retire their fork). They might already be willing to fund this; subql or another downstream just needs to make the ask coordinated.
- Once the splits exist as opened PRs, Paolo's per-PR review cost drops, regardless of whether anyone pays him.
- This routes money to where the unpaid effort actually accumulates (PR author + rebase ownership) rather than to the reviewer, which is culturally easier.

A Polar/Algora bounty pinned on PR #778 itself, that downstreams could chip into, is the cleanest shape for this. Materialize might be willing to seed it; Supabase has been transparent about depending on the fork; individual users could add small contributions.

### The cultural caveat

Paying for open-source review work is becoming more normalized in the Rust ecosystem (Tokio Foundation, Rust Foundation grants, sponsored maintainers at AWS/Cloudflare/Google) but it remains situationally fraught. Specifically:

- For volunteer-shaped maintainers, "let me pay you to merge this" can read transactional and might harden a "no" that would otherwise have been a "yes."
- For maintainers running through their company (like Paolo through M4SS-Code), a proper consulting arrangement is normal and not awkward — it's just a B2B service contract.
- Bounties on issues with strong commercial backing are usually fine. Bounties on issues with no commercial backing, paid by a single individual, can attract eyebrows.

Practical rule: if the money is coming from a company that benefits commercially, route it as a normal commercial engagement. If it's coming from individuals, route it via a transparent pooled-bounty platform (Polar) so it doesn't look like quiet personal pressure. Avoid the shape "individual A pays individual B to merge a specific PR" — that's the configuration that goes wrong most often.

### Decision flowchart for THIS PR

```
   Step 1: Ping Paolo on personal channel.
                    │
       ┌────────────┼────────────┐
       │            │            │
       ▼            ▼            ▼
 "No idea,    "Aware, no    "Not
  let me      time"          interested"
  look"            │              │
       │           │              │
       ▼           ▼              ▼
 Splits go   Money lever     Stop pushing
  forward    activates       upstream.
  as drafted (M4SS-Code      Switch to
             consulting,      sibling-crate
             Polar bounty     or hand-roll
             on #778,         strategy.
             paid splitting   No funding
             via Petros)      conversation
                              needed.
```

Money is a Step-2 conversation in two of three branches and irrelevant in the third. Don't pre-empt the Step-1 ping with it.

## Anticipated reviewer objections + responses

> **"Why split? Just review the whole thing."**

Because reviewing the whole thing has not happened in 4 years. Smaller PRs lower the activation energy for a maintainer to engage. Each one provides standalone value (PR #2 fixes a protocol-parser gap; PR #3 unlocks management commands).

> **"This is just PR #778 in pieces."**

Yes, and with `Co-Authored-By:` trailers credit is preserved. The splitting is to make review tractable, not to claim authorship.

> **"PR #3 alone is useless without the streaming."**

False. PR #3 alone enables IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT, DROP_REPLICATION_SLOT, and other management commands via `simple_query`. That covers slot lifecycle for users who only need to set up / tear down slots and stream via other tooling (e.g. `pglogrepl`-style external consumers, libpq-direct readers).

> **"Why not wait for PR #778 to land?"**

It has been waiting since 2021. Multiple downstreams have moved on (Materialize fork → Supabase ETL → individual forks). The waiting strategy has measurably failed; trying a different approach is warranted.

> **"This adds dead code."**

Each of PRs #2, #3, #6 lands functional code with standalone tests. PR #5 lands the internal driver with unit tests of the state machine — internal but not dead. If maintainers reject PR #5 as "internal-only," fold it into PR #6.

## Out of scope for this strategy

- **Physical replication.** The PR series targets logical replication. Physical replication uses CopyBoth too, so the driver (#5) supports it, but no integration tests for physical are included. A follow-up could add them.
- **Hot standby feedback.** Mentioned in the original PR thread as a future helper API. Not in #778, not in this series.
- **Output plugin negotiation.** The `pgoutput` plugin is handled by the `START_REPLICATION SLOT ... LOGICAL 0/0 (publication_names '...', proto_version '1')` query string, which is plain SQL syntax that already works through `simple_query`. No additional driver support needed.

## What to do today

The first concrete action is the smallest possible thing: **find out if Paolo even knows the PR exists**. Don't write code, don't open issues, don't tag him on GitHub. Just send one out-of-band message and listen to the answer.

1. **Ping Paolo via personal channel** (Mastodon / Discord / Telegram / meetup / email — NOT a GitHub `@`-mention; that's the channel that already failed for 4 years). Use the draft message from the coordination section. The question you want answered is "are you aware of this PR, and if so is there a reason it hasn't moved?" Everything else flows from his answer.
2. **Branch based on Paolo's response:**
   - *"No idea, let me look"* → proceed with the split strategy. Continue from step 3.
   - *"Aware, but [reason]"* → take the reason at face value and recalibrate. The reason might be "I want a different API," "sfackler holds veto," "the scope is too big for me to support," "I just don't have time." Each of those changes the strategy differently. Don't sink time into splits before understanding the constraint.
   - *"Not interested in adding replication to tokio-postgres at all"* → strategy shifts entirely. Stop pushing upstream. Options become: (a) Materialize publishes their fork under a different name to crates.io; (b) you publish a `tokio-postgres-replication` sibling crate that depends on base `tokio-postgres`; (c) subql hand-rolls and considers upstream a non-goal.
3. **Comment on #778 tagging Petros**, mentioning Paolo's signal. Offer co-authorship.
4. Pick a copy of #778 at a known-good commit. Fork rust-postgres locally if you haven't.
5. **Open PR #2** (postgres-protocol CopyBothResponse, +34 lines). Tag Paolo in the PR body. Wait a week.
6. If PR #2 lands, **open PR #3** (Config + handshake, +52). Same pattern.
7. If PR #3 lands, **open PR #5** (driver, ~+220 with unit tests). This is the heavy review — give it more runway.
8. If PR #5 lands, **open PR #6** (public API + integration tests, ~+285 with tests).
9. Coordinate with Petros to close #778 once PR #5 or #6 lands.

If after a month no PR lands despite Paolo agreeing to the plan, the situation calls for a different read: Paolo wanted to be supportive but doesn't have the focused review bandwidth even for the small slices. At that point the highest-leverage move shifts from "split the PR" to "offer to take on review responsibility for the replication surface yourself" — i.e. ask Paolo if he'd add you to the org as a reviewer/maintainer focused on this area. That's a longer-term play but it's the structural fix: the project gets a domain owner for replication, you get to actually unblock the work, and Paolo gets one less surface to context-switch onto.

**The asymmetry to keep in mind**: 30 seconds to send Paolo a message; 4+ years of compounding ecosystem cost from not having sent it. The expected value of step 1 is enormous regardless of which branch his response sends you down.

# subql website design spec

The site deploys at **subql.luca.phd** and follows the look of **tsne.luca.phd**
(local repo `~/github/dioxus-decompositions`, stylesheet `src/style.css`). It is
one long page: a software **presentation** on top, the interactive **demo** at the
bottom (a preset schema OR bring-your-own).

This spec was reconstructed decision-by-decision after a prior design discussion
was lost. The build is a large, staged effort (see Implementation note) - this
document is the source of truth; update it whenever a decision changes.

## Positioning (the pitch)

subql is the **general backend engine for real-time SQL subscriptions**. On every
data change it decides which consumer sees what change in THEIR subscribed query
(rows in/out + aggregate deltas). A **consumer is anything that needs to stay
current on a slice of data**: a browser/live UI, a service, or an **LLM/agent/bot**
that registers interest in a topic and is kept up to date automatically (no
re-polling, no stale context). **Transport-agnostic** - you bring the socket
(WebSocket/SSE), subql is the "what to push, to whom" core. General,
set-it-up-and-forget-it, any schema. Replaces the per-app plumbing everyone
hand-writes. (Dogfood: used in `connetto` as a general transport layer.)

## Presentation (top) - sections in order

1. **Hero.** Headline "SQL subscriptions, without the plumbing"; subtitle "A
   general engine that tracks who's affected by every change - instead of code
   you'd hand-write per query."
2. **What it is / problem.** Approved copy:
   > The problem. Anything that needs to stay current on a slice of your data - a
   > live dashboard, a service, an LLM agent watching a topic - needs the same
   > backend: when data changes, work out who cares and how their view changed.
   > Today you hand-write that, per app and per query.
   >
   > subql. The general engine for it. A consumer registers a SQL SELECT - a
   > standing interest - and on every change subql says, per consumer, exactly how
   > their result changed: rows in/out, aggregate deltas. A browser, a service, or
   > a bot kept up to date automatically. Bring your own transport (WebSocket,
   > SSE); subql is the "what to push, to whom" core. One setup, any schema.
3. **Features.** Highlight (1) row + aggregate subscriptions (per-consumer
   view-relative insert/update/delete + COUNT/SUM/AVG/variance/stddev deltas);
   (2) set-and-forget internals (predicate dedup, eviction, durable shards,
   multiple dialects). NOT marketed: wasm/no-std (only the mechanism that makes the
   live demo a credible proof). CDC/source support lives under How-it-works.
   Copy drafted at build.
4. **How it works.** BOTH explanatory diagram(s) AND the live demo. Several NOVEL
   concepts need explaining well from a few angles: (a) view-relative per-consumer
   deltas (one base change = INSERT for one consumer, DELETE for another, UPDATE
   for a third, per WHERE-filtered result set); (b) aggregate deltas (incremental
   signed COUNT/SUM/AVG, no re-run); (c) one event fanned out to many consumers via
   deduped predicates. Diagrams + worked examples; the live demo is the payoff.
   Diagram content drafted at build.

## Visual system

- **Font:** Inter (grotesk sans), headings + body, shipped as a web font. Headings
  weight ~600, letter-spacing ~-0.01 to -0.02em. Type scale TBD.
- **Palette (tsne tokens):** ink `#0a0a0a` on bg `#fff`; muted `rgba(10,10,10,.55)`;
  hairline `rgba(10,10,10,.16)`; hover `rgba(10,10,10,.06)`; panel
  `rgba(255,255,255,.92)`. Monochrome.
- **Dark mode:** YES - light + OS-preference dark (`@media prefers-color-scheme`),
  bg `#0a0a0a` / ink `#f4f4f4` flip; black logo inverts via `filter: invert(1)`.
- **Action color:** red `#dc2626`, destructive only (delete / truncate); all other
  chrome monochrome. (tsne pattern: color only on specific action hover/active.)
- **Density & shape:** airy + rounded - generous whitespace, ~8-10px corners; pills
  999px.
- **Code blocks:** keep dark (Tokyo Night) as a deliberate accent.
- **Logo:** `subql-demo/assets/logo.svg`, already `fill="currentColor"` (free
  monochrome + dark-mode invert).

## Page structure

- **Flow:** one long page, continuous scroll - presentation flows top-to-bottom
  into the demo as the final section.
- **Width:** wider centered envelope (~1200-1280px), not full-bleed (tsne is
  full-bleed only because it is a plot app).
- **Header (follow tsne):** floats; logo top-left links to root; monochrome
  icon-links top-right = **GitHub, Docs/crates.io, Sponsor heart** (hovers red);
  chromeless icon buttons, hover opacity .6; no about overlay.

## Demo (bottom)

- **Multi-table.** subql is multi-table (engine `partitions: HashMap<TableId,..>`;
  only a single subscription is single-table, no joins - `sql_shape.rs:528`). The
  demo shows EVERY table in the schema; subscribe to queries on any table, mutate
  any. Requires reworking `DemoState` from single-table fields to a per-table
  model, and making every panel (schema view, consumer list, sim, event log)
  table-aware. Significant rework.
- **Presets vs bring-your-own.** Presets (orders/readings/users) ship seed rows + a
  row generator, so the random auto-sim works. **BYO = paste DDL** (textarea) and
  **require actual data** (user supplies real rows / INSERTs) - do NOT auto-generate
  (synthetic rows can't satisfy arbitrary CHECK/FK/UNIQUE). So BYO is user-driven
  (they issue their own DML via the query console); auto-sim is a preset-only
  feature.
- **pgoutput round-trip:** value uncertain; do NOT feature. Tuck behind a button /
  into a log, revealed on demand. (This is what `DemoState.table_id` scaffolds.)
- TBD: panel arrangement within the page; DDL parse-error display; the preset-pill
  + custom-DDL entry UX (mirror tsne's example-pills + entry pattern).

## Reference: tsne.luca.phd = `~/github/dioxus-decompositions/src/style.css`

Mine it directly for exact values when building: design tokens, the floating topbar
(brand left / icon actions right), pill buttons (transparent, 1px line, 999px
radius, icon+label, hover fills), the empty/drop state (centered example pills +
entry hint over a hidden input), and the accent-on-action discipline.

## Implementation note (large, staged effort)

Not a small change. Building it touches: a full CSS rebuild to the monochrome token
system + dark mode + the Inter web font; new presentation sections
(hero/what-it-is/features/how-it-works) with diagrams; a multi-table rework of
`DemoState` and every demo panel; the bring-your-own-DDL flow; tucking pgoutput
into a log. Build in stages, not one pass. Open copy/diagrams (Features,
How-it-works) drafted at build against the positioning above.

## Verification (after build)

`cargo check --workspace`, clippy, then render-check the wasm demo (`dx serve` /
`wasm-pack`) in the FULL shell env (snap Firefox dies under `env -i`). Eyeball:
monochrome + dark mode via the OS toggle, Inter loading, red only on
delete/truncate, multi-table switching, BYO paste-DDL + data.

# Row Pattern Recognition (`MATCH_RECOGNIZE`)

`MATCH_RECOGNIZE` (SQL:2016 row pattern recognition) finds matches of a regular-expression-like
pattern over the rows of a partition, ordered by a time column, and emits one row per match. It is
the streaming-SQL form of complex event processing (CEP): chains like "a login, then three failed
actions, then a withdrawal within five minutes".

This document covers the streaming implementation. The supported v1 subset is:

- **append-only input only** — a retraction mid-partial-match would invalidate an in-progress or
  completed match, so the semantics over a changelog are ill-defined. The binder/planner rejects
  non-append-only input. (Flink restricts `MATCH_RECOGNIZE` to append-only for the same reason.)
- **`ONE ROW PER MATCH`** — `ALL ROWS PER MATCH` is not yet supported.
- `PARTITION BY` (required, plain columns) and `ORDER BY` (required, leading column must carry a
  watermark).
- `PATTERN`: concatenation, alternation (`|`), grouping, quantifiers (`*`, `+`, `?`, `{n,m}` and
  their reluctant `*?` forms), and `PERMUTE`.
- `DEFINE` predicates with full running navigation (`PREV`/`NEXT`/`FIRST`/`LAST` and bare `A.col`).
- `MEASURES` with `FIRST`/`LAST`/bare `A.col`, `CLASSIFIER()`, `SUBSET`, and the aggregates
  `COUNT(*)`/`COUNT`/`MIN`/`MAX`/`SUM`/`AVG`.
- `AFTER MATCH SKIP PAST LAST ROW` / `TO NEXT ROW` / `TO FIRST|LAST <var>`.
- `WITHIN <interval>` (a streaming time bound on the match span).

## Feature support

The clause is modeled on the two reference implementations RisingWave users come from: Apache Flink
SQL (streaming) and Google BigQuery (batch). The table summarizes RisingWave's v1 support against
them. Flink and BigQuery columns reflect their public documentation as of June 2026 (see Sources);
✅ supported, ❌ not supported, ➖ not applicable.

| Feature | Flink SQL | BigQuery | RisingWave v1 |
| --- | :---: | :---: | :---: |
| Streaming | ✅ | ❌ | ✅ |
| Batch | ✅ | ✅ | ❌ |
| `ONE ROW PER MATCH` | ✅ | ✅ ² | ✅ |
| `ALL ROWS PER MATCH` | ✅ | ❌ | ❌ |
| Concatenation, `*` `+` `?` `{n,m}` | ✅ | ✅ | ✅ |
| Reluctant quantifiers (`*?`) | ✅ ¹ | ✅ | ✅ |
| Alternation (`A \| B`) | ❌ | ✅ | ✅ |
| Grouping + quantifier (`(A B)+`) | ❌ | ✅ | ✅ |
| `PERMUTE` | ❌ | ❌ | ✅ |
| Anchors (`^` `$`) | ❌ | ✅ | ❌ |
| Exclusion (`{- … -}`) | ❌ | ❌ | ❌ |
| Running nav in `DEFINE` (`A.col`, `FIRST`/`LAST`) | ✅ | ✅ | ✅ |
| Physical `PREV`/`NEXT` | ❌ ³ | ✅ | ✅ |
| `MEASURES` `FIRST`/`LAST` | ✅ | ✅ | ✅ |
| Aggregates in `MEASURES` (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) | ✅ | ✅ | ✅ |
| `CLASSIFIER()` | ❌ | ✅ | ✅ |
| `MATCH_NUMBER()` | ❌ | ✅ | ❌ |
| `SUBSET` | ❌ | ❌ | ✅ |
| `AFTER MATCH SKIP PAST LAST ROW` / `TO NEXT ROW` | ✅ | ✅ | ✅ |
| `AFTER MATCH SKIP TO FIRST`/`LAST <var>` | ✅ | ❌ | ✅ |
| `WITHIN` (time bound) | ✅ | ❌ | ✅ |
| Checkpoint / recovery / rescaling | ✅ | ➖ | ✅ |

¹ Flink supports reluctant `+?` / `*?` but not the reluctant optional `??`.
² BigQuery has no `ROWS PER MATCH` keyword; it emits one row per match and requires aggregation in
`MEASURES` (use `ARRAY_AGG` for all-rows-style output).
³ Flink expresses physical offsets through `LAST(expr, n)` rather than `PREV`/`NEXT`.

Sources: [Apache Flink — Pattern Recognition](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/match_recognize/),
[BigQuery — `MATCH_RECOGNIZE` clause](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#match_recognize_clause).

## Planning pipeline

The clause flows through the usual layers; each is a thin, conventional addition:

- **Parser** (`src/sqlparser`): `TableFactor::MatchRecognize` plus the `Measure`, `RowsPerMatch`,
  `AfterMatchSkip`, `MatchRecognizePattern`, `RepetitionQuantifier`, and `SubsetDefinition` AST
  nodes.
- **Binder** (`src/frontend/src/binder/relation/match_recognize.rs`): produces `BoundMatchRecognize`
  and registers the output columns (`PARTITION BY` columns, then the measures). The interesting work
  is *lowering* `MEASURES` and `DEFINE` (see below).
- **Logical plan** (`logical_match_recognize.rs`): `LogicalMatchRecognize` with the standard trait
  set. `PredicatePushdown` is a barrier (a predicate over computed output columns must not push below
  the operator); `ColPrunable` prunes the input to the columns the clause's expressions actually
  read. `to_stream` enforces the v1 restrictions and shards the input by the `PARTITION BY` key.
- **Stream plan** (`stream_match_recognize.rs` + `generic/match_recognize.rs`): `StreamMatchRecognize`
  is append-only and hash-sharded on the partition columns. It declares one internal state table (see
  [State and fault tolerance](#state-and-fault-tolerance)).

### Lowering `MEASURES` and `DEFINE`

Pattern-variable references (`A.price`, `FIRST(B.ts)`, `PREV(price)`) have no direct analog in the
expression framework, so the binder lowers each measure and define to **an ordinary expression over
a synthetic row**, plus a list of *slots* that describe how to build that synthetic row from a
match:

- A `MeasureSlot` / `DefineSlot` records a navigation kind (`First`, `Last`, `Classifier`, `Prev`,
  `Next`, `RunningFirst`, `RunningLast`, the aggregates, …), the pattern variables it ranges over
  (several, for a `SUBSET`), and the input column it reads.
- The lowered expression is a normal `ExprImpl` whose `InputRef(i)` reads `slots[i]`.

This keeps all type checking, coercion, and constant folding in the existing expression machinery:
the executor materializes the synthetic row per match (or per candidate, for `DEFINE`) and evaluates
the expression over it. `DEFINE` navigation functions are pulled out of the predicate by an AST
pre-walk into a synthetic placeholder relation so the remaining predicate binds normally.

### The hidden match id

A partition can contain many matches, and two matches may produce byte-identical `PARTITION BY` +
`MEASURES` output, so those columns are not a unique key. The output therefore carries a **hidden
`_match_id` column** (the same mechanism sources use for `_row_id`); the stream key is the partition
columns plus `_match_id`. It is hidden, so `SELECT *` returns only the user columns.

The executor fills `_match_id` with the **match's start row's `seq`** (the buffered row's hidden
snowflake PK tiebreaker, assigned once at ingest). This is unique forever — an emitted match's start
row is always evicted, so no later match can share it (the same invariant that prevents
cross-watermark double emits) — and, unlike an id minted at emission time, it is **deterministic
across recovery replay**: re-emitting a match after a rollback reproduces byte-identical output. A
stable, replay-deterministic match identity is also the foundation a future emit-on-update mode
would retract against.

### Emit semantics

The operator emits **only final matches, at the watermark** — a match is output once no late row
can change it. In RisingWave's emit taxonomy this is Emit-On-Window-Close behavior: the plan node
declares it (`emit_on_window_close = true`) and the query is **required** to state it — a
`MATCH_RECOGNIZE` materialized view must be declared `EMIT ON WINDOW CLOSE`, and the plain
(default-emit) form is rejected with guidance. The plain form is deliberately reserved: if an
emit-on-update mode (provisional matches corrected by retractions, keyed by the deterministic
`_match_id`) is added later, it can adopt RisingWave's default emit semantics under the plain form
without silently changing the meaning of any existing query — every existing query is explicit by
construction.

One composition consequence: the operator does not emit a downstream watermark, so stateful
operators that need one under Emit-On-Window-Close (e.g. an aggregation) cannot sit above
`MATCH_RECOGNIZE` **inside the same** `EMIT ON WINDOW CLOSE` view. Compose across views instead:
the `MATCH_RECOGNIZE` view's output is append-only, and a plain (default-emit) view can aggregate
it. Stateless operators (projections, filters) compose freely within the same view.

## The NFA

`src/stream/src/executor/match_recognize/nfa.rs` is a self-contained, pure module (unit-tested
without a cluster). A `Pattern` (variable / concat / alternation / quantified / permute) is compiled
by Thompson construction into an `Nfa` whose labelled transitions are pattern variables.

Matching is **predicate-driven**: rather than precomputing which variables each row satisfies, the
matcher consults a `CandidateMatcher` as it walks the NFA, so `DEFINE` predicates that depend on the
running match (e.g. `B AS B.price > A.price`) can be evaluated against the rows matched so far. The
matcher returns the *first accepting path in transition order*; greedy quantifiers order the loop
edge first (longest match), reluctant quantifiers order the exit edge first (shortest), and
alternation prefers its first branch. `PERMUTE` expands to the alternation of all orderings (capped
to keep the factorial bounded).

## The executor

`MatchRecognizeExecutor` follows the standard append-only, watermark-driven executor shape (compare
`eowc_over_window`):

- **Buffering.** Each input row is written through to the state table; nothing is held in memory
  between watermarks. Rows may arrive out of order.
- **Matching on watermark.** When the watermark on the leading `ORDER BY` column advances to `w`,
  the executor visits only the partitions that need attention, found via the **wakeup frontier** (see
  [The wakeup frontier](#the-wakeup-frontier)): per owned vnode it range-scans `frontier_index_table`
  for entries with `next_wakeup <= w` (the index is PK-ordered by `next_wakeup`, so it stops at the
  first entry past `w`), then for each candidate partition reads just that partition from the buffer
  table with `iter_with_prefix`, in PK order — `(partition, order_key, seq)`, already `ORDER BY`
  ordered, so no in-memory sort. Every row with `order_key <= w` is final; the matcher runs over the
  safe prefix, a match is emitted once a later safe row follows it (so the greedy match is known
  maximal), and `AFTER MATCH SKIP` decides where the scan resumes. Matches stream straight into a
  `StreamChunkBuilder`, flushed a chunk at a time. After processing, the partition's frontier entry is
  recomputed (the earlier of its next future row and the earliest WITHIN expiry of a retained
  partial) or dropped. Work per watermark is therefore proportional to the
  partitions that need attention, not to the number of live partitions; the working set is the largest
  single candidate partition's live rows plus one output chunk.
- **Measures at match time.** Measures reference specific matched rows (`FIRST(a.ts)`, `LAST(b.v)`),
  known only once the match and its per-row variable labels are found, so each measure's synthetic
  row is built from the matched rows and the expression evaluated then. `WITHIN` is enforced during
  matching, pruning candidates that would push the span past the bound.
- **Eviction.** Rows before the earliest position that could still *begin* a match are evicted. Each
  candidate partition is read with its own `iter_with_prefix` scan, which is dropped before the
  evicting deletes run (a state-table delete cannot interleave with an open iterator over the same
  table), so deletes apply in place per partition. Together with the watermark this bounds state to
  the live (unfinalized) window (see [State bound and `WITHIN`](#state-bound-and-within)).

  Eviction fires for every partition the frontier wakes. A partition gaining a newly-safe row is
  woken by the row term of `next_wakeup`; an *idle* partition holding a retained partial bounded by
  `WITHIN` is woken by the deadline term (see [The wakeup frontier](#the-wakeup-frontier)) when that
  partial times out, so its dead rows are released even with no further input.

Matching is **not incremental**: each advancing watermark re-runs the matcher from the start of the
buffer rather than resuming partial NFA state. Eviction keeps that work bounded by the live window
rather than the partition's history; carrying incremental NFA state across watermarks is possible
future work.

## State and fault tolerance

The operator declares three internal state tables: the **buffer table** plus the two **wakeup
frontier** tables (below). The buffer table has layout `[ seq (i64), <input columns…> ]`, keyed by
`(partition columns, ORDER BY columns, seq)` and distributed by the partition key. Keying by the
order columns keeps the buffer physically sorted by `(partition, order key)`, so a partition can be
read back in key order and processed without an in-memory sort. `seq` is a per-actor monotonic id
that breaks ties between rows with equal `ORDER BY` keys. Only the raw buffered rows are persisted —
the NFA is recompiled from the pattern at startup and `DEFINE`/`MEASURES` are evaluated at match
time, so neither is stored. (This is less state than Flink's CEP, which persists the partial-match
SharedBuffer.)

- **Recovery.** The state table is authoritative, so there is no in-memory buffer to rebuild: after
  recovery the next watermark simply scans the (restored) state table per owned vnode (an empty-prefix
  scan cannot compute a vnode on a distributed table).
- **Rescaling.** On a vnode-bitmap change the set of partitions an actor owns shifts; the state table
  migrates the affected vnodes, and the next watermark scans whatever the actor now owns. There is no
  in-memory cache to reload or drop.
- **Parallelism.** Matching is independent per partition, so the input is hash-sharded by the
  `PARTITION BY` key and each actor owns its partitions' state.

### The wakeup frontier

A watermark only needs to touch partitions that have a newly-safe row (or, eventually, a retained
partial expiring by `WITHIN`). Without help, finding them means scanning every live partition each
watermark — `O(#live partitions)`, which dominates for high-cardinality `PARTITION BY` (per-player,
per-session, …). The frontier makes the work proportional to the partitions that actually need
attention. It is two internal tables, for two access patterns:

- `frontier_meta_table` — pk `(partition…)` → `next_wakeup_order_key`. Point-looked-up by partition
  on the insert path, so a chunk can re-point a partition's wakeup in one lookup.
- `frontier_index_table` — pk `(next_wakeup_order_key, partition…)`, **distributed by partition**.
  Range-scanned per owned vnode for `next_wakeup <= watermark`. The PK leads with the wakeup so the
  scan is a key-prefix scan that stops at the first entry past the watermark, while distributing by
  the partition keeps a partition's index entry on the same vnode as its buffered rows (so they
  re-shard together on rescale).

`next_wakeup_order_key` is the earliest `order_key` at which the partition next needs attention,
which is the earlier of two events:

- **the next row to become safe** — its earliest unprocessed `order_key`. On insert this is moved
  earlier when a chunk brings an earlier row (aggregated once per partition per chunk, not per row).
- **the earliest `WITHIN` expiry of a retained partial** — `first_order_key + interval`, computed
  from the `within_deadline` expression. Without it, an idle partition (no future row) holding a
  partial bounded by `WITHIN` would drop its frontier entry and never be revisited, leaking the
  partial until — if ever — a new row arrived. With it, the partition is woken exactly when the
  partial times out, and the eviction predicate (which already honours `WITHIN`) releases it.

On watermark the value is recomputed to the minimum of those two after processing, or the entry is
dropped when neither applies (a later insert re-schedules it). Both tables are committed with the
buffer table at each barrier, so the frontier and the buffer stay consistent across recovery and
rescale.

### State bound and `WITHIN`

State is bounded to the **live (unfinalized) window** — the rows that could still begin or extend a
match. What bounds that window depends on whether the pattern carries a `WITHIN` clause:

- **With `WITHIN <interval>`** the span of any match is capped, so once the watermark passes a row's
  `order_key + interval` that row can no longer begin or extend a match and is evicted. State per
  partition is bounded by the `WITHIN` window, and total state by that window times the number of
  live partitions.
- **Without `WITHIN`** a buffered prefix can be completed by an arbitrarily distant future row — e.g.
  `PATTERN (A B)` retains an `A` until some later `B` arrives, however long that takes — so an
  unmatched partial is kept indefinitely. This is correct SQL semantics (a streaming join without a
  time bound retains its build side the same way), but it means state is bounded only by the number
  of distinct `PARTITION BY` keys, not by time. For an unbounded key space (per-session, per-device,
  …) it grows without limit.

Resident memory is bounded either way — the executor streams partitions from the state table and
holds nothing between watermarks — so the unbounded quantity is the *persisted* state on the storage
engine, not process memory. To bound it, add a `WITHIN` clause; the binder emits a `NOTICE` when a
`MATCH_RECOGNIZE` has none, as a reminder. An opt-in state TTL (dropping partials older than a
configurable age, trading completeness for a hard bound) is possible future work.

## Limitations and future work

- `ALL ROWS PER MATCH`, batch execution, and non-append-only input are not supported.
- Matching is non-incremental (re-runs over the live window per watermark).
- Without a `WITHIN` clause, unmatched partials are retained indefinitely, so persisted state is
  bounded only by `PARTITION BY` key cardinality (see [State bound and `WITHIN`](#state-bound-and-within));
  the binder emits a `NOTICE` in that case.
- Anchors (`^`, `$`) and pattern exclusions (`{- … -}`) are parsed but rejected at planning time.

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
columns plus `_match_id`. It is hidden, so `SELECT *` returns only the user columns. The executor
fills it from a monotonic counter seeded from, and re-seeded to, the barrier epoch — so ids are
unique within a run and strictly increasing across restarts.

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
  the executor scans the state table for its owned vnodes in key order — `(partition, order_key,
  seq)` — so rows arrive grouped by partition and already ordered within each partition. It processes
  one partition at a time: the contiguous run for a partition is accumulated, matched (no in-memory
  sort — the scan order already is the `ORDER BY` order), emitted, then dropped before the next
  partition. Every row with `order_key <= w` is final; the matcher runs over the safe prefix, a match
  is emitted once a later safe row follows it (so the greedy match is known maximal), and `AFTER MATCH
  SKIP` decides where the scan resumes. The per-watermark working set is therefore bounded by the
  largest single partition's live rows plus one output chunk — not by a whole vnode — and nothing is
  retained between watermarks.
- **Measures at match time.** Measures reference specific matched rows (`FIRST(a.ts)`, `LAST(b.v)`),
  known only once the match and its per-row variable labels are found, so each measure's synthetic
  row is built from the matched rows and the expression evaluated then. `WITHIN` is enforced during
  matching, pruning candidates that would push the span past the bound.
- **Eviction.** Rows before the earliest position that could still *begin* a match are collected
  during the scan and deleted in a batch after the iterator is dropped (a state-table delete cannot
  interleave with an open iterator over it). Because every watermark scans all live partitions,
  expired rows are released even from partitions that receive no new input — an idle partition does
  not retain dead rows. Together with the watermark this bounds state to the live (unfinalized)
  window (see [State bound and `WITHIN`](#state-bound-and-within)).

Matching is **not incremental**: each advancing watermark re-runs the matcher from the start of the
buffer rather than resuming partial NFA state. Eviction keeps that work bounded by the live window
rather than the partition's history; carrying incremental NFA state across watermarks is possible
future work.

## State and fault tolerance

The operator declares one internal state table, layout `[ seq (i64), <input columns…> ]`, keyed by
`(partition columns, ORDER BY columns, seq)` and distributed by the partition key. Keying by the
order columns keeps the buffer physically sorted by `(partition, order key)`, so the watermark pass
scans it in key order and processes one partition at a time without an in-memory sort. `seq` is a
per-actor monotonic id that breaks ties between rows with equal `ORDER BY` keys. Only the raw
buffered rows are persisted — the NFA is recompiled from the pattern at startup and
`DEFINE`/`MEASURES` are evaluated at match time, so neither is stored. (This is less state than
Flink's CEP, which persists the partial-match SharedBuffer.)

- **Recovery.** The state table is authoritative, so there is no in-memory buffer to rebuild: after
  recovery the next watermark simply scans the (restored) state table per owned vnode (an empty-prefix
  scan cannot compute a vnode on a distributed table).
- **Rescaling.** On a vnode-bitmap change the set of partitions an actor owns shifts; the state table
  migrates the affected vnodes, and the next watermark scans whatever the actor now owns. There is no
  in-memory cache to reload or drop.
- **Parallelism.** Matching is independent per partition, so the input is hash-sharded by the
  `PARTITION BY` key and each actor owns its partitions' state.

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

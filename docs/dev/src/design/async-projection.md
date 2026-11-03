# Asynchronous projection

Streaming scalar projections separate synchronous computations from asynchronous expressions.
An expression containing an async call stays together, including conditional evaluation inside
`CASE`, `COALESCE`, `AND`, `OR`, and lambdas. The optimizer moves each async expression occurrence
once and forwards its result by column reference. Column references may appear in both layers to
preserve output order, stream keys, distribution, and watermarks. Project merging cannot create a
mixed layer, and a final split runs after expression rewrites. This applies when planning new
jobs; changing configuration or recovering a job does not rewrite a previously stored plan.

The following compute-node settings under `[streaming.developer]` apply only to asynchronous
`ProjectExecutor`s:

| Setting | Default | Meaning |
| --- | --- | --- |
| `project_expr_concurrency` | `1` | Maximum chunks awaiting ordered output; `0` removes the window limit. |
| `project_expr_inflight_request_concurrency` | `0` | Visible rows currently being evaluated; `0` removes the row limit. |

The row budget charges `StreamChunk::cardinality()`, excluding invisible rows. A whole chunk can
start while usage is below the budget, even if it takes usage above the budget. Further nonempty
chunks wait until usage drops below the budget. This admits a chunk larger than the budget without
deadlock and allows only one chunk to cross the limit at a time. Completion, expression errors, and
cancellation release the rows. Completed chunks waiting behind earlier results consume window
space but no row budget. Chunks, barriers, and watermarks retain their output order.

These limits belong to each executor instance. They are not shared across actors or different
projection operators, and the row budget is not an exact count of HTTP requests: a function may
batch several rows into one request or make several requests per row.

## Job overrides

Session settings capture an initial override when a new job is created:

```sql
SET streaming_project_expr_concurrency = 8;
SET streaming_project_expr_inflight_request_concurrency = 256;
CREATE MATERIALIZED VIEW projected AS
SELECT id, openai_embedding('{"model":"text-embedding-3-small"}'::jsonb, text)
FROM input;
```

Session override values must be nonnegative and fit a TOML signed 64-bit integer
(`0` through `9223372036854775807`). `SET` rejects larger values without changing the session.

Changing those session settings later does not affect an existing job. Use the existing job
configuration mechanism to change or remove its stored override:

```sql
ALTER MATERIALIZED VIEW projected SET CONFIG (
    streaming.developer.project_expr_concurrency = 4,
    streaming.developer.project_expr_inflight_request_concurrency = 128
);
ALTER MATERIALIZED VIEW projected RESET CONFIG (
    streaming.developer.project_expr_inflight_request_concurrency
);
```

An explicit job override takes precedence over the compute-node configuration. An unset session
setting, `SET ... = DEFAULT`, or a reset job override inherits the compute-node value. An explicit
`0` remains an override and means unlimited. As with other executor configuration changes,
`ALTER ... SET/RESET CONFIG` requires recovery of the affected job to take effect. The settings
are merged independently for each job and do not mutate the compute-node configuration.

## Materialized expressions

`MaterializedExprsExecutor` remains a sequential stateful operator and does not use these two
projection settings. It stores impure expression results on inserts and reuses them on deletions,
so retracting a row does not repeat an external call. The planner selects this path for impure
expressions on retract streams and, when needed for stream-key expressions, on upsert streams.
Append-only input and upsert projections of non-key columns use regular scalar projections.
The explicit unsafe option for unmaterialized impure expressions also uses regular projections.
Cross-chunk concurrency for the stateful materialization path requires separate state-ordering
work.

## Executor mock tests and benchmark

`executor::test_utils::async_project` runs the real `ProjectExecutor` with a test-only expression
that models one batch RPC per chunk. Each batch specifies physical capacity, visible rows, and
latency. It returns the input identifiers unchanged so the harness can verify every output chunk,
visibility bitmap, and checkpoint barrier in order. The mock is available only in test builds or
with the stream crate's `test` feature.

Run the deterministic, virtual-time configuration regressions with:

```sh
cargo test --locked -p risingwave_stream --lib executor::test_utils::async_project::tests
```

They exercise both limits through the complete executor, including unlimited settings,
single oversized admissions, invisible and zero-visible rows, release before ordered output,
slow-head buffering, checkpoint order, and cancellation. Existing gated-expression tests also
cover expression errors and watermarks. No wall-time performance threshold is asserted in CI.

Run the wall-clock benchmark with:

```sh
PROJECT_BENCH_CHUNKS=64 PROJECT_BENCH_ROWS=128 PROJECT_BENCH_DELAY_MS=10 \
PROJECT_BENCH_REPEATS=3 \
cargo bench --locked -p risingwave_stream --bench async_project > async-project.jsonl
```

The environment variables above are optional and show their defaults. Each case warms up once
and writes one JSON object per measured repetition. The benchmark scans chunk limits `1/2/4/8/16`
with unlimited rows, then scans row limits with a chunk window of 16. Additional cases model
deterministic latency jitter, varying batch sizes and visibility, a slow first request, and
zero-latency scheduling. The unlimited-window baseline always uses finite input.

Reported statistics include visible rows/s, total elapsed time, chunk output p50/p95/p99,
evaluation p95, completed-to-output wait p95, checkpoint admission-to-output p95, and peak active
chunks, active visible rows, window chunks, and completed-but-not-emitted chunks. Output latency
starts when all finite input is queued, so it includes source backlog; checkpoint latency starts
when the source hands a barrier to the executor. The completed-buffer count includes a completed
result that is immediately ready for output. Peaks are counts rather than byte-memory estimates.

Construction and result aggregation are outside the timed interval; instrumentation and output
validation are inside it. Compare repetitions on the same host and use the zero-latency cases to
identify instrumentation overhead. For a quick build using existing development artifacts,
`cargo bench --profile dev` runs the same scenarios, but its numbers should be labeled as development
measurements. This finite, single-executor latency model isolates scheduling effects; it does not
predict external-service capacity, multi-actor resource contention, or cluster-wide throughput.

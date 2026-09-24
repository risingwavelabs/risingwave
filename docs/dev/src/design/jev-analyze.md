# Jev evaluation

`jev_analyze` evaluates SQL-built context using a System One compatible HTTP service:

```sql
jev_analyze(
    state JSONB,
    model_url VARCHAR,
    model VARCHAR,
    questions JSONB,
    api_key VARCHAR
) -> JSONB
```

All five arguments may be expressions or columns. `model_url` is the complete
HTTP(S) evaluation endpoint, for example `https://api.impossibl.com/v1/systemone`.
It is separate from the model identifier, such as
`convaiinnovations/laya-multilingual`. The function sends one POST request per
non-null input row, with a Bearer authorization header and a JSON body containing
`state`, `model`, and `questions`. An array in `state` is one context, not a batch
of independent requests.

The entire successful JSON response is returned as JSONB, including metadata.
The service interprets question definitions; RisingWave does not infer a SQL
schema from them. Extract and cast answer fields with SQL as needed. Any SQL NULL
argument produces SQL NULL without a request. A JSON `null` value is sent normally.

This version accepts an ordinary VARCHAR API key; SQL `SECRET` references are
not supported. Jev expression displays redact the key argument and HTTP errors
exclude request and response contents. This does not make a key literal secret:
SQL statement text, binding errors, catalog definitions, and argument columns can
still contain it. Use dummy credentials in checked-in SQL and test fixtures.

## Constructing grouped or windowed context

The following examples assume an `events` table with `user_id`, `seq`, `ts`, and
`payload JSONB`. Replace `YOUR_API_KEY` before executing.

```sql
CREATE MATERIALIZED VIEW user_judgments AS
SELECT user_id,
       jev_analyze(
           jsonb_agg(payload ORDER BY seq),
           'https://api.impossibl.com/v1/systemone',
           'convaiinnovations/laya-multilingual',
           '{"resolved":{"type":"noul","instructions":"Is the issue resolved?"}}'::jsonb,
           'YOUR_API_KEY'
       ) AS judgment
FROM events
GROUP BY user_id;
```

For a rolling context, construct the window result first:

```sql
CREATE MATERIALIZED VIEW rolling_judgments AS
SELECT user_id, ts,
       jev_analyze(
           to_jsonb(array_agg(payload) OVER (
               PARTITION BY user_id ORDER BY ts
               ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
           )),
           'https://api.impossibl.com/v1/systemone',
           'convaiinnovations/laya-multilingual',
           '{"resolved":{"type":"noul","instructions":"Is the issue resolved?"}}'::jsonb,
           'YOUR_API_KEY'
       ) AS judgment
FROM events;
```

Immediate emission evaluates each state update emitted by the upstream operator.
For on-close emission, use an append-only input with an event-time watermark and
append `EMIT ON WINDOW CLOSE` to a supported window query. For example, group
`tumble(events, ts, INTERVAL '5 minutes')` by `user_id, window_start`, or use the
rolling query above. Jev evaluation runs after the aggregate or OVER window emits
its context; it does not run inside the window aggregate. Existing watermark and
late-data rules apply. A plain unbounded GROUP BY has no window to close.

## Execution and recovery

The function is volatile and is never evaluated during constant folding. On
ordinary retractable streams, the planner uses `StreamMaterializedExprs` to store
the actual result. Updates and deletes retract that stored JSON value without
calling the service again. Append-only results, including on-close results, use
the ordinary asynchronous projection path. Existing restrictions on volatile
expressions in other SQL positions still apply; put the call in a SELECT
projection. Leave `streaming_unsafe_allow_unmaterialized_impure_expr` disabled.

Each expression instance reuses an HTTP client and allows at most eight requests
in flight, including across overlapping chunk evaluations. Results retain input
order. Watermarks and barriers wait for preceding results. One slow request can
therefore delay the chunk and checkpoint progress; this version does not provide
independent per-key scheduling.

Each attempt has a 30-second timeout. Transport errors, HTTP 429, and HTTP 5xx
responses retry up to three attempts total, using 100 ms then 200 ms delays.
A numeric `Retry-After` header overrides the delay, capped at five seconds.
Other HTTP failures, redirects, invalid success JSON, and responses larger than
16 MiB fail immediately. A failed request propagates an execution error even
through non-strict streaming expressions or `coalesce`; it is not saved as a NULL
judgment. Ordinary SQL calculation errors retain their existing non-strict behavior.

Retries and recovery may repeat requests that were not checkpointed. Exactly-once
HTTP invocation is not guaranteed, and repeated inference can incur additional
cost or return a different answer. After local retries are exhausted, streaming
recovery may attempt the same input again. Persistent service or configuration
errors therefore prevent progress until resolved.

## Regression coverage

- Expression mock HTTP tests: request shape, full response, retry classification,
  timeout, SQL NULL and invisible rows, ordered results, shared concurrency limit.
- Project tests: saved results across updates, deletes and executor recovery;
  fatal error propagation and barrier/watermark ordering.
- Planner fixture `jev_analyze`: dynamic arguments, volatility, result storage,
  GROUP BY/OVER placement, both emission modes, and signature errors.
- `./risedev slt './e2e_test/streaming/jev_analyze.slt'`: a temporary local HTTP
  service verifies SQL-to-HTTP evaluation with synthetic data and credentials.
  Its changing responses detect reevaluation on retraction; the service is closed
  and SQL objects are dropped when the test completes. The HTTP fixture is skipped
  under madsim.

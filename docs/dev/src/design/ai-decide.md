# AI decisions

`ai_decide` asks a fixed set of typed questions about each SQL-built context using
a System One compatible HTTP service. One call can mix probability, choice and
score questions. The result is a named STRUCT, with one field per question.

```sql
ai_decide(
    state JSONB,
    model_url VARCHAR,
    model VARCHAR,
    questions JSONB,
    api_key VARCHAR
) -> STRUCT<...>
```

Use the constructors below to define questions without writing JSON. All their
arguments must be constant expressions; they are evaluated and validated during
planning and do not make HTTP requests. The question names determine the result
field names. Question definitions cannot depend on input rows.

```sql
-- Assumes ticket_context(ticket_id, state JSONB, model_url, model, api_key).
CREATE MATERIALIZED VIEW ticket_answers AS
SELECT ticket_id,
       ai_decide(
           state, model_url, model,
           ai_questions(
               ai_prob_question('refund', 'Does the customer request a refund?'),
               ai_choice_question('team', 'Which team should handle this?', ARRAY[
                   ai_option('billing', 'Charges, invoices and refunds'),
                   ai_option('technical', 'Errors and integration problems'),
                   ai_option('sales', 'Pricing, purchases and upgrades')
               ]),
               ai_score_question('urgency', 'How urgent is this?', ARRAY[
                   'Can wait several days',
                   'Needs attention today',
                   'Service is blocked; immediate attention needed'
               ])
           ),
           api_key
       ) AS answer
FROM ticket_context;

SELECT ticket_id,
       (answer).refund AS refund_probability,
       (answer).team.choice AS team,
       (answer).team.confidence AS confidence,
       (answer).team.probabilities['billing'] AS billing_probability,
       (answer).urgency.score AS urgency
FROM ticket_answers
WHERE (answer).refund > 0.8;
```

Reading fields from this materialized view reads the saved answer. Place one
`ai_decide` call in a subquery projection when several output columns need its
fields. Writing separate calls for separate questions does not combine them into
one request. Retries and recovery can still repeat a logical request.

## Question constructors

| Constructor | Meaning |
| --- | --- |
| `ai_questions(question, ...)` | Combine one or more definitions; names must be unique. |
| `ai_prob_question(name, instructions)` | Probability that the statement is true (`noul`). |
| `ai_choice_question(name, instructions, options)` | Select one option. Pass `VARCHAR[]` labels or `JSONB[]` built with `ai_option`. |
| `ai_option(label, description)` | Pair a stable output label with its description. Use SQL NULL for an omitted description. |
| `ai_score_question(name, instructions, levels)` | Rate against 2–10 distinct level descriptions, ordered lowest to highest. |

Names, instructions, labels and supplied descriptions must be nonempty strings.
Choice supports 1–255 unique labels. A simple choice can use
`ARRAY['billing', 'technical', 'sales']`. To describe only some options, use
`ai_option` for every element and pass NULL for descriptions you wish to omit.
The descriptions guide the model; the returned choice is always the label.

The constructors return JSONB definitions. A constant JSONB question object is
also accepted by the same `ai_decide` entry point and undergoes the same schema
validation. This version accepts string instructions/descriptions only; nested
JSON instructions and per-row definitions are not supported.

## Result types

| Question | SQL field type |
| --- | --- |
| Probability | `DOUBLE PRECISION` in [0, 1] |
| Choice | `STRUCT<choice VARCHAR, confidence DOUBLE PRECISION, probabilities MAP(VARCHAR, DOUBLE PRECISION)>` |
| Score | `STRUCT<score DOUBLE PRECISION, confidence DOUBLE PRECISION, probabilities DOUBLE PRECISION[]>` |

A probability remains numeric; apply your own threshold in SQL. Choice confidence
is separate from the probability of the winning label. A score is a fractional
position from 0 to the number of levels minus one, not a probability. Score
probabilities follow the declared level order; SQL arrays are one-based, so
`probabilities[1]` corresponds to level 0.

The service must return an `answers` object with each declared question and its
matching `type`. Required fields cannot be absent or null. Choices must name a
declared label. Confidence and probabilities must be numeric values in [0, 1];
probability distributions must contain exactly the declared labels or level
indices. Probabilities are preserved as returned, including service-side rounding,
without renormalization or a sum-to-one check. Invalid responses fail the
query, including through non-strict streaming expressions. They do not silently
become NULL, zero, or an empty label. Additional response metadata is ignored.

## Connection configuration

State, endpoint, model and API key may be expressions or columns. A SQL NULL in
any of these four arguments produces a NULL STRUCT without a request; JSON null
is sent as a real state. Questions must be a non-null constant. The function
accepts exactly five arguments; there is no execution-options argument.

`model_url` is the complete HTTP(S) evaluation endpoint, for example
`https://api.impossibl.com/v1/systemone`. It is separate from the model identifier,
such as `convaiinnovations/laya-multilingual`. The function sends one POST request
per non-null input row with Bearer authentication and `state`, `model` and
`questions` in the JSON body. All questions share the state. An array in `state`
is one context, not a batch of independent requests. The AI name does not imply
support for arbitrary model-provider protocols.

This version accepts an ordinary VARCHAR API key; SQL SECRET references are not
supported. Expression displays redact the key argument and HTTP/response errors
exclude request and response contents. SQL statement text, binding errors,
catalog definitions and argument columns can still contain key literals. Use
dummy credentials in checked-in SQL and test fixtures.

## Constructing grouped or windowed context

The following examples assume an `events` table with `user_id`, `seq`, `ts`, and
`payload JSONB`. Replace `YOUR_API_KEY` before executing.

```sql
CREATE MATERIALIZED VIEW user_judgments AS
SELECT user_id,
       ai_decide(
           jsonb_agg(payload ORDER BY seq),
           'https://api.impossibl.com/v1/systemone',
           'convaiinnovations/laya-multilingual',
           ai_questions(ai_prob_question('resolved', 'Is the issue resolved?')),
           'YOUR_API_KEY'
       ) AS judgment
FROM events
GROUP BY user_id;
```

For a rolling context, construct the window result first:

```sql
CREATE MATERIALIZED VIEW rolling_judgments AS
SELECT user_id, ts,
       ai_decide(
           to_jsonb(array_agg(payload) OVER (
               PARTITION BY user_id ORDER BY ts
               ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
           )),
           'https://api.impossibl.com/v1/systemone',
           'convaiinnovations/laya-multilingual',
           ai_questions(ai_prob_question('resolved', 'Is the issue resolved?')),
           'YOUR_API_KEY'
       ) AS judgment
FROM events;
```

Immediate emission evaluates each state update emitted by the upstream operator.
For on-close emission, use an append-only input with an event-time watermark and
append `EMIT ON WINDOW CLOSE` to a supported window query. For example, group
`tumble(events, ts, INTERVAL '5 minutes')` by `user_id, window_start`, or use the
rolling query above. AI evaluation runs after the aggregate or OVER window emits
its context; it does not run inside the window aggregate. Existing watermark and
late-data rules apply. A plain unbounded GROUP BY has no window to close.

## Execution and recovery

The function is volatile and is never evaluated during constant folding. On
ordinary retractable streams, the planner uses `StreamMaterializedExprs` to store
the actual result. Updates and deletes retract that stored STRUCT value without
calling the service again. Append-only results, including on-close results, use
the ordinary asynchronous projection path. Existing restrictions on volatile
expressions in other SQL positions still apply; put the call in a SELECT
projection. Leave `streaming_unsafe_allow_unmaterialized_impure_expr` disabled.

Each expression instance reuses an HTTP client. By default all eligible rows may
run concurrently. `ai_decide_max_concurrency` optionally limits logical requests
across all overlapping chunks of one expression instance; `0` keeps the default
unlimited behavior. Each visible row with non-NULL arguments makes one logical
request, retaining its permit through sequential retries and backoff. Cancelling
local evaluation releases permits. Separate expressions and actors have separate
limits, so this is not a job-wide or endpoint-wide quota. Fixed questions
and the inferred result type are cached per expression. Other constant arguments
keep their scalar representation instead of being expanded into per-row arrays.
Each eligible request still serializes its own state and questions.

Requests complete independently of row order. Results are collected by input row
index and returned in their original positions, including
NULL and invisible rows. The expression still waits for the entire chunk before
returning its array. Watermarks and barriers wait for preceding results. One slow
request can therefore delay the chunk and checkpoint progress; this version does
not provide independent per-key scheduling.

Ordinary streaming projection can evaluate multiple chunks with
`streaming.developer.project_expr_concurrency` (default 1). The separately configured
`project_expr_inflight_request_concurrency` budgets visible rows being evaluated,
not individual HTTP requests. A whole chunk may cross the row budget once; even
rows with NULL arguments count toward this budget although they make no request.
Both settings use `0` for unlimited. The planner identifies AI_DECIDE as async
and separates it from synchronous computations while preserving conditional
evaluation and shared results.

Set `streaming_project_expr_concurrency` and
`streaming_project_expr_inflight_request_concurrency` before creating a job to
capture per-job overrides, or use the existing `ALTER ... SET/RESET CONFIG`
mechanism for existing jobs (effective after recovery). See
[asynchronous projection](async-projection.md) for the limits and override rules.
These settings do not apply to batch execution. `StreamMaterializedExprs` continues
to evaluate chunks sequentially without these Project limits, while AI_DECIDE
evaluates eligible rows within each chunk concurrently. The row budget is not an
exact HTTP-request limit: a budget of eight still admits a whole 1,024-row chunk,
which can issue up to 1,024 concurrent requests for one AI_DECIDE call per row
when the separate AI_DECIDE request limit is disabled.

By default, each attempt has a 30-second timeout. Transport errors, HTTP 429, and HTTP 5xx
responses retry up to three attempts total, using 100 ms then 200 ms delays.
This includes failures while reading response bytes, such as a truncated 200
response; a complete response with invalid JSON or typed answers does not retry.
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

On evaluation failure, other local futures for that chunk are dropped. Requests
already received by the service may continue running; dropping local futures does
not guarantee remote cancellation.

## Execution settings

These settings preserve the five-argument SQL function. They can be set as session
variables for batch queries and new streaming jobs. For streaming job creation,
the same names are accepted in `WITH`, with precedence over the session value;
this does not change the session. Unset streaming values inherit the compute-node
`[streaming.ai_decide]` configuration. Unset batch values use the defaults below.

| Session / WITH option | Default | Valid values |
|---|---:|---|
| `ai_decide_request_timeout_ms` | 30000 | Positive, per HTTP attempt |
| `ai_decide_max_attempts` | 3 | Positive, includes the initial attempt |
| `ai_decide_retry_delay_ms` | 100 | Initial exponential delay; 0 disables it |
| `ai_decide_max_retry_after_ms` | 5000 | Cap on numeric Retry-After; 0 disables that delay |
| `ai_decide_max_response_bytes` | 16777216 | Positive, before JSON decoding |
| `ai_decide_max_concurrency` | 0 | Per expression instance; 0 means unlimited |

All values are unsigned 32-bit integers. Exponential delay saturates at the
maximum representable millisecond value instead of overflowing.

```sql
SET ai_decide_max_concurrency = 16;
CREATE MATERIALIZED VIEW decisions WITH (
    ai_decide_max_concurrency = 8,
    ai_decide_request_timeout_ms = 10000
) AS SELECT id, ai_decide(state, endpoint, model, ai_questions(ai_prob_question('accept', 'Accept?')), api_key) AS result
FROM inputs;
```

The job captures the explicit session and WITH settings in its
streaming config override, which survives recovery and rescheduling. Existing
jobs can be changed with the usual configuration mechanism:

```sql
ALTER MATERIALIZED VIEW decisions SET CONFIG (
    streaming.ai_decide.max_concurrency = 4
);
RECOVER;
```

`SET/RESET CONFIG` takes effect after recovery; it does not resize a running
expression's semaphore. RESET removes that job override and restores inheritance
from the compute-node configuration. The same request cap applies to batch,
ordinary streaming Project and MaterializedExprs. It does not change chunk output
ordering, saved retractions, or barrier ordering. Error classification and bounded
retry behavior remain as described above.

## Tuning

Tune against successful row throughput, HTTP attempt rate, latency distributions,
429s, memory and checkpoint progress. Use finite Project windows. Increasing
parallelism can increase service queueing or rejection rather than throughput;
coordinate shared endpoint quotas outside individual expressions.
AI_DECIDE-specific metrics and dashboards are deferred.
Result buffering remains bounded by the input chunks being evaluated, and each
response is limited to 16 MiB by default. There is no separate aggregate byte budget.

## Regression coverage

- Schema tests: named mixed result types and invalid question definitions.
- Mock HTTP tests: request shape, response decoding and contract failures,
  retries/timeouts and truncated bodies, known/chunked response size limits,
  credential redaction, scalar arguments, NULL and invisible rows, empty chunks, concurrent requests
  across chunks, ordered results despite a blocked first row, and prompt errors
  from later rows.
- Project tests: saved results across updates, deletes and recovery; fatal errors
  and barrier/watermark ordering.
- Planner fixture `ai_decide`: fixed definitions, constructors, typed field
  access, early validation, volatility, result storage and GROUP BY/OVER placement.
- `./risedev slt './e2e_test/streaming/ai_decide.slt'`: a temporary local HTTP
  service checks mixed answers, descriptions, single evaluation for multiple
  field reads, saved-result retractions, GROUP BY/OVER and emission modes.
  The external-service fixture is skipped under madsim.

## Local synthetic performance checks

`e2e_test/streaming/ai_decide_bench.py` runs an opt-in SQL-to-HTTP benchmark against
an asyncio Jev mock. It requires an isolated local RisingWave instance, Python
and `psql`; it does not contact a model service. For example:

```sh
python3 e2e_test/streaming/ai_decide_bench.py --port 4566 \
  --output /tmp/ai-decide-bench.json --rounds 3 --faults
```

On Linux, add `--pid <standalone-pid>` to sample the whole RisingWave process's
RSS and CPU time. Use `--only fast_64b,fast_64k` or `--rows 512` for a focused
comparison. The script checks output values and exact request counts, marks
one warmup per case for exclusion from analysis, and includes NULL inputs, large
fixed questions, delayed/long-tail responses, a capacity-limited backend,
streaming Project windows, saved-result deletions and optional body truncation.
It creates uniquely named tables/views and drops them after each case.

Compare identical builds and settings on the same host while other build/load
jobs are idle. SQL timing includes client startup and, for streaming, INSERT,
FLUSH and result visibility. Backend latency measures receipt-to-response time,
not client-observed row latency. RSS is sampled every 5 ms and includes the whole
standalone process. Debug builds, local mock parsing, in-memory storage and
artificial delays cannot establish production model throughput. Large fanout can
also hit local socket/ephemeral-port limits; this is separate from inference
capacity. Batch Project currently admits up to 16 chunks concurrently, so rows
across those chunks can all initiate requests.

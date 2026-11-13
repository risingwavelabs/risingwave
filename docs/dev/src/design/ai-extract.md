# AI extraction

`ai_extract` extracts typed entity spans or structured records from each text
using an extraction-compatible HTTP endpoint. It shares the asynchronous
execution, retry, cancellation, result materialization and recovery semantics of
[AI decisions](ai-decide.md). It does not load a model inside RisingWave.

```sql
ai_extract(
    content VARCHAR,
    model_url VARCHAR,
    model VARCHAR,
    extraction_spec JSONB,
    api_key VARCHAR [, headers MAP(VARCHAR, VARCHAR)]
) -> STRUCT<...>
```

The extraction specification is a non-null constant, validated during planning.
It determines the result's field names and SQL types. Content, endpoint, model,
API key and headers can depend on input rows. A SQL NULL in any supplied argument
other than the constant specification returns a NULL STRUCT without HTTP. An
omitted headers argument or an empty map adds no custom headers. Header validation
and credential redaction are identical to AI_DECIDE, including rejection of
Authorization overrides. Statement text and catalogs can still contain literals;
SQL SECRET integration is not part of this interface.

## Entity specification

```sql
CREATE MATERIALIZED VIEW extracted_entities AS
SELECT id, ai_extract(
    body, endpoint, model,
    '{"task":"entities","labels":["person","company"],
      "word_splitter":"whitespace"}'::jsonb,
    api_key
) AS extraction
FROM documents;

SELECT id, (extraction).person FROM extracted_entities;
SELECT id, ((extraction).person[1]).text AS first_person FROM extracted_entities;
```

`labels` is a nonempty list of distinct, nonempty names. Each named result field
is an array of spans, including an empty array when no entity matches. Multiple,
overlapping and repeated spans are preserved, in the service's order; RisingWave
does not choose a single candidate, deduplicate or renormalize model scores.

A span has SQL type
`STRUCT<text VARCHAR, start INTEGER, end INTEGER, confidence DOUBLE PRECISION>`.
Offsets are zero-based Unicode code points, with an exclusive end. The decoded
span must be nonempty, inside the original content, and exactly match its text.
Confidence must be finite and in [0, 1]. SQL array subscripts remain one-based;
SQL substring positions are also different from these zero-based offsets.

## Record specification

```sql
SELECT ai_extract(
    body, endpoint, model,
    '{"task":"records","record_name":"product_launch","anchor":"product",
      "fields":[{"name":"product"},{"name":"company"},
                {"name":"prices","dtype":"list","description":"Product prices"}],
      "word_splitter":"whitespace"}'::jsonb,
    api_key
) AS extraction
FROM documents;
```

The result has one field named `product_launch`: an array of records. Every
record has the declared fields, in specification order, followed by its own
`confidence DOUBLE PRECISION`. A `str` field (the default dtype) is a nullable
span; a `list` field is a non-null span array, empty if nothing was extracted.
An empty record array is a successful extraction with no records.

`anchor` must name one of the declared fields and controls service-side record
grouping. Field names must be distinct and may not be `confidence`. Descriptions
are optional strings. No numeric/date conversion, arbitrary nested schema,
classification, relation extraction, OCR or generated text is supported in this
version. For example, a price span retains `$799`, not an inferred number 799.

## HTTP contract

The endpoint is a complete HTTP(S) URL, such as an extraction service's
`/v1/extract`. One non-null input row is one logical request. The JSON body
contains `model`, `text`, and the specification's fields at the top level:

```json
{"model":"example/extractor","text":"Tim Cook works at Apple.",
 "task":"entities","labels":["person","company"],
 "word_splitter":"whitespace"}
```

`word_splitter` is required and must be `whitespace` or `char`; it is not inferred
from the text. Optional `threshold` is a finite number in [0, 1]. Omission leaves
the threshold to the service. Unknown specification keys are rejected, including
keys that could replace the separately supplied model or text. Endpoint-specific
model and input-size limits still apply; RisingWave never truncates input to fit.
Empty or whitespace-only content is a local input error.

Entity responses have the following shape:

```json
{"task":"entities","offset_unit":"unicode_code_point","end_exclusive":true,
 "word_splitter":"whitespace",
 "result":{"entities":{"person":[{"text":"Tim Cook","start":0,"end":8,
 "confidence":0.99}],"company":[{"text":"Apple","start":18,"end":23,
 "confidence":0.98}]}}}
```

Record responses instead contain `result[record_name]`, an array of records with
all declared fields plus record-level confidence. Missing fields are invalid:
an absent string span must be explicit JSON null, and an absent list must be `[]`.
The task, splitter and offset declaration must match the contract. Unknown or
missing entity/record fields are invalid. Additional outer metadata (for example,
model revision, timing and usage) is not part of the inferred SQL result.

## Execution, failure and recovery

The function is volatile and asynchronous. No request runs during planning or
constant folding. Like AI_DECIDE, eligible rows may complete out of order but
return to their original positions; the expression awaits the entire chunk.
Barriers and watermarks cannot overtake preceding results. Repeated field reads
from a saved result do not call the service again. Use a single call in a
projection when several output fields are needed.

The existing planner selects ordinary Project or MaterializedExprs according to
the input stream kind and stream key. Stateful materialization reuses the actual
saved value when retracting a row; a changed input evaluates its new value.
Append-only and supported window emissions use existing asynchronous projections.
No new operator or per-key scheduling policy is introduced.

Failure behavior deliberately matches AI_DECIDE. All remote failures, including
non-success statuses, invalid JSON, oversized responses and invalid spans, retry
indefinitely until valid success or cancellation. Permanent failures can therefore
block a chunk and checkpoint progress. Local request-construction errors follow
ordinary expression strictness: strict evaluation reports the error, and NonStrict
evaluation preserves successful rows while turning failed rows into NULL. There
is no error envelope or bounded-retry policy specific to extraction.

Each logical request holds its concurrency permit during retries and backoff.
Cancellation releases local resources but does not cancel work already accepted
by the service. Uncheckpointed work can be sent again after recovery. Exactly-once
HTTP execution is not guaranteed. An endpoint supporting idempotency can receive
a stable input-version identifier through headers; merely sending a header does
not guarantee that the endpoint deduplicates it.

## Execution settings

`ai_extract_*` settings mirror AI_DECIDE defaults and scope:

| Session / WITH option | Default |
| --- | ---: |
| `ai_extract_request_timeout_ms` | 30000 |
| `ai_extract_max_retry_delay_ms` | 10000 |
| `ai_extract_retry_delay_ms` | 100 |
| `ai_extract_max_retry_after_ms` | 5000 |
| `ai_extract_max_response_bytes` | 16777216 |
| `ai_extract_max_concurrency` | 0 (unlimited) |

Timeout and response limits must be positive. Limits apply to one expression
instance, not an entire job or endpoint. Batch plans carry the resolved settings;
streaming jobs capture session/WITH overrides, and inherit unset values from
`streaming.ai_extract`. WITH takes precedence over session settings. Existing
jobs use `ALTER ... SET/RESET CONFIG (streaming.ai_extract...)`; changes take effect
after recovery. Generic asynchronous Project limits retain their existing scope,
including their absence from the sequential MaterializedExprs path.

## Validation

Coverage must include binding and typed field access, unknown/duplicate schema
fields, scalar and list record fields, Unicode spans and mismatched text, NULL
and invisible rows, empty successes, headers and redaction, partial local errors,
remote retry behavior, concurrency and cancellation, ordered output, both streaming
projection paths, stored retractions and recovery. HTTP fixtures use only local
servers and synthetic keys. External-process SQL fixtures are skipped in madsim.
Real model smoke tests are separate, explicit runs, not a CI dependency.

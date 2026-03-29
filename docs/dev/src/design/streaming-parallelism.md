# Streaming Parallelism

Streaming parallelism is configured by two kinds of session parameters:

- `streaming_parallelism`
- `streaming_parallelism_for_<type>`

Supported `<type>` values in this document:

- `table`
- `materialized_view`
- `index`
- `source`
- `sink`

## Value Semantics

The unified parallelism value accepts the following forms:

- `0`
- `adaptive`
- `default`
- `<n>`
- `bounded(<n>)`
- `ratio(<r>)`

The meaning is:

- `0` and `adaptive` mean adaptive scheduling with the full worker set.
- `<n>` means fixed parallelism.
- `bounded(<n>)` means adaptive scheduling with an upper bound.
- `ratio(<r>)` means adaptive scheduling with a ratio of the available worker parallelism.

The stored default value for all unified parameters is `default`.

The effective defaults preserve the legacy behavior:

- `streaming_parallelism = default`, which resolves to `bounded(64)`
- `streaming_parallelism_for_table = default`, which resolves to `bounded(4)` only when `streaming_parallelism` is also `default`
- `streaming_parallelism_for_source = default`, which resolves to `bounded(4)` only when `streaming_parallelism` is also `default`
- all other `streaming_parallelism_for_<type>` values default to `default` and follow `streaming_parallelism`

For these parameters, `SET ... = DEFAULT` and `SET ... = 'default'` both restore the stored
`default` value, so `SHOW` output remains round-trippable.

## Resolution Rules

Resolution happens in a single step:

1. Resolve `streaming_parallelism`.
   - `default` resolves to the legacy global default `bounded(64)`.
2. Resolve `streaming_parallelism_for_<type>`.
   - For `table` and `source`, `default` resolves to the legacy type-specific default `bounded(4)` only when `streaming_parallelism` is still `default`; otherwise it follows the resolved global value.
   - For other job types, `default` falls back to the resolved global value.
3. Convert the resolved value into:
   - a fixed parallelism, or
   - an adaptive strategy stored in the streaming job metadata.

This means the frontend always writes the final adaptive strategy into the job context, and meta no
longer depends on a separate system-wide adaptive strategy parameter.

## Examples

```sql
SET streaming_parallelism = adaptive;
SET streaming_parallelism = 8;
SET streaming_parallelism = 'bounded(16)';
SET streaming_parallelism = 'ratio(0.5)';
```

```sql
SET streaming_parallelism = 'ratio(0.5)';
SET streaming_parallelism_for_materialized_view = 'bounded(4)';
SET streaming_parallelism_for_materialized_view = DEFAULT;
```

## Migration

Older releases exposed these deprecated parameters:

- `streaming_parallelism_strategy`
- `streaming_parallelism_strategy_for_<type>`
- `adaptive_parallelism_strategy`

On startup, meta derives the final `streaming_parallelism` and
`streaming_parallelism_for_<type>` values once from the legacy parameters, persists the new values,
and drops the deprecated entries. If the legacy system parameter
`adaptive_parallelism_strategy` is missing, the migration interprets it using the released legacy
default `AUTO`. Existing jobs without a stored job-level strategy are materialized as `AUTO` to
preserve their released behavior, while untouched sessions on the new runtime still resolve the
unified defaults (`streaming_parallelism = default` to `bounded(64)`, table/source `default` to
`bounded(4)`). New clusters only expose the unified parameters.

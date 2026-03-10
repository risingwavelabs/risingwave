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

- `0`, `adaptive`, and the global default all mean adaptive scheduling with the full worker set.
- `<n>` means fixed parallelism.
- `bounded(<n>)` means adaptive scheduling with an upper bound.
- `ratio(<r>)` means adaptive scheduling with a ratio of the available worker parallelism.

Built-in defaults are:

- `streaming_parallelism = adaptive`
- `streaming_parallelism_for_table = bounded(4)`
- `streaming_parallelism_for_source = bounded(4)`
- all other `streaming_parallelism_for_<type>` values default to `default`

For type-specific parameters, `SET ... = DEFAULT` resets the value to the built-in default for
that parameter. This means `table` and `source` reset to `bounded(4)`, while other job types fall
back to `streaming_parallelism`.

## Resolution Rules

Resolution happens in a single step:

1. Read `streaming_parallelism_for_<type>`.
2. If the value is `default`, fall back to `streaming_parallelism`.
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
and drops the deprecated entries. The legacy `BOUNDED(4)` defaults for `table` and `source` are
materialized into the new parameters during this migration. New clusters only expose the unified
parameters.

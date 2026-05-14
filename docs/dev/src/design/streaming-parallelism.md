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

The deprecated parameters fall into two groups:

1. `adaptive_parallelism_strategy`

   This parameter existed in official releases and was the old cluster-wide knob for adaptive
   scheduling. Users could previously set it through `ALTER SYSTEM` or config files to choose how
   `adaptive` should expand by default, for example `AUTO`, `BOUNDED(n)`, or `RATIO(r)`.

   This is no longer supported as a separate user-facing setting. After the migration, users must
   express the final policy directly with `streaming_parallelism` and
   `streaming_parallelism_for_<type>`, such as `adaptive`, `bounded(64)`, or `ratio(0.5)`. There
   is no longer a separate "adaptive strategy default" to configure.

2. `streaming_parallelism_strategy` and `streaming_parallelism_strategy_for_<type>`

   These parameters only existed in Nightly builds between v2.8.0 and v3.0.0, so this part of the migration is
   not a breaking change for official releases. It still matters for Nightly users because their
   stored values are migrated to the unified parameters.

   These parameters were also separate strategy-only knobs: users could combine
   `streaming_parallelism = adaptive` with a matching `streaming_parallelism_strategy = ...`, or do
   the same per job type. This split representation is no longer supported. After the migration,
   users set only the unified `streaming_parallelism*` parameters, and each value already carries
   its own strategy.


On startup, meta derives the final `streaming_parallelism` and
`streaming_parallelism_for_<type>` values once from the legacy parameters, persists the new values,
and drops the deprecated entries. For `adaptive_parallelism_strategy`, if the legacy system
parameter is missing, the migration interprets it using the released legacy default `AUTO`.
Existing jobs without a stored job-level strategy are materialized as `AUTO` to preserve their
released behavior, while untouched sessions on the new runtime still resolve the unified defaults
(`streaming_parallelism = default` to `bounded(64)`, table/source `default` to `bounded(4)`). New
clusters only expose the unified parameters.

In other words, an upgraded user may still observe the same effective parallelism for existing jobs
or migrated session defaults, but they can no longer keep adjusting that behavior through the old
`*_strategy` parameters. Any further changes must use the unified `streaming_parallelism*`
parameters.

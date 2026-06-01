# RFC: Bootstrap Session Defaults from `risingwave.toml`

## Summary

This RFC proposes a new `[session_init]` section in `risingwave.toml` to initialize selected
persisted session parameters in Meta store.

The new config section is intended for **cluster bootstrap only**:

- On a new cluster, values from `[session_init]` are written into the persisted
  `session_parameter` table.
- On an existing cluster, persisted values in Meta store take precedence.
- After a parameter is persisted, later edits to `[session_init]` do not change the effective
  value for that cluster.

The first supported parameters are:

- `streaming_parallelism`
- `streaming_parallelism_for_backfill`
- `streaming_parallelism_for_table`
- `streaming_parallelism_for_source`
- `streaming_parallelism_for_sink`
- `streaming_parallelism_for_index`
- `streaming_parallelism_for_materialized_view`

## Motivation

Today, cluster-level default session parameters are stored in Meta store and can be changed with
`ALTER SYSTEM SET`. They are not configurable from `risingwave.toml`.

This has two drawbacks:

1. Operators cannot seed cluster-specific default session parameters as part of deployment.
2. For parameters such as `streaming_parallelism` and `streaming_parallelism_for_<type>`, users
   who want a non-default initial cluster behavior must issue SQL after the cluster is started.

At the same time, directly making `risingwave.toml` a continuous source of truth for session
parameters would conflict with the existing persisted `ALTER SYSTEM` model. We need a mechanism
that supports deployment-time initialization without introducing ambiguity about who owns the
effective value after cluster bootstrap.

## Goals

- Allow operators to configure initial values for selected persisted session parameters in
  `risingwave.toml`.
- Preserve the existing `ALTER SYSTEM SET` and Meta-store persistence model.
- Avoid introducing two continuously active sources of truth for the same session parameter.
- Preserve the existing `streaming_parallelism` / `streaming_parallelism_for_<type>` fallback and
  `default` placeholder semantics.

## Non-goals

- Make `risingwave.toml` a runtime override layer for persisted session parameters.
- Expand the first version to all session parameters.
- Change the resolution rules of `streaming_parallelism` or
  `streaming_parallelism_for_<type>`.
- Replace `ALTER SYSTEM SET` as the main interface for changing persisted session defaults on an
  existing cluster.

## Background

RisingWave currently manages session parameters as follows:

- Each frontend session is initialized from a cluster-wide default `SessionConfig`.
- The cluster-wide default `SessionConfig` is persisted in Meta store in the `session_parameter`
  table.
- `ALTER SYSTEM SET` updates that persisted state.
- On Meta startup, the in-memory session defaults are reconstructed from the persisted
  `session_parameter` table and then flushed back to Meta store.

For `streaming_parallelism` and `streaming_parallelism_for_<type>`, the stored value `default` is
semantically meaningful. It is not just a textual alias for a concrete value such as `bounded(64)`.
The placeholder preserves whether a parameter is still on the default fallback path.

For example:

- `streaming_parallelism = default` resolves to `bounded(64)`.
- `streaming_parallelism_for_table = default` resolves to `bounded(4)` only when
  `streaming_parallelism` is also still `default`.
- If the global value is explicitly set to `bounded(64)`, then
  `streaming_parallelism_for_table = default` should follow the global value instead of resolving to
  `bounded(4)`.

Because of this, the stored value must remain `default` rather than being eagerly materialized into
an effective concrete value.

## Proposal

### New config section

Add a new section to `risingwave.toml`:

```toml
[session_init]
streaming_parallelism = "default"
streaming_parallelism_for_backfill = "default"
streaming_parallelism_for_table = "default"
streaming_parallelism_for_source = "default"
streaming_parallelism_for_sink = "default"
streaming_parallelism_for_index = "default"
streaming_parallelism_for_materialized_view = "default"
```

The value syntax is identical to the SQL/session syntax of the same parameters, including:

- `default`
- `adaptive`
- `0`
- `<n>`
- `bounded(<n>)`
- `ratio(<r>)`

### Semantics

`[session_init]` is a **bootstrap initializer**, not a runtime override mechanism.

Its precedence is:

1. Persisted value in Meta store (`session_parameter`)
2. Explicit value in `[session_init]`
3. Built-in `SessionConfig::default()`

This means:

- On first cluster startup, `[session_init]` can seed values into Meta store.
- On later restarts, existing persisted values are retained.
- Editing `risingwave.toml` after bootstrap does not retroactively update an existing cluster's
  persisted session defaults.

### Why `session_init`

The section name must communicate one-time initialization semantics as clearly as possible.

Alternative names such as `[session]` or `[session_defaults]` imply that the file remains an
authoritative source of effective values during the cluster lifetime, which is not true. The
`[session_init]` name makes it explicit that the section only participates in initialization.

### First supported scope

The first rollout should intentionally be narrow and only cover the streaming parallelism family.

Reasons:

- These parameters are already modeled as persisted session parameters.
- They are especially useful to configure during deployment.
- Their `default` placeholder behavior is subtle and benefits from a clearly documented bootstrap
  path.
- Limiting scope reduces the risk of unintentionally turning every session variable into a
  deployment-time config surface.

## Detailed design

### Config model

Introduce a `SessionInitConfig` under `RwConfig`.

`SessionInitConfig` should not reuse `SessionConfig` directly. Instead, it should use
`Option<...>` fields so the implementation can distinguish:

- parameter omitted in `risingwave.toml`
- parameter explicitly configured to `default`

This distinction is needed to:

- preserve the built-in defaults for omitted fields
- record which fields were explicitly configured
- only emit precedence warnings for parameters that were explicitly set in `[session_init]`

### Meta startup flow

At Meta startup:

1. Parse `[session_init]` into an initial `SessionConfig`.
2. Build a map of explicitly configured session-init entries.
3. Load persisted rows from `session_parameter`.
4. For each persisted parameter:
   - if the parameter was explicitly set in `[session_init]` and differs from the persisted value,
     log a warning
   - apply the persisted value over the initialized `SessionConfig`
5. Flush the final in-memory `SessionConfig` back to Meta store

This preserves the existing persisted session parameter lifecycle while allowing deployment-time
seeding for new clusters and for newly added fields.

### Warning behavior

When `[session_init]` explicitly configures a parameter and Meta store already contains a different
value, Meta should log a warning like:

> session_init value differs from persisted session parameter, using persisted value

The warning is important because otherwise operators may edit `risingwave.toml` on an existing
cluster and incorrectly assume the change has taken effect.

### `default` placeholder handling

For `streaming_parallelism*`, the stored value from `[session_init]` must be exactly the configured
logical value, including `default`.

Examples:

- `streaming_parallelism = "default"` must persist `"default"`, not `bounded(64)`
- `streaming_parallelism_for_table = "default"` must persist `"default"`, not `bounded(4)`

The normal resolution into effective job parallelism still happens later in the existing frontend
resolution path.

## Example scenarios

### Scenario 1: new cluster bootstrap

Config:

```toml
[session_init]
streaming_parallelism = "bounded(8)"
streaming_parallelism_for_table = "default"
```

Behavior:

- Meta store starts empty.
- Meta writes:
  - `streaming_parallelism = "bounded(8)"`
  - `streaming_parallelism_for_table = "default"`
- New sessions inherit these persisted defaults.

### Scenario 2: existing cluster restart after `ALTER SYSTEM`

Persisted state:

- `streaming_parallelism = "ratio(0.5)"`

Config:

```toml
[session_init]
streaming_parallelism = "bounded(8)"
```

Behavior:

- Meta logs a warning that `session_init.streaming_parallelism` is ignored.
- Persisted `ratio(0.5)` remains effective.

### Scenario 3: old cluster upgraded with a new supported field

Persisted state:

- older cluster has no row for `streaming_parallelism_for_materialized_view`

Config:

```toml
[session_init]
streaming_parallelism_for_materialized_view = "bounded(4)"
```

Behavior:

- Because the field is missing from `session_parameter`, Meta uses the value from `[session_init]`
  and persists it.

## Alternatives considered

### Alternative 1: use `[session]` as a persistent override layer

Rejected because it creates two long-lived sources of truth:

- `risingwave.toml`
- Meta-store-backed `ALTER SYSTEM`

This leads to confusing restart semantics and operational ambiguity.

### Alternative 2: let `risingwave.toml` always override Meta store on startup

Rejected because it breaks the current persisted session parameter model. A value changed with
`ALTER SYSTEM SET` would silently revert on the next restart if the file differs.

### Alternative 3: only support SQL, not `risingwave.toml`

This keeps the model simple, but misses an important deployment and bootstrap use case.

### Alternative 4: eagerly materialize `default` into concrete values

Rejected because it breaks the distinction between:

- untouched default path
- explicitly configured concrete global value

That distinction is required for correct `streaming_parallelism_for_table/source = default`
behavior.

## Compatibility and upgrade

This change is backward compatible.

- Existing clusters continue using persisted `session_parameter` values.
- Existing SQL interfaces remain unchanged.
- Existing `streaming_parallelism` resolution behavior remains unchanged.

The only user-visible addition is a new optional `[session_init]` section in `risingwave.toml`.

## Rollout plan

1. Add `SessionInitConfig` and document `[session_init]`.
2. Support only the streaming parallelism family in the first version.
3. Gather operator feedback on whether the naming and bootstrap-only semantics are clear enough.
4. Consider expanding to more persisted session parameters later, case by case.

## Open questions and answers

1. Should Meta expose a dedicated startup log when `[session_init]` is present, explicitly stating
   that it only affects bootstrap? Yes
2. Should we add a SQL-visible marker somewhere to distinguish values that originated from
   bootstrap initialization versus later `ALTER SYSTEM SET`? Not for now
3. If we expand this mechanism to more session parameters later, should we gate additions on
   whether the parameter is operationally meaningful at cluster scope? Can leave it later.

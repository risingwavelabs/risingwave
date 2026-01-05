# RFC: Unsafe Source Properties Update Without Dropping the Pipeline

- **Author**: (to be filled)
- **Status**: Draft
- **Tracking issue**: (to be filled)
- **Created**: 2025-12-17

## Summary

This RFC proposes an *explicitly unsafe* operational workflow (and supporting meta/streaming APIs) to allow operators to update **any** source connector properties **without dropping the pipeline**.

The workflow deliberately makes **no assumptions about upstream compatibility** (e.g., topic continuity, offset monotonicity, connector semantics). As a tradeoff, it may violate exactly-once / consistency guarantees and can cause duplicates, gaps, or reprocessing.

Key deliverables:

- A `risectl`/meta API to update *any* source properties and rewrite the persisted normalized SQL/definition.
- A `risectl`/meta API to **reset split assignment metadata** so the system re-discovers and reassigns splits after the change.
- (Optional) A barrier mutation to **inject a desired split state (offsets)** into the source executor state table, to force-start from specified offsets on next resume/restart.

## Motivation

Today, RisingWave supports online `ALTER ... SET PROPERTIES` for a restricted set of connector fields, validated by connector-specific allowlists. This is correct for safety, but operators sometimes need to apply changes outside the allowlist (e.g., endpoint migration, auth changes, topic/namespace moves, CDC configuration changes) without dropping and recreating the streaming job.

In these cases, the operator may accept that the pipeline becomes *logically discontinuous* (and thus inconsistent) across the change boundary, and only needs a best-effort mechanism to keep the job alive and continue processing under the new configuration.

## Goals

- Support updating **any** source connector properties without dropping the pipeline.
- Explicitly **disallow changing connector type** (the `connector` of the source). The workflow only updates connector *properties* for the existing connector implementation.
- Ensure the updated properties are reflected in:
  - the catalog (`WITH (...)` properties and secret refs),
  - the persisted normalized SQL/definition,
  - the running dataflow (source executors and source enumerator workers).
- Provide an explicit mechanism to “forget” previously discovered/assigned splits so that split discovery and assignment can restart after a major change.
- Optionally allow operators to force a source to restart from explicitly provided offsets/checkpoints.
- Make unsafe behavior explicit, auditable, and gated.

## Non-goals

- Changing the connector type of an existing source (e.g., `kafka` -> `pulsar`, or `kafka` -> `kafka` with a different source implementation). This RFC only covers changing properties for the *current* connector type.
- Guarantee exactly-once semantics across the change boundary.
- Automatically infer safe offsets or ensure upstream continuity.
- Provide a general-purpose “arbitrary state rewriting” framework beyond source split state.
- Support schema evolution or operator graph changes; this RFC only targets connector property changes and source state.

## Background: Current Implementation

### Catalog update and definition rewrite

Meta already supports updating a source’s properties and rewriting the stored SQL definition. It updates the catalog `source.with_properties` and `source.definition`, and for table-associated sources also updates the table definition accordingly. It also updates fragments’ embedded source config.

### Propagation to running executors

Meta can push a `ConnectorPropsChange` barrier mutation. Source executors handle it by updating the reader config and rebuilding the stream reader.

### Source enumerator (split discovery) in meta

Meta’s `SourceManager` runs `ConnectorSourceWorker`s which maintain a `SplitEnumerator` created from connector properties. The worker supports an `UpdateProps` command that refreshes/recreates the enumerator in-place when it receives updated props (triggered by the same barrier command).

### Split assignment persistence

Meta persists split information via the `fragment_splits` table (union of splits for a fragment). This is used in planning/scaling and as part of recovery logic. Resetting split state therefore requires more than clearing in-memory caches.

### Offset persistence in compute

Source executors persist split offsets into an internal state table (`SourceStateTableHandler`), and on (re)initialization they prefer recovering offsets from that state table over any “initial offset” coming from meta assignments.

Therefore:

- Updating props alone does not reset offsets.
- Restarting the cluster alone will still recover offsets from the state table and continue from prior checkpoints (if split IDs match).

## What’s missing today (to make the workflow truly end-to-end)

The high-level operational flow described in this RFC is *directionally compatible* with the current architecture, but two capabilities are missing in the current implementation. Without them, operators can change props, but cannot reliably “reset” the source’s split/offset state after a major upstream change.

### A) Reset split assignment metadata in meta

Current behavior:

- Meta persists split assignments in the catalog (e.g., `fragment_splits`) and also keeps split assignments in in-memory structures for recovery and scaling.
- Clearing only the SourceManager’s in-memory “discovered splits” cache is insufficient, because recovery/scaling can still consult persisted fragment splits.

Required capability:

- Add an explicit admin API / `risectl` command to reset split assignment metadata for the affected source fragments by:
  - deleting the corresponding `fragment_splits` rows, or
  - setting `fragment_splits.splits` to `NULL`.

Expected result:

- After reset, SourceManager can re-discover and reassign splits based on the new connector properties.

### B) (Optional) Inject offsets into source state tables via a barrier mutation

Current behavior:

- Source executors persist offsets/checkpoints in their internal state table (`SourceStateTableHandler`) on checkpoint barriers.
- On (re)initialization, they prefer recovering offsets from this state table over any initial offsets provided by meta split assignment.

Implication:

- Even if meta reassigns splits, the compute side may continue from old offsets (or fail to match if split identities changed).

Required capability (optional, but needed for the “force offsets” step):

- Introduce a new *unsafe* barrier mutation that carries a mapping of `split_id -> split_state` (serialized `SplitImpl` including the desired offset/checkpoint), and implement executor-side logic to upsert those states into the source state table during the barrier.

## Proposed Design

### 1) Operator workflow (high level)

The workflow is intentionally *ops-driven* and uses explicit “unsafe” commands.

1. **Checkpoint + Pause**
   - Issue a checkpoint/flush barrier (optional but recommended).
   - Pause streaming via a Pause barrier mutation, ensuring source executors stop consuming upstream.

2. **Unsafe update of source properties**
   - Update catalog properties and secret refs.
   - Rewrite the stored `definition` (normalized SQL) so the change is visible via `SHOW CREATE ...` and system catalogs.
   - Propagate the new properties to:
     - Source executors (barrier mutation: `ConnectorPropsChange`).
     - Meta source enumerators (source manager worker `UpdateProps`).

3. **Reset split assignment metadata (optional, but recommended for “major” upstream moves)**
   - Clear persisted split metadata for the affected source’s fragments, so split discovery and assignment can restart.
   - This causes SourceManager tick to re-discover splits and emit new `SourceChangeSplit` assignments.

4. **Inject offsets into source state tables (optional)**
   - If the operator needs to force a specific start offset, inject a split->offset mapping to the source executors’ state tables via a barrier mutation.
   - This is explicitly unsafe; it can skip or duplicate data depending on correctness of the provided offsets.

5. **Resume**
   - Resume streaming via a Resume barrier mutation.

**Note**: The above does not require process restart. If operators still prefer a restart as an operational guardrail, it can remain optional, but the workflow should be correct without it.

### 2) New/extended meta APIs

#### 2.1 UnsafeAlterSourceProps

Add a new meta service endpoint (and `risectl` wrapper) that:

- Accepts `source_id` (and optionally database/schema context).
- Accepts property updates (plaintext and secret refs).
- Has an explicit `--unsafe` / `--bypass-allowlist` flag.
- Optionally skips “validate once” (enumerator connectivity check), controlled by a flag.

Behavior:

- Update catalog (`source.with_properties`, `source.secret_ref`) and rewrite `source.definition` SQL.
- Update embedded source config in fragments for dependent jobs, similarly to existing alter-source flow.
- Push a `ConnectorPropsChange` barrier command to propagate to compute nodes.

Gating:

- Only enabled when meta config `enable_unsafe_ops = true`.
- Additionally requires a request flag `unsafe = true` to avoid accidental use.

#### 2.2 ResetSourceSplits (clear split assignment metadata)

Add an admin endpoint to reset the source’s split-related metadata:

- Identify the set of fragments containing the source operator (shared source fragments and dependent non-shared copies).
- Clear persisted split metadata for those fragments.
  - Implementation detail: store `NULL` in `fragment_splits.splits` or delete rows for those fragment IDs.
- Optionally also trigger an immediate `SourceManager` tick (or a dedicated command) to re-discover splits and assign them as soon as possible.

Semantics:

- After reset, SourceManager will treat the source as having no prior split context and will reassign splits based on latest discovery.
- For connectors where split IDs are stable, this may still interact with existing compute-side state unless combined with offset reset/injection.

### 3) Optional: New barrier mutation to inject offsets

Add a new barrier mutation type, tentatively:

- `SourceInjectSplitStateMutation`

Payload:

- `source_id` (or actor IDs directly).
- Mapping from `split_id` to serialized split state (e.g., JSON form of `SplitImpl` including desired offset).

Executor behavior:

- On receiving this mutation, each source executor:
  - Filters entries to splits it is responsible for (based on current split assignment and/or vnode ownership rules).
  - Writes/upserts those split states into the source state table.
  - Commits on the barrier epoch.

Ordering constraints:

- Must run while paused (or in a controlled barrier sequence) to avoid races with normal checkpointing.
- Should be executed after split reassignment if the operator intends to inject offsets for newly discovered splits.

Safety:

- This is a “footgun by design”; it must be guarded behind `enable_unsafe_ops` and explicit `unsafe` flags.
- Audit logs should record the mutation request (with redaction of secrets).

### 4) `risectl` UX

Add commands (names illustrative):

- `risectl streaming pause --reason unsafe-alter-source`
- `risectl streaming resume --reason unsafe-alter-source`
- `risectl connector unsafe-alter-source-props --source-id <id> --set k=v ... [--set-secret k=secret_name] [--skip-validate]`
- `risectl connector reset-source-splits --source-id <id>`
- `risectl connector inject-source-offsets --source-id <id> --file offsets.json` (optional)

The tool should:

- Print the affected source name and dependent jobs.
- Require interactive confirmation unless `--yes` is passed.
- Emit a clear warning about possible duplicates/gaps.

## Correctness and Failure Modes

This design intentionally does not preserve cross-boundary consistency. Operators should expect:

- **Duplicates**: if offsets are set earlier than last committed.
- **Gaps**: if offsets are set later than last committed.
- **Split identity mismatch**: if upstream change causes split IDs to change, prior state won’t apply and the source may start at connector-defined initial offsets.
- **Connector-specific behavior**: some connectors interpret offsets or checkpoints differently; wrong injection can break consumption.

## Observability

- Meta logs and event logs should record:
  - who invoked unsafe operations,
  - which source/job IDs were affected,
  - property keys modified (values redacted for secrets),
  - split reset performed or not,
  - offset injection performed or not (and number of splits affected).
- Metrics:
  - count of unsafe operations invoked,
  - count of injected split states,
  - per-source “last unsafe op timestamp”.

## Testing Plan

Prefer SQLLogicTest + targeted unit tests:

- **Unit tests (meta)**:
  - Resetting split metadata clears `fragment_splits` as expected.
  - Unsafe alter rewrites definition and updates catalog fields.
- **E2E (SQLLogicTest)**:
  - Create a source + MV; pause; unsafe alter props; resume; ensure job remains running.
  - Reset source splits; verify a `SourceChangeSplit` barrier is observed (via logs/metrics hooks if available).
  - (Optional) inject offsets; verify ingestion starts from expected offsets in a deterministic connector test harness.

## Rollout

- Behind a feature flag `enable_unsafe_ops` (default false).
- Documented as “last resort” operational tooling.
- Initially restricted to internal builds or nightly if desired.

## Alternatives Considered

- **Force users to drop/recreate**: safest but operationally expensive and breaks downstream dependencies.
- **Allow all props in existing ALTER SOURCE**: too easy to misuse; safety boundary becomes unclear.
- **Assume upstream continuity and enforce stronger semantics**: contradicts the requirement (“no assumptions about upstream”).

## Open Questions

- The exact persistence and recovery story for per-actor split assignment should be validated for all connector types (including shared vs non-shared sources).
- For offset injection:
  - Do we require injection by actor ID (post-assignment), or by split ID with executor-side filtering?
  - How do we validate that injected split states are structurally valid for the connector?
- Should reset/injection imply a forced checkpoint barrier before resume?

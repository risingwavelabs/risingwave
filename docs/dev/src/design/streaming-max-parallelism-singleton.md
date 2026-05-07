# `streaming_max_parallelism = 1` and singleton semantics

## Goal

Allow `SET streaming_max_parallelism = 1` without making the system treat
`hash-distributed with vnode_count = 1` as a true singleton.

## Background

Today the codebase already distinguishes two concepts:

- `singleton`: encoded by distribution semantics such as
  `Distribution::Single` / `PbFragmentDistributionType::Single`, and by
  `requires_singleton` in the stream fragmenter.
- `hash(1)`: a hash-distributed table or fragment whose vnode count happens to
  be `1`.

However, some abstractions still blur the boundary. In particular, the
stream-graph scheduler models `AnySingleton` as `AnyVnodeCount(1)`, which makes
the distinction harder to reason about when `streaming_max_parallelism = 1` is
allowed.

## Correctness requirements

After this change:

1. `streaming_max_parallelism = 1` must be accepted by session config.
2. A fragment with default parallelism/vnode count `1` but without singleton
   requirements must still be scheduled as hash-distributed.
3. A fragment with explicit singleton requirements must still be scheduled as
   singleton.
4. Persisted metadata must continue to distinguish singleton from hash(1) by
   `distribution_type`, even though both can carry `maybe_vnode_count = 1`.
5. Existing special cases that require true singleton semantics
   (`Simple` edges, `Now`, `Values`, non-parallel CDC backfill, etc.) must keep
   using singleton semantics instead of relying on vnode count.

## Implementation plan

1. Make scheduler requirements model singleton explicitly instead of aliasing it
   to `AnyVnodeCount(1)`.
2. Keep `AnyVnodeCount(1)` valid and mergeable with singleton when the fragment
   is required to be singleton, because singleton fragments also have vnode
   count `1`.
3. Allow `streaming_max_parallelism = 1`.
4. Add focused tests that exercise:
   - scheduler output for default hash(1) vs singleton;
   - vnode-count compatibility for singleton vs non-singleton protobuf objects;
   - session config validation for value `1`.
5. Keep runtime semantics unchanged for true singleton operators and tables.

## Expected non-goals

- This change does not redefine singleton as “any fragment with vnode count 1”.
- This change does not try to remove all historical uses of vnode count `1` in
  comments or compatibility code, except where they are misleading for the new
  behavior.

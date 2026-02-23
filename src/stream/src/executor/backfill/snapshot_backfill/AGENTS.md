# AGENTS.md - Snapshot Backfill

## 1. Scope

Policies for the `src/stream/src/executor/backfill/snapshot_backfill` directory, containing epoch-based snapshot backfill for historical data synchronization.

## 2. Purpose

This directory implements snapshot backfill using epoch-based consistency:
- **SnapshotBackfillExecutor**: Main executor for epoch-based snapshot backfill
- **UpstreamTableExecutor**: Consumes upstream table changes during backfill
- **Per-vnode streaming**: Parallel snapshot reads by virtual node
- **State tracking**: Per-vnode backfill progress persistence

Snapshot backfill provides consistent historical data loading while processing real-time upstream changes.

## 3. Structure

```
src/stream/src/executor/backfill/snapshot_backfill/
├── mod.rs                    # Module exports
├── executor.rs               # SnapshotBackfillExecutor implementation
├── state.rs                  # Per-vnode backfill state tracking
├── vnode_stream.rs           # Per-vnode streaming for snapshot reads
├── utils.rs                  # Snapshot backfill utilities
└── consume_upstream/         # Upstream table consumption
    ├── mod.rs                # Module exports
    ├── executor.rs           # UpstreamTableExecutor
    ├── stream.rs             # Upstream streaming logic
    └── upstream_table_trait.rs  # Trait for upstream table access
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `executor.rs` | `SnapshotBackfillExecutor` main implementation |
| `state.rs` | `BackfillState` per-vnode progress tracking |
| `vnode_stream.rs` | `VnodeStream` for parallel snapshot iteration |
| `consume_upstream/executor.rs` | `UpstreamTableExecutor` for real-time changes |
| `utils.rs` | Shared backfill utilities |

## 5. Edit Rules (Must)

- Use `VnodeStream` for parallel snapshot reading by vnode
- Track per-vnode progress in `BackfillState`
- Handle upstream changes during snapshot reading
- Use `StateTable` for progress persistence
- Support barrier-driven checkpoint/restore
- Handle vnode bitmap changes for scaling
- Call `mark_chunk()` for upstream deduplication
- Run `cargo test -p risingwave_stream snapshot_backfill` after changes

## 6. Forbidden Changes (Must Not)

- Remove per-vnode parallelism for snapshot reads
- Skip state persistence on checkpoint barriers
- Change state table schema without migration
- Use blocking operations in snapshot iteration
- Bypass upstream change consumption
- Remove epoch-based consistency checks

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Snapshot backfill tests | `cargo test -p risingwave_stream snapshot_backfill` |
| Backfill tests | `cargo test -p risingwave_stream backfill` |
| Upstream tests | `cargo test -p risingwave_stream upstream` |

## 8. Dependencies & Contracts

- **Parent module**: `executor/backfill/` provides utilities
- **State tables**: Per-vnode progress persistence
- **Upstream table**: Real-time change consumption
- **Vnode mapping**: Consistent hashing for parallel reads
- **Barrier protocol**: Checkpoint/restore on barriers

## 9. Overrides

None. Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/executor/backfill/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New snapshot backfill strategy added
- Per-vnode streaming mechanism changed
- Upstream consumption logic modified
- State tracking schema updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/executor/backfill/AGENTS.md

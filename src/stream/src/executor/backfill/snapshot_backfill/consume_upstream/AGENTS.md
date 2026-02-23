# AGENTS.md - Snapshot Backfill Upstream Consumption

## 1. Scope

Policies for `src/stream/src/executor/backfill/snapshot_backfill/consume_upstream` - the upstream table consumption component of snapshot backfill, handling real-time changes during historical data loading.

## 2. Purpose

This directory implements the `UpstreamTableExecutor` for consuming upstream table changes during snapshot backfill:

- **Upstream consumption**: Reads real-time changes from upstream tables while snapshot backfill is in progress
- **Change buffering**: Buffers upstream changes to apply after snapshot completion
- **Epoch alignment**: Aligns upstream changes with snapshot backfill epochs
- **Deduplication**: Prevents duplicate processing of data during snapshot-to-streaming transition
- **Barrier handling**: Coordinates with snapshot backfill barrier protocol

The upstream consumption mechanism ensures consistency when transitioning from snapshot loading to real-time streaming.

## 3. Structure

```
src/stream/src/executor/backfill/snapshot_backfill/consume_upstream/
├── mod.rs                    # Module exports: UpstreamTableExecutor
├── executor.rs               # UpstreamTableExecutor implementation
├── stream.rs                 # Upstream streaming logic and buffering
└── upstream_table_trait.rs   # Trait for upstream table access abstraction
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module exports, re-exports `UpstreamTableExecutor` |
| `executor.rs` | `UpstreamTableExecutor` main implementation with barrier handling |
| `stream.rs` | `UpstreamStream` for buffering and delivering upstream changes |
| `upstream_table_trait.rs` | `UpstreamTable` trait for abstracting upstream table access |

## 5. Edit Rules (Must)

- Implement `Execute` trait for `UpstreamTableExecutor`
- Handle all `BarrierKind` variants correctly (Initial, Checkpoint, Barrier, SyncCheckpoint)
- Buffer upstream changes during snapshot reading phase
- Apply buffered changes after snapshot completion
- Use `mark_chunk()` for upstream data deduplication
- Coordinate with `SnapshotBackfillExecutor` for phase transitions
- Implement proper error handling with retry for transient failures
- Use `StateTable` for any upstream consumption state persistence
- Support vnode-based parallelism for scalable upstream consumption
- Run `cargo test -p risingwave_stream snapshot_backfill` after modifications

## 6. Forbidden Changes (Must Not)

- Skip upstream change buffering during snapshot phase
- Remove deduplication logic for upstream data
- Break barrier protocol contract with parent executor
- Use blocking operations in upstream stream iteration
- Skip state persistence on checkpoint barriers
- Remove support for vnode-based parallelism
- Bypass upstream table trait abstraction
- Change upstream change ordering semantics

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Snapshot backfill tests | `cargo test -p risingwave_stream snapshot_backfill` |
| Upstream consumption tests | `cargo test -p risingwave_stream consume_upstream` |
| Upstream table tests | `cargo test -p risingwave_stream upstream_table` |
| Backfill integration | `cargo test -p risingwave_stream --test integration_tests` |

## 8. Dependencies & Contracts

- **Parent executor**: `SnapshotBackfillExecutor` coordinates phase transitions
- **Upstream table**: `UpstreamTable` trait for accessing upstream data
- **Barrier protocol**: Must align with snapshot backfill checkpoint semantics
- **State table**: For persisting consumption progress
- **Deduplication**: `mark_chunk()` integration with parent backfill
- **Stream types**: `BoxedMessageStream` for upstream change delivery
- **Actor context**: `ActorContextRef` for metrics and configuration

## 9. Overrides

None. Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/executor/backfill/snapshot_backfill/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- Upstream consumption logic changes
- New upstream table types supported
- Barrier protocol modifications
- Snapshot backfill phase transitions updated
- Deduplication mechanism changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/executor/backfill/snapshot_backfill/AGENTS.md

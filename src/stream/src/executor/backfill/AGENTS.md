# AGENTS.md - RisingWave Streaming Backfill Executors

## 1. Scope

Policies for the `src/stream/src/executor/backfill` directory, containing backfill executor implementations for historical data processing in the RisingWave stream engine.

## 2. Purpose

This directory implements backfill operators that synchronize historical snapshot data with real-time streaming data:

- **ArrangementBackfill**: Scalable backfill using replicated state tables for MV-on-MV creation
- **NoShuffleBackfill**: Single-node backfill for co-located upstream tables
- **CdcBackfill**: Change Data Capture backfill for external databases (MySQL, Postgres, etc.)
- **SnapshotBackfill**: Epoch-based snapshot backfill with upstream consumption

All backfill executors follow the barrier-driven checkpoint protocol and use state tables for progress persistence.

## 3. Structure

```
src/stream/src/executor/backfill/
├── mod.rs                      # Module exports: arrangement, no_shuffle, cdc, snapshot
├── arrangement_backfill.rs     # Scalable backfill with ReplicatedStateTable
├── no_shuffle_backfill.rs      # Co-located backfill with BatchTable
├── utils.rs                    # Shared utilities: chunk mapping, state management, builders
├── cdc/                        # CDC backfill for external databases
│   ├── mod.rs                  # Exports CdcBackfillExecutor, ParallelizedCdcBackfillExecutor
│   ├── cdc_backfill.rs         # Single-threaded CDC backfill implementation
│   ├── cdc_backill_v2.rs       # Parallelized CDC backfill (v2)
│   ├── state.rs                # CDC backfill state management (v1)
│   ├── state_v2.rs             # CDC backfill state management (v2)
│   └── upstream_table/         # External table reading
│       ├── mod.rs              # Exports ExternalStorageTable
│       ├── external.rs         # External storage table interface
│       └── snapshot.rs         # Snapshot reading from upstream tables
└── snapshot_backfill/          # Epoch-based snapshot backfill
    ├── mod.rs                  # Exports SnapshotBackfillExecutor, UpstreamTableExecutor
    ├── executor.rs             # Main snapshot backfill executor implementation
    ├── state.rs                # Backfill progress state per vnode
    ├── vnode_stream.rs         # Per-vnode streaming for snapshot reads
    ├── utils.rs                # Snapshot backfill utilities
    └── consume_upstream/       # Upstream table consumption
        ├── mod.rs              # Exports UpstreamTableExecutor
        ├── executor.rs         # Upstream table consumption executor
        ├── stream.rs           # Upstream streaming logic
        └── upstream_table_trait.rs  # Trait for upstream table access
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module exports and public API surface |
| `arrangement_backfill.rs` | `ArrangementBackfillExecutor` - scalable MV-on-MV backfill |
| `no_shuffle_backfill.rs` | `BackfillExecutor` - single-node backfill for local tables |
| `utils.rs` | Shared utilities: `BackfillState`, chunk mapping, position tracking |
| `cdc/cdc_backfill.rs` | `CdcBackfillExecutor` - CDC from external databases |
| `cdc/cdc_backill_v2.rs` | `ParallelizedCdcBackfillExecutor` - parallel CDC backfill |
| `cdc/state.rs` | `CdcBackfillState` - state persistence for CDC backfill |
| `cdc/upstream_table/external.rs` | `ExternalStorageTable` - interface to external tables |
| `snapshot_backfill/executor.rs` | `SnapshotBackfillExecutor` - epoch-based snapshot backfill |
| `snapshot_backfill/state.rs` | `BackfillState` - per-vnode progress tracking |

## 5. Edit Rules (Must)

- Run `cargo test -p risingwave_stream` after modifying any backfill executor
- Use `StateTable` for all backfill progress persistence
- Handle `CdcOffset` correctly in CDC backfill - must parse and serialize consistently
- Implement `Execute` trait: `fn execute(self: Box<Self>) -> BoxedMessageStream`
- Update `from_proto/` converters when adding new backfill parameters
- Use `RateLimiter` for all snapshot read operations
- Propagate `CreateMviewProgressReporter` updates during backfill progress
- Handle all `BarrierKind` variants: `Initial`, `Checkpoint`, `Barrier`
- Use `vnode`-based parallelism for scalable backfill implementations
- Call `mark_chunk()` or `mark_cdc_chunk()` for upstream data deduplication
- Use `mapping_chunk()` and `mapping_message()` from `utils.rs` for output transformation

## 6. Forbidden Changes (Must Not)

- Modify `CdcOffset` serialization format without migration path
- Remove `backfill_finished` state checks - required for recovery correctness
- Change `METADATA_STATE_LEN` constants without updating state table schemas
- Bypass rate limiting in snapshot read operations
- Use blocking IO in CDC external table readers
- Skip progress reporting to meta node during backfill
- Modify state table primary key structure without migration
- Use `Expression::eval` directly (use `NonStrictExpression::eval_infallible`)
- Break barrier protocol: must persist state on checkpoint barriers

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream backfill` |
| CDC backfill tests | `cargo test -p risingwave_stream cdc_backfill` |
| Snapshot backfill tests | `cargo test -p risingwave_stream snapshot_backfill` |
| Arrangement backfill tests | `cargo test -p risingwave_stream arrangement_backfill` |
| NoShuffle backfill tests | `cargo test -p risingwave_stream no_shuffle_backfill` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |

## 8. Dependencies & Contracts

- **State storage**: `StateTable<S>` from `common/table/` for progress persistence
- **CDC connector**: `risingwave_connector::source::cdc` for external database access
- **Rate limiting**: `risingwave_common_rate_limit::RateLimiter` for backpressure
- **State store**: `risingwave_storage::StateStore` for table operations
- **Metrics**: `CdcBackfillMetrics` and `StreamingMetrics` for observability
- **Task context**: `ActorContextRef` and `CreateMviewProgressReporter` for progress reporting

## 9. Overrides

Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New backfill executor type added (e.g., new CDC variant)
- New subdirectory created for backfill submodules
- Backfill state schema changes
- CDC offset format changes
- Rate limiting or backpressure mechanisms modified
- State table interface changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/executor/AGENTS.md

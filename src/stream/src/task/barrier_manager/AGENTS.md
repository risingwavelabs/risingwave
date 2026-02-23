# AGENTS.md - Barrier Manager

## 1. Scope

Policies for the `src/stream/src/task/barrier_manager` directory, containing the per-actor barrier event bridge and progress tracking for the stream engine.

## 2. Purpose

This directory implements the actor-to-coordinator communication:
- **LocalBarrierManager**: Per-actor barrier sender registration and event dispatch
- **CreateMviewProgressReporter**: Materialized view creation progress tracking
- **CdcTableBackfillProgress**: CDC table backfill progress reporting
- **Actor failure notification**: Error reporting to barrier worker

The barrier manager acts as the bridge between individual actors and the central barrier worker, handling barrier collection and progress reporting.

## 3. Structure

```
src/stream/src/task/barrier_manager/
├── mod.rs                    # LocalBarrierManager and LocalBarrierEvent
├── progress.rs               # CreateMviewProgressReporter and BackfillState
└── cdc_progress.rs           # CDC table backfill progress tracking
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `LocalBarrierManager` for barrier sender registration, failure notification |
| `progress.rs` | MV creation progress with `BackfillState` enum |
| `cdc_progress.rs` | `CdcTableBackfillState` for CDC backfill progress |

## 5. Edit Rules (Must)

- Use `LocalBarrierManager::collect()` for barrier acknowledgment
- Call `notify_failure()` on unexpected actor exit
- Report backfill progress via `CreateMviewProgressReporter`
- Use `subscribe_barrier()` to receive barriers for new actors
- Handle `LocalBarrierEvent` variants in barrier worker
- Register local upstream outputs for local exchange
- Report CDC progress via `report_cdc_table_backfill_progress()`
- Run `cargo test -p risingwave_stream barrier_manager` after changes

## 6. Forbidden Changes (Must Not)

- Bypass `LocalBarrierManager` for actor-to-coordinator communication
- Remove progress reporting for materialized view creation
- Skip failure notification on actor errors
- Use direct channel communication instead of event dispatch
- Modify `LocalBarrierEvent` enum without updating handlers
- Remove CDC progress tracking without migration

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Barrier manager tests | `cargo test -p risingwave_stream barrier_manager` |
| Progress tests | `cargo test -p risingwave_stream progress` |
| CDC progress tests | `cargo test -p risingwave_stream cdc_progress` |

## 8. Dependencies & Contracts

- **Parent module**: `task/` provides actor context
- **Barrier worker**: Events handled by `barrier_worker/managed_state.rs`
- **Actor context**: Each actor holds a `LocalBarrierManager` reference
- **Progress types**: `PbCreateMviewProgress`, `PbCdcTableBackfillProgress`
- **Event channels**: Unbounded mpsc for barrier events

## 9. Overrides

None. Follows parent AGENTS.md at `./src/stream/src/task/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New progress tracking type added
- LocalBarrierEvent variants changed
- Actor-to-coordinator communication modified
- Progress reporting protocol updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/task/AGENTS.md

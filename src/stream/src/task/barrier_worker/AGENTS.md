# AGENTS.md - Barrier Worker

## 1. Scope

Policies for the `src/stream/src/task/barrier_worker` directory, containing the central barrier coordination worker for distributed consistency in the stream engine.

## 2. Purpose

This directory implements the core barrier coordination:
- **LocalBarrierWorker**: Central event loop for barrier injection and collection
- **ManagedBarrierState**: Multi-epoch barrier state machine per partial graph
- **Control stream handling**: gRPC communication with meta service
- **Epoch completion**: State store sync and progress reporting

The barrier worker manages the distributed snapshot isolation protocol, ensuring all actors commit state consistently at checkpoint barriers.

## 3. Structure

```
src/stream/src/task/barrier_worker/
├── mod.rs                    # LocalBarrierWorker and ControlStreamHandle
├── managed_state.rs          # ManagedBarrierState and PartialGraphState
└── tests.rs                  # Barrier protocol unit tests
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `LocalBarrierWorker::run()` event loop, barrier injection/completion |
| `managed_state.rs` | `ManagedBarrierState` for multi-partial-graph coordination |
| `tests.rs` | Unit tests for barrier state transitions |

## 5. Edit Rules (Must)

- Use `ManagedBarrierState` for tracking barrier collection across actors
- Handle all `BarrierKind` variants: `Initial`, `Barrier`, `Checkpoint`
- Call `sync_epoch()` for `Checkpoint` barriers to persist state
- Use `ControlStreamHandle` for gRPC communication with meta
- Report progress via `BarrierCompleteResult` to meta service
- Handle partial graph failures via `on_partial_graph_failure()`
- Use `await_tree` instrumentation for async operation tracing
- Run `cargo test -p risingwave_stream barrier_worker` after changes

## 6. Forbidden Changes (Must Not)

- Modify barrier epoch semantics without meta service coordination
- Skip state store sync on checkpoint barriers
- Remove partial graph failure handling
- Use blocking operations in `run()` event loop
- Bypass `ManagedBarrierState` for barrier tracking
- Change control stream protocol without versioning

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Barrier worker tests | `cargo test -p risingwave_stream barrier_worker` |
| Managed state tests | `cargo test -p risingwave_stream managed_state` |
| Barrier tests | `cargo test -p risingwave_stream barrier` |

## 8. Dependencies & Contracts

- **Parent module**: `task/` provides actor management integration
- **Meta service**: gRPC control stream for barrier commands
- **State store**: `risingwave_storage` for epoch sync
- **Actor events**: `LocalBarrierEvent` from `barrier_manager/`
- **Metrics**: `StreamingMetrics` for barrier latency tracking

## 9. Overrides

None. Follows parent AGENTS.md at `./src/stream/src/task/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- Barrier protocol changes (new barrier kinds)
- Partial graph state machine modifications
- Control stream handling updates
- Epoch completion logic changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/task/AGENTS.md

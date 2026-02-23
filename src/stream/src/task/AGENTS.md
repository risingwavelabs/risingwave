# AGENTS.md - Stream Task Management

## 1. Scope

Policies for the `src/stream/src/task` directory, containing actor lifecycle management and barrier coordination for the stream compute engine.

## 2. Purpose

This directory implements the core task management system for streaming computation:
- Actor spawning, supervision, and recovery
- Distributed barrier protocol for checkpoint consistency
- Meta service coordination via control streams
- Materialized view creation progress tracking
- Actor-to-system event bridging

## 3. Structure

```
src/stream/src/task/
├── mod.rs                      # Module root with detailed architecture docs
├── env.rs                      # StreamEnvironment: global execution context
├── stream_manager.rs           # LocalStreamManager: public API handler
├── actor_manager.rs            # StreamActorManager: actor factory/spawner
├── barrier_manager/            # Actor-to-system event bridge
│   ├── mod.rs                  # LocalBarrierManager: per-actor barrier/events
│   ├── progress.rs             # MV creation progress tracking
│   └── cdc_progress.rs         # CDC table backfill progress
└── barrier_worker/             # Central barrier coordination
    ├── mod.rs                  # LocalBarrierWorker: event loop & coordination
    ├── managed_state.rs        # ManagedBarrierState: multi-graph barrier state
    └── tests.rs                # Barrier protocol tests
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | Architecture overview with three-layer design documentation |
| `stream_manager.rs` | `LocalStreamManager`: handles StreamService/StreamExchangeService APIs |
| `actor_manager.rs` | `StreamActorManager`: actor creation, executor graph building |
| `barrier_manager/mod.rs` | `LocalBarrierManager`: per-actor barrier sender registration, failure notification |
| `barrier_worker/mod.rs` | `LocalBarrierWorker`: central event loop, control stream handling |
| `barrier_worker/managed_state.rs` | `ManagedBarrierState`: multi-partial-graph barrier coordination |
| `env.rs` | `StreamEnvironment`: state store, config, worker ID, client pool |

## 5. Edit Rules (Must)

- Follow the three-layer architecture documented in `mod.rs` for all changes
- Use `LocalBarrierManager::notify_failure()` for actor error reporting
- Register new actors through `StreamActorManager::spawn_actor()`
- Send barriers via `LocalBarrierWorker::send_barrier()` only
- Use `ActorEvalErrorReport` for expression evaluation error reporting
- Handle all `BarrierKind` variants (`Initial`, `Barrier`, `Checkpoint`) in state changes
- Access actor config via `actor_context.config`, not global config directly
- Run `cargo test -p risingwave_stream` after barrier protocol modifications
- Update `await_tree` instrumentation when adding async operations
- Use `dispatch_state_store!` macro for state store type dispatch

## 6. Forbidden Changes (Must Not)

- Bypass `LocalBarrierManager` for actor-to-coordinator communication
- Modify barrier epoch semantics without coordinating with meta node
- Use `StreamingConfig` directly in executors; always use per-actor config
- Remove or alter `consistency_error!`/`consistency_panic!` macro usage
- Change partial graph state machine without updating `ManagedBarrierState`
- Use blocking operations in `LocalBarrierWorker::run()` event loop
- Modify `ControlStreamHandle` gRPC flow without understanding tonic lifecycle
- Skip actor failure notification on unexpected actor exit
- Use `unsafe` without explicit safety justification in comments

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream task::` |
| Barrier tests | `cargo test -p risingwave_stream barrier_worker::tests` |
| Stream manager tests | `cargo test -p risingwave_stream stream_manager` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |

## 8. Dependencies & Contracts

- **Parent**: `executor/` (Actor, Barrier types), `from_proto/` (executor building)
- **State store**: `risingwave_storage` via `StreamEnvironment::state_store()`
- **Meta client**: `risingwave_rpc_client::MetaClient` for control stream
- **Config**: Per-actor `StreamingConfig` override support via config cache
- **Barrier protocol**: Epoch-based with `Checkpoint`/`Barrier`/`Initial` kinds
- **Partial graphs**: Independent failure domains with separate barrier state
- **Metrics**: `StreamingMetrics` for barrier latency and actor monitoring

## 9. Overrides

None. Follows parent AGENTS.md at `./src/stream/src/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- Barrier protocol or state machine changes
- Actor lifecycle management modifications
- Control stream handling changes
- New progress tracking types added
- Partial graph failure/recovery logic updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/AGENTS.md

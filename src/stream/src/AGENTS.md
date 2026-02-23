# AGENTS.md - RisingWave Stream Engine Source

## 1. Scope
Policies for the `src/stream/src` directory, containing the core source implementation of the `risingwave_stream` crate.

## 2. Purpose
This directory implements the stream compute engine's core runtime:
- Actor-based execution with message-driven concurrency
- Stateful streaming operators with checkpoint/restore
- Barrier-based distributed consistency protocol
- Protobuf plan deserialization and executor instantiation

## 3. Structure

```
src/stream/src/
├── lib.rs                    # Crate root, feature flags, global CONFIG access
├── error.rs                  # StreamError types with root-cause scoring
├── telemetry.rs              # Telemetry and observability hooks
├── cache/                    # Managed LRU cache for operator state
│   └── managed_lru.rs        # Spill-aware LRU cache implementation
├── common/                   # Shared utilities across executors
│   ├── table/                # State table abstractions
│   ├── state_cache/          # State caching utilities
│   ├── metrics.rs            # Prometheus metrics definitions
│   └── rate_limit.rs         # Backpressure and rate limiting
├── executor/                 # Streaming operators (60+ files)
│   ├── mod.rs                # Core types: Message, Barrier, Watermark, StreamChunk
│   ├── actor.rs              # Actor execution loop
│   ├── prelude.rs            # Common imports for executor authors
│   ├── aggregate/            # HashAgg, SimpleAgg implementations
│   ├── backfill/             # Historical data backfill operators
│   ├── exchange/             # Data shuffle between compute nodes
│   ├── join/                 # Join state management
│   ├── mview/                # Materialized view sink
│   ├── over_window/          # Window function operators
│   ├── source/               # Stream source ingestion
│   ├── top_n/                # Streaming TopN variants
│   ├── watermark/            # Watermark generation/propagation
│   └── ...                   # 30+ individual executor implementations
├── from_proto/               # Protobuf Node -> Executor conversion (50 files)
│   ├── mod.rs                # Dispatcher and registry
│   ├── hash_join.rs          # HashJoin executor builder
│   ├── hash_agg.rs           # HashAgg executor builder
│   └── ...                   # One file per executor type
└── task/                     # Actor lifecycle and cluster coordination
    ├── stream_manager.rs     # Actor spawning and management
    ├── actor_manager.rs      # Actor supervision and recovery
    ├── barrier_manager/      # Distributed barrier protocol
    └── barrier_worker/       # Per-actor barrier handling
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `lib.rs` | Crate entry point, feature flags, CONFIG task-local, consistency macros |
| `error.rs` | Error scoring for root-cause analysis of actor failures |
| `executor/mod.rs` | Core streaming types: `Message`, `Barrier`, `Watermark`, `StreamChunk` |
| `executor/actor.rs` | Actor execution loop and message routing |
| `executor/prelude.rs` | Standard imports for new executor implementations |
| `task/stream_manager.rs` | Actor lifecycle management |
| `from_proto/mod.rs` | Executor factory dispatch from protobuf plan nodes |
| `common/table/` | State table interface for checkpoint/restore |

## 5. Edit Rules (Must)

- Run `cargo test -p risingwave_stream` after any executor modification
- Use `executor/prelude.rs` imports when adding new executors
- Add `from_proto/` converter when adding new executor types
- Use `crate::consistency_error!` or `crate::consistency_panic!` for consistency violations
- Access config via `crate::CONFIG` task-local, not global statics
- Register new executors in `from_proto/mod.rs` dispatcher
- Update `expect_test` snapshots when changing executor output format
- Ensure new executors handle all `BarrierKind` variants correctly
- Add `#[derive(Clone)]` to executor configs only if deep-clone safe

## 6. Forbidden Changes (Must Not)

- Add new feature flags without updating `lib.rs` header comments
- Modify `StreamError` scoring logic without understanding error chains
- Remove or bypass `consistency_error!`/`consistency_panic!` macros
- Access `StreamingConfig` directly; always use `crate::CONFIG` task-local
- Use `unsafe` blocks without explicit justification in comments
- Break the barrier protocol contract in stateful operators
- Change `Message` enum variants without updating all pattern matches
- Use blocking operations in async executor contexts

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |
| Executor tests | `cargo test -p risingwave_stream <executor_name>` |
| Spill tests | `cargo test -p spill_test` |
| Benchmarks | `cargo bench -p risingwave_stream` |

## 8. Dependencies & Contracts

- **Within crate**: executor/ <- common/, task/; from_proto/ -> executor/
- **Task-local**: `crate::CONFIG` must be set before spawning actors
- **Consistency**: `consistency::insane()` and `consistency::enable_strict_consistency()` gates
- **Error handling**: `ScoredError` ranking for root-cause analysis
- **State management**: All state changes must be wrapped in state tables

## 9. Overrides

None. Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New top-level module added to `src/`
- Executor architecture changes (actor loop, barrier handling)
- Consistency checking mechanisms modified
- Global config access patterns change
- New feature flags added to crate root

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/AGENTS.md

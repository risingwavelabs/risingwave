# AGENTS.md - Compute Node Crate

## 1. Scope

This directory contains the compute node implementation for RisingWave - the worker node that executes query plans and handles data ingestion and output.

**Policy Inheritance:**
This file inherits rules from `/home/k11/risingwave/AGENTS.md` (root).
When working in this directory:
1. Read THIS file first for directory-specific rules
2. Then read PARENT files up to root for inherited rules
3. Child rules override parent rules (nearest-wins)
4. If code changes conflict with these docs, update AGENTS.md to match reality

## 2. Purpose

The compute crate implements the compute node binary that:
- Executes batch (OLAP) queries through the batch execution engine
- Runs streaming (continuous) queries through the stream execution engine
- Manages memory allocation between compute and storage components
- Handles data exchange between compute nodes
- Registers with and receives commands from the meta service
- Provides monitoring and configuration gRPC endpoints
- Optionally runs an embedded compactor for Hummock storage

## 3. Structure

```
src/compute/
├── src/
│   ├── lib.rs              # Main entry, ComputeNodeOpts CLI args
│   ├── server.rs           # Bootstrap logic, gRPC server setup
│   ├── telemetry.rs        # Compute node telemetry reporting
│   ├── memory/
│   │   ├── mod.rs          # Memory management exports
│   │   ├── manager.rs      # MemoryManager for LRU watermark control
│   │   ├── controller.rs   # LruWatermarkController logic
│   │   └── config.rs       # Memory allocation calculations
│   ├── rpc/
│   │   ├── mod.rs          # RPC module
│   │   └── service/        # gRPC service implementations
│   │       ├── batch_exchange_service.rs   # Batch data exchange
│   │       ├── stream_exchange_service.rs  # Stream data exchange
│   │       ├── stream_service.rs           # Stream commands from meta
│   │       ├── config_service.rs           # Config management
│   │       ├── monitor_service.rs          # Diagnostics and profiling
│   │       └── health_service.rs           # Health checks
│   └── observer/
│       ├── mod.rs          # Observer module
│       └── observer_manager.rs  # Meta state synchronization
└── tests/
    ├── integration_tests.rs  # Batch/stream integration tests
    └── cdc_tests.rs          # CDC-specific tests
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `lib.rs` | CLI options (ComputeNodeOpts), entry point |
| `server.rs` | Node bootstrap, service initialization, shutdown |
| `memory/manager.rs` | MemoryManager for controlling jemalloc memory usage |
| `memory/config.rs` | Memory allocation formulas and validation |
| `rpc/service/stream_service.rs` | Handle meta streaming commands |
| `rpc/service/monitor_service.rs` | Profiling, heap dumps, await-tree diagnostics |
| `observer/observer_manager.rs` | Sync with meta service state changes |

## 5. Edit Rules (Must)

- Run `cargo fmt` before committing changes
- Add unit tests for new memory calculation functions in `memory/config.rs`
- Update integration tests when changing batch/stream interaction logic
- Pass `./risedev c` (clippy) before submitting
- Memory configuration changes require testing with both streaming and serving roles

## 6. Forbidden Changes (Must Not)

- Change `ComputeNodeOpts` field names without updating documentation
- Modify memory reservation constants without understanding the gradient calculation
- Remove or bypass the memory manager's watermark sequence mechanism
- Disable the observer manager's system parameter sync
- Skip the meta service activation step during bootstrap

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_compute` |
| Integration tests | `cargo test -p risingwave_compute --test integration_tests` |
| Memory config tests | `cargo test -p risingwave_compute memory::config::tests` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- Depends on: `risingwave_batch`, `risingwave_stream`, `risingwave_storage`, `risingwave_pb`, `risingwave_rpc_client`
- gRPC services: TaskService, BatchExchangeService, StreamExchangeService, StreamService, MonitorService, ConfigService, Health
- Memory management uses jemalloc statistics via `tikv-jemalloc-ctl`
- Registers as `WorkerType::ComputeNode` with meta service
- State store URL stored in static `STATE_STORE_URL` for JNI crate access

## 9. Overrides

None. Follows parent AGENTS.md policies.

## 10. Update Triggers

Regenerate this file when:
- New gRPC services are added to the compute node
- Memory management architecture changes
- CLI options in ComputeNodeOpts are modified
- New compute node roles beyond Streaming/Serving/Both are added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

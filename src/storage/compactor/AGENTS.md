# AGENTS.md - RisingWave Compactor Node

## 1. Scope

Policies for `./src/storage/compactor` and all subdirectories. This crate provides the compactor binary (risingwave_compactor), a stateless worker node that performs background compaction for the Hummock LSM-tree storage engine.

## 2. Purpose

The compactor node is responsible for:
- Executing compaction tasks from the meta service to merge and optimize SSTables
- Running in multiple modes: Dedicated (direct meta connection), Shared (via proxy), and Iceberg variants
- Managing memory limits for compaction operations with configurable memory limits and meta cache
- Providing gRPC services for task dispatch and monitoring (stack traces, profiling, heap analysis)
- Maintaining a local catalog view for table schema information during compaction

## 3. Structure

```
src/storage/compactor/
├── Cargo.toml              # Crate manifest (risingwave_compactor)
└── src/
    ├── lib.rs              # CLI options (CompactorOpts), compactor modes, start entry
    ├── server.rs           # Server initialization: compactor_serve(), shared_compactor_serve()
    ├── rpc.rs              # gRPC implementations: CompactorServiceImpl, MonitorServiceImpl
    ├── telemetry.rs        # Telemetry reporting: CompactorTelemetryCreator, CompactorTelemetryReport
    └── compactor_observer/
        ├── mod.rs          # Observer module exports
        └── observer_manager.rs  # CompactorObserverNode for catalog/system param updates
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | CompactorOpts CLI definition, CompactorMode enum, start() entry point |
| `src/server.rs` | Main server logic, memory configuration, heartbeat loops |
| `src/rpc.rs` | gRPC service handlers for task dispatch and monitoring |
| `src/telemetry.rs` | Telemetry reporting implementation for compactor node |
| `src/compactor_observer/observer_manager.rs` | Observer node handling catalog updates from meta |

## 5. Edit Rules (Must)

- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Add unit tests for new RPC handlers in `rpc.rs`
- Update memory configuration validation when changing `prepare_start_parameters()`
- Test both Dedicated and Shared compactor modes for RPC changes
- Verify telemetry events are properly logged with correct report types
- Maintain graceful shutdown handling for all background tasks

## 6. Forbidden Changes (Must Not)

- Remove or modify the `start()` function signature (must return `Pin<Box<dyn Future>>`)
- Change gRPC message size defaults without compatibility testing
- Break backward compatibility in CompactorService proto definitions
- Remove memory limit validation checks in `prepare_start_parameters()`
- Disable failpoints feature usage in test code paths
- Modify shutdown token handling without testing graceful termination

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Compactor unit tests | `cargo test -p risingwave_compactor` |
| Storage integration | `cargo test -p risingwave_hummock_test` |
| All storage tests | `./risedev test -- storage` |
| Clippy check | `./risedev c` |
| Build check | `./risedev b` |

## 8. Dependencies & Contracts

- Rust edition 2021
- tokio (madsim-tokio): Async runtime with multi-thread support
- tonic: gRPC server framework for CompactorService
- risingwave_storage: Hummock compactor implementation and SSTable operations
- risingwave_rpc_client: MetaClient for registration and heartbeat
- risingwave_object_store: Object store abstraction for S3/GCS/Azure/OSS
- Compactor modes: Dedicated, Shared, DedicatedIceberg, SharedIceberg (not yet implemented)
- Memory limits: Configurable via `RW_COMPACTOR_TOTAL_MEMORY_BYTES` and `RW_COMPACTOR_META_CACHE_MEMORY_BYTES`

## 9. Overrides

- Section 5 (Edit Rules): Additional requirement to test both Dedicated and Shared modes for RPC changes
- Section 6 (Forbidden Changes): Additional prohibition on modifying `start()` function signature

## 10. Update Triggers

Regenerate this file when:
- New compactor mode is added to CompactorMode enum
- gRPC service definitions change (proto changes)
- Memory configuration or validation logic changes
- New observer notification types are handled
- Telemetry reporting format changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/storage/AGENTS.md

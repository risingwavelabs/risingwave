# AGENTS.md - Batch Compute Engine

## 1. Scope

Policies specific to `./src/batch` and its subdirectories. This covers the batch query execution engine, including task management, executor implementations, and distributed execution.

**Policy Inheritance:**
This file inherits rules from `./AGENTS.md` (root).
When working in this directory:
1. Read THIS file first for directory-specific rules
2. Then read PARENT files up to root for inherited rules
3. Child rules override parent rules (nearest-wins)
4. If code changes conflict with these docs, update AGENTS.md to match reality

## 2. Purpose

The batch compute engine executes SQL queries in a distributed, parallel manner. It receives query plans from the frontend, manages task execution across worker nodes, and coordinates data exchange between operators. It supports both OLAP-style analytical queries and DML operations (INSERT, UPDATE, DELETE).

## 3. Structure

```
src/batch/
├── Cargo.toml              # Main crate manifest (risingwave_batch)
├── src/
│   ├── lib.rs              # Crate root, module declarations
│   ├── error.rs            # BatchError enum and error handling
│   ├── exchange_source.rs  # Data exchange abstraction
│   ├── execution/          # Exchange implementations (gRPC, local)
│   ├── executor/           # Core executor trait and builder
│   ├── monitor/            # Execution statistics and metrics
│   ├── rpc/                # gRPC service definitions
│   ├── spill/              # Disk spilling for memory-intensive ops
│   ├── task/               # Task lifecycle and channel management
│   └── worker_manager/     # Worker node discovery and management
└── executors/              # Separate crate: risingwave_batch_executors
    ├── Cargo.toml
    ├── benches/            # Criterion micro-benchmarks
    └── src/executor/       # Executor implementations
        ├── join/           # Join operators (hash, nested loop, lookup)
        ├── aggregation/    # Aggregation helpers
        ├── filter.rs       # Filter/selection operator
        ├── hash_agg.rs     # Hash aggregation
        ├── insert.rs       # DML operators
        ├── project.rs      # Projection operator
        ├── row_seq_scan.rs # Table scan operators
        └── ...             # 50+ executor implementations
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Module exports and crate features |
| `src/executor/mod.rs` | `Executor` trait, `BoxedExecutor` type, builder pattern |
| `src/task/task_manager.rs` | Task lifecycle management |
| `src/error.rs` | `BatchError` enum with `#[derive(Construct)]` |
| `executors/src/lib.rs` | Executor registration via `linkme` distributed slice |
| `executors/src/executor.rs` | `Executor2` trait and macro-based registration |

## 5. Edit Rules (Must)

- All executors must implement the `Executor` trait in `src/executor/mod.rs`
- Use `#[async_trait::async_trait]` for async trait implementations
- Register new executors in `executors/src/lib.rs` using `#[linkme::distributed_slice]`
- Implement `BoxedExecutorBuilder` for executor construction from protobuf plans
- Add unit tests with `#[cfg(test)]` modules in the same file as the implementation
- Ensure `DataChunk` output cardinality is never zero (enforced by contract)
- Use `BatchError` for error propagation; avoid `anyhow` in public APIs
- Run `cargo fmt` before committing

## 6. Forbidden Changes (Must Not)

- Change `Executor::execute()` return type `BoxedDataChunkStream` without updating all 50+ executors
- Remove or modify `ExecutorInfo` fields (schema, id) - used by scheduler
- Add blocking I/O in executor `execute()` methods (must be async)
- Break gRPC exchange protocol backward compatibility without versioning
- Use `unsafe` for memory management (use safe Rust abstractions)
- Modify benchmark statistical thresholds without justification
- Skip unit tests for new executor implementations

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests (main) | `cargo test -p risingwave_batch` |
| Unit tests (executors) | `cargo test -p risingwave_batch_executors` |
| Benchmarks | `cargo bench -p risingwave_batch_executors` |
| Specific benchmark | `cargo bench -p risingwave_batch_executors -- <filter>` |
| Clippy | `./risedev c` or `cargo clippy -p risingwave_batch` |

## 8. Dependencies & Contracts

- Depends on: `risingwave_common`, `risingwave_pb`, `risingwave_storage`, `risingwave_expr`
- Uses `futures-async-stream` for async stream implementations
- Uses `linkme` for distributed executor registration
- Protocol: gRPC for inter-node data exchange (`TaskService`, `ExchangeService`)
- Storage: Delegates to `risingwave_storage` for table scans
- Metrics: Prometheus-based via `monitor/stats.rs`

## 9. Overrides

None. Inherits all rules from parent AGENTS.md.

## 10. Update Triggers

Regenerate this file when:
- New executor trait or builder pattern introduced
- gRPC service definitions change
- Task management architecture modified
- New benchmark harness added
- Crate splitting or merging occurs

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `./AGENTS.md`

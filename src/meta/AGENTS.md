# AGENTS.md - Meta Service Crate

## 1. Scope

Policies for the `/src/meta` directory containing the RisingWave metadata management service.

**Policy Inheritance:**
This file inherits rules from `/home/k11/risingwave/AGENTS.md` (root).
When working in this directory:
1. Read THIS file first for directory-specific rules
2. Then read PARENT files up to root for inherited rules
3. Child rules override parent rules (nearest-wins)
4. If code changes conflict with these docs, update AGENTS.md to match reality

## 2. Purpose

The meta service is the central metadata management and cluster coordination service for RisingWave. It manages cluster membership, catalog metadata, barrier coordination, Hummock storage metadata, streaming job lifecycle, and DDL operations.

## 3. Structure

```
src/meta/
├── node/              # Meta node server executable
├── service/           # gRPC service implementations
└── src/
    ├── lib.rs         # Crate root, module exports
    ├── error.rs       # MetaError, MetaResult types
    ├── telemetry.rs   # Telemetry reporting
    ├── backup_restore/# Metadata backup and restore
    ├── barrier/       # Distributed barrier coordination
    │   ├── checkpoint/# Checkpoint lifecycle management
    │   ├── context/   # Recovery context
    │   └── tests/     # Integration tests
    ├── controller/    # SQL metadata controllers (sea-orm)
    │   ├── catalog/   # Catalog controller
    │   ├── cluster.rs # Cluster state
    │   ├── fragment.rs# Fragment metadata
    │   ├── scale.rs   # Scaling operations
    │   ├── streaming_job.rs # Job lifecycle
    │   └── user.rs    # User management
    ├── dashboard/     # Web dashboard (non-madsim)
    ├── hummock/       # Hummock storage metadata
    │   ├── compaction/# Compaction scheduling
    │   ├── manager/   # Hummock manager
    │   └── model/     # Metadata models
    ├── manager/       # Core managers
    │   ├── env.rs     # MetaSrvEnv, MetaOpts
    │   ├── metadata.rs# MetadataManager
    │   ├── notification.rs # Notification dispatch
    │   └── sink_coordination/ # Exactly-once sink
    ├── model/         # In-memory metadata models
    ├── rpc/           # RPC utilities, election client
    ├── serving/       # Serving layer management
    └── stream/        # Stream job management
        ├── scale.rs   # Rescaling logic
        └── stream_manager.rs
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Module declarations, MetaStoreBackend enum |
| `src/error.rs` | MetaError, MetaResult types with thiserror |
| `src/manager/env.rs` | MetaSrvEnv, MetaOpts configuration |
| `src/manager/metadata.rs` | MetadataManager facade |
| `src/rpc/ddl_controller.rs` | DDL operation orchestration |
| `src/barrier/mod.rs` | Barrier manager entry |
| `src/hummock/manager/mod.rs` | Hummock metadata manager |
| `Cargo.toml` | Dependencies: sea-orm, tonic, tokio, fail |

## 5. Edit Rules (Must)

- Use `MetaResult<T>` for fallible functions; define errors in `error.rs`
- Wrap async trait implementations with `#[async_trait::async_trait]`
- Use `MetadataModelError` for persistence layer errors
- Run `cargo fmt` and `./risedev c` before committing
- Add `#[cfg(test)]` modules for unit tests, use `#[tokio::test]` for async tests
- Use `fail::cfg` for failpoint injection in fault-tolerance tests
- Follow sea-orm patterns in `controller/` modules for SQL metadata
- Use `tracing` for logging, include context fields

## 6. Forbidden Changes (Must Not)

- Modify `MetaErrorInner` variants without updating tonic::Status conversion
- Use blocking database operations in async contexts
- Commit without running hummock manager tests (`cargo test -p risingwave_meta hummock`)
- Add unsafe code without justification and safety comments
- Remove failpoint annotations without updating corresponding tests
- Change `MetaStoreBackend` variants without updating node initialization

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_meta` |
| Hummock tests | `cargo test -p risingwave_meta hummock` |
| Barrier tests | `cargo test -p risingwave_meta barrier` |
| Controller tests | `cargo test -p risingwave_meta controller` |
| With failpoints | `cargo test -p risingwave_meta --features failpoints` |
| Clippy | `./risedev c` |

## 8. Dependencies & Contracts

- Core: tokio (madsim-tokio in simulation), tonic, sea-orm, parking_lot
- Internal: risingwave_meta_model, risingwave_meta_model_migration, risingwave_pb
- Storage: etcd-client for election, SQL backend (SQLite/Postgres/MySQL)
- Features: `test` for test utilities, `failpoints` for fault injection
- gRPC services defined in risingwave_pb, implemented in `service/`
- Uses `sync-point` utility for deterministic testing

## 9. Overrides

None. Inherits all rules from root AGENTS.md.

## 10. Update Triggers

Regenerate this file when:
- New manager module added to `src/manager/`
- New controller added to `src/controller/`
- MetaStoreBackend or MetaOpts configuration changes
- Error type hierarchy modified
- New gRPC service added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

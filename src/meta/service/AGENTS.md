# AGENTS.md - Meta Service Implementations

## 1. Scope

Policies for the `src/meta/service/` directory containing gRPC service implementations for the RisingWave meta service.

## 2. Purpose

The service crate implements the gRPC service handlers defined in the protobuf specifications. These services handle cluster management, DDL operations, Hummock storage coordination, streaming job lifecycle, and user management.

## 3. Structure

```
src/meta/service/
└── src/
    ├── lib.rs                    # Service module exports
    ├── backup_service.rs         # Backup/restore operations
    ├── cloud_service.rs          # Cloud provider integration
    ├── cluster_limit_service.rs  # Resource limit management
    ├── cluster_service.rs        # Cluster membership operations
    ├── ddl_service.rs            # DDL statement handling
    ├── event_log_service.rs      # Event logging
    ├── health_service.rs         # Health check endpoints
    ├── heartbeat_service.rs      # Worker heartbeat handling
    ├── hosted_iceberg_catalog_service.rs # Iceberg catalog
    ├── hummock_service.rs        # Hummock storage management
    ├── meta_member_service.rs    # Meta cluster membership
    ├── monitor_service.rs        # Monitoring and metrics
    ├── notification_service.rs   # Push notifications to workers
    ├── scale_service.rs          # Scaling operations
    ├── serving_service.rs        # Serving layer management
    ├── session_config.rs         # Session configuration
    ├── sink_coordination_service.rs # Exactly-once sink coordination
    ├── stream_service.rs         # Streaming job management
    ├── system_params_service.rs  # System parameters
    ├── telemetry_service.rs      # Telemetry reporting
    └── user_service.rs           # User management
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Service exports and registration |
| `src/ddl_service.rs` | CREATE/DROP/ALTER statement handling |
| `src/stream_service.rs` | Job creation, scaling, recovery |
| `src/hummock_service.rs` | SSTable, compaction, version management |
| `src/cluster_service.rs` | Worker registration and heartbeat |
| `src/notification_service.rs` | Catalog change notifications |
| `src/backup_service.rs` | Metadata backup and restore |
| `src/scale_service.rs` | Rescaling and parallelism changes |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Implement `#[async_trait::async_trait]` for service traits
- Use `MetaResult<T>` for fallible operations
- Add request logging with `tracing`
- Validate RPC inputs before processing
- Return appropriate `tonic::Status` codes

## 6. Forbidden Changes (Must Not)

- Change gRPC method signatures without proto updates
- Remove service handlers without deprecation
- Skip authorization checks for sensitive operations
- Use blocking operations in async service methods
- Panic in service handlers - always return errors

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Service tests | `cargo test -p risingwave_meta_service` |
| DDL tests | `cargo test -p risingwave_meta ddl` |
| Hummock tests | `cargo test -p risingwave_meta hummock` |
| Stream tests | `cargo test -p risingwave_meta stream` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- tonic: gRPC service framework
- tokio: Async runtime
- Internal: `risingwave_meta`, `risingwave_pb`
- RPC definitions: `proto/meta.proto` and related
- Authorization: Via `risingwave_meta` middleware

## 9. Overrides

None. Inherits rules from parent `src/meta/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New gRPC service added
- Service implementation patterns changed
- New service category introduced
- RPC error handling conventions updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/meta/AGENTS.md

# AGENTS.md - Object Store Crate

## 1. Scope

Directory: `/home/k11/risingwave/src/object_store`

This crate provides a unified abstraction layer for object storage backends used by RisingWave. It supports multiple storage systems including AWS S3, Google Cloud Storage (GCS), Azure Blob Storage, Alibaba OSS, Huawei OBS, HDFS, WebHDFS, MinIO, and local filesystem.

**Policy Inheritance:**
This file inherits rules from `/home/k11/risingwave/AGENTS.md` (root).
When working in this directory:
1. Read THIS file first for directory-specific rules
2. Then read PARENT files up to root for inherited rules
3. Child rules override parent rules (nearest-wins)
4. If code changes conflict with these docs, update AGENTS.md to match reality

## 2. Purpose

The object_store crate serves as the storage interface layer for RisingWave's cloud-native architecture. It provides:

- A trait-based abstraction (`ObjectStore`) for consistent storage operations across backends
- Native AWS S3 implementation with multipart upload support
- OpenDAL-based implementations for various cloud and on-premise storage systems
- In-memory storage for testing and development
- Metrics collection for monitoring storage operations
- Retry logic with configurable exponential backoff
- Simulation support for deterministic testing

## 3. Structure

```
src/object_store/
├── Cargo.toml
└── src/
    ├── lib.rs                    # Crate entry point
    └── object/
        ├── mod.rs                # Core ObjectStore trait, MonitoredObjectStore, dispatch macros
        ├── error.rs              # ObjectError, ObjectErrorInner error types
        ├── s3.rs                 # Native AWS S3 implementation
        ├── mem.rs                # In-memory object store for testing
        ├── prefix.rs             # Object key prefix utilities
        ├── object_metrics.rs     # Prometheus metrics collection
        ├── sim/                  # Simulation object store (madsim)
        │   ├── mod.rs
        │   ├── client.rs
        │   ├── service.rs
        │   ├── rpc_server.rs
        │   └── error.rs
        └── opendal_engine/       # OpenDAL-based backends
            ├── mod.rs
            ├── opendal_object_store.rs
            ├── opendal_s3.rs
            ├── gcs.rs
            ├── oss.rs
            ├── obs.rs
            ├── azblob.rs
            ├── webhdfs.rs
            ├── hdfs.rs
            └── fs.rs
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/object/mod.rs` | Core `ObjectStore` trait definition, `MonitoredObjectStore` wrapper, operation dispatch macros |
| `src/object/error.rs` | Error types: `ObjectError`, `ObjectErrorInner` with retry classification |
| `src/object/s3.rs` | Native AWS S3 implementation with multipart upload and lifecycle management |
| `src/object/opendal_engine/opendal_object_store.rs` | OpenDAL wrapper providing unified access to multiple backends |
| `src/object/mem.rs` | In-memory object store implementation for testing |
| `src/object/object_metrics.rs` | Prometheus metrics for operation latency, bytes transferred, and failures |
| `Cargo.toml` | Crate manifest with AWS SDK, OpenDAL, and testing dependencies |

## 5. Edit Rules (Must)

- All code comments must be in English
- Run `cargo fmt` before committing changes
- Pass `./risedev c` (clippy) before submitting changes
- Add unit tests for new public functions in object store implementations
- Maintain backward compatibility for `ObjectStore` trait changes
- Update retry logic tests when modifying error classification
- Ensure new backends implement all `ObjectStore` trait methods
- Follow existing macro patterns for `for_all_object_store!` dispatch

## 6. Forbidden Changes (Must Not)

- Do not break the `ObjectStore` trait contract without updating all implementations
- Do not remove retry logic from storage operations
- Do not disable metrics collection in production code paths
- Do not bypass fail points in test configurations
- Do not modify `ObjectError` classification without reviewing retry implications
- Do not add new backend dependencies without workspace coordination
- Do not use blocking I/O operations in async contexts

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_object_store` |
| Clippy check | `./risedev c` or `cargo clippy -p risingwave_object_store` |
| Build check | `./risedev b` or `cargo build -p risingwave_object_store` |

Note: This crate does not contain standalone tests. Testing is primarily done through:
- Unit tests embedded in source files (e.g., `mem.rs`, `s3.rs`)
- Integration tests via storage crate
- E2E tests using in-memory or simulation backends
- Fail point testing for error injection

## 8. Dependencies & Contracts

- Rust edition 2021
- Key dependencies:
  - `aws-sdk-s3` (madsim-aws-sdk-s3) - Native S3 client
  - `opendal` - Unified storage abstraction for multiple backends
  - `tokio` (madsim-tokio) - Async runtime
  - `prometheus` - Metrics collection
  - `tokio-retry` - Retry logic with backoff
  - `await-tree` - Async operation instrumentation
  - `fail` - Fail point injection for testing
- Workspace dependencies: `risingwave_common` for configuration

## 9. Overrides

None. This crate follows the root AGENTS.md policies without overrides.

## 10. Update Triggers

Regenerate this file when:
- New storage backend is added
- ObjectStore trait interface changes
- Retry configuration structure changes
- New metrics are added or existing ones modified
- Fail point behavior changes
- OpenDAL or AWS SDK major version upgrades

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `/home/k11/risingwave/AGENTS.md`

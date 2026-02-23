# AGENTS.md - RisingWave Storage Engine

## 1. Scope

Policies for `/home/k11/risingwave/src/storage` and all subdirectories. This covers the cloud-native storage engine including Hummock LSM-tree implementation, backup/restore, and compactor components.

**Policy Inheritance:**
This file inherits rules from `/home/k11/risingwave/AGENTS.md` (root).
When working in this directory:
1. Read THIS file first for directory-specific rules
2. Then read PARENT files up to root for inherited rules
3. Child rules override parent rules (nearest-wins)
4. If code changes conflict with these docs, update AGENTS.md to match reality

## 2. Purpose

The storage engine provides a distributed, cloud-native state store for RisingWave's streaming and batch processing. Key capabilities:
- Hummock: A tiered LSM-tree storage engine optimized for cloud object stores (S3/GCS/Azure/OSS)
- StateStore trait: Unified interface for key-value operations with epoch-based versioning
- Compaction: Background compaction service for optimizing storage layout
- Backup: Point-in-time recovery and metadata backup to object storage

## 3. Structure

```
src/storage/
├── src/                      # Main storage crate (risingwave_storage)
│   ├── hummock/              # LSM-tree implementation
│   │   ├── sstable/          # SSTable format and builders
│   │   ├── compactor/        # Compaction logic
│   │   ├── iterator/         # Merge and user iterators
│   │   ├── shared_buffer/    # In-memory write buffer
│   │   └── event_handler/    # Hummock event processing
│   ├── store.rs              # StateStore trait definitions
│   ├── store_impl.rs         # StateStore implementations
│   ├── mem_table.rs          # In-memory write buffer (MemTable)
│   ├── memory.rs             # In-memory state store
│   ├── table/                # Table-specific storage abstractions
│   └── row_serde/            # Row serialization/deserialization
├── hummock_sdk/              # Shared Hummock types (risingwave_hummock_sdk)
│   ├── key.rs                # Key encoding/decoding
│   ├── sstable_info.rs       # SSTable metadata
│   ├── version.rs            # Hummock version management
│   └── compaction_group.rs   # Compaction group definitions
├── compactor/                # Compactor binary crate (risingwave_compactor)
├── backup/                   # Backup/restore crate (risingwave_backup)
├── hummock_test/             # Integration tests (risingwave_hummock_test)
├── hummock_trace/            # Hummock operation tracing
└── benches/                  # Storage benchmarks
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/store.rs` | StateStore trait and read/write APIs |
| `src/store_impl.rs` | StateStore implementation dispatch |
| `src/hummock/mod.rs` | Hummock module exports |
| `src/hummock/sstable/builder.rs` | SSTable builder implementation |
| `src/hummock/sstable/block.rs` | SSTable block format |
| `hummock_sdk/src/key.rs` | FullKey, TableKey encoding |
| `hummock_sdk/src/version.rs` | Hummock version delta management |
| `compactor/src/server.rs` | Compactor service entry point |
| `backup/src/lib.rs` | Backup manifest and operations |

## 5. Edit Rules (Must)

- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Add unit tests for new public functions in `store.rs` or `hummock/`
- Update SSTable format tests when modifying block encoding
- Run storage benchmarks when changing hot paths (`benches/`)
- Use `failpoints` feature for failure injection tests (see `storage_failpoints/`)
- Maintain backward compatibility for persisted data formats

## 6. Forbidden Changes (Must Not)

- Modify SSTable block format without backward compatibility tests
- Change FullKey encoding without migration strategy
- Remove `test` feature gate from public APIs used by tests
- Delete hummock version checkpoint files manually
- Commit with `failpoints` feature enabled in production code
- Break StateStore trait backward compatibility without adapter

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Storage unit tests | `cargo test -p risingwave_storage` |
| Hummock integration | `cargo test -p risingwave_hummock_test` |
| Backup tests | `cargo test -p risingwave_backup` |
| Compactor tests | `cargo test -p risingwave_compactor` |
| Failpoint tests | `cargo test -p risingwave_storage --features failpoints` |
| Storage benches | `cargo bench -p risingwave_storage` |
| All storage tests | `./risedev test -- storage` |

## 8. Dependencies & Contracts

- Rust edition 2021
- foyer: Block cache implementation
- lz4/zstd: SSTable compression
- risingwave_object_store: Abstraction over S3/GCS/Azure/OSS
- Hummock SDK version: Must be compatible across meta/compute/compactor nodes
- SSTable format: Versioned with forward compatibility requirements
- Epoch-based versioning: 64-bit epoch with 16-bit spill offset (EpochWithGap)

## 9. Overrides

None. Follows parent AGENTS.md rules.

## 10. Update Triggers

Regenerate this file when:
- SSTable format or block encoding changes
- StateStore trait methods are added/removed
- New storage backend is added
- Hummock version protocol changes
- Backup format or manifest structure changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

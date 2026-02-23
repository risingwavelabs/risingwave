# AGENTS.md - Hummock SDK Crate

## 1. Scope

Policies for the `src/storage/hummock_sdk/` directory containing shared types and utilities for the Hummock storage engine.

## 2. Purpose

The Hummock SDK crate defines core types used across the Hummock storage system including key encoding, version management, compaction group definitions, and SSTable metadata. It provides a stable interface between meta service, compute nodes, and compactor processes.

## 3. Structure

```
src/storage/hummock_sdk/
└── src/
    ├── lib.rs                    # Module exports
    ├── key.rs                    # Key encoding (FullKey, TableKey)
    ├── key_cmp.rs                # Key comparison utilities
    ├── key_range.rs              # Key range definitions
    ├── prost_key_range.rs        # Protobuf key range conversions
    ├── sstable_info.rs           # SSTable metadata
    ├── version.rs                # Hummock version management
    ├── compact.rs                # Compaction definitions
    ├── compact_task.rs           # Compaction task types
    ├── compaction_group/         # Compaction group logic
    ├── level.rs                  # LSM level definitions
    ├── change_log.rs             # Change log for time travel
    ├── table_watermark.rs        # Table watermark tracking
    ├── time_travel.rs            # Time travel support
    ├── vector_index.rs           # Vector index metadata
    ├── table_stats.rs            # Table statistics
    ├── state_table_info.rs       # State table metadata
    └── filter_utils.rs           # Bloom filter utilities
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/key.rs` | FullKey, TableKey, UserKey encoding/decoding |
| `src/version.rs` | HummockVersion, VersionDelta management |
| `src/sstable_info.rs` | SSTableInfo, SstableObjectId |
| `src/compact_task.rs` | Compaction task definitions |
| `src/compaction_group/` | Compaction group configuration |
| `src/table_watermark.rs` | Epoch-based watermark tracking |
| `src/time_travel.rs` | Time travel metadata |
| `Cargo.toml` | Lightweight crate dependencies |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Maintain backward compatibility for serialized types
- Add `#[derive(Clone, Debug, PartialEq)]` for public types
- Use `prost` attributes for protobuf-compatible types
- Document invariants in type documentation
- Version format changes require migration strategy

## 6. Forbidden Changes (Must Not)

- Change key encoding without backward compatibility
- Modify version serialization format without migration
- Remove public types without deprecation period
- Break protobuf compatibility for network types
- Add heavy dependencies to this lightweight crate

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| SDK unit tests | `cargo test -p risingwave_hummock_sdk` |
| Key encoding tests | `cargo test -p risingwave_hummock_sdk key` |
| Version tests | `cargo test -p risingwave_hummock_sdk version` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- `risingwave_pb`: Protobuf definitions
- `bytes`: Byte buffer handling
- `itertools`: Iterator utilities
- Must be lightweight - avoid heavy dependencies
- Types shared across meta, compute, and compactor
- Version compatibility required across node types

## 9. Overrides

None. Inherits rules from parent `src/storage/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New core Hummock type added
- Key encoding format changed
- Version management updated
- New compaction-related type introduced

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/storage/AGENTS.md

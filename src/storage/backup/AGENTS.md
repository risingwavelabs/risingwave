# AGENTS.md - RisingWave Backup/Restore Crate

## 1. Scope

Policies for `/home/k11/risingwave/src/storage/backup` and all subdirectories. This covers the backup and restore functionality for RisingWave cluster metadata and Hummock storage state.

## 2. Purpose

The backup crate provides point-in-time recovery capabilities for RisingWave metadata and Hummock version state. Key capabilities:
- Meta snapshot: Captures complete metadata state (catalog, users, configs, streaming jobs)
- Hummock version preservation: Ensures SSTable references are valid during restore
- Object store integration: Stores backups in S3/GCS/Azure/OSS-compatible storage
- Versioned format: Supports multiple snapshot format versions (V1, V2) for backward compatibility
- Checksum verification: XXHash64 integrity checks for all snapshot data

## 3. Structure

```
src/storage/backup/
├── src/
│   ├── lib.rs                  # Core types: MetaSnapshotMetadata, MetaSnapshotManifest
│   ├── meta_snapshot.rs        # MetaSnapshot<T> generic and Metadata trait
│   ├── meta_snapshot_v1.rs     # V1 format: ClusterMetadata with protobuf encoding
│   ├── meta_snapshot_v2.rs     # V2 format: MetadataV2 with JSON encoding (current)
│   ├── storage.rs              # ObjectStoreMetaSnapshotStorage trait impl
│   └── error.rs                # BackupError enum definitions
├── tests/
│   └── metadata_models_v2.rs   # CI guard: ensures model coverage sync with risingwave_meta_model
└── integration_tests/
    ├── run_all.sh              # Entry point for all integration tests
    ├── test_basic.sh           # Basic backup/restore test
    ├── test_pin_sst.sh         # SSTable pinning during backup test
    ├── test_query_backup.sh    # Query backup data test
    ├── test_set_config.sh      # Configuration setting test
    └── test_overwrite_endpoint.sh # Endpoint override test
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | MetaSnapshotMetadata, MetaSnapshotManifest, checksum utilities |
| `src/meta_snapshot.rs` | Generic MetaSnapshot<T> with encode/decode logic |
| `src/meta_snapshot_v1.rs` | V1 format: ClusterMetadata with protobuf encoding |
| `src/meta_snapshot_v2.rs` | V2 format: MetadataV2 with macro-generated model list |
| `src/storage.rs` | ObjectStoreMetaSnapshotStorage implementation |
| `src/error.rs` | BackupError types (Storage, Encoding, Checksum, etc.) |
| `tests/metadata_models_v2.rs` | Ensures all meta model tables are covered in backup |

## 5. Edit Rules (Must)

- Run `cargo fmt` before committing
- Pass `cargo clippy -p risingwave_backup` before submitting changes
- Add unit tests for new public functions in `src/`
- Update `for_all_metadata_models_v2!` macro when adding new metadata tables
- Update `excluded` list in `tests/metadata_models_v2.rs` for runtime-only tables
- Maintain backward compatibility for existing snapshot formats (V1, V2)
- Add checksum verification for any new encoded data fields
- Run integration tests with `./integration_tests/run_all.sh` when changing restore logic

## 6. Forbidden Changes (Must Not)

- Remove fields from ClusterMetadata (V1) - only append for backward compatibility
- Change format_version handling logic without migration testing
- Modify `for_all_metadata_models_v2!` order - append only at the end
- Skip checksum verification in decode paths
- Break MetaSnapshotStorage trait backward compatibility without adapter
- Remove existing snapshot format support without deprecation period
- Delete or modify backup manifest files manually

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_backup` |
| Model sync test | `cargo test -p risingwave_backup --test metadata_models_v2` |
| Integration tests | `bash integration_tests/run_all.sh` |
| Single integration test | `bash integration_tests/test_basic.sh` |

## 8. Dependencies & Contracts

- risingwave_object_store: Storage abstraction for backup destination
- risingwave_meta_model: Source of truth for metadata table definitions
- risingwave_hummock_sdk: HummockVersion and related types
- XXHash64: Checksum algorithm for data integrity (0 seed)
- JSON encoding: V2 format uses serde_json with length prefix
- Protobuf encoding: V1 format uses prost for backward compatibility
- Manifest format: JSON file listing all valid snapshots with metadata

## 9. Overrides

None. Follows parent AGENTS.md rules from `src/storage/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New metadata model added to risingwave_meta_model
- New snapshot format version (V3+) introduced
- Backup manifest structure changes
- MetaSnapshotStorage trait methods added/removed
- Object store integration changes
- Checksum algorithm changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/storage/AGENTS.md

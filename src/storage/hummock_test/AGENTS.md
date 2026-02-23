# AGENTS.md - Hummock Integration Tests

## 1. Scope

Policies for the `src/storage/hummock_test/` directory containing comprehensive integration tests for the Hummock storage engine.

## 2. Purpose

The hummock_test crate provides extensive integration tests covering state store operations, compaction logic, recovery scenarios, and concurrency patterns. These tests validate Hummock behavior across multiple nodes and failure conditions.

## 3. Structure

```
src/storage/hummock_test/
├── benches/                      # Hummock benchmarks
├── src/
│   ├── lib.rs                    # Test utilities
│   ├── hummock_storage_tests.rs  # Core storage tests
│   ├── state_store_tests.rs      # StateStore trait tests
│   ├── compactor_tests.rs        # Compaction logic tests
│   ├── sync_point_tests.rs       # Deterministic sync tests
│   ├── failpoint_tests.rs        # Failure injection tests
│   ├── snapshot_tests.rs         # Snapshot isolation tests
│   ├── hummock_read_version_tests.rs # Version management tests
│   ├── hummock_vector_tests.rs   # Vector storage tests
│   ├── auto_rebuild_iter_tests.rs # Iterator rebuild tests
│   ├── test_utils.rs             # Shared test utilities
│   ├── local_state_store_test_utils.rs # Local store helpers
│   └── mock_notification_client.rs # Test mock objects
└── bin/                          # Test binaries
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Test utilities and shared fixtures |
| `src/hummock_storage_tests.rs` | Core Hummock storage operations |
| `src/state_store_tests.rs` | StateStore interface compliance |
| `src/compactor_tests.rs` | Compaction scheduling and execution |
| `src/sync_point_tests.rs` | Deterministic concurrency tests |
| `src/failpoint_tests.rs` | Fault tolerance validation |
| `src/snapshot_tests.rs` | Snapshot isolation verification |
| `src/test_utils.rs` | Common test setup utilities |
| `benches/` | Performance benchmarks |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Use `#[tokio::test]` for async tests
- Add `fail::cfg` points for fault injection tests
- Clean up test resources in teardown
- Use `sync-point` for deterministic ordering tests
- Document test scenarios and invariants

## 6. Forbidden Changes (Must Not)

- Skip integration tests without documented reason
- Remove failpoint tests without coverage replacement
- Leave test data files unreleased
- Use non-deterministic assertions in sync tests
- Modify test expectations without running tests

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All tests | `cargo test -p risingwave_hummock_test` |
| Storage tests | `cargo test -p risingwave_hummock_test storage` |
| Compactor tests | `cargo test -p risingwave_hummock_test compactor` |
| Failpoint tests | `cargo test -p risingwave_hummock_test --features failpoints` |
| Sync point tests | `cargo test -p risingwave_hummock_test sync_point` |
| Benchmarks | `cargo bench -p risingwave_hummock_test` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- Test framework: `tokio::test`, `fail` for failpoints
- Internal: `risingwave_storage`, `risingwave_hummock_sdk`
- Sync points: `sync-point` crate for determinism
- Mock objects: `mockall` for interface mocking
- Features: `failpoints` for fault injection

## 9. Overrides

None. Inherits rules from parent `src/storage/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New test category added
- Test framework or utilities changed
- New failure scenario tests added
- Benchmark suite expanded

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/storage/AGENTS.md

# AGENTS.md - Compaction Tests

## 1. Scope

Directory: `src/tests/compaction_test`
Crate: `risingwave_compaction_test`

Integration tests for RisingWave LSM-tree compaction strategies and storage layer.

## 2. Purpose

Validates compaction behavior in the storage engine:
- Test compaction picker algorithms (min overlapping, tier compaction)
- Verify compaction task generation and execution
- Validate state table compaction correctness
- Test compaction with various data patterns and workloads
- Ensure data consistency before and after compaction

## 3. Structure

```
src/tests/compaction_test/
├── src/
│   ├── lib.rs                   # Library exports and test utilities
│   ├── bin/
│   │   └── compaction.rs        # CLI binary for running compaction tests
│   └── compaction_test_runner.rs # Main test runner and test cases
├── Cargo.toml                   # Dependencies: storage, meta, test utilities
├── Makefile.toml                # Test automation tasks
└── AGENTS.md                    # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/compaction_test_runner.rs` | Core test cases for compaction scenarios |
| `src/bin/compaction.rs` | CLI entry point for running compaction tests |
| `src/lib.rs` | Test utilities and shared helper functions |
| `Makefile.toml` | cargo-make tasks for test execution |

## 5. Edit Rules (Must)

- Write tests using deterministic data patterns for reproducibility
- Test both manual compaction triggers and automatic compaction
- Verify data integrity with checksums before and after compaction
- Use `risingwave_storage` test utilities for state management
- Document test scenario purpose and expected behavior
- Include tests for edge cases: empty SSTs, tombstones, large values
- Run `cargo fmt` before committing
- Run `./risedev c` before submitting changes
- Ensure tests pass with `./risedev test -p risingwave_compaction_test`

## 6. Forbidden Changes (Must Not)

- Modify compaction tests without understanding storage internals
- Remove existing compaction test coverage
- Use non-deterministic data generation in test cases
- Skip verification of data integrity after compaction
- Hardcode SST file paths or sizes
- Bypass test isolation requirements
- Modify compaction algorithm without updating tests

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_compaction_test` |
| Run compaction binary | `cargo run -p risingwave_compaction_test --bin compaction` |
| Build check | `cargo check -p risingwave_compaction_test` |
| Clippy check | `./risedev c -p risingwave_compaction_test` |

## 8. Dependencies & Contracts

- `risingwave_storage`: LSM-tree storage engine under test
- `risingwave_meta`: Metadata management for compaction scheduling
- `risingwave_common`: Common types and utilities
- `risingwave_hummock_sdk`: Hummock storage format definitions
- Test runner integrates with compaction picker and scheduler
- Uses test harness for isolated state table testing

## 9. Overrides

None. Follows parent test policies from `src/tests/` hierarchy.

## 10. Update Triggers

Regenerate this file when:
- New compaction test scenarios added
- Compaction algorithm or strategy changes
- Storage format changes affecting compaction
- New compaction picker implementations
- Test framework or utilities updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `/home/k11/risingwave/AGENTS.md`

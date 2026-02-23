# AGENTS.md - RisingWave MemTable Spill Test

## 1. Scope

Policies for the `risingwave_mem_table_spill_test` subcrate. This is a test-only crate for validating memory table spill-to-disk functionality in the RisingWave streaming engine.

## 2. Purpose

This crate tests the spill-to-disk mechanism for streaming state tables. When memory usage exceeds configured thresholds, state table data should flush from memory to persistent storage (Hummock) while maintaining consistency and read visibility. Tests verify:
- Correct data retention after spill operations
- Multiple spill cycles with data overwrites
- Read consistency during and after spills
- State table epoch management with spilled data

## 3. Structure

```
src/stream/spill_test/
├── Cargo.toml                  # Crate manifest
└── src/
    ├── lib.rs                  # Crate root, test framework setup
    └── test_mem_table.rs       # Spill test implementations
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Crate root with test framework features enabled |
| `src/test_mem_table.rs` | Test cases for mem table spill behavior |
| `Cargo.toml` | Dependencies on risingwave_stream, risingwave_hummock_test |

## 5. Edit Rules (Must)

- Use `prepare_hummock_test_env()` for test environment setup
- Create large test data (5MB+) to trigger spill behavior
- Verify row data before and after `try_flush()` calls
- Use `StateTable::from_table_catalog_inconsistent_op` for test table creation
- Initialize epochs with `EpochPair::new_test_epoch()` before operations
- Assert exact row equality after spill operations
- Test both single spills and multiple consecutive spills

## 6. Forbidden Changes (Must Not)

- Reduce test data size below spill threshold (would not trigger spill)
- Remove epoch initialization before state table operations
- Use production state table constructors in tests
- Skip post-spill assertions that verify data integrity
- Modify spill threshold configuration without testing edge cases

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Spill tests | `cargo test -p risingwave_mem_table_spill_test` |
| Specific test | `cargo test -p risingwave_mem_table_spill_test test_mem_table_spill_in_streaming` |
| Multiple spills test | `cargo test -p risingwave_mem_table_spill_test test_mem_table_spill_in_streaming_multiple_times` |

## 8. Dependencies & Contracts

- **Internal**: risingwave_stream (StateTable), risingwave_hummock_test (test environment)
- **Test framework**: madsim-tokio for async test runtime
- **State backend**: Hummock LSM-Tree via test environment
- **Test data**: Large varchar columns (5MB) to exceed memory limits
- **Consistency**: Epoch-based state management with gap tracking

## 9. Overrides

- Tests use `from_table_catalog_inconsistent_op` instead of production constructors
- Memory limits are controlled via test data size, not explicit configuration

## 10. Update Triggers

Regenerate this file when:
- StateTable spill interface changes (try_flush, init_epoch)
- Hummock test environment setup changes
- Memory table spill threshold mechanisms change
- New spill test scenarios added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/AGENTS.md

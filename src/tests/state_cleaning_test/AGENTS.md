# AGENTS.md - State Cleaning Tests

## 1. Scope

Directory: `src/tests/state_cleaning_test`
Crate: `risingwave_state_cleaning_test`

Integration tests for verifying state record cleanup before watermark advancement in streaming queries.

## 2. Purpose

Validates RisingWave's ability to clean outdated state records:
- Test state cleaning triggers before watermark reaches time boundaries
- Verify state table compaction with temporal filters
- Validate aggregation state cleanup for sliding windows
- Test join state eviction policies
- Ensure correct results after state cleaning operations
- Prevent memory leaks in long-running streaming queries

## 3. Structure

```
src/tests/state_cleaning_test/
├── src/
│   └── bin/
│       └── state_cleaning_test.rs   # CLI test runner
├── data/
│   ├── agg.toml                     # Aggregation state cleaning test configs
│   ├── join.toml                    # Join state cleaning test configs
│   └── temporal_filter.toml         # Temporal filter test configs
├── Cargo.toml                       # Test dependencies
├── README.md                        # Test configuration documentation
└── AGENTS.md                        # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/bin/state_cleaning_test.rs` | Main test runner for state cleaning validation |
| `data/agg.toml` | TOML configs for aggregation state cleaning tests |
| `data/join.toml` | TOML configs for stream-stream join state cleaning |
| `data/temporal_filter.toml` | TOML configs for temporal filter tests |
| `README.md` | TOML configuration format documentation |

## 5. Edit Rules (Must)

- Define test cases in TOML format with clear structure
- Include `init_sqls` for test environment setup
- Specify `bound_tables` with patterns and row limits
- Test state cleaning with various watermark advancement patterns
- Verify row counts before and after cleaning operations
- Use deterministic test data for reproducible results
- Document test purpose in TOML comments
- Run `cargo fmt` before committing
- Test against actual RisingWave streaming queries

## 6. Forbidden Changes (Must Not)

- Remove existing state cleaning test coverage
- Skip watermark-based state cleanup validation
- Use non-deterministic data generation
- Remove aggregation, join, or temporal filter test categories
- Modify without understanding streaming state management
- Break TOML configuration format compatibility

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run all tests | `cargo run -p risingwave_state_cleaning_test` |
| Specific config | `cargo run -p risingwave_state_cleaning_test -- --config data/agg.toml` |
| Build check | `cargo check -p risingwave_state_cleaning_test` |

## 8. Dependencies & Contracts

- `risingwave_stream`: Stream executors under test
- `risingwave_storage`: State store for state table testing
- `risingwave_common`: Types and utilities
- `toml`: Configuration file parsing
- TOML format: `[[test]]` sections with `name`, `init_sqls`, `bound_tables`
- Requires running RisingWave cluster for streaming query execution

## 9. Overrides

None. Follows parent test policies from `src/tests/` hierarchy.

## 10. Update Triggers

Regenerate this file when:
- New state cleaning test scenarios added
- TOML configuration format changes
- New streaming operator state cleaning added
- Watermark handling changes
- State store compaction integration updates

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `/home/k11/risingwave/AGENTS.md`

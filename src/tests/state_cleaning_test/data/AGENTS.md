# AGENTS.md - State Cleaning Test Data

## 1. Scope

Policies for `/home/k11/risingwave/src/tests/state_cleaning_test/data` - TOML configuration files for streaming state cleaning validation tests.

## 2. Purpose

The state cleaning test data defines test scenarios for verifying that RisingWave correctly cleans up outdated state records before watermark advancement:
- Configure test table schemas and materialized views
- Define watermark specifications for temporal processing
- Set row count bounds for state table validation
- Test aggregation state cleanup for windowed queries
- Test join state eviction policies
- Test temporal filter state management

These configurations drive the state cleaning integration tests.

## 3. Structure

```
src/tests/state_cleaning_test/data/
├── agg.toml                  # Aggregation state cleaning tests
├── join.toml                 # Stream-stream join state cleaning tests
└── temporal_filter.toml      # Temporal filter state cleaning tests
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `agg.toml` | TOML configs for windowed aggregation state cleaning (tumble, hop windows) |
| `join.toml` | TOML configs for stream-stream join state eviction tests |
| `temporal_filter.toml` | TOML configs for temporal filter state management tests |

## 5. Edit Rules (Must)

- Write all comments in English
- Use TOML format with `[[test]]` array of tables
- Define `name` for each test case (unique identifier)
- Include `init_sqls` array with CREATE TABLE and CREATE MATERIALIZED VIEW statements
- Specify `WATERMARK FOR` clauses in table definitions
- Define `bound_tables` with regex patterns and row limits
- Use datagen connector for reproducible test data
- Set appropriate `max_past` and watermark intervals for timely state cleanup
- Document test purpose in TOML comments
- Validate TOML syntax before committing

## 6. Forbidden Changes (Must Not)

- Remove existing state cleaning test coverage
- Skip watermark configuration (required for state cleanup)
- Use non-deterministic data sources
- Remove aggregation, join, or temporal filter test categories
- Modify row limits without understanding state table sizing
- Break TOML configuration format compatibility
- Remove internal table pattern definitions

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run all tests | `cargo run -p risingwave_state_cleaning_test` |
| Specific config file | `cargo run -p risingwave_state_cleaning_test -- --config data/agg.toml` |
| Specific test | `cargo run -p risingwave_state_cleaning_test -- --test <test_name>` |

## 8. Dependencies & Contracts

- `toml`: Configuration file parsing
- `risingwave_state_cleaning_test`: Test runner using these configs
- Test format: `[[test]]` sections with `name`, `init_sqls`, `bound_tables`
- `init_sqls`: Array of SQL statements to set up test environment
- `bound_tables`: Array of `{ pattern = "regex", limit = N }` for state table validation
- Internal table patterns: Use regex like `__internal_mv_name_\d+_operator_\d+`
- Requires running RisingWave cluster with datagen connector support

## 9. Overrides

None. Inherits all rules from parent `/home/k11/risingwave/src/tests/state_cleaning_test/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New state cleaning test scenarios added
- TOML configuration format changes
- New streaming operator state cleaning support
- Internal table naming conventions change
- Watermark handling configuration updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/tests/state_cleaning_test/AGENTS.md

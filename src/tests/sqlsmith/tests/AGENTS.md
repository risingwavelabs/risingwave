# AGENTS.md - SQLSmith Test Runner

## 1. Scope

Policies for `./src/tests/sqlsmith/tests` - test runner and test data for the SQLSmith fuzzing framework.

## 2. Purpose

The SQLSmith test runner provides:
- Unit test execution for SQLSmith's SQL generation components
- Frontend testing with generated SQL queries
- Test data schemas for SQL generation context
- Validation of generated SQL against RisingWave's parser and binder

SQLSmith generates random but valid SQL queries to discover edge cases and bugs in query processing.

## 3. Structure

```
src/tests/sqlsmith/tests/
├── test_runner.rs            # Unit test runner entry point
├── frontend/                 # Frontend-specific test modules
└── testdata/                 # Test schemas and seed data
    └── (schema definitions)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `test_runner.rs` | Unit test harness for SQLSmith frontend tests |
| `testdata/` | Schema definitions used by SQLSmith for query generation |
| `frontend/` | Frontend test implementations |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Use `#[cfg(feature = "enable_sqlsmith_unit_test")]` for unit test code
- Maintain testdata schemas synchronized with SQL generator capabilities
- Update schema definitions when new SQL features are added
- Test generated SQL against both parser and binder
- Run `cargo test -p risingwave_sqlsmith --features enable_sqlsmith_unit_test` before submitting
- Document test purpose and SQL features being tested
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Remove testdata schemas without updating SQL generator
- Skip frontend validation for generated SQL
- Add non-deterministic test cases without seed control
- Remove error classification for SQLSmith queries
- Break feature flag conditional compilation
- Modify without testing with madsim simulation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_sqlsmith --features enable_sqlsmith_unit_test` |
| With madsim | `MADSIM_TEST_SEED=1 cargo test -p risingwave_sqlsmith --features enable_sqlsmith_unit_test` |
| Full SQLSmith run | `./target/debug/sqlsmith test --testdata ./src/tests/sqlsmith/tests/testdata` |
| Differential testing | `./target/debug/sqlsmith test --testdata ./src/tests/sqlsmith/tests/testdata --differential-testing` |

## 8. Dependencies & Contracts

- `risingwave_sqlsmith`: SQL generation and fuzzing framework
- `risingwave_frontend`: Frontend validation (parser, binder, planner)
- `madsim`: Deterministic simulation for reproducible tests
- Feature flag: `enable_sqlsmith_unit_test` controls test compilation
- Testdata format: Schema definitions for query generation context
- SQL generation: Deterministic with seed-based randomness

## 9. Overrides

None. Inherits all rules from parent `./src/tests/sqlsmith/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- Test runner implementation changes
- Testdata format or location changes
- Feature flag configuration changes
- Frontend testing integration modified
- New test categories added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/tests/sqlsmith/AGENTS.md

# AGENTS.md - PostgreSQL Regression Test Data

## 1. Scope

Policies for `./src/tests/regress/data` - PostgreSQL-compatible regression test data for RisingWave SQL compatibility testing.

## 2. Purpose

The regression test data provides comprehensive SQL compatibility testing adapted from PostgreSQL's regression suite:
- Test SQL syntax compatibility with PostgreSQL
- Verify semantic behavior alignment
- Compare query outputs between PostgreSQL and RisingWave
- Support parallel test execution schedules
- Provide expected output files for comparison

This test suite ensures RisingWave maintains PostgreSQL compatibility where applicable.

## 3. Structure

```
src/tests/regress/data/
├── COPYRIGHT                 # PostgreSQL license attribution
├── schedule                  # Parallel test execution schedule
├── sql/                      # Input SQL test files (*.sql)
├── expected/                 # Expected output files (*.out)
├── input/                    # Alternative input files
└── output/                   # Generated output files (gitignored)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `COPYRIGHT` | PostgreSQL license attribution (must be preserved) |
| `schedule` | Defines test execution order and parallel groups |
| `sql/*.sql` | SQL test case input files from PostgreSQL suite |
| `expected/*.out` | Expected output for comparison (from PostgreSQL) |
| `input/` | Alternative input files for RisingWave-specific tests |

## 5. Edit Rules (Must)

- Preserve `COPYRIGHT` file with PostgreSQL attribution
- Use `--@ ` prefix to mark ignored/disabled test queries
- Keep expected output files synchronized with input changes
- Follow PostgreSQL regression test format conventions
- Document schedule file format in comments
- Support both RisingWave and PostgreSQL modes via `--mode` flag
- Clean output directory before test runs
- Update expected outputs when behavior intentionally changes
- Document differences from PostgreSQL behavior in comments

## 6. Forbidden Changes (Must Not)

- Remove or modify COPYRIGHT file
- Delete expected outputs without justification
- Break PostgreSQL schedule format compatibility
- Leave output files from failed tests without cleanup
- Remove existing test coverage from PostgreSQL suite
- Skip parallel schedule support
- Modify without testing against both PostgreSQL and RisingWave

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run against RisingWave | `cargo run -p risingwave_regress_test -- --host 127.0.0.1 -p 4566 --input ./src/tests/regress/data --output ./src/tests/regress/output --schedule ./src/tests/regress/data/schedule --mode risingwave` |
| Run against PostgreSQL | `cargo run -p risingwave_regress_test -- --host 127.0.0.1 -p 5432 --database postgres --input ./src/tests/regress/data --output ./src/tests/regress/output --schedule ./src/tests/regress/data/schedule --mode postgres` |

## 8. Dependencies & Contracts

- `risingwave_regress_test`: Test runner for this data
- Requires `psql` binary in PATH for both PostgreSQL and RisingWave
- Uses PostgreSQL wire protocol for database connections
- Schedule format compatible with PostgreSQL parallel_schedule
- Test cases derived from PostgreSQL regression suite (PostgreSQL License)
- Output comparison ignores row order for queries without ORDER BY

## 9. Overrides

None. Inherits all rules from parent `./src/tests/regress/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New test schedule format features added
- PostgreSQL regression test suite updated
- Output comparison logic modified
- New test categories added
- Copyright or licensing terms change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/tests/regress/AGENTS.md

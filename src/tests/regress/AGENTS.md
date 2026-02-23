# AGENTS.md - Regression Tests

## 1. Scope

Directory: `src/tests/regress`
Crate: `risingwave_regress_test`

PostgreSQL-compatible regression test framework ported to Rust for RisingWave.

## 2. Purpose

Provides comprehensive SQL compatibility testing:
- Execute SQL test cases from PostgreSQL regression suite
- Compare actual output against expected output files
- Support parallel test execution schedules
- Verify SQL syntax and semantic compatibility
- Detect behavioral differences from PostgreSQL
- Run identical tests against both PostgreSQL and RisingWave

## 3. Structure

```
src/tests/regress/
├── src/
│   ├── lib.rs          # Library exports
│   ├── bin/
│   │   └── risingwave_regress_test.rs  # CLI entry point
│   ├── env.rs          # Environment variable handling
│   ├── file.rs         # File I/O utilities for test cases
│   ├── opts.rs         # Command-line options
│   ├── psql.rs         # PostgreSQL psql process management
│   └── schedule.rs     # Parallel schedule parsing and execution
├── data/
│   ├── sql/            # Input SQL test files (*.sql)
│   ├── expected/       # Expected output files (*.out)
│   ├── input/          # Alternative input files
│   ├── output/         # Generated output files
│   └── schedule        # Test schedule definition
└── AGENTS.md           # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/schedule.rs` | Parses and executes parallel test schedules |
| `src/psql.rs` | Manages psql subprocess execution |
| `src/file.rs` | Handles input/output file operations |
| `data/sql/*.sql` | SQL test case inputs |
| `data/expected/*.out` | Expected output for comparison |
| `data/schedule` | Defines test execution order and parallel groups |

## 5. Edit Rules (Must)

- Use `--@ ` syntax to mark ignored/disabled test queries
- Keep expected output files synchronized with input changes
- Follow PostgreSQL regression test format conventions
- Support both RisingWave and PostgreSQL modes via `--mode` flag
- Clean output directory before test runs
- Handle psql process lifecycle correctly
- Document schedule file format in comments
- Run `cargo fmt` before committing
- Update expected outputs when behavior intentionally changes

## 6. Forbidden Changes (Must Not)

- Modify expected outputs without justification
- Break PostgreSQL compatibility in schedule format
- Leave output files from failed tests without cleanup
- Remove existing test coverage from PostgreSQL suite
- Skip parallel schedule support
- Modify without running against both PostgreSQL and RisingWave

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run against RisingWave | `cargo run -p risingwave_regress_test -- --host 127.0.0.1 -p 4566 --input ./src/tests/regress/data --output ./src/tests/regress/output --schedule ./src/tests/regress/data/schedule --mode risingwave` |
| Run against PostgreSQL | `cargo run -p risingwave_regress_test -- --host 127.0.0.1 -p 5432 --database postgres --input ./src/tests/regress/data --output ./src/tests/regress/output --schedule ./src/tests/regress/data/schedule --mode postgres` |
| Build check | `cargo check -p risingwave_regress_test` |

## 8. Dependencies & Contracts

- Requires `psql` binary in PATH
- Uses PostgreSQL wire protocol for both databases
- Schedule format compatible with PostgreSQL parallel_schedule
- Test cases derived from PostgreSQL regression suite
- Output comparison ignores order for queries without ORDER BY

## 9. Overrides

None. Follows parent test policies from `src/tests/` hierarchy.

## 10. Update Triggers

Regenerate this file when:
- New test schedule format features added
- PostgreSQL regression test suite updated
- psql integration changes
- Output comparison logic modified
- New test categories added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `./AGENTS.md`

# AGENTS.md - PostgreSQL Regression Test SQL Files

## 1. Scope

Policies for `src/tests/regress/data/sql` - PostgreSQL-compatible SQL test input files for RisingWave's regression testing framework.

## 2. Purpose

This directory contains SQL test case inputs adapted from PostgreSQL's regression suite:

- **SQL compatibility**: Tests PostgreSQL syntax and semantic compatibility
- **Comprehensive coverage**: 150+ SQL files covering all SQL feature areas
- **Standard validation**: Based on PostgreSQL's own regression tests
- **Parallel execution**: Tests are organized for parallel test execution
- **Cross-database testing**: Can run against both PostgreSQL and RisingWave

These tests ensure RisingWave maintains compatibility with PostgreSQL where applicable.

## 3. Structure

```
src/tests/regress/data/sql/
├── aggregates.sql            # Aggregate functions (COUNT, SUM, AVG, etc.)
├── join.sql                  # JOIN operations
├── select.sql                # SELECT queries
├── insert.sql                # INSERT statements
├── update.sql                # UPDATE statements
├── delete.sql                # DELETE statements
├── create_table.sql          # CREATE TABLE
├── alter_table.sql           # ALTER TABLE
├── indexes.sql               # Index creation and usage
├── transactions.sql          # Transaction control
├── types.sql                 # Data types
├── arrays.sql                # Array operations
├── jsonb.sql                 # JSONB operations
├── window.sql                # Window functions
├── with.sql                  # CTEs (WITH clauses)
├── text.sql                  # Text/string functions
├── numeric.sql               # Numeric operations
├── timestamp.sql             # Date/time functions
├── copy.sql                  # COPY command
└── *.sql                     # 150+ test files
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `aggregates.sql` | Aggregate function testing |
| `join.sql` | JOIN types and behavior |
| `select.sql` | SELECT statement variations |
| `arrays.sql` | Array constructors and operations |
| `jsonb.sql` | JSONB type and functions |
| `window.sql` | Window functions and frames |
| `types.sql` | Data type casting and literals |
| `indexes.sql` | Index creation and query plans |
| `transactions.sql` | BEGIN, COMMIT, ROLLBACK |

## 5. Edit Rules (Must)

- Preserve PostgreSQL test file structure and naming conventions
- Use `--@ ` prefix to mark ignored/disabled test queries
- Document RisingWave-specific differences in comments
- Keep SQL syntax compatible with both PostgreSQL and RisingWave
- Follow existing test organization by feature area
- Update corresponding expected output files when modifying tests
- Run against both PostgreSQL and RisingWave to verify behavior
- Preserve COPYRIGHT attribution from PostgreSQL

## 6. Forbidden Changes (Must Not)

- Remove PostgreSQL COPYRIGHT or license attribution
- Delete test files without justification
- Break PostgreSQL SQL compatibility without documentation
- Remove existing test coverage
- Skip parallel schedule support
- Modify without testing against both databases

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run against RisingWave | `cargo run -p risingwave_regress_test -- --host 127.0.0.1 -p 4566 --input ./src/tests/regress/data --output ./src/tests/regress/output --schedule ./src/tests/regress/data/schedule --mode risingwave` |
| Run against PostgreSQL | `cargo run -p risingwave_regress_test -- --host 127.0.0.1 -p 5432 --database postgres --input ./src/tests/regress/data --output ./src/tests/regress/output --schedule ./src/tests/regress/data/schedule --mode postgres` |

## 8. Dependencies & Contracts

- Test runner: `risingwave_regress_test` crate
- Schedule: `../schedule` file defines test order and parallel groups
- Expected outputs: `../expected/*.out` for comparison
- PostgreSQL license: Tests derived from PostgreSQL regression suite
- psql: Requires `psql` binary for test execution

## 9. Overrides

None. Inherits all rules from parent `/home/k11/risingwave/src/tests/regress/data/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- PostgreSQL regression suite updated
- New test categories added
- SQL file format conventions changed

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/tests/regress/data/AGENTS.md
- License: Portions derived from PostgreSQL (PostgreSQL License)

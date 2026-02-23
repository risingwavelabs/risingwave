# AGENTS.md - PostgreSQL Regression Expected Outputs

## 1. Scope

Policies for `src/tests/regress/data/expected` - expected output files for PostgreSQL-compatible regression testing.

## 2. Purpose

This directory contains expected output files for comparison during regression testing:

- **Output validation**: Expected query results and error messages
- **PostgreSQL-derived**: Based on PostgreSQL regression test outputs
- **Cross-database comparison**: Used for both PostgreSQL and RisingWave validation
- **Version controlled**: Changes to expected outputs are tracked in git
- **CI verification**: Tests fail when actual output differs from expected

Expected outputs ensure consistent behavior and detect regressions.

## 3. Structure

```
src/tests/regress/data/expected/
├── aggregates.out            # Expected aggregate function outputs
├── join.out                  # Expected JOIN outputs
├── select.out                # Expected SELECT outputs
├── arrays.out                # Expected array operation outputs
├── jsonb.out                 # Expected JSONB outputs
├── window.out                # Expected window function outputs
├── *.out                     # One file per SQL test file
└── (corresponds to ../sql/*.sql)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `*.out` | Expected output for corresponding SQL test file |
| Format | Text format matching psql output format |

## 5. Edit Rules (Must)

- Keep expected outputs synchronized with SQL input changes
- Update when RisingWave behavior intentionally changes
- Document intentional differences from PostgreSQL in comments
- Follow psql output format conventions
- Review diffs carefully before committing changes
- Run tests against both PostgreSQL and RisingWave

## 6. Forbidden Changes (Must Not)

- Delete expected output files without removing SQL inputs
- Modify outputs to mask actual behavioral differences
- Break PostgreSQL output format compatibility
- Skip output validation
- Ignore unexpected output changes without investigation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run regression tests | `cargo run -p risingwave_regress_test -- --host 127.0.0.1 -p 4566 --input ./src/tests/regress/data --output ./src/tests/regress/output --schedule ./src/tests/regress/data/schedule --mode risingwave` |
| Compare outputs | Automatic via test runner (compares actual vs expected) |

## 8. Dependencies & Contracts

- Corresponds to: `../sql/*.sql` files
- Format: psql output format (rows, headers, separators)
- Comparison: Line-by-line diff (order ignored for queries without ORDER BY)
- Runner: `risingwave_regress_test` crate

## 9. Overrides

None. Inherits all rules from parent `/home/k11/risingwave/src/tests/regress/data/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- Output format changes
- Comparison logic modified
- PostgreSQL test suite updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/tests/regress/data/AGENTS.md

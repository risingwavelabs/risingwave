# AGENTS.md - SQL Parser Test Data

## 1. Scope

Policies for `src/sqlparser/tests/testdata` - YAML-based test data for RisingWave's SQL parser validation, covering syntax parsing, AST generation, and error handling.

## 2. Purpose

This directory contains YAML test cases for comprehensive parser testing:

- **Syntax validation**: Tests SQL parsing for all supported constructs
- **AST verification**: Validates abstract syntax tree structure
- **Round-trip testing**: Ensures parse -> format -> parse produces identical results
- **Error testing**: Validates error messages for invalid SQL
- **PostgreSQL compatibility**: Tests Postgres-specific syntax extensions
- **Golden-file testing**: YAML files serve as both input and expected output

The test data ensures the SQL parser correctly handles all supported SQL syntax.

## 3. Structure

```
src/sqlparser/tests/testdata/
├── create.yaml               # CREATE statements (TABLE, SOURCE, DATABASE, etc.)
├── alter.yaml                # ALTER statements
├── drop.yaml                 # DROP statements
├── select.yaml               # SELECT queries
├── insert.yaml               # INSERT statements
├── copy.yaml                 # COPY statements
├── array.yaml                # Array syntax
├── operator.yaml             # Operators and expressions
├── precedence.yaml           # Operator precedence
├── lambda.yaml               # Lambda expressions
├── window.yaml               # Window functions
├── cte.yaml                  # Common table expressions
├── join.yaml                 # JOIN syntax
├── agg.yaml                  # Aggregate functions
└── *.yaml                    # Additional test categories
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `create.yaml` | CREATE TABLE, CREATE SOURCE, CREATE DATABASE, CREATE MATERIALIZED VIEW |
| `alter.yaml` | ALTER TABLE, ALTER SOURCE, ALTER SYSTEM |
| `select.yaml` | SELECT queries, subqueries, set operations |
| `array.yaml` | Array constructors, array access, array functions |
| `operator.yaml` | Arithmetic, comparison, logical operators |
| `precedence.yaml` | Operator precedence and associativity |
| `lambda.yaml` | Lambda expressions for higher-order functions |
| `window.yaml` | OVER clause, window frames, window functions |
| `copy.yaml` | COPY FROM/TO syntax |

## 5. Edit Rules (Must)

- Update test expectations with `./risedev update-parser-test` (not manually)
- Use `input` field for SQL to parse
- Use `formatted_sql` for expected SQL output after formatting
- Use `formatted_ast` for expected AST debug representation
- Use `error_msg` for expected error messages (for invalid SQL)
- Each test case is a YAML list item with `-`
- Ensure deterministic output (sort when necessary)
- Test both success and error cases
- Follow existing file organization by SQL statement type
- Run `cargo test --test parser_test -p risingwave_sqlparser` after changes

## 6. Forbidden Changes (Must Not)

- Modify YAML files manually (always use `update-parser-test`)
- Remove existing parser test coverage
- Add non-deterministic test cases
- Skip error case validation
- Break YAML test file format compatibility
- Remove `formatted_sql` or `formatted_ast` validation without justification
- Use absolute paths in test cases

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Parser tests | `cargo test --test parser_test -p risingwave_sqlparser` |
| Update expectations | `./risedev update-parser-test` |
| Specific file | `cargo test --test parser_test -p risingwave_sqlparser -- <file_stem>` |
| All parser tests | `cargo test -p risingwave_sqlparser` |

## 8. Dependencies & Contracts

- Test runner: `parser_test.rs` using `libtest-mimic`
- YAML format: List of test cases with `input`, `formatted_sql`, `formatted_ast`, `error_msg`
- Update mechanism: `UPDATE_PARSER_TEST=1` environment variable
- Parser under test: `risingwave_sqlparser`
- Comparison: Exact string matching

## 9. Overrides

None. Follows parent AGENTS.md at `/home/k11/risingwave/src/sqlparser/tests/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- YAML test case format changes
- New test categories added
- Parser test runner modified
- Test data organization changed

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/sqlparser/tests/AGENTS.md

# AGENTS.md - SQL Parser Tests

## 1. Scope

Policies for `/home/k11/risingwave/src/sqlparser/tests` - comprehensive test suite for the RisingWave SQL parser.

## 2. Purpose

The SQL parser tests validate:
- SQL syntax parsing correctness
- Abstract Syntax Tree (AST) structure validation
- SQL round-trip parsing (parse -> format -> parse)
- Error message quality and accuracy
- PostgreSQL-specific syntax support
- Edge cases and boundary conditions

Tests use YAML-based golden files for maintainability and use libtest-mimic for integration with Cargo's test framework.

## 3. Structure

```
src/sqlparser/tests/
├── parser_test.rs            # YAML-based test runner (libtest-mimic)
├── sqlparser_common.rs       # Common SQL parsing tests
├── sqlparser_postgres.rs     # PostgreSQL-specific tests
├── test_utils/               # Test utility modules
└── testdata/                 # YAML test case files
    ├── alter.yaml
    ├── array.yaml
    ├── create.yaml
    └── *.yaml                # 20+ test categories
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `parser_test.rs` | Test harness for YAML-based golden file tests |
| `sqlparser_common.rs` | Common SQL parsing tests (SELECT, INSERT, DDL) |
| `sqlparser_postgres.rs` | PostgreSQL-specific syntax tests |
| `testdata/*.yaml` | YAML test cases with input and expected output |
| `test_utils/` | Shared test utilities and helpers |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Add new test cases in YAML format to `testdata/`
- Update parser test expectations with `./risedev update-parser-test`
- Use `TestCase` format: `input`, `formatted_sql`, `formatted_ast`, `error_msg`
- Ensure deterministic test output (sort when necessary)
- Test both success and error cases
- Validate round-trip parsing (SQL -> AST -> SQL)
- Pass `./risedev c` (clippy) before submitting changes
- Document complex test cases with comments

## 6. Forbidden Changes (Must Not)

- Modify testdata YAML files manually (use `update-parser-test`)
- Remove existing parser test coverage
- Add non-deterministic test cases
- Skip error case validation
- Break YAML test file format compatibility
- Remove `formatted_sql` or `formatted_ast` validation without justification
- Use absolute paths in test files

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| YAML parser tests | `cargo test --test parser_test -p risingwave_sqlparser` |
| Update expectations | `./risedev update-parser-test` |
| Common tests | `cargo test --test sqlparser_common -p risingwave_sqlparser` |
| Postgres tests | `cargo test --test sqlparser_postgres -p risingwave_sqlparser` |
| All parser tests | `cargo test -p risingwave_sqlparser` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- `libtest-mimic`: Dynamic test discovery and execution
- `serde_yaml`: YAML test case serialization
- `expect_test`: Snapshot testing support
- `console`: Styled output for test diffs
- `risingwave_sqlparser`: Parser under test
- Test case format: `input` (required), `formatted_sql`, `formatted_ast`, `error_msg`
- Environment variable: `UPDATE_PARSER_TEST=1` to update YAML files

## 9. Overrides

None. Inherits all rules from parent `/home/k11/risingwave/src/sqlparser/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- Test runner implementation changes (parser_test.rs)
- Test data format or location changes
- New test categories added
- YAML schema for test cases modified
- libtest-mimic configuration updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/sqlparser/AGENTS.md

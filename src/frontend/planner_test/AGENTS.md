# AGENTS.md - RisingWave Planner Test

## 1. Scope

Policies for `./src/frontend/planner_test` - the golden-file testing crate for RisingWave's SQL binder, planner, and optimizer. This crate provides data-driven testing infrastructure that validates query plans through YAML-based test cases.

## 2. Purpose

The planner_test crate is a standalone testing framework that:
- Executes SQL test cases defined in YAML format
- Validates binder behavior (parsing, name resolution, catalog lookup)
- Validates logical plan generation from bound AST
- Validates optimized plans for both batch and streaming queries
- Validates physical execution plans and distributed stream graphs
- Produces deterministic golden-file outputs for regression testing

## 3. Structure

```
src/frontend/planner_test/
├── Cargo.toml                    # Crate manifest (risingwave_planner_test)
├── planner_test.toml             # RiseDev task definitions
├── README.md                     # Test authoring guide
├── src/
│   ├── lib.rs                    # Test framework core (TestCase, TestType, runner)
│   └── resolve_id.rs             # Test case ID resolution utilities
└── tests/
    ├── planner_test_runner.rs    # Test harness entry point (libtest-mimic)
    └── testdata/
        ├── input/*.yaml          # Test input files (hand-written)
        └── output/*.plan         # Expected plan outputs (auto-generated)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Core test framework with `TestCase`, `TestType` enums, and execution logic |
| `src/resolve_id.rs` | Test case ID resolution for `before` dependencies |
| `tests/planner_test_runner.rs` | Test harness using `libtest-mimic` for dynamic test discovery |
| `tests/testdata/input/*.yaml` | Test input files containing SQL and expected_outputs |
| `tests/testdata/output/*.plan` | Golden-file outputs (auto-generated, do not edit manually) |
| `planner_test.toml` | RiseDev task definitions for running and updating tests |
| `Cargo.toml` | Crate manifest with dependencies on frontend and sqlparser |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Add new test cases in `tests/testdata/input/*.yaml` for new SQL features
- Use `TestType` variants to specify expected outputs (`logical_plan`, `batch_plan`, `stream_plan`, etc.)
- Reference test cases by ID in `before` field when dependencies are needed
- Use `create_source` or `create_table_with_connector` for connector-based tests
- Update planner tests with `./risedev do-apply-planner-test` when expected outputs change
- Follow existing test case patterns in YAML files
- Ensure test SQL is deterministic (avoid non-deterministic functions in plan-sensitive tests)

## 6. Forbidden Changes (Must Not)

- Modify output files in `tests/testdata/output/` manually (auto-generated via `do-apply-planner-test`)
- Delete input files without removing corresponding output files
- Add test cases with non-deterministic output (unstable plans, timestamps, memory addresses)
- Break test determinism (plans must be stable across test runs)
- Modify `TestType` enum without updating `TestCaseResult` and `check_result` logic
- Use `unsafe` blocks without explicit safety justification
- Skip the `check_result` validation in test runs
- Rename existing test case IDs without updating all `before` references

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All planner tests | `./risedev run-planner-test` |
| Single test file | `./risedev run-planner-test <name>` (e.g., `./risedev run-planner-test tpch`) |
| Apply test outputs | `./risedev do-apply-planner-test` (or `./risedev dapt`) |
| Unit tests | `cargo test -p risingwave_planner_test` |
| Direct test runner | `cargo test -p risingwave_planner_test --test planner_test_runner -- [test_name]` |

## 8. Dependencies & Contracts

- Rust edition 2021
- Depends on: `risingwave_frontend`, `risingwave_sqlparser`, `risingwave_expr_impl`
- Test harness: `libtest-mimic` for dynamic test discovery
- Serialization: `serde_yaml` for test case parsing
- Expect testing: `expect-test` for golden file comparison
- Async runtime: `tokio` (madsim-tokio) for test execution
- File walking: `walkdir` for input file discovery
- Output format: YAML inputs generate `.plan` output files
- Test case schema: `sql`, `expected_outputs`, optional `before`, `id`, `name`, `create_source`, `with_config_map`

## 9. Overrides

None. Inherits all rules from parent `./src/frontend/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New `TestType` variant added for output validation
- Test data directory structure changes (input/output paths)
- New test configuration options added to `TestCase`
- RiseDev task definitions modified in `planner_test.toml`
- Test runner harness (`planner_test_runner.rs`) significantly refactored
- Dependencies related to test framework change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/frontend/AGENTS.md

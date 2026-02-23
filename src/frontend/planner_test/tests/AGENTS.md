# AGENTS.md - Planner Test Runner

## 1. Scope

Policies for `./src/frontend/planner_test/tests` - the test harness and test data for RisingWave's planner golden-file testing framework.

## 2. Purpose

The planner test runner executes YAML-based test cases that validate SQL query planning:
- Discovers test cases dynamically from `testdata/input/*.yaml`
- Executes binder, planner, and optimizer validation
- Compares actual outputs against golden files in `testdata/output/`
- Uses libtest-mimic for integration with Cargo's test runner
- Supports parallel test execution

This directory contains the test harness entry point and all test data files.

## 3. Structure

```
src/frontend/planner_test/tests/
├── planner_test_runner.rs    # Test harness using libtest-mimic
├── testdata/
│   ├── input/                # YAML test input files (hand-written)
│   │   ├── agg.yaml          # Aggregation tests
│   │   ├── join.yaml         # Join planning tests
│   │   ├── tpch.yaml         # TPC-H benchmark tests
│   │   ├── nexmark.yaml      # Nexmark streaming tests
│   │   └── *.yaml            # 100+ test categories
│   └── output/               # Golden file outputs (auto-generated)
│       └── *.plan            # Expected plan outputs
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `planner_test_runner.rs` | Test harness entry point using `libtest-mimic` for dynamic test discovery |
| `testdata/input/*.yaml` | Test input files containing SQL and expected output specifications |
| `testdata/output/*.plan` | Golden-file outputs (auto-generated via `do-apply-planner-test`) |
| `testdata/input/tpch.yaml` | TPC-H benchmark query tests |
| `testdata/input/nexmark.yaml` | Nexmark streaming query tests |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Add new test cases in `testdata/input/*.yaml` using the TestCase format
- Use unique IDs for test cases that may be referenced by `before` field
- Follow existing YAML structure: `sql`, `expected_outputs`, optional `before`, `id`, `name`
- Update golden files with `./risedev do-apply-planner-test` when expected outputs change
- Ensure test SQL is deterministic (avoid non-deterministic functions)
- Reference existing test cases by ID in `before` for dependency ordering
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Modify output files in `testdata/output/` manually (auto-generated only)
- Delete input files without removing corresponding output files
- Add test cases with non-deterministic output
- Break test determinism (plans must be stable across test runs)
- Rename existing test case IDs without updating all `before` references
- Add tests that depend on external services or network
- Skip the output validation in test runs
- Use absolute paths in test files

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run all planner tests | `./risedev run-planner-test` |
| Single test file | `./risedev run-planner-test <name>` (e.g., `tpch`, `nexmark`) |
| Apply test outputs | `./risedev do-apply-planner-test` or `./risedev dapt` |
| Direct test runner | `cargo test -p risingwave_planner_test --test planner_test_runner` |
| Specific test case | `cargo test -p risingwave_planner_test --test planner_test_runner -- <test_name>` |

## 8. Dependencies & Contracts

- Test harness: `libtest-mimic` for dynamic test discovery and execution
- File walking: `walkdir` for input file discovery
- Async runtime: `tokio` for test execution
- Error handling: `thiserror_ext::AsReport` for error formatting
- Test case format: YAML with `sql`, `expected_outputs`, optional fields
- Output format: `.plan` files with golden-file comparison
- Input directory: `tests/testdata/input/` (relative to crate root)
- Output directory: `tests/testdata/output/` (relative to crate root)

## 9. Overrides

None. Inherits all rules from parent `./src/frontend/planner_test/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- Test harness implementation changes (planner_test_runner.rs)
- Test data directory structure changes
- New test input/output format introduced
- libtest-mimic configuration modified
- Test discovery logic updated
- New test category patterns added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/frontend/planner_test/AGENTS.md

# AGENTS.md - Planner Test Data

## 1. Scope

Policies for `src/frontend/planner_test/tests/testdata` - golden-file test data for RisingWave's SQL planner, optimizer, and distributed execution planning validation.

## 2. Purpose

This directory contains YAML-based test cases for validating SQL query planning:

- **Input files**: Hand-written YAML test cases defining SQL queries and expected outputs
- **Output files**: Auto-generated `.plan` files with expected query plans
- **Comprehensive coverage**: 100+ test categories covering binder, planner, optimizer, and stream fragmenter
- **Golden-file testing**: Detects plan changes through version-controlled expected outputs
- **CI validation**: Ensures query plans remain stable across code changes

The test data validates that RisingWave generates correct and optimal execution plans for SQL queries.

## 3. Structure

```
src/frontend/planner_test/tests/testdata/
├── input/                    # Hand-written YAML test cases
│   ├── basic_query.yaml      # Basic SELECT, VALUES queries
│   ├── join.yaml             # JOIN planning and reordering
│   ├── agg.yaml              # Aggregation planning
│   ├── tpch.yaml             # TPC-H benchmark queries
│   ├── nexmark.yaml          # Nexmark streaming queries
│   ├── subquery.yaml         # Subquery decorrelation
│   ├── stream_*.yaml         # Streaming-specific tests
│   ├── batch_*.yaml          # Batch-specific tests
│   └── *.yaml                # 100+ test categories
└── output/                   # Auto-generated expected outputs
    └── *.plan                # Golden files (DO NOT EDIT MANUALLY)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `input/basic_query.yaml` | Basic SQL queries (SELECT, VALUES, filters) |
| `input/join.yaml` | JOIN planning, reordering, and optimization |
| `input/agg.yaml` | Aggregation planning (hash agg, simple agg) |
| `input/tpch.yaml` | TPC-H benchmark query plans |
| `input/nexmark.yaml` | Nexmark streaming benchmark queries |
| `input/subquery.yaml` | Subquery decorrelation and optimization |
| `input/stream_*.yaml` | Streaming plan tests (watermarks, append-only) |
| `input/batch_*.yaml` | Batch plan tests (distributed execution) |
| `output/*.plan` | Auto-generated expected outputs |

## 5. Edit Rules (Must)

- Add new test cases in `input/*.yaml` files using the TestCase format
- Use unique `id` field for test cases that may be referenced by `before`
- Specify `expected_outputs` to validate: `binder_error`, `logical_plan`, `batch_plan`, `stream_plan`, etc.
- Use `before` field to reference prerequisite test cases by ID
- Provide descriptive `name` for complex test cases
- Ensure test SQL is deterministic (avoid non-deterministic functions in plan-sensitive tests)
- Update golden files with `./risedev do-apply-planner-test` when expected outputs change
- Follow existing YAML structure: `sql`, `expected_outputs`, optional `before`, `id`, `name`
- Run `./risedev run-planner-test` to validate new test cases

## 6. Forbidden Changes (Must Not)

- Modify output files in `output/` manually (auto-generated only via `do-apply-planner-test`)
- Delete input files without removing corresponding output files
- Add test cases with non-deterministic output (timestamps, memory addresses)
- Break test determinism (plans must be stable across test runs)
- Rename existing test case IDs without updating all `before` references
- Add tests that depend on external services or network
- Use absolute paths in test files
- Skip output validation in test runs

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run all planner tests | `./risedev run-planner-test` |
| Single test file | `./risedev run-planner-test <name>` (e.g., `tpch`, `nexmark`) |
| Apply test outputs | `./risedev do-apply-planner-test` or `./risedev dapt` |
| Direct test runner | `cargo test -p risingwave_planner_test --test planner_test_runner` |
| Specific test case | `cargo test -p risingwave_planner_test --test planner_test_runner -- <test_name>` |

## 8. Dependencies & Contracts

- Test harness: `planner_test_runner.rs` using `libtest-mimic`
- Input format: YAML with `sql`, `expected_outputs`, optional metadata fields
- Output format: `.plan` files with deterministic plan output
- Test discovery: Walks `input/` directory for YAML files
- Comparison: Exact string match against golden files
- Update mechanism: `UPDATE_EXPECT=1` or `./risedev do-apply-planner-test`

## 9. Overrides

None. Follows parent AGENTS.md at `./src/frontend/planner_test/tests/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New test input format or fields added
- Output file format changes
- Test category organization modified
- Golden-file update mechanism changed
- New expected output types added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/frontend/planner_test/tests/AGENTS.md

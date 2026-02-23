# AGENTS.md - Planner Test Input Files

## 1. Scope

Policies for `src/frontend/planner_test/tests/testdata/input` - hand-written YAML test input files for RisingWave's planner golden-file testing framework.

## 2. Purpose

This directory contains YAML test case definitions that drive planner testing:

- **Test case definitions**: SQL queries with expected output specifications
- **Declarative testing**: YAML format separates test intent from implementation
- **Comprehensive coverage**: 100+ YAML files covering all SQL planning aspects
- **Benchmark queries**: TPC-H, Nexmark, and CH-benCHmark workloads
- **Feature coverage**: Joins, aggregations, subqueries, window functions, streaming operators

Input files define what to test; the test runner validates against actual planner output.

## 3. Structure

```
src/frontend/planner_test/tests/testdata/input/
├── basic_query.yaml          # Basic SELECT, VALUES, simple filters
├── join.yaml                 # INNER, LEFT, RIGHT, FULL joins
├── agg.yaml                  # COUNT, SUM, AVG, GROUP BY
├── subquery.yaml             # Correlated and non-correlated subqueries
├── tpch.yaml                 # TPC-H benchmark (22 queries)
├── nexmark.yaml              # Nexmark streaming benchmark
├── ch_benchmark.yaml         # CH-benCHmark mixed workload
├── stream_*.yaml             # Streaming-specific features
├── batch_*.yaml              # Batch execution planning
├── over_window_function.yaml # Window functions
├── temporal_*.yaml           # Temporal filters and joins
├── create_*.yaml             # DDL statement planning
├── sink.yaml                 # Sink planning
├── source.yaml               # Source planning
└── *.yaml                    # 100+ additional categories
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `basic_query.yaml` | Fundamentals: SELECT, WHERE, VALUES, expressions |
| `join.yaml` | Join types, join reordering, join algorithms |
| `agg.yaml` | Aggregation planning, group by, distinct aggregates |
| `tpch.yaml` | Industry-standard TPC-H benchmark queries |
| `nexmark.yaml` | Streaming benchmark (bids, auctions, persons) |
| `subquery.yaml` | Subquery decorrelation and unnesting |
| `stream_dist_agg.yaml` | Distributed streaming aggregation |
| `batch_dist_agg.yaml` | Distributed batch aggregation |
| `over_window_function.yaml` | ROW_NUMBER, RANK, LAG, LEAD |
| `temporal_filter.yaml` | Event time filtering with watermarks |

## 5. Edit Rules (Must)

- Use YAML format with top-level list of test cases
- Each test case must have `sql` field with the query to test
- Use `expected_outputs` list to specify what outputs to validate
- Valid output types: `binder_error`, `logical_plan`, `batch_plan`, `stream_plan`, `distributed_plan`, `stream_dist_plan`, `create_source`, `create_source_plan`
- Use `name` field for descriptive test case names (especially for complex queries)
- Use `id` field when test cases need to be referenced by `before`
- Use `before` field to specify prerequisite test case IDs
- Use `create_source` for tests requiring connector sources
- Use `with_config_map` for session configuration tests
- Follow existing file organization by feature area
- Run `./risedev run-planner-test <filename>` to validate changes

## 6. Forbidden Changes (Must Not)

- Use non-deterministic SQL (random(), now() without mocks)
- Skip `expected_outputs` specification
- Reference non-existent test IDs in `before` fields
- Create circular dependencies with `before` references
- Duplicate test cases across files without justification
- Remove existing test coverage for supported features
- Use absolute paths in SQL statements
- Include sensitive data in test cases

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All input tests | `./risedev run-planner-test` |
| Single file | `./risedev run-planner-test <name>` (e.g., `join`, `tpch`) |
| Specific test case | `cargo test -p risingwave_planner_test --test planner_test_runner -- <test_name>` |
| Validate inputs | `cargo test -p risingwave_planner_test --test planner_test_runner` |

## 8. Dependencies & Contracts

- Test runner: `planner_test_runner.rs` with `libtest-mimic`
- YAML parser: `serde_yaml` for test case deserialization
- Output generation: RisingWave frontend (binder, planner, optimizer)
- Golden files: Corresponding `.plan` files in `../output/`
- Test case schema: Defined in `risingwave_planner_test::TestCase`

## 9. Overrides

None. Inherits all rules from parent `/home/k11/risingwave/src/frontend/planner_test/tests/testdata/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New test case fields added
- YAML schema changes
- New test categories created
- File organization modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/frontend/planner_test/tests/testdata/AGENTS.md

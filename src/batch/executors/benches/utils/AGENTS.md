# AGENTS.md - Batch Benchmark Utilities

## 1. Scope

Policies for the `src/batch/executors/benches/utils` directory, containing shared utilities for batch executor micro-benchmarks.

## 2. Purpose

This directory provides common infrastructure for batch executor benchmarks:
- **Executor execution**: `execute_executor()` for running benchmark iterations
- **Input creation**: `create_input()` for mock data generation
- **Join benchmarking**: `bench_join()` for parameterized join benchmarks
- **Batch configuration**: Standard chunk sizes and iteration counts

The utilities ensure consistent benchmarking methodology across all batch operator benchmarks.

## 3. Structure

```
src/batch/executors/benches/utils/
└── mod.rs                    # Shared benchmark utilities
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `execute_executor()`, `create_input()`, `bench_join()` helpers |

## 5. Edit Rules (Must)

- Use `std::hint::black_box()` to prevent compiler optimizations
- Create executors with `BatchSize::SmallInput` for accurate measurements
- Use `tokio::runtime::Runtime` for async benchmark execution
- Test multiple chunk sizes (32, 128, 512, 1024, etc.)
- Use `BenchmarkId::new()` with descriptive parameter names
- Generate consistent mock data via `MockExecutor` and `gen_data()`
- Ensure benchmark reproducibility with fixed seeds where applicable
- Document benchmark scenarios and expected behavior

## 6. Forbidden Changes (Must Not)

- Remove `black_box()` wrapping on results
- Change `BatchSize::SmallInput` without statistical analysis
- Use synchronous execution without Tokio runtime
- Modify standard chunk size ranges without review
- Skip `MockExecutor` for real storage dependencies
- Use non-deterministic test data generation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All benchmarks | `cargo bench -p risingwave_batch_executors` |
| Specific benchmark | `cargo bench -p risingwave_batch_executors -- <filter>` |
| Hash join bench | `cargo bench -p risingwave_batch_executors -- HashJoin` |
| Hash agg bench | `cargo bench -p risingwave_batch_executors -- HashAgg` |

## 8. Dependencies & Contracts

- **Parent module**: `batch/executors/benches/` provides Criterion harness
- **Batch executors**: `risingwave_batch_executors` for operator implementations
- **Test utils**: `risingwave_batch_executors::test_utils` for mocks
- **Common types**: `risingwave_common::catalog` for schema definitions
- **Criterion**: Benchmark harness with async support

## 9. Overrides

None. Follows parent AGENTS.md at `./src/batch/executors/benches/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New benchmark utility added
- Mock data generation changed
- Standard benchmark parameters modified
- Criterion configuration updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/batch/executors/benches/AGENTS.md

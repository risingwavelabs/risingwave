# AGENTS.md - Batch Executors Benchmarks

## 1. Scope

Policies specific to `/home/k11/risingwave/src/batch/executors/benches`. This directory contains Criterion micro-benchmarks for batch executor operators, measuring performance of core query execution primitives.

## 2. Purpose

The benchmarks directory provides performance regression testing for batch executors. Each benchmark file targets a specific operator (filter, hash aggregation, hash join, sort, top-n, etc.) with varying data sizes and configurations. Benchmarks use the Criterion framework with async Tokio runtime to measure throughput under realistic execution conditions.

## 3. Structure

```
src/batch/executors/benches/
├── README.md              # Benchmark running and authoring guide
├── utils/
│   └── mod.rs             # Shared utilities: execute_executor, create_input, bench_join
├── filter.rs              # Filter operator benchmark
├── hash_agg.rs            # Hash aggregation benchmark (multiple agg types)
├── hash_join.rs           # Hash join benchmark (all join types)
├── nested_loop_join.rs    # Nested loop join benchmark
├── sort.rs                # Sort/OrderBy benchmark (single/multi column)
├── top_n.rs               # TopN executor benchmark
├── expand.rs              # Expand executor benchmark
└── limit.rs               # Limit operator benchmark
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `utils/mod.rs` | `execute_executor()`, `create_input()`, `bench_join()` helpers |
| `filter.rs` | Filter operator with predicate evaluation benchmark |
| `hash_agg.rs` | Hash aggregation with Sum, Count, Min, StringAgg variants |
| `hash_join.rs` | Hash join with all join types (Inner, Outer, Semi, Anti) |
| `sort.rs` | External sort with single/multi-column ordering |
| `top_n.rs` | TopN with offset/limit and column order variations |

## 5. Edit Rules (Must)

- Declare `pub mod utils;` at the top of each benchmark file
- Call `enable_jemalloc!()` macro for consistent memory allocation metrics
- Use `criterion_group!` and `criterion_main!` macros for benchmark entry points
- Create executors with `BatchSize::SmallInput` in `iter_batched()` calls
- Use `tokio::runtime::Runtime::new().unwrap()` for async benchmark execution
- Test multiple chunk sizes: 32, 128, 512, 1024, 2048, 4096
- Use `BenchmarkId::new()` with descriptive parameter formatting
- Register new benchmarks in `../Cargo.toml` with `[[bench]]` section (`harness = false`)
- Generate input data via `utils::create_input()` or `MockExecutor` with `gen_data()`
- Use `std::hint::black_box()` on results to prevent compiler optimizations

## 6. Forbidden Changes (Must Not)

- Remove `enable_jemalloc!()` call - skews memory allocation measurements
- Change `BatchSize::SmallInput` without statistical justification
- Use synchronous execution without Tokio runtime in async benchmarks
- Skip chunk size variations - must cover 32 to 4096 range
- Remove black_box() wrapping on benchmark results
- Modify statistical thresholds (sample size, measurement time) without analysis
- Add benchmarks without corresponding `[[bench]]` entry in Cargo.toml
- Use real storage dependencies - benchmarks must use MockExecutor

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All benchmarks | `cargo bench -p risingwave_batch_executors` |
| Specific benchmark | `cargo bench -p risingwave_batch_executors -- <filter>` |
| Filter only | `cargo bench -p risingwave_batch_executors -- Filter` |
| HashAgg with params | `cargo bench -p risingwave_batch_executors -- "HashAggExecutor/1024"` |
| List benchmarks | `cargo bench -p risingwave_batch_executors -- --list` |

## 8. Dependencies & Contracts

- Depends on: `risingwave_batch_executors`, `risingwave_batch`, `risingwave_common`
- Uses Criterion with `async_tokio` feature for async benchmark support
- Uses `tikv-jemallocator` for stable memory allocation metrics
- Mock data generation via `MockExecutor` and `gen_data()` from `test_utils`
- Standard dataset size: 1,048,576 rows (1024 * 1024)
- All benchmarks must complete within default Criterion measurement time

## 9. Overrides

None. Inherits all rules from parent AGENTS.md.

## 10. Update Triggers

Regenerate this file when:
- New benchmark harness or utility added
- Criterion configuration or statistical parameters change
- New executor operator benchmark added
- Chunk size ranges or data volume standards change
- MockExecutor API changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `/home/k11/risingwave/src/batch/AGENTS.md`

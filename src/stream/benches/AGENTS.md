# AGENTS.md - Stream Engine Benchmarks

## 1. Scope

Performance benchmarks for the `risingwave_stream` crate. This directory contains Criterion-based benchmarks for measuring streaming operator performance, memory usage, and state table operations.

## 2. Purpose

Benchmarks measure critical performance characteristics of the stream engine:
- State table write throughput (inserts, chunk writes)
- Hash aggregation performance (Q17-style workloads)
- Hash join runtime and memory consumption (cache hit/miss scenarios)
- Memory profiling with dhat-heap for allocation analysis

## 3. Structure

```
benches/
├── bench_state_table.rs      # StateTable operation benchmarks
├── stream_hash_agg.rs        # Hash aggregation benchmark (Q17 pattern)
├── stream_hash_join_rt.rs    # Hash join runtime benchmark (Criterion)
├── stream_hash_join_mem.rs   # Hash join memory benchmark (dhat-heap)
└── stream_hash_join.py       # Full benchmark orchestration script
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `bench_state_table.rs` | Benchmarks StateTable::insert and write_chunk with varying column counts |
| `stream_hash_agg.rs` | Benchmarks HashAggExecutor with count/sum aggregations and filters |
| `stream_hash_join_rt.rs` | Runtime benchmarks for hash join with different amplification sizes |
| `stream_hash_join_mem.rs` | Memory profiling for hash join using dhat heap profiler |
| `stream_hash_join.py` | Python script to run full benchmark matrix and output JSON |

## 5. Edit Rules (Must)

- Use `criterion_group!` and `criterion_main!` macros for all benchmark entry points
- Set appropriate `sample_size` for benchmark stability (10-100 depending on duration)
- Use `BatchSize::SmallInput` for async benchmarks with setup overhead
- Use `std::hint::black_box` to prevent compiler optimization of benchmark results
- Include `risingwave_expr_impl::enable!()` for benchmarks using expressions
- Run benchmarks before submitting performance-critical changes: `cargo bench -p risingwave_stream`
- Document benchmark invocation commands in file headers

## 6. Forbidden Changes (Must Not)

- Remove `harness = false` from Cargo.toml bench definitions (breaks criterion)
- Use `cargo test` to run benchmarks (use `cargo bench` instead)
- Modify benchmark data generation without verifying statistical stability
- Disable `black_box` usage (allows compiler to optimize away benchmark)
- Change iteration counts without verifying results are statistically significant

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All benchmarks | `cargo bench -p risingwave_stream` |
| State table bench | `cargo bench -p risingwave_stream --bench bench_state_table` |
| Hash agg bench | `cargo bench -p risingwave_stream --bench stream_hash_agg` |
| Hash join runtime | `cargo bench -p risingwave_stream --bench stream_hash_join_rt` |
| Hash join memory | `ARGS=40000,NotInCache,Inner cargo bench -p risingwave_stream --features dhat-heap --bench stream_hash_join_mem` |
| Full benchmark suite | `python3 benches/stream_hash_join.py` |
| Generate flamegraph | `sudo cargo flamegraph --bench stream_hash_join_rt -- hash_join_rt_40000_InCache_Inner` |

## 8. Dependencies & Contracts

- **Benchmark framework**: criterion with async_tokio support
- **Memory profiling**: dhat (optional feature, requires global allocator configuration)
- **Async runtime**: tokio runtime created per benchmark for async executors
- **State store**: MemoryStateStore for isolated benchmark runs
- **Test utilities**: risingwave_stream test_utils for MockSource and executor setup
- **Expression framework**: risingwave_expr_impl for aggregation functions

## 9. Overrides

- Benchmarks may use `block_on` from futures-executor despite parent forbidding direct async patterns
- Python script uses subprocess shell=True (acceptable for local benchmark automation)

## 10. Update Triggers

Regenerate this file when:
- New benchmark target added to Cargo.toml [[bench]] section
- Benchmark framework changes (criterion version updates)
- New streaming executor type requiring performance benchmarks
- Memory profiling methodology changes
- State table interface changes affecting benchmark setup

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/AGENTS.md

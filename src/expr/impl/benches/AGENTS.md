# AGENTS.md - Expression Implementation Benchmarks

## 1. Scope

Policies for `./src/expr/impl/benches` - Criterion-based performance benchmarks for RisingWave expression and function implementations.

## 2. Purpose

The expression benchmark suite measures and tracks performance of:
- Scalar function evaluation across all data types
- Aggregate function computation performance
- Array/chunk processing throughput
- Expression evaluation pipelines
- Function dispatch overhead
- UDF execution performance

Benchmarks ensure performance regressions are caught early and provide data for optimization decisions.

## 3. Structure

```
src/expr/impl/benches/
├── expr.rs                   # Main benchmark definitions
└── AGENTS.md                 # This file
```

Benchmarks are organized by function category and data type, testing with representative chunk sizes (typically 1024 rows).

## 4. Key Files

| File | Purpose |
|------|---------|
| `expr.rs` | Criterion benchmark definitions for expression evaluation |
| `Cargo.toml` (parent) | Benchmark configuration in `[[bench]]` sections |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Use `criterion` for statistical benchmarking with adequate sample sizes
- Include benchmarks for all major data types (bool, i16, i32, i64, f32, f64, decimal, date, timestamp, interval, varchar, bytea)
- Use consistent chunk sizes (CHUNK_SIZE = 1024) for comparability
- Use `FuturesExecutor` for async expression benchmarks
- Document benchmark purpose in code comments
- Benchmark both `eval()` (chunk) and `eval_row()` (single row) where applicable
- Include edge cases (NULL handling, overflow, empty inputs)
- Pass `./risedev c` (clippy) before submitting changes
- Ensure benchmarks compile: `cargo check --benches -p risingwave_expr_impl`

## 6. Forbidden Changes (Must Not)

- Remove statistical rigor (minimum sample sizes, warm-up iterations)
- Benchmark only trivial expressions (must represent real SQL workloads)
- Hardcode paths or use file I/O in benchmark loops
- Skip CI benchmark compilation checks
- Use `#[bench]` (deprecated) instead of criterion
- Add blocking operations in async benchmarks
- Use non-deterministic data generation (breaks reproducibility)
- Ignore performance-critical code paths without benchmarks

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run all benchmarks | `cargo bench -p risingwave_expr_impl` |
| Check benches compile | `cargo check --benches -p risingwave_expr_impl` |
| Specific benchmark | `cargo bench -p risingwave_expr_impl -- <benchmark_name>` |
| With performance profile | `cargo bench -p risingwave_expr_impl -- --profile-time 10` |

## 8. Dependencies & Contracts

- `criterion`: Statistical benchmarking framework with HTML reports
- `risingwave_expr_impl`: Expression implementations under test
- `risingwave_common`: Array types and data types
- `risingwave_expr`: Expression trait definitions
- `FuturesExecutor`: Async runtime for expression evaluation
- `itertools`: Iterator utilities for data generation
- Benchmark output: `target/criterion/` with HTML reports
- Chunk size: Standard 1024 rows for batch processing benchmarks

## 9. Overrides

None. Inherits all rules from parent `./src/expr/impl/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New benchmark cases added
- Benchmark framework changes (criterion version)
- Expression API changes affecting benchmark code
- New function categories requiring benchmark coverage
- Chunk size conventions change
- New data types added to benchmark suite

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/expr/impl/AGENTS.md

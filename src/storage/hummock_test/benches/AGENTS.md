# AGENTS.md - Hummock Benchmarks

## 1. Scope

Policies for the `src/storage/hummock_test/benches` directory, covering Criterion-based performance benchmarks for the Hummock storage engine.

## 2. Purpose

The Hummock Benchmarks module provides performance testing for Hummock storage operations using the Criterion benchmark framework. It measures iterator performance, read throughput, and storage operation latency to identify optimization opportunities and prevent performance regressions.

## 3. Structure

```
src/storage/hummock_test/benches/
└── bench_hummock_iter.rs     # Hummock iterator performance benchmarks
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `bench_hummock_iter.rs` | Iterator traversal and read performance benchmarks |

## 5. Edit Rules (Must)

- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting
- Use Criterion's statistical analysis features
- Configure appropriate sample sizes for accuracy
- Document benchmark purpose and methodology
- Clean up test resources after benchmarks
- Use async runtime for storage operations

## 6. Forbidden Changes (Must Not)

- Add benchmarks without proper setup/cleanup
- Use hardcoded values that should be configurable
- Skip statistical validation in Criterion
- Leave test data in storage after benchmarks
- Benchmark without appropriate warm-up

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run benchmarks | `cargo bench -p risingwave_hummock_test` |
| Specific benchmark | `cargo bench -p risingwave_hummock_test bench-hummock-iter` |
| Benchmark with output | `cargo bench -p risingwave_hummock_test -- --verbose` |

## 8. Dependencies & Contracts

- Criterion benchmark framework
- Tokio runtime for async operations
- risingwave_hummock_test utilities
- risingwave_storage for HummockStorage
- risingwave_meta for MockHummockMetaClient

## 9. Overrides

Inherits from `./src/storage/hummock_test/AGENTS.md`:
- Override: Edit Rules - Criterion-specific requirements
- Override: Test Entry - Benchmark commands

## 10. Update Triggers

Regenerate this file when:
- New benchmark files are added
- Criterion configuration changes
- Benchmark methodology evolves
- New performance metrics are added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/storage/hummock_test/AGENTS.md

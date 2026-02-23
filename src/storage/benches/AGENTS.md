# AGENTS.md - RisingWave Storage Benchmarks

## 1. Scope

Policies for `./src/storage/benches` directory. This directory contains Criterion-based performance benchmarks for the RisingWave storage engine components.

## 2. Purpose

Storage benchmarks measure and track performance of critical storage operations. These benchmarks help:
- Detect performance regressions in hot paths
- Compare caching strategies (Moka, LRU, Foyer)
- Validate SSTable build/scan performance
- Measure compaction throughput
- Benchmark row encoding/decoding strategies
- Test merge iterator performance

## 3. Structure

```
src/storage/benches/
├── bench_block_cache.rs      # Block cache comparison (Moka, LRU, Foyer)
├── bench_block_iter.rs       # SSTable block iteration performance
├── bench_compactor.rs        # Compaction and SSTable build/scan
├── bench_compression.rs      # LZ4/Zstd compression benchmarks
├── bench_fs_operation.rs     # Filesystem operation performance
├── bench_imm_compact.rs      # In-memory immutable batch compaction
├── bench_merge_iter.rs       # Merge iterator performance
├── bench_multi_builder.rs    # Multi-table builder benchmarks
├── bench_row.rs              # Row encoding/decoding comparison
└── bench_table_watermarks.rs # Table watermark processing
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `bench_block_cache.rs` | Compare block cache implementations |
| `bench_compactor.rs` | SSTable build, scan, and compaction benchmarks |
| `bench_merge_iter.rs` | Merge iterator and watermark skip performance |
| `bench_row.rs` | Row encoding (memcmp, basic, column-aware) |
| `bench_compression.rs` | LZ4 vs Zstd compression benchmarks |
| `Cargo.toml` | Benchmark definitions in `[[bench]]` sections |

## 5. Edit Rules (Must)

- Run `cargo fmt` before committing benchmark code
- Pass `./risedev c` (clippy) before submitting changes
- Use Criterion's `criterion_group!` and `criterion_main!` macros
- Set `harness = false` in Cargo.toml for all benchmarks
- Document benchmark purpose in code comments
- Use consistent sample sizes for comparable results
- Run benchmarks before and after storage hot path changes

## 6. Forbidden Changes (Must Not)

- Remove or rename existing benchmarks without justification
- Change benchmark measurement methodology without version bump
- Commit benchmarks with `debug = true` left enabled in Cargo.toml
- Use non-deterministic data generation (always use fixed seeds)
- Remove historical benchmark baselines without migration path
- Disable benchmarks by commenting out without TODO/issue reference

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All storage benches | `cargo bench -p risingwave_storage` |
| Single benchmark | `cargo bench -p risingwave_storage --bench <name>` |
| Specific function | `cargo bench -p risingwave_storage -- <pattern>` |
| With profiling | Uncomment `debug = true` in Cargo.toml |

## 8. Dependencies & Contracts

- Criterion framework for measurement and reporting
- Criterion features: `async_futures`, `async_tokio`
- Benchmark harness: disabled (custom main)
- Dependencies: Same as risingwave_storage dev-dependencies
- Runtime: Tokio for async benchmarks
- Data generation: Fixed seeds for reproducibility

## 9. Overrides

None. Follows parent `./src/storage/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New benchmark file is added
- Benchmark framework version changes
- Criterion configuration options change
- Storage benchmark policy changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/storage/AGENTS.md

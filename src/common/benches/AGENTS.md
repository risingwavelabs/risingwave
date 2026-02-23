# AGENTS.md - Common Crate Benchmarks

## 1. Scope

Policies for the `src/common/benches` directory, containing performance benchmarks for core data structures and utilities in the common crate.

## 2. Purpose

This directory provides Criterion-based benchmarks for critical common crate components:
- **Encoding benchmarks**: Memcomparable and value encoding performance
- **Array benchmarks**: Data chunk operations and array processing
- **Data structure benchmarks**: Bitmap, LRU cache, sequencer performance
- **Hash key benchmarks**: Hash key encoding for distributed processing

These benchmarks guide optimization decisions and detect performance regressions in foundational data structures.

## 3. Structure

```
src/common/benches/
├── bench_encoding.rs              # Memcomparable/value encoding benchmarks
├── bench_column_aware_row_encoding.rs  # Column-aware row encoding
├── bench_array.rs                 # Array operation benchmarks
├── bench_data_chunk_compact.rs    # Data chunk compaction
├── bench_data_chunk_encoding.rs   # Data chunk serialization
├── bench_hash_key_encoding.rs     # Hash key encoding benchmarks
├── bench_lru.rs                   # LRU cache performance tests
├── bench_sequencer.rs             # Atomic sequence benchmarks
└── bitmap.rs                      # Bitmap operation benchmarks
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `bench_encoding.rs` | Scalar value encoding/decoding performance |
| `bench_column_aware_row_encoding.rs` | Row encoding with column awareness |
| `bench_array.rs` | Array builder and operation benchmarks |
| `bench_hash_key_encoding.rs` | Consistent hash key generation |
| `bench_lru.rs` | LRU cache insertion and eviction |
| `bitmap.rs` | Bitmap set/get/iteration performance |

## 5. Edit Rules (Must)

- Use Criterion for all benchmarks with proper statistical analysis
- Include `std::hint::black_box()` to prevent compiler optimizations
- Test with realistic data sizes and distributions
- Document benchmark scenarios and expected throughput
- Use environment variables for optional benchmark configuration
- Compare against baselines when optimizing
- Add new benchmarks when introducing hot-path data structures
- Run benchmarks on release profile for accurate results

## 6. Forbidden Changes (Must Not)

- Remove existing benchmarks without performance justification
- Skip `black_box()` usage on benchmark results
- Use debug profile for performance measurements
- Change statistical thresholds without analysis
- Remove baseline comparisons for optimized code paths
- Commit debug print statements in benchmark code

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All benchmarks | `cargo bench -p risingwave_common` |
| Encoding benchmark | `cargo bench -p risingwave_common --bench bench_encoding` |
| LRU benchmark | `cargo bench -p risingwave_common --bench bench_lru` |
| Specific benchmark | `cargo bench -p risingwave_common -- <pattern>` |

## 8. Dependencies & Contracts

- **Common crate**: `risingwave_common` for data structures
- **Criterion**: Benchmark harness with statistical analysis
- **Data types**: All RisingWave SQL types for encoding tests
- **Memory**: `EstimateSize` for size calculation benchmarks
- **Release profile**: Required for accurate performance measurements

## 9. Overrides

None. Follows parent AGENTS.md at `./src/common/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New benchmark added for common crate components
- Criterion configuration or statistical parameters changed
- New data structure requiring performance testing added
- Benchmark harness pattern changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/common/AGENTS.md

# AGENTS.md - Common Crate

## 1. Scope

Policies for the `common` crate and its sub-crates. This directory contains shared types, data structures, and utilities used across all RisingWave components (frontend, batch, stream, storage, meta).

**Policy Inheritance:**
This file inherits rules from `./AGENTS.md` (root).
When working in this directory:
1. Read THIS file first for directory-specific rules
2. Then read PARENT files up to root for inherited rules
3. Child rules override parent rules (nearest-wins)
4. If code changes conflict with these docs, update AGENTS.md to match reality

## 2. Purpose

The `common` crate provides foundational infrastructure for RisingWave:
- **Array types**: Columnar data representation (DataChunk, StreamChunk, primitive arrays)
- **Data types**: SQL type system (integers, decimals, timestamps, structs, lists, JSONB)
- **Memory management**: Allocation tracking, memory limits, heap profiling
- **Configuration**: Server, streaming, batch, storage, and session configs
- **Catalog**: Column definitions, schema, table metadata
- **Utilities**: Hashing, encoding, bitmaps, LRU cache, row representations

## 3. Structure

```
src/common/
├── Cargo.toml                    # Main common crate manifest
├── src/
│   ├── lib.rs                    # Crate root, re-exports
│   ├── array/                    # Columnar data structures
│   │   ├── data_chunk.rs         # Batch data representation
│   │   ├── stream_chunk.rs       # Streaming data with operations
│   │   └── arrow/                # Arrow format interoperability
│   ├── types/                    # SQL type system
│   │   ├── scalar_impl.rs        # Scalar value types
│   │   ├── decimal.rs            # Decimal arithmetic
│   │   └── interval.rs           # Interval/time types
│   ├── row/                      # Row representations (owned, borrowed)
│   ├── catalog/                  # Schema and column metadata
│   ├── hash/                     # Consistent hashing, key encoding
│   ├── config/                   # Configuration definitions
│   ├── memory/                   # Memory tracking and limits
│   ├── util/                     # Encoding, sorting, utilities
│   ├── bitmap.rs                 # Bitmap data structure
│   └── cache.rs                  # LRU cache implementation
├── common_service/               # Shared service utilities
├── estimate_size/                # Memory size estimation trait
├── fields-derive/                # Procedural macros for fields
├── heap_profiling/               # JVM-style heap profiling
├── log/                          # Logging utilities
├── metrics/                      # Prometheus metrics wrappers
├── proc_macro/                   # Common procedural macros
├── rate_limit/                   # Rate limiting utilities
├── secret/                       # Secret/redacted value handling
├── telemetry_event/              # Telemetry event collection
└── benches/                      # Benchmarks for arrays, encoding
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Crate root with module declarations |
| `src/array/mod.rs` | Array trait definitions and implementations |
| `src/types/mod.rs` | Data type system and conversions |
| `src/catalog/mod.rs` | Schema and column catalog types |
| `src/config/mod.rs` | Configuration struct definitions |
| `src/util/mencmp_encoding.rs` | Memcomparable serialization (storage key format) |
| `src/util/value_encoding/` | Value serialization format |
| `src/bitmap.rs` | RoaringBitmap-like bitmap implementation |
| `Cargo.toml` | Main crate dependencies |

## 5. Edit Rules (Must)

- **Maintain serialization compatibility**: Changes to `memcmp_encoding` or `value_encoding` must preserve backward compatibility
- **Update both array builder and array**: When modifying array types, update both `XxxArray` and `XxxArrayBuilder`
- **Add `EstimateSize` for new collections**: Implement memory size estimation for new data structures
- **Follow type safety patterns**: Use `ArrayError`, ` RwError` for error propagation
- **Run benchmarks when modifying hot paths**: Use `./risedev bench` for array/encoding changes
- **Add unit tests for new data types**: Test serialization, equality, and hash consistency
- **Document unsafe blocks**: Explain invariants for any `unsafe` usage
- **Use `risingwave_error` macros**: Prefer `bail!`, `ensure!` from error crate

## 6. Forbidden Changes (Must Not)

- **Break memcomparable encoding format**: This affects storage compatibility
- **Change `DataChunk` or `StreamChunk` layout** without thorough review
- **Remove or rename exported types** without deprecation cycle (used across workspace)
- **Modify `ScalarImpl` variants** without updating all match sites in the codebase
- **Add blocking operations** in async contexts (common is used in all async runtimes)
- **Use `std::sync::Mutex`** - prefer `parking_lot::Mutex` or `tokio::sync::Mutex`
- **Bypass feature flags** for optional dependencies (arrow versions, jemalloc)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_common` |
| Sub-crate tests | `cargo test -p risingwave_common_metrics` (etc.) |
| Specific module | `cargo test -p risingwave_common array::` |
| Benchmarks | `cargo bench -p risingwave_common --bench bench_encoding` |
| All common tests | `cargo test -p risingwave_common -p risingwave_common_metrics -p risingwave_common_log ...` |

## 8. Dependencies & Contracts

- **Arrow**: Dual-version support (arrow-54, arrow-56) for Iceberg/DeltaLake compatibility
- **Serialization**: `memcomparable` for keys, `serde` for RPC/config
- **Hashing**: `xxhash-rust` for consistent hashing across nodes
- **Allocation**: `jemalloc` profiling support via `tikv-jemalloc-ctl`
- **Procedural macros**: `risingwave-fields-derive`, `risingwave_common_proc_macro`
- **Re-exports**: `risingwave_error` macros, `risingwave_common_metrics` functions
- **Version constants**: `PG_VERSION`, `RW_VERSION`, `SERVER_VERSION_NUM`

## 9. Overrides

- **Nightly Rust features**: Extensive use of unstable features (see `lib.rs` `#![feature(...)]`)
- **Workspace-hack**: Uses `workspace-hack` crate for dependency unification

## 10. Update Triggers

Regenerate this file when:
- New sub-crate added to `src/common/`
- Array type system changes (new array variant)
- Configuration structure changes
- Serialization format changes
- New data type added to `ScalarImpl`
- Memory management infrastructure changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

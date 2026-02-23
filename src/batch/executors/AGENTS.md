# AGENTS.md - Batch Executors Crate

## 1. Scope

Policies for the `risingwave_batch_executors` crate providing concrete executor implementations for batch query execution.

## 2. Purpose

The batch executors crate provides:
- **Scan Executors**: Table scans, index scans, values
- **Join Executors**: Hash join, nested loop join, lookup join
- **Aggregation**: Hash aggregation, sort-based aggregation
- **Sort/TopN**: Order by, limit, offset, top-N
- **Set Operations**: Union, intersect, except
- **DML**: Insert, update, delete
- **Exchange**: Distributed data exchange

This contains the actual operator implementations for batch queries.

## 3. Structure

```
src/batch/executors/
├── Cargo.toml                    # Crate manifest
├── benches/                      # Criterion micro-benchmarks
└── src/
    ├── lib.rs                    # Executor registrations
    ├── executor.rs               # Executor2 trait and builder
    └── executor/                 # Executor implementations
        ├── join/                 # Join operators
        ├── aggregation/          # Aggregation helpers
        ├── filter.rs             # Selection operator
        ├── hash_agg.rs           # Hash aggregation
        ├── hash_join.rs          # Hash join
        ├── insert.rs             # INSERT operator
        ├── limit.rs              # LIMIT operator
        ├── project.rs            # Projection
        ├── row_seq_scan.rs       # Sequential scan
        ├── sort.rs               # ORDER BY
        └── ...                   # 50+ executors
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `#[linkme::distributed_slice]` registrations |
| `src/executor.rs` | `Executor2` trait, `BoxedExecutor2` type |
| `src/executor/hash_join.rs` | Hash join implementation |
| `src/executor/hash_agg.rs` | Hash aggregation |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Register executors with `#[linkme::distributed_slice(EXECUTORS)]`
- Implement `Executor2` trait for all executors
- Use `BoxedExecutorBuilder2` for construction
- Add benchmarks for performance-critical executors
- Test with various data distributions (sparse, dense, NULLs)
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Change `Executor2` trait without updating all executors
- Remove executor without updating planner
- Break data type compatibility in exchange format
- Add blocking I/O in executor `execute()` methods
- Use `unsafe` without justification
- Modify benchmark baselines without justification

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_batch_executors` |
| Executor tests | `cargo test -p risingwave_batch_executors hash_join` |
| Benchmarks | `cargo bench -p risingwave_batch_executors` |
| Specific bench | `cargo bench -p risingwave_batch_executors -- hash_join` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **risingwave_batch**: Core batch types, `Executor` trait
- **risingwave_common**: Data types, arrays
- **risingwave_expr**: Expression evaluation
- **risingwave_storage**: Table storage access
- **risingwave_connector**: External connector support
- **linkme**: Distributed executor registration
- **futures-async-stream**: Async stream implementations

Contracts:
- Executors implement both `Executor` and `Executor2` traits
- Output `DataChunk` has non-zero cardinality
- Exchange format compatible across nodes
- Spilling to disk when memory limits exceeded

## 9. Overrides

None. Follows parent `src/batch/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New executor trait added
- Executor registration mechanism changed
- Benchmark harness modified
- New executor category added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/batch/AGENTS.md

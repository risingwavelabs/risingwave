# AGENTS.md - Streaming Aggregation Executors

## 1. Scope

Policies for the `src/stream/src/executor/aggregate` directory, containing streaming aggregation operator implementations for grouped and non-grouped aggregations.

## 2. Purpose

This directory implements three streaming aggregation executor variants:
- `HashAggExecutor`: Grouped aggregation with group key indexing (stateful)
- `SimpleAggExecutor`: Non-grouped global aggregation (stateful)
- `StatelessSimpleAggExecutor`: Global aggregation without state persistence (stateless)

All aggregations support incremental computation via retraction (DELETE operations), maintain state in `StateTable`s for fault tolerance, and handle distinct aggregation with deduplication tables.

## 3. Structure

```
src/stream/src/executor/aggregate/
├── mod.rs                     # Common args, filter logic, storage iterator
├── hash_agg.rs                # HashAggExecutor: grouped aggregation with LRU cache
├── simple_agg.rs              # SimpleAggExecutor: global aggregation, single group
├── stateless_simple_agg.rs    # StatelessSimpleAggExecutor: no state persistence
├── agg_group.rs               # AggGroup: per-group state management, output strategies
├── agg_state.rs               # AggState: per-agg-call state (value vs materialized)
├── agg_state_cache.rs         # Cache for extreme agg states (min/max)
├── distinct.rs                # Distinct deduplication for distinct aggregations
└── minput.rs                  # MaterializedInput state management
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `AggExecutorArgs`, `agg_call_filter_res`, `iter_table_storage` |
| `hash_agg.rs` | `HashAggExecutor<K, S>` with LRU group cache, EOWC support |
| `simple_agg.rs` | `SimpleAggExecutor<S>` with `AlwaysOutput` strategy |
| `stateless_simple_agg.rs` | `StatelessSimpleAggExecutor` for append-only streams |
| `agg_group.rs` | `AggGroup<S, Strategy>`: `OnlyOutputIfHasInput` vs `AlwaysOutput` |
| `agg_state.rs` | `AggStateStorage<S>` and `AggState` enums for state management |
| `distinct.rs` | `DistinctDeduplicater` for distinct aggregation semantics |

## 5. Edit Rules (Must)

- Run `cargo test -p risingwave_stream agg` after any aggregation changes
- Use `AggGroup` for per-group state; implement `Strategy` trait for output behavior
- Use `AggStateStorage::Value` for scalar states (sum, count), `MaterializedInput` for ordered states (min, max, string_agg)
- Call `distinct_dedup.dedup_chunk()` before applying chunks for distinct aggregations
- Flush state tables via `commit()` on barrier, after yielding output chunks
- Handle `emit_on_window_close` mode separately in `HashAggExecutor` (uses `SortBuffer`)
- Use `build_retractable` for aggregate functions to enable retraction support
- Apply `agg_call_filter_res` to respect filter clauses and NULL handling for min/max/string_agg
- Update `intermediate_state_table` with `build_states_change()` results before commit
- Propagate watermarks through `buffered_watermarks` and call `update_watermark()` on state tables

## 6. Forbidden Changes (Must Not)

- Remove `row_count_index` validation in `AggGroup` (required for correctness)
- Skip `distinct_dedup.flush()` before committing state tables
- Modify `OnlyOutputIfHasInput` strategy without checking downstream key dependencies
- Use non-retractable aggregate functions (`build_retractable` is required)
- Break the invariant: group key == prefix of intermediate state table PK
- Remove `consistency_panic!` for negative row counts in `AggGroup`
- Skip cache eviction (`agg_group_cache.evict()`) in the execution loop
- Modify barrier handling without updating vnode bitmap logic for state tables
- Use `AggStateStorage::MaterializedInput` without proper `order_columns` configuration

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All agg tests | `cargo test -p risingwave_stream agg` |
| Hash agg tests | `cargo test -p risingwave_stream hash_agg` |
| Simple agg tests | `cargo test -p risingwave_stream simple_agg` |
| Stateless agg tests | `cargo test -p risingwave_stream stateless_simple_agg` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |

## 8. Dependencies & Contracts

- **State storage**: `StateTable<S>` from `common/table/` for persistence
- **Aggregate functions**: `risingwave_expr::aggregate` for computation logic
- **Caching**: `ManagedLruCache` for group caching with spill awareness
- **EOWC support**: `SortBuffer` for emit-on-window-close semantics
- **State versions**: `PbAggNodeVersion` for backward compatibility handling
- **Consistency**: `consistency_panic!` for row count validation

## 9. Overrides

Follows parent AGENTS.md at `./src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New aggregation executor type added (e.g., windowed aggregation)
- `AggGroup` or `AggState` structure changes
- New aggregate storage type added to `AggStateStorage`
- Distinct deduplication logic modified
- EOWC (emit-on-window-close) semantics changed
- Barrier protocol changes affecting state table commit order

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/AGENTS.md

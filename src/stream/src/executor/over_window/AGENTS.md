# AGENTS.md - RisingWave Streaming Window Functions (OVER Window)

## 1. Scope

Policies for the `src/stream/src/executor/over_window` directory, containing streaming window function executors that implement SQL OVER clause semantics for continuous query processing.

## 2. Purpose

This directory implements streaming window function operators that compute aggregate, ranking, and analytic functions over ordered partitions of data:

- **General executor** (`general.rs`): Handles retractable input streams with `INSERT`/`DELETE`/`UPDATE` operations, uses range caching for partition state
- **EOWC executor** (`eowc.rs`): End-of-Window-Computation variant for ordered input with watermark-driven output, append-only processing
- **Window frame types**: `ROWS` (row-based) and `RANGE` (value-based) frame bounds
- **Window functions**: `row_number()`, `rank()`, `dense_rank()`, `lag()`, `lead()`, `first_value()`, `last_value()`, aggregate window functions (`sum`, `avg`, `count`, etc.)

The executors maintain per-partition state tables and range caches to efficiently compute window function results as data arrives.

## 3. Structure

```
src/stream/src/executor/over_window/
├── mod.rs                    # Module entry, exports OverWindowExecutor and EowcOverWindowExecutor
├── general.rs                # General over window executor for retractable streams
│   ├── OverWindowExecutor    # Main executor struct implementing Execute trait
│   ├── OverWindowExecutorArgs # Constructor arguments
│   ├── Calls                 # Window function call metadata and frame analysis
│   ├── ExecutionVars         # Runtime execution variables (caches, stats)
│   └── RowConverter          # StateKey <-> table sub-PK conversion utilities
├── eowc.rs                   # End-of-Window-Computation executor for ordered input
│   ├── EowcOverWindowExecutor
│   ├── EowcOverWindowExecutorArgs
│   ├── Partition             # Per-partition window states and row buffer
│   └── ExecutionVars
├── over_partition.rs         # Partition-level state management and change building
│   ├── OverPartition         # Wrapper for partition cache operations
│   ├── PartitionDelta        # Changes within a partition
│   ├── AffectedRange         # Range of keys affected by delta
│   └── OverPartitionStats    # Cache and computation statistics
├── range_cache.rs            # Range cache implementation for partition data
│   ├── PartitionCache        # BTreeMap-based cache with sentinel support
│   └── CacheKey              # Sentinelled<StateKey> for cache keys
└── frame_finder.rs           # Window frame boundary calculation helpers
    ├── merge_rows_frames()   # Merge multiple ROWS frames into super frame
    ├── find_first_curr_for_rows_frame()
    ├── find_last_curr_for_rows_frame()
    ├── find_frame_start_for_rows_frame()
    ├── find_frame_end_for_rows_frame()
    └── calc_logical_curr_for_range_frames()
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | Public exports: `OverWindowExecutor`, `EowcOverWindowExecutor`, and their argument structs |
| `general.rs` | General-purpose executor with range caching, handles all window frame types and retractions |
| `eowc.rs` | EOWC variant for watermark-ordered streams, simpler logic, append-only input |
| `over_partition.rs` | `OverPartition::build_changes()` - core algorithm for computing window function outputs from deltas |
| `range_cache.rs` | `PartitionCache` with sentinels (Smallest/Largest) and cache eviction policies |
| `frame_finder.rs` | Frame boundary finding algorithms for ROWS and RANGE frames |

## 5. Edit Rules (Must)

- Run `cargo test -p risingwave_stream over_window` after any changes to window function executors
- Preserve sentinel semantics in `PartitionCache`: never have only one sentinel, sentinels only at boundaries
- Use `OverWindowCachePolicy` config for cache sizing decisions; default to `Full` for unbounded frames
- Handle frame bounds correctly: `UNBOUNDED PRECEDING`, `UNBOUNDED FOLLOWING`, `CURRENT ROW`, `n PRECEDING/FOLLOWING`
- Maintain `Calls` struct invariants: `super_rows_frame_bounds` must be canonical (include CURRENT ROW)
- Use `RowConverter` for all `StateKey` <-> table sub-PK conversions to handle deduplicated partition keys
- Call `consistency_panic!` for state table row lookup failures in `general.rs` key-change handling
- Update `OverPartitionStats` when adding new cache or computation metrics
- Ensure watermark propagation is handled in `general.rs` (currently ignored with TODO)
- Run frame_finder tests: `cargo test -p risingwave_stream frame_finder`

## 6. Forbidden Changes (Must Not)

- Remove sentinel handling from `PartitionCache` without ensuring boundary detection still works
- Change `CacheKey` from `Sentinelled<StateKey>` without updating all cursor operations
- Modify `MAGIC_BATCH_SIZE` (512) in `over_partition.rs` without performance testing
- Remove `consistency_panic!` calls in `general.rs` merge_changes path for key-change updates
- Use `unwrap()` without justification in frame boundary calculations
- Break the invariant that `partition key | order key | input pk` forms the state table PK
- Modify `WindowStates` alignment assumptions in `eowc.rs` recovery path
- Change cache policy to non-Full for unbounded frames (would cause incorrect results)
- Remove `recently_accessed_ranges` tracking in `general.rs` without updating shrink logic

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream over_window` |
| Frame finder tests | `cargo test -p risingwave_stream frame_finder` |
| Range cache tests | `cargo test -p risingwave_stream range_cache` |
| Partition cache tests | `cargo test -p risingwave_stream partition_cache` |
| E2E window tests | `./risedev slt './e2e_test/**/*.slt' -- --grep "window"` |
| Over window integration | `cargo test -p risingwave_stream test_over_window` |

## 8. Dependencies & Contracts

- **Within crate**: Uses `crate::cache::ManagedLruCache` for partition caching, `crate::common::table::StateTable` for persistence
- **Expression crate**: `risingwave_expr::window_function::{WindowFuncCall, WindowStates, StateKey, create_window_state}`
- **Delta BTree map**: `delta_btree_map::{DeltaBTreeMap, Change}` for cache + delta operations
- **Storage**: `risingwave_storage::StateStore` for state table backend
- **Common**: `memcmp_encoding` for order key encoding, `Sentinelled` type for cache boundaries
- **Parent module**: Executors implement `Execute` trait from `crate::executor`

## 9. Overrides

Follows parent AGENTS.md at `./src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New window function type added (beyond ranking/aggregate/value functions)
- New cache policy added to `OverWindowCachePolicy`
- Frame boundary algorithm changes (affects affected range calculation)
- State table schema changes for window executors
- `PartitionCache` sentinel semantics modified
- New window frame type added (beyond ROWS and RANGE)

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/AGENTS.md

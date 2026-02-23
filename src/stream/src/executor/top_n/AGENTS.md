# AGENTS.md - Streaming TopN Executors

## 1. Scope

Policies for the `src/stream/src/executor/top_n` directory, implementing 4 streaming TopN executor variants with stateful ranking and limit/offset semantics.

## 2. Purpose

This directory implements streaming TopN operators that maintain the top-N rows based on ORDER BY columns, supporting both global and grouped ranking:

- **Global TopN**: `TopNExecutor` and `AppendOnlyTopNExecutor` for ranking across all rows
- **Group TopN**: `GroupTopNExecutor` and `AppendOnlyGroupTopNExecutor` for ranking within groups
- **WITH TIES support**: Handles `FETCH FIRST n ROWS WITH TIES` and `RANK() <= n` semantics
- **Offset/Limit**: Supports SQL `OFFSET m LIMIT n` patterns with efficient cache management

All variants use a three-tier cache structure (low/middle/high) to minimize state store access while maintaining incremental view semantics.

## 3. Structure

```
src/stream/src/executor/top_n/
├── mod.rs                      # Module exports: 4 executor types, cache, state
├── utils.rs                    # TopNExecutorBase trait, TopNExecutorWrapper
├── top_n_cache.rs              # TopNCache with low/middle/high tier structure
├── top_n_state.rs              # ManagedTopNState for persistent storage access
├── top_n_plain.rs              # Mutable TopN (handles updates/deletes)
├── top_n_appendonly.rs         # Append-only TopN optimization
├── group_top_n.rs              # Mutable Group TopN with LRU group cache
└── group_top_n_appendonly.rs   # Append-only Group TopN optimization
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | Public exports: `TopNExecutor`, `GroupTopNExecutor`, `AppendOnlyTopNExecutor`, `AppendOnlyGroupTopNExecutor` |
| `utils.rs` | `TopNExecutorBase` trait defining executor interface; `TopNExecutorWrapper` for common execution loop |
| `top_n_cache.rs` | `TopNCache<WITH_TIES>` with three-tier caching (low/middle/high) and `CacheKey` type |
| `top_n_state.rs` | `ManagedTopNState` wrapping `StateTable` for checkpoint/restore semantics |
| `top_n_plain.rs` | Reference implementation for mutable TopN with update/delete handling |
| `top_n_appendonly.rs` | Optimized variant that only handles inserts (no retraction logic) |
| `group_top_n.rs` | Grouped ranking with per-group `TopNCache` stored in LRU cache |
| `group_top_n_appendonly.rs` | Append-only grouped variant with simplified state management |

## 5. Edit Rules (Must)

- Run `cargo test -p risingwave_stream top_n` after any TopN modification
- Use `TopNCache<WITH_TIES>` const generic consistently across executor variants
- Maintain cache consistency: low + middle + high row counts must match state table
- Handle `WITH_TIES=true` case where result set can exceed limit due to ties
- Call `managed_state.flush()` on checkpoint barriers to persist state
- Use `CacheKey = (Vec<u8>, Vec<u8>)` for serialization: `(order_by, remaining_pk)`
- Update `top_n_cache.rs` when changing cache tier semantics
- Ensure append-only variants handle INSERT-only streams (no Update/Delete)
- Propagate watermarks via `ManagedTopNState::update_watermark()` for TTL

## 6. Forbidden Changes (Must Not)

- Modify `CacheKey` structure without updating all serialization/deserialization sites
- Remove the three-tier cache structure (low/middle/high) without performance validation
- Bypass `TopNExecutorWrapper` and implement `Execute` directly on inner executors
- Change `WITH_TIES` const generic to runtime flag (impacts code generation)
- Use append-only executors for non-append-only inputs (will miss retractions)
- Modify `TopNCache::high_cache_capacity` calculation without benchmarking
- Remove LRU cache eviction from `GroupTopNExecutor` (will cause memory issues)
- Skip barrier handling in stateful TopN (breaks checkpoint consistency)
- Change `TOPN_CACHE_HIGH_CAPACITY_FACTOR` without understanding spill behavior

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All TopN tests | `cargo test -p risingwave_stream top_n` |
| Plain TopN | `cargo test -p risingwave_stream top_n_plain` |
| Group TopN | `cargo test -p risingwave_stream group_top_n` |
| Append-only | `cargo test -p risingwave_stream append_only` |
| Cache tests | `cargo test -p risingwave_stream top_n_cache` |
| State tests | `cargo test -p risingwave_stream top_n_state` |

## 8. Dependencies & Contracts

- **Within crate**: Uses `common/table/` for `StateTable`, `cache/` for `ManagedLruCache`
- **Parent module**: `from_proto/top_n.rs` and `from_proto/group_top_n.rs` construct executors
- **State management**: All changes buffered in `TopNCache`, flushed on barriers
- **Cache tiers**:
  - `low`: Rows `[0, offset)` - maintained but not output
  - `middle`: Rows `[offset, offset+limit)` - output result set
  - `high`: Rows `[offset+limit, ...)` - cached overflow for efficiency
- **WITH_TIES**: When true, middle/high may contain ties extending beyond strict limit
- **Group cache**: `GroupTopNExecutor` uses `ManagedLruCache<GroupKey, TopNCache>` for per-group state

## 9. Overrides

Follows parent AGENTS.md at `./src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New TopN executor variant added (e.g., windowed TopN)
- `CacheKey` or serialization format changes
- Three-tier cache structure modified
- `WITH_TIES` semantics expanded
- Group key handling changes
- New cache eviction policy introduced
- `TopNExecutorBase` trait signature changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/AGENTS.md

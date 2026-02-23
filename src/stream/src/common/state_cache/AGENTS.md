# AGENTS.md - State Cache

## 1. Scope

Policies for the `src/stream/src/common/state_cache` directory, containing state caching utilities for incremental view maintenance in streaming operators.

## 2. Purpose

This directory provides caching abstractions for stateful streaming operators:
- **StateCache trait**: Common interface for operator-local caching
- **OrderedStateCache**: BTreeMap-based full cache for ordered data access
- **TopNStateCache**: Capacity-limited ordered cache for TopN operators
- **StateCacheFiller**: Sync mechanism between state table and cache

State caches improve performance by avoiding repeated state table lookups for frequently accessed keys.

## 3. Structure

```
src/stream/src/common/state_cache/
├── mod.rs                    # StateCache and StateCacheFiller traits
├── ordered.rs                # OrderedStateCache - BTreeMap-based implementation
└── top_n.rs                  # TopNStateCache - capacity-limited ordered cache
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `StateCache` trait with sync/insert/delete/clear operations |
| `ordered.rs` | `OrderedStateCache` for range queries and ordered iteration |
| `top_n.rs` | `TopNStateCache` with capacity limit and eviction |

## 5. Edit Rules (Must)

- Implement `EstimateSize` for memory tracking in all cache implementations
- Use `begin_syncing()` and `Filler::finish()` for state table sync
- Call `apply_batch()` for atomic batch cache updates
- Ensure `is_synced()` returns true only when cache reflects state table
- Handle cache invalidation correctly on operator scaling
- Use `first_key_value()` for peek operations without full iteration
- Test cache consistency after state table modifications
- Run `cargo test -p risingwave_stream state_cache` after changes

## 6. Forbidden Changes (Must Not)

- Modify `StateCache` trait without updating all implementations
- Remove `is_synced()` checks that ensure cache validity
- Bypass `apply_batch()` for individual cache modifications
- Use unbounded caches without memory limits
- Ignore cache staleness on vnode reassignment
- Change cache key ordering without updating TopN logic

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| State cache tests | `cargo test -p risingwave_stream state_cache` |
| TopN tests | `cargo test -p risingwave_stream top_n` |
| Ordered cache tests | `cargo test -p risingwave_stream ordered` |

## 8. Dependencies & Contracts

- **Within crate**: Used by TopN and other stateful executors
- **Parent module**: `common/` provides state table integration
- **Memory tracking**: `EstimateSize` for heap size estimation
- **State sync**: `StateTable` scan for cache population
- **Operations**: `Op` enum for insert/delete batch operations

## 9. Overrides

None. Follows parent AGENTS.md at `./src/stream/src/common/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New cache implementation type added
- StateCache trait interface changes
- Cache sync mechanism modified
- Memory estimation requirements change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/common/AGENTS.md

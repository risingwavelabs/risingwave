# AGENTS.md - Stream Engine LRU Cache

## 1. Scope

Policies for the `src/stream/src/cache` directory, containing the managed LRU cache implementation for streaming operator state caching.

## 2. Purpose

This directory provides memory-bounded caching for streaming executors:
- **ManagedLruCache**: Spill-aware LRU cache integrated with MemoryManager
- **Keyed cache freshness tracking**: VNode-based cache staleness detection for scaling
- **Memory tracking**: Accurate heap size estimation and metrics reporting

The cache ensures operators can maintain hot state in memory while respecting cluster-wide memory limits through epoch-based eviction.

## 3. Structure

```
src/stream/src/cache/
├── mod.rs                    # Module exports and keyed cache staleness detection
└── managed_lru.rs            # ManagedLruCache with spill awareness
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `managed_lru.rs` | `ManagedLruCache<K, V>` with watermark-based eviction |
| `mod.rs` | `keyed_cache_may_stale()` for scaling consistency checks |

## 5. Edit Rules (Must)

- Use `EstimateSize` trait for accurate memory tracking of cached entries
- Update metrics via `HeapSizeReporter` when entries are added/removed
- Handle `watermark_sequence` correctly for epoch-based eviction
- Use `MutGuard` for mutable access to track size changes on drop
- Call `evict()` when memory pressure is detected by MemoryManager
- Test with both scale-out and scale-in scenarios for cache consistency
- Run `cargo test -p risingwave_stream cache` after modifications

## 6. Forbidden Changes (Must Not)

- Remove `EstimateSize` bounds from cache key/value types
- Bypass `HeapSizeReporter` for direct metrics updates
- Ignore `watermark_sequence` updates from MemoryManager
- Use unbounded caches without explicit memory limits
- Modify `keyed_cache_may_stale` logic without understanding scaling implications
- Remove lazy eviction strategy for scale-out optimization

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Cache tests | `cargo test -p risingwave_stream cache` |
| LRU tests | `cargo test -p risingwave_stream lru` |
| Managed cache tests | `cargo test -p risingwave_stream managed` |

## 8. Dependencies & Contracts

- **Within crate**: Used by `common/state_cache/` and executors
- **Memory management**: Integrated with `MemoryManager` via `watermark_sequence`
- **Metrics**: `LabelGuardedIntGauge` for per-actor memory tracking
- **Size estimation**: `risingwave_common_estimate_size::EstimateSize` trait
- **Sequence**: `AtomicSequence` for epoch-based eviction ordering

## 9. Overrides

None. Follows parent AGENTS.md at `./src/stream/src/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New cache eviction policy added
- Memory tracking mechanism changes
- ManagedLruCache interface modified
- New cache staleness detection logic added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/AGENTS.md

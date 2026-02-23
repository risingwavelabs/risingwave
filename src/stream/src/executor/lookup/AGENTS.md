# AGENTS.md - Lookup Operations

## 1. Scope

Policies for the `src/stream/src/executor/lookup` directory, containing lookup join implementation and supporting utilities.

## 2. Purpose

The lookup module implements lookup joins for stream-table joins:
- Real-time lookup of stream rows against materialized arrangements
- Caching for reducing repeated storage lookups
- Support for inner and left outer joins
- Column reordering for output schema alignment

Lookup joins enable efficient streaming joins where one side is a stream and the other is a pre-built index (arrangement) from a table or materialized view.

## 3. Structure

```
src/stream/src/executor/lookup/
├── cache.rs                  # LookupCache for arrangement side caching
├── impl_.rs                  # LookupExecutor implementation
├── sides.rs                  # StreamJoinSide and ArrangeJoinSide definitions
└── tests.rs                  # Unit tests for lookup operations
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `cache.rs` | LRU cache for arrangement rows with watermark-based eviction |
| `impl_.rs` | Core lookup join implementation with async lookup logic |
| `sides.rs` | Join side abstractions for stream and arrangement inputs |
| `tests.rs` | Comprehensive tests for lookup join correctness |

## 5. Edit Rules (Must)

- Use `LookupCache` for arrangement side to reduce storage queries
- Handle column reordering for correct output schema
- Support both inner and left outer join semantics
- Implement proper error handling for storage lookup failures
- Clear cache on vnode bitmap updates
- Handle watermark messages for cache eviction
- Validate join key compatibility between stream and arrangement

## 6. Forbidden Changes (Must Not)

- Do not bypass cache for performance-critical lookups
- Never ignore lookup errors that may indicate consistency issues
- Do not modify join semantics without planner coordination
- Avoid unbounded cache growth without eviction policies
- Never skip barrier handling in stateful lookups

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream lookup` |
| Lookup cache | `cargo test -p risingwave_stream lookup_cache` |
| Integration | Tests in `tests.rs` |

## 8. Dependencies & Contracts

- **Arrangement**: Uses `ArrangeJoinSide` from upstream materialized view
- **Cache**: `LookupCache` with `ManagedLruCache` backend
- **Storage**: State table lookups for cache misses
- **Join types**: Inner and left outer join supported

## 9. Overrides

Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New join types added to lookup
- Cache management changes
- Arrangement interface modifications
- Join key handling updates

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/executor/AGENTS.md

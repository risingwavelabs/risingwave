# AGENTS.md - Deduplication Executor

## 1. Scope

Policies for the `src/stream/src/executor/dedup` directory, containing the append-only deduplication executor.

## 2. Purpose

The deduplication executor removes duplicate rows based on specified key columns. It operates on append-only streams where rows are only inserted, never updated or deleted.

Key responsibilities:
- Detect and filter duplicate rows using primary key columns
- Maintain LRU cache for efficient duplicate detection
- Persist seen keys to state table for fault tolerance
- Support watermark-based cache eviction

This executor is essential for ensuring exactly-once processing semantics in scenarios like Kafka consumption where messages may be redelivered.

## 3. Structure

```
src/stream/src/executor/dedup/
├── mod.rs                    # Module exports
└── append_only_dedup.rs      # AppendOnlyDedupExecutor implementation
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `append_only_dedup.rs` | Deduplication executor for append-only streams with LRU cache and state table persistence |

## 5. Edit Rules (Must)

- Use `ManagedLruCache` for in-memory duplicate key tracking
- Query state table before cache miss to ensure correctness across restarts
- Handle watermark messages to trigger cache eviction
- Commit state table on barrier to ensure exactly-once semantics
- Clear cache on vnode bitmap updates to maintain consistency
- Use concurrent state table existence checks (buffer_unordered) for performance

## 6. Forbidden Changes (Must Not)

- Do not modify deduplication logic without considering cache consistency
- Never bypass state table persistence for deduplication keys
- Do not change the append-only constraint without updating state management
- Avoid blocking operations during cache population
- Never skip barrier handling in stateful operations

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream dedup` |
| Single executor test | `cargo test -p risingwave_stream test_dedup_executor` |

## 8. Dependencies & Contracts

- **Cache**: Uses `crate::cache::ManagedLruCache` for key storage
- **State**: Persists to `StateTable` for recovery across restarts
- **Input constraint**: Only accepts append-only streams (all INSERT operations)
- **Output guarantee**: Produces append-only output with duplicates removed

## 9. Overrides

Follows parent AGENTS.md at `./src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New deduplication strategy added
- Cache management approach changes
- State table interface modifications
- Watermark handling updates

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/AGENTS.md

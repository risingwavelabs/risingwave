# AGENTS.md - Materialized View Sink

## 1. Scope

Policies for the `src/stream/src/executor/mview` directory, containing the materialized view sink executor and related components.

## 2. Purpose

The mview module implements the sink for materialized views:
- Persists streaming changes to materialized view state tables
- Manages output row caches for lookup join support
- Tracks refresh progress for refreshable materialized views
- Supports concurrent writes with conflict resolution

This executor is the terminal point for materialized view computation, making results queryable and maintaining consistency with upstream changes.

## 3. Structure

```
src/stream/src/executor/mview/
├── mod.rs                    # Module exports
├── materialize.rs            # MaterializeExecutor implementation
├── cache.rs                  # Output row cache for lookup joins
├── refresh_progress_table.rs # Progress tracking for refreshable MVs
└── test_utils.rs             # Testing utilities
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `materialize.rs` | Main materialize executor with state table writes and conflict handling |
| `cache.rs` | `MViewOutputCache` for caching rows to support lookup joins |
| `refresh_progress_table.rs` | Progress tracking for non-streaming (refreshable) materialized views |

## 5. Edit Rules (Must)

- Write all changes to state table with proper conflict handling
- Maintain output cache consistency with state table
- Handle vnode bitmap updates with cache invalidation
- Support both streaming and batch materialization modes
- Implement proper error handling for storage write failures
- Track progress for refreshable materialized views
- Use atomic operations for cache updates

## 6. Forbidden Changes (Must Not)

- Do not skip state table writes for materialized views
- Never allow cache inconsistency with persisted state
- Do not break conflict resolution semantics
- Avoid blocking operations during materialization
- Never ignore barrier commits for MV state

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream mview` |
| Materialize | `cargo test -p risingwave_stream materialize` |
| Cache tests | `cargo test -p risingwave_stream mview_cache` |

## 8. Dependencies & Contracts

- **State**: Uses `StateTable` for persistence
- **Cache**: `MViewOutputCache` for lookup join support
- **Conflict**: Primary key conflict resolution on INSERT/UPDATE
- **Lookup**: Enables `LookupExecutor` to query MV state

## 9. Overrides

Follows parent AGENTS.md at `./src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New materialization modes added
- Conflict resolution changes
- Cache management updates
- Refresh progress tracking modifications

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/AGENTS.md

# AGENTS.md - Stream Engine Common Utilities

## 1. Scope

Policies for the `src/stream/src/common` directory, containing shared utilities and abstractions used across streaming executors in the `risingwave_stream` crate.

## 2. Purpose

This directory provides common infrastructure for streaming operators:
- State table abstractions for checkpoint/restore operations
- State caching utilities for incremental view maintenance
- Log store implementations for sink durability and recovery
- Change buffering and stream chunk compaction
- Rate limiting and backpressure mechanisms
- Column mapping between upstream and state table schemas

## 3. Structure

```
src/stream/src/common/
├── mod.rs                    # Module exports: table, state_cache, log_store_impl, etc.
├── column_mapping.rs         # StateTableColumnMapping for upstream->table column translation
├── change_buffer.rs          # Change aggregation buffer with inconsistency handling
├── compact_chunk.rs          # StreamChunkCompactor for deduplicating stream chunks
├── metrics.rs                # MetricsInfo wrapper for streaming metrics
├── rate_limit.rs             # Rate-limited chunk size computation
├── table/                    # State table interface
│   ├── mod.rs                # Module exports
│   ├── state_table.rs        # Core state table implementation with checkpoint/restore
│   ├── test_state_table.rs   # State table unit tests
│   ├── test_storage_table.rs # Storage table tests
│   └── test_utils.rs         # Test utilities for state tables
├── state_cache/              # State caching for operator performance
│   ├── mod.rs                # StateCache and StateCacheFiller traits
│   ├── ordered.rs            # OrderedStateCache - BTreeMap-based full cache
│   └── top_n.rs              # TopNStateCache - capacity-limited ordered cache
└── log_store_impl/           # Log store backends for sink durability
    ├── mod.rs                # Module exports
    ├── in_mem.rs             # In-memory log store (testing)
    └── kv_log_store/         # KV-based persistent log store
        ├── mod.rs            # KvLogStoreFactory and progress tracking
        ├── buffer.rs         # Log store buffer management
        ├── reader.rs         # Log reader with rewind support
        ├── writer.rs         # Log writer implementation
        ├── serde.rs          # Log store row serialization
        ├── state.rs          # Log store state management
        └── test_utils.rs     # Test utilities
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `table/state_table.rs` | Core state table interface for all stateful operators |
| `state_cache/mod.rs` | StateCache trait for operator-local caching |
| `log_store_impl/kv_log_store/mod.rs` | Persistent log store for exactly-once sink delivery |
| `change_buffer.rs` | Buffer for aggregating changes with consistency checking |
| `compact_chunk.rs` | Deduplicate and compact stream chunks by key |
| `column_mapping.rs` | Map upstream columns to state table columns |

## 5. Edit Rules (Must)

- Run `cargo test -p risingwave_stream` after modifying state table or cache logic
- Use `StateCache::apply_batch` for batch cache updates to maintain consistency
- Handle all `InconsistencyBehavior` variants when using `ChangeBuffer`
- Update log store tests when changing `KvLogStoreFactory` parameters
- Ensure state table operations respect the current epoch barrier
- Use `LogStoreFactory::ALLOW_REWIND` correctly for rewindable sinks
- Maintain backward compatibility when modifying log store schema versions (v1/v2)

## 6. Forbidden Changes (Must Not)

- Remove deprecated log store versions without migration plan
- Modify state table checkpoint semantics without updating all operators
- Bypass `InconsistencyBehavior` handling in `ChangeBuffer`
- Change `StateCache` trait without updating all implementations
- Remove test utilities used by other crates
- Use unbounded channels in log store implementations

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream common` |
| State table tests | `cargo test -p risingwave_stream state_table` |
| Log store tests | `cargo test -p risingwave_stream kv_log_store` |
| State cache tests | `cargo test -p risingwave_stream state_cache` |

## 8. Dependencies & Contracts

- **Within crate**: Used by `executor/` operators; depends on `cache/` for LRU
- **State table**: Wraps `risingwave_storage` for distributed state
- **Log store**: Uses `risingwave_storage::StateStore` for persistence
- **State cache**: Implements `EstimateSize` for memory tracking
- **Epoch protocol**: All state changes tied to barrier epochs

## 9. Overrides

None. Follows parent AGENTS.md at `./src/stream/src/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New state cache implementation added
- Log store schema version changes (v3+)
- State table interface modifications
- New common utility modules added
- Test utility patterns change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/AGENTS.md

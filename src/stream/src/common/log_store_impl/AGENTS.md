# AGENTS.md - Log Store Implementations

## 1. Scope

Policies for the `src/stream/src/common/log_store_impl` directory, containing log store implementations for sink durability and exactly-once delivery guarantees.

## 2. Purpose

This directory provides persistent log storage for streaming sinks:
- **KvLogStore**: Key-value based persistent log store using state store backend
- **InMemLogStore**: In-memory log store for testing scenarios
- **Progress tracking**: Log reader/writer with rewind support for recovery

Log stores enable exactly-once semantics by persisting sink input data before delivery and tracking delivery progress.

## 3. Structure

```
src/stream/src/common/log_store_impl/
├── mod.rs                    # Module exports
├── in_mem.rs                 # In-memory log store implementation
└── kv_log_store/             # KV-based persistent log store
    ├── mod.rs                # KvLogStoreFactory and reader/writer
    ├── buffer.rs             # Log buffer management
    ├── reader.rs             # Log reader with rewind capability
    ├── writer.rs             # Log writer implementation
    ├── serde.rs              # Row serialization for log entries
    ├── state.rs              # Log store state management
    └── test_utils.rs         # Test utilities for log store
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `kv_log_store/mod.rs` | `KvLogStoreFactory` and factory methods |
| `kv_log_store/reader.rs` | `KvLogReader` with checkpoint/rewind support |
| `kv_log_store/writer.rs` | `KvLogWriter` for appending log entries |
| `kv_log_store/state.rs` | `LogStoreState` for progress persistence |
| `in_mem.rs` | `InMemLogStore` for unit testing |

## 5. Edit Rules (Must)

- Use `LogStoreFactory::ALLOW_REWIND` correctly for rewindable sinks
- Maintain backward compatibility for log store schema versions (v1/v2)
- Update test utilities when changing `KvLogStoreFactory` parameters
- Handle all error cases in reader/writer with proper state cleanup
- Use `StateStore` for persistence with proper epoch isolation
- Call `truncate()` on log store after successful delivery
- Support both schema versions during migration periods
- Run `cargo test -p risingwave_stream kv_log_store` after modifications

## 6. Forbidden Changes (Must Not)

- Remove deprecated log store versions without migration path
- Change log store row schema without backward compatibility
- Bypass state store persistence for log entries
- Remove `ALLOW_REWIND` support without checking sink requirements
- Use unbounded buffers in log store implementations
- Skip truncation after successful delivery (causes storage leak)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Log store tests | `cargo test -p risingwave_stream kv_log_store` |
| Sink tests | `cargo test -p risingwave_stream sink` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |

## 8. Dependencies & Contracts

- **State store**: `risingwave_storage::StateStore` for persistence
- **Sink integration**: Used by `SyncLogStoreExecutor` for delivery guarantees
- **Schema versions**: Support v1 and v2 during transitions
- **Progress tracking**: `LogStoreState` for recovery positioning
- **Within crate**: Used by `from_proto/sync_log_store.rs`

## 9. Overrides

None. Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/common/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New log store schema version added (v3+)
- Log store factory interface changes
- New persistent storage backend added
- Sink delivery guarantee mechanisms modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/common/AGENTS.md

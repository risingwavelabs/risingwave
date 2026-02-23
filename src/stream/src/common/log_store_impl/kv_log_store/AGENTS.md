# AGENTS.md - KV Log Store Implementation

## 1. Scope

Policies for `src/stream/src/common/log_store_impl/kv_log_store` - the key-value based persistent log store implementation for streaming sink durability and exactly-once delivery guarantees.

## 2. Purpose

This directory implements the `KvLogStore` - a durable log storage system built on top of RisingWave's state store backend:

- **Persistent buffering**: Stores sink input data durably before delivery to external systems
- **Exactly-once semantics**: Provides checkpoint-based delivery guarantees with progress tracking
- **Rewind capability**: Supports replay from previous checkpoints for failure recovery
- **Progress tracking**: Maintains per-vnode consumption progress for incremental recovery
- **Schema versioning**: Supports multiple log store formats (v1, v2) for backward compatibility

The KvLogStore is critical for sink connectors requiring exactly-once delivery (Kafka, JDBC, etc.).

## 3. Structure

```
src/stream/src/common/log_store_impl/kv_log_store/
├── mod.rs                # KvLogStoreFactory, KvLogStoreReader, KvLogStoreWriter
├── buffer.rs             # Log store buffer management and chunk aggregation
├── reader.rs             # KvLogStoreReader with checkpoint and rewind support
├── writer.rs             # KvLogStoreWriter for appending log entries
├── serde.rs              # LogStoreRowSerde for row serialization/deserialization
├── state.rs              # LogStoreState for progress persistence
└── test_utils.rs         # Test utilities for log store testing
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `KvLogStoreFactory` implementation, progress tracking enums, vnode-aligned consumption |
| `buffer.rs` | `LogStoreBuffer` for aggregating chunks and managing buffer limits |
| `reader.rs` | `KvLogStoreReader` with rewind capability and progress tracking |
| `writer.rs` | `KvLogStoreWriter` for writing log entries with sequence IDs |
| `serde.rs` | `LogStoreRowSerde` for serializing rows to KV format (supports v1/v2 schemas) |
| `state.rs` | `LogStoreState` for persisting consumption progress to state store |
| `test_utils.rs` | `test_kv_log_store` helper and mock implementations |

## 5. Edit Rules (Must)

- Maintain backward compatibility for log store schema versions (v1/v2)
- Use `LogStoreVnodeProgress` correctly for per-vnode or aligned progress tracking
- Handle `ALLOW_REWIND` flag properly when creating readers/writers
- Call `truncate()` after successful delivery to prevent storage leaks
- Use proper epoch isolation when reading/writing to state store
- Update `test_utils.rs` when changing `KvLogStoreFactory` constructor parameters
- Handle all error cases in reader/writer with proper state cleanup
- Support both schema versions during migration periods
- Use `REWIND_BASE_DELAY`, `REWIND_MAX_DELAY`, `REWIND_BACKOFF_FACTOR` for retry logic
- Run `cargo test -p risingwave_stream kv_log_store` after modifications

## 6. Forbidden Changes (Must Not)

- Remove deprecated log store versions without migration path
- Change log store row schema without backward compatibility layer
- Bypass state store persistence for log entries
- Remove `ALLOW_REWIND` support without checking sink requirements
- Use unbounded buffers in log store implementations
- Skip truncation after successful delivery (causes storage leak)
- Modify `LogStoreVnodeProgress` enum without updating all consumers
- Change sequence ID generation logic without considering recovery

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Log store unit tests | `cargo test -p risingwave_stream kv_log_store` |
| Reader tests | `cargo test -p risingwave_stream log_store_reader` |
| Writer tests | `cargo test -p risingwave_stream log_store_writer` |
| Serde tests | `cargo test -p risingwave_stream log_store_serde` |
| Sink integration | `cargo test -p risingwave_stream sink` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |

## 8. Dependencies & Contracts

- **State store**: `risingwave_storage::StateStore` for persistence
- **Sink integration**: Used by `SyncLogStoreExecutor` for delivery guarantees
- **Schema versions**: Support v1 and v2 during transitions
- **Progress tracking**: `LogStoreState` with `Epoch` and `SeqId` for recovery positioning
- **Row format**: `LogStoreRowSerde` with versioned encoding
- **Within crate**: Used by `from_proto/sync_log_store.rs`

## 9. Overrides

None. Follows parent AGENTS.md at `./src/stream/src/common/log_store_impl/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New log store schema version added (v3+)
- Log store factory interface changes
- Persistent storage backend integration modified
- Sink delivery guarantee mechanisms changed
- Progress tracking schema updated
- Test utility patterns change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/common/log_store_impl/AGENTS.md

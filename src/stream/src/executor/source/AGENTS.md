# AGENTS.md - Source Operators

## 1. Scope

Policies for the `src/stream/src/executor/source` directory, containing stream source ingestion and file source executors.

## 2. Purpose

The source module implements data ingestion from external systems:
- Real-time source ingestion (Kafka, Pulsar, Kinesis, etc.)
- Source backfill for historical data catch-up
- File source processing (S3, GCS, Iceberg)
- State management for exactly-once consumption
- Rate limiting for backpressure control

These executors are the entry points for all data flowing into RisingWave streams.

## 3. Structure

```
src/stream/src/executor/source/
├── mod.rs                          # Module exports and utility functions
├── executor_core.rs                # StreamSourceCore for source management
├── reader_stream.rs                # Source reader stream utilities
├── source_executor.rs              # Main source executor for real-time ingestion
├── dummy_source_executor.rs        # No-op source for testing
├── source_backfill_executor.rs     # Historical data backfill
├── source_backfill_state_table.rs  # Backfill state persistence
├── state_table_handler.rs          # Source state table management
├── fs_list_executor.rs             # File system file listing
├── fs_fetch_executor.rs            # File content fetching
├── iceberg_list_executor.rs        # Iceberg table file listing
├── iceberg_fetch_executor.rs       # Iceberg file content fetching
└── batch_source/                   # Batch source executors for refreshable sources
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `source_executor.rs` | Primary source executor with split assignment and offset management |
| `source_backfill_executor.rs` | Backfills historical data while handling real-time updates |
| `fs_fetch_executor.rs` | Reads files from object storage with format parsing |
| `iceberg_fetch_executor.rs` | Reads Iceberg table data files with snapshot isolation |

## 5. Edit Rules (Must)

- Implement exactly-once semantics with offset tracking
- Handle split reassignment on scaling events
- Apply rate limiting using `apply_rate_limit` utility
- Persist source state to state table for recovery
- Handle schema changes gracefully where possible
- Support both streaming and batch source modes
- Implement infinite retry with backoff for transient failures

## 6. Forbidden Changes (Must Not)

- Do not modify offset tracking without considering recovery
- Never skip state persistence for consumed offsets
- Do not break exactly-once semantics in source consumption
- Avoid blocking IO in async source reading
- Never ignore split assignment changes from meta service
- Do not remove support for existing source types without deprecation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream source` |
| Source executor | `cargo test -p risingwave_stream source_executor` |
| Backfill tests | `cargo test -p risingwave_stream source_backfill` |
| File source | `cargo test -p risingwave_stream fs_fetch` |

## 8. Dependencies & Contracts

- **Connector**: `risingwave_connector` for source implementations
- **State**: Offset and split state in `StateTable`
- **Protocol**: Split assignment from meta service via barrier
- **Formats**: Supports JSON, Avro, Protobuf, Parquet, CSV, etc.

## 9. Overrides

Follows parent AGENTS.md at `./src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New source types added
- Source state management changes
- Backfill strategy modifications
- New file formats supported
- Split assignment protocol updates

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/AGENTS.md

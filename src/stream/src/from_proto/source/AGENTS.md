# AGENTS.md - Source Executor Builders

## 1. Scope

Policies for the `src/stream/src/from_proto/source` directory, containing executor builders for source ingestion operators.

## 2. Purpose

This directory implements the protobuf-to-executor conversion for source operators:
- **SourceExecutorBuilder**: Traditional streaming source executor construction
- **FsFetchExecutorBuilder**: Filesystem fetch executor for batch file sources
- **Connector utilities**: Shared source connector configuration helpers

Source builders deserialize protobuf plan nodes and construct source executors with proper connector configurations and schema mappings.

## 3. Structure

```
src/stream/src/from_proto/source/
├── mod.rs                    # Module exports and connector utilities
├── trad_source.rs            # SourceExecutorBuilder for streaming sources
└── fs_fetch.rs               # FsFetchExecutorBuilder for file sources
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `trad_source.rs` | `SourceExecutorBuilder` for Kafka, Pulsar, CDC sources |
| `fs_fetch.rs` | `FsFetchExecutorBuilder` for S3, GCS file ingestion |
| `mod.rs` | Connector name extraction, refresh mode helpers |

## 5. Edit Rules (Must)

- Implement `ExecutorBuilder` trait for source executor builders
- Use `create_source_desc_builder()` for connector configuration
- Handle both streaming and batch source modes
- Support schema registry for Avro/Protobuf sources
- Configure watermark generation for event-time processing
- Handle source properties via `WITH` options
- Support refresh modes for batch sources (FullReload, etc.)
- Run `cargo test -p risingwave_stream source` after modifications

## 6. Forbidden Changes (Must Not)

- Modify source schema without backward compatibility
- Remove connector support without deprecation
- Bypass source descriptor validation
- Use blocking connector initialization
- Skip watermark configuration for time-based sources
- Remove filesystem source support

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Source builder tests | `cargo test -p risingwave_stream source` |
| Fs fetch tests | `cargo test -p risingwave_stream fs_fetch` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |

## 8. Dependencies & Contracts

- **Parent module**: `from_proto/` provides `ExecutorBuilder` trait
- **Connector crate**: `risingwave_connector` for source implementations
- **Protobuf**: `StreamSourceInfo` for source configuration
- **Source types**: Kafka, Pulsar, Kinesis, CDC, filesystem
- **Schema**: `SourceColumnDesc` for column mapping

## 9. Overrides

None. Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/from_proto/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New source connector type added
- Source executor configuration changed
- Protobuf source node structure modified
- New filesystem source type added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/from_proto/AGENTS.md

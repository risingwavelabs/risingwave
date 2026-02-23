# AGENTS.md - Connector Codec Crate

## 1. Scope

Policies for the `src/connector/codec/` directory, a standalone sub-crate for encoding and decoding data formats used by RisingWave connectors.

## 2. Purpose

The codec crate provides efficient serialization and deserialization for connector data formats. It handles parsing and encoding of structured data including Avro, JSON, and Protocol Buffers, with support for schema evolution and type conversions.

## 3. Structure

```
src/connector/codec/
├── src/
│   ├── lib.rs                    # Crate root, module exports
│   ├── common/                   # Shared codec utilities
│   │   ├── mod.rs
│   │   └── macros.rs
│   └── decoder/                  # Decoding implementations
│       ├── mod.rs
│       ├── avro.rs               # Apache Avro decoder
│       ├── avro_schema.rs        # Avro schema handling
│       ├── json.rs               # JSON decoder
│       ├── protobuf.rs           # Protobuf decoder
│       └── utils.rs
├── tests/
│   ├── integration_tests/        # Integration test suite
│   └── test_data/                # Test fixtures and sample data
└── build.rs                      # Build script for version info
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Crate root with public exports |
| `src/decoder/avro.rs` | Avro format decoder with schema registry |
| `src/decoder/json.rs` | JSON format decoder with flexible typing |
| `src/decoder/protobuf.rs` | Protobuf format decoder |
| `src/common/` | Shared utilities for all decoders |
| `tests/integration_tests/` | End-to-end codec tests |
| `Cargo.toml` | Crate manifest with format features |
| `build.rs` | Build-time version generation |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Add unit tests for new decoder implementations
- Follow existing decoder patterns in `src/decoder/`
- Maintain backward compatibility for existing format versions
- Use `thiserror` for error type definitions
- Update integration tests when adding new formats

## 6. Forbidden Changes (Must Not)

- Modify decoder output types without updating connector consumers
- Remove existing format support without deprecation period
- Break Avro schema resolution compatibility
- Change Protobuf wire format handling without extensive testing
- Delete test fixtures in `tests/test_data/`

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_connector_codec` |
| Integration tests | `cargo test -p risingwave_connector_codec --test integration_tests` |
| Decoder tests | `cargo test -p risingwave_connector_codec decoder` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- Apache Avro: `apache-avro` crate for schema and encoding
- Protobuf: `prost` for protobuf decoding
- JSON: `simd-json` for high-performance JSON parsing
- Schema Registry: Confluent Schema Registry client integration
- Internal: `risingwave_common`, `risingwave_pb` for types
- No async runtime dependency - pure encoding/decoding

## 9. Overrides

None. Inherits rules from parent `src/connector/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New decoder format added (e.g., CSV, Parquet)
- Schema resolution logic changes
- Test fixture format changes
- New codec dependencies added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/connector/AGENTS.md

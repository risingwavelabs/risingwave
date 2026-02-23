# AGENTS.md - Codec Integration Tests

## 1. Scope

Policies for the `src/connector/codec/tests/integration_tests` directory, containing data-driven integration tests for schema and data conversion.

## 2. Purpose

This directory provides integration testing for codec conversions:
- **Schema mapping**: Avro/Protobuf schema to RisingWave schema conversion
- **Data conversion**: External format data to RisingWave datum conversion
- **Type coverage**: All supported data types and nested structures
- **Round-trip validation**: Encoding and decoding consistency

Tests validate the correctness of schema resolution and data parsing for connector data formats.

## 3. Structure

```
src/connector/codec/tests/integration_tests/
├── main.rs                   # Test entry point and documentation
├── utils.rs                  # Shared test utilities
├── avro.rs                   # Avro schema and data conversion tests
├── json.rs                   # JSON parsing and conversion tests
├── protobuf.rs               # Protobuf schema and data tests
└── protobuf/                 # Protobuf-specific test modules
    ├── all_types.rs          # All Protobuf types coverage
    └── recursive.rs          # Recursive/nested message tests
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `main.rs` | Test modules declaration with update instructions |
| `avro.rs` | Avro schema-to-columns and data conversion tests |
| `json.rs` | JSON parsing and type mapping tests |
| `protobuf.rs` | Protobuf schema resolution and data access tests |
| `utils.rs` | Shared test helpers and assertion utilities |

## 5. Edit Rules (Must)

- Use `UPDATE_EXPECT=1 cargo test -p risingwave_connector_codec` for updates
- Test both schema mapping and data conversion together
- Cover all data types including nested structures
- Use `expect_test` for expected output verification
- Document test data format and expected behavior
- Add tests for new type mappings
- Verify round-trip encoding/decoding
- Run `cargo test -p risingwave_connector_codec` before submitting

## 6. Forbidden Changes (Must Not)

- Remove existing type coverage tests
- Skip round-trip validation
- Use hardcoded binary data without documentation
- Modify test expectations without `UPDATE_EXPECT=1`
- Remove Protobuf recursive type tests
- Skip error case testing

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All codec tests | `cargo test -p risingwave_connector_codec` |
| Integration tests | `cargo test -p risingwave_connector_codec --test integration_tests` |
| Avro tests | `cargo test -p risingwave_connector_codec avro` |
| JSON tests | `cargo test -p risingwave_connector_codec json` |
| Update expects | `UPDATE_EXPECT=1 cargo test -p risingwave_connector_codec` |

## 8. Dependencies & Contracts

- **Codec crate**: `risingwave_connector_codec` decoder implementations
- **Test framework**: `expect_test` for snapshot assertions
- **Formats**: Apache Avro, Protocol Buffers, JSON
- **Schema resolution**: Confluent Schema Registry patterns
- **Data types**: All RisingWave SQL types coverage

## 9. Overrides

None. Follows parent AGENTS.md at `/home/k11/risingwave/src/connector/codec/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New codec format added (e.g., CSV, Parquet)
- New data type mapping added
- Test fixture format changes
- Schema resolution logic updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/connector/codec/AGENTS.md

# AGENTS.md - Output Dispatching

## 1. Scope

Policies for the `src/stream/src/executor/dispatch` directory, containing output mapping utilities for the dispatch executor.

## 2. Purpose

The dispatch module handles output transformation and mapping:
- Column projection and reordering for downstream consumers
- Type casting and transformation between upstream and downstream schemas
- Struct field mapping with ID-based field resolution
- Composite type transformations (structs, lists, maps)
- Watermark propagation through column index remapping

This module ensures that data dispatched to downstream operators conforms to expected schemas, supporting schema evolution and view composition.

## 3. Structure

```
src/stream/src/executor/dispatch/
└── output_mapping.rs         # DispatchOutputMapping with column and type transformations
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `output_mapping.rs` | Output mapping with simple projection and complex type transformations |

## 5. Edit Rules (Must)

- Support both simple column projection and type mapping modes
- Handle struct field mapping by field ID for schema evolution
- Validate type compatibility before transformation
- Preserve watermark propagation through column mapping
- Handle list and map type element transformation recursively
- Eliminate adjacent no-op updates after transformation
- Support vector type dimension validation during mapping
- Handle NULL values correctly in all type transformations
- Implement proper error messages for incompatible type mappings

## 6. Forbidden Changes (Must Not)

- Do not break backward compatibility for existing output mappings
- Never bypass type validation in transformation paths
- Do not modify struct field ID resolution without migration
- Avoid data loss in type narrowing conversions
- Never skip watermark transformation for mapped columns
- Do not ignore schema mismatches in dispatch output
- Never use blocking operations in type transformations

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream dispatch` |
| Mapping tests | Test via dispatch executor integration tests |
| Type mapping | `cargo test -p risingwave_stream type_mapping` |

## 8. Dependencies & Contracts

- **Input**: `PbDispatchOutputMapping` protobuf definitions
- **Types**: Supports struct, list, map, and primitive type transformations
- **Watermark**: Transformed through column index mapping
- **Schema evolution**: Field ID-based struct field resolution
- **Vector types**: Dimension compatibility validation required

## 9. Overrides

Follows parent AGENTS.md at `./src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New output mapping types added
- Type transformation logic changes
- Schema evolution support updated
- New complex type mappings required
- Watermark propagation rules modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/AGENTS.md

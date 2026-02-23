# AGENTS.md - JSON Benchmark Utilities

## 1. Scope

Policies for the `src/connector/benches/json_common` directory, containing shared utilities for JSON parser benchmarks.

## 2. Purpose

This directory provides common infrastructure for JSON parsing benchmarks:
- **Data generation**: Random JSON row generation for consistent benchmarks
- **Schema definitions**: Standard column descriptors for parser testing
- **Benchmark constants**: Shared configuration (record counts, data types)

The utilities ensure consistent test data across different JSON parser benchmarks.

## 3. Structure

```
src/connector/benches/json_common/
└── mod.rs                    # Shared JSON benchmark utilities
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `generate_json_row()`, `get_descs()`, `NUM_RECORDS` constant |

## 5. Edit Rules (Must)

- Use deterministic random seed for reproducible benchmarks
- Include all common data types in test schema (int, float, bool, string, date, timestamp)
- Keep `NUM_RECORDS` consistent across JSON benchmarks (~250,000)
- Generate valid JSON strings that match schema expectations
- Document any changes to data generation logic
- Re-run all JSON benchmarks when modifying utilities

## 6. Forbidden Changes (Must Not)

- Change `NUM_RECORDS` without updating all dependent benchmarks
- Remove data types from standard schema without justification
- Use non-deterministic random generation
- Modify `generate_json_row` format without updating parsers
- Skip re-running benchmarks after utility changes

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| JSON benchmarks | `cargo bench -p risingwave_connector -- json` |
| All benchmarks | `cargo bench -p risingwave_connector` |

## 8. Dependencies & Contracts

- **Parent module**: `connector/benches/` provides Criterion harness
- **Common types**: `risingwave_common::types` for data types
- **Connector**: `risingwave_connector::source` for column descriptors
- **Random**: `rand` crate for deterministic generation

## 9. Overrides

None. Follows parent AGENTS.md at `./src/connector/benches/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New JSON benchmark utility added
- Data generation parameters changed
- Standard schema modified
- New benchmark format introduced

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/connector/benches/AGENTS.md

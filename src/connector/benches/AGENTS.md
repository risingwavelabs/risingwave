# AGENTS.md - Connector Benchmarks

## 1. Scope

Policies for the `src/connector/benches/` directory containing performance benchmarks for connector components.

## 2. Purpose

The benchmarks directory provides Criterion-based performance tests for critical connector paths including JSON parsing, Avro decoding, and message parsing throughput. These benchmarks help detect performance regressions and guide optimization decisions.

## 3. Structure

```
src/connector/benches/
├── json_common/                  # Shared JSON benchmark utilities
│   └── data/                     # Test data files
├── debezium_json_parser.rs       # Debezium JSON parsing benchmark
├── json_parser_case_insensitive.rs # Case-insensitive JSON benchmark
├── json_vs_plain_parser.rs       # JSON vs plain format comparison
├── nexmark_integration.rs        # Nexmark source benchmark
└── protobuf_bench.rs             # Protobuf parsing benchmark
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `debezium_json_parser.rs` | CDC JSON parsing performance |
| `json_parser_case_insensitive.rs` | Case-insensitive field matching |
| `json_vs_plain_parser.rs` | Format comparison benchmarks |
| `protobuf_bench.rs` | Protocol Buffers decoding |
| `nexmark_integration.rs` | Nexmark source throughput |
| `json_common/data/` | Shared test datasets |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Use Criterion for all new benchmarks
- Include baseline measurements for comparison
- Add test data in `json_common/data/` for consistency
- Document benchmark scenarios and expected throughput

## 6. Forbidden Changes (Must Not)

- Remove existing benchmarks without justification
- Change benchmark measurements without noting in PR
- Use non-deterministic test data that skews results
- Commit debug print statements in benchmark code

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run all benchmarks | `cargo bench -p risingwave_connector` |
| Specific benchmark | `cargo bench -p risingwave_connector -- <name>` |
| JSON parser bench | `cargo bench -p risingwave_connector -- json` |
| Protobuf bench | `cargo bench -p risingwave_connector -- protobuf` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- Criterion: Benchmark harness and statistics
- Test data: JSON, Avro, Protobuf sample messages
- Internal: `risingwave_connector`, `risingwave_common`
- Benchmarks run against `release` profile for accurate results

## 9. Overrides

None. Inherits rules from parent `src/connector/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New parser or decoder benchmark added
- Benchmark framework changed (Criterion updates)
- New test data format introduced

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/connector/AGENTS.md

# AGENTS.md - Compute Integration Tests

## 1. Scope

Policies for the `src/compute/tests/` directory containing integration tests for the RisingWave compute node.

## 2. Purpose

The tests directory provides integration-level test coverage for compute node functionality, including batch/stream query execution, data exchange between nodes, and CDC (Change Data Capture) integration scenarios.

## 3. Structure

```
src/compute/tests/
├── integration_tests.rs          # Batch/stream integration tests
└── cdc_tests.rs                  # CDC-specific integration tests
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `integration_tests.rs` | End-to-end batch and streaming tests |
| `cdc_tests.rs` | CDC source integration tests |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Use `#[tokio::test]` for async tests
- Set appropriate timeouts for integration tests
- Clean up resources in test teardown
- Use `risingwave_simulation` for deterministic tests where applicable

## 6. Forbidden Changes (Must Not)

- Skip integration tests without documented reason
- Add tests with external dependencies without feature gates
- Leave test resources unreleased (open files, ports)
- Use hardcoded sleep durations - prefer synchronization primitives

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All integration tests | `cargo test -p risingwave_compute --test integration_tests` |
| CDC tests | `cargo test -p risingwave_compute --test cdc_tests` |
| Specific test | `cargo test -p risingwave_compute --test integration_tests <name>` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- Test framework: `tokio::test` for async
- Internal: `risingwave_compute`, `risingwave_batch`, `risingwave_stream`
- Simulation: `madsim` for deterministic testing
- May require: External services (Kafka, Postgres) for CDC tests

## 9. Overrides

None. Inherits rules from parent `src/compute/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New integration test file added
- Test framework or patterns changed
- New compute node functionality requiring integration tests

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/compute/AGENTS.md

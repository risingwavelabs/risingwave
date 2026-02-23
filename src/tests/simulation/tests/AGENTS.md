# AGENTS.md - Simulation Integration Tests

## 1. Scope

Policies for `/home/k11/risingwave/src/tests/simulation/tests/integration_tests` - deterministic integration tests using the madsim simulation framework.

## 2. Purpose

The simulation integration tests provide deterministic, reproducible testing of RisingWave's distributed behavior:
- Test cluster recovery and failover scenarios
- Validate scaling operations (scale out/in)
- Test backfill behavior during recovery
- Verify compaction and storage correctness
- Test sink coordination and exactly-once semantics
- Validate license and resource limit enforcement
- Test batch query execution in simulated clusters

These tests use madsim for deterministic async execution with controlled fault injection.

## 3. Structure

```
src/tests/simulation/tests/integration_tests/
├── main.rs                   # Test module aggregator (combines all tests)
├── utils.rs                  # Shared test utilities and helpers
├── backfill_tests.rs         # Backfill behavior tests
├── batch/                    # Batch query tests
├── compaction/               # Compaction tests
├── default_parallelism.rs    # Default parallelism tests
├── license_cpu_limit.rs      # License enforcement tests
├── log_store/                # Log store tests
├── recovery/                 # Cluster recovery tests
├── scale/                    # Scaling operation tests
├── sink/                     # Sink connector tests
├── storage.rs                # Storage layer tests
└── throttle.rs               # Throttling tests
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `main.rs` | Test module entry point combining all integration tests |
| `utils.rs` | Shared test utilities, cluster setup helpers |
| `backfill_tests.rs` | Backfill behavior during recovery and scaling |
| `storage.rs` | Hummock storage layer tests |
| `license_cpu_limit.rs` | CPU limit and license validation |
| `recovery/` | Cluster failure and recovery scenarios |
| `scale/` | Rescaling tests (parallelism changes) |
| `sink/` | Sink connector integration tests |
| `batch/` | Batch query execution tests |
| `compaction/` | Compaction scheduling and execution tests |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Use madsim for deterministic async execution
- Set explicit random seeds for reproducibility (`MADSIM_TEST_SEED`)
- Add new test modules to `main.rs` for inclusion
- Use shared utilities from `utils.rs` for cluster setup
- Implement failpoints for fault injection testing
- Test with various cluster sizes (1-node, 3-node configurations)
- Pass `./risedev c` (clippy) before submitting changes
- Document test scenario and expected behavior in comments
- Use `#[cfg(madsim)]` for simulation-specific code

## 6. Forbidden Changes (Must Not)

- Use non-deterministic operations (random without seed, real time)
- Add tests that depend on external services or network
- Bypass madsim for I/O operations
- Remove failpoints without updating corresponding tests
- Use real time instead of simulated time
- Skip determinism validation
- Add blocking operations in async test contexts
- Remove existing test coverage without justification

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All simulation tests | `cargo test -p risingwave_simulation` |
| Specific test module | `cargo test -p risingwave_simulation <module_name>` |
| With specific seed | `MADSIM_TEST_SEED=1 cargo test -p risingwave_simulation` |
| Recovery tests | `cargo test -p risingwave_simulation recovery` |
| Scale tests | `cargo test -p risingwave_simulation scale` |
| Backfill tests | `cargo test -p risingwave_simulation backfill` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- `madsim`: Deterministic async runtime and simulation framework
- `madsim-tokio`: Simulated tokio runtime
- `fail`: Failpoint injection for fault testing
- `risingwave_simulation`: Simulation test utilities and cluster setup
- `risingwave_meta`, `risingwave_stream`, `risingwave_batch`: Components under test
- Determinism: Same seed produces identical execution
- Time: Simulated and controllable via madsim
- Network: Simulated with configurable latency and partitions

## 9. Overrides

None. Inherits all rules from parent `/home/k11/risingwave/src/tests/simulation/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New test module categories added
- Test module aggregation pattern changes
- madsim version or API updates
- Failpoint patterns modified
- New integration test scenarios introduced
- Test utility patterns change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/tests/simulation/AGENTS.md

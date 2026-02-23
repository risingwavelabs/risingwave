# AGENTS.md - Simulation Tests Crate

## 1. Scope

Policies for the `risingwave_simulation` crate providing deterministic simulation tests for RisingWave.

## 2. Purpose

The simulation crate provides:
- **Deterministic Testing**: Reproducible concurrent execution with madsim
- **Chaos Testing**: Controlled fault injection
- **Scale Testing**: Multi-node cluster simulation
- **SQL Logic Tests**: SLT runner in simulated environment
- **Nexmark Benchmarks**: Streaming benchmark in simulation

This enables testing distributed behavior deterministically.

## 3. Structure

```
src/tests/simulation/
├── Cargo.toml                    # Crate manifest
└── src/
    ├── lib.rs                    # Test utilities and re-exports
    ├── main.rs                   # Test runner entry
    ├── client.rs                 # Simulated SQL client
    ├── cluster.rs                # Test cluster setup
    ├── ctl_ext.rs                # risectl extensions
    ├── kafka.rs                  # Simulated Kafka connector
    ├── nexmark.rs                # Nexmark benchmark
    ├── slt.rs                    # SQL logic test runner
    ├── parse.rs                  # Test file parsing
    ├── utils/                    # Test helpers
    └── *.toml                    # Test configurations
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Test utilities, `Cluster` type |
| `src/cluster.rs` | Simulated RisingWave cluster setup |
| `src/slt.rs` | SQL logic test integration |
| `src/nexmark.rs` | Nexmark benchmark runner |
| `src/*.toml` | Test scenario configurations |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Use madsim for deterministic async execution
- Set random seeds for reproducibility
- Add determinism checks (verify same output for same seed)
- Test with various cluster sizes (1-node, 3-node, etc.)
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Use non-deterministic operations (random without seed, time)
- Bypass madsim for I/O operations
- Remove failpoints without test updates
- Add tests that depend on external services
- Use real time instead of simulated time

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_simulation` |
| SLT tests | `cargo test -p risingwave_simulation slt` |
| Nexmark | `cargo test -p risingwave_simulation nexmark` |
| Scale test | `cargo test -p risingwave_simulation scale` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **madsim**: Deterministic async runtime
- **madsim-tokio**: Simulated tokio runtime
- **fail**: Failpoint injection
- **sqllogictest**: SQL logic testing
- **risingwave_***: All RisingWave crates for integration

Contracts:
- Tests are deterministic for given seed
- Failpoints enable controlled fault injection
- Cluster setup mirrors production deployment
- Time is simulated and controllable

## 9. Overrides

None. Follows parent `src/tests/AGENTS.md` if it exists.

## 10. Update Triggers

Regenerate this file when:
- New simulation test type added
- madsim version upgraded
- Failpoint patterns changed
- Test configuration format updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

# AGENTS.md - Benchmarking Utilities Crate

## 1. Scope

Policies for the `risingwave_bench` crate - Benchmarking and performance testing utilities for RisingWave.

## 2. Purpose

Provides comprehensive benchmarking tools for measuring and analyzing RisingWave performance characteristics. Includes sink benchmarking with throughput metrics, latency analysis, visualization capabilities, and performance regression detection for continuous integration.

## 3. Structure

```
src/bench/
├── sink_bench/
│   └── main.rs          # Sink throughput benchmarking binary
├── Cargo.toml           # Binary targets and benchmark dependencies
└── AGENTS.md            # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `sink_bench/main.rs` | Sink performance benchmarking with metrics |
| `Cargo.toml` | Binary definitions and benchmark-specific dependencies |
| Schema config | YAML configuration for table schemas |
| Sink options | YAML configuration for sink parameters |

## 5. Edit Rules (Must)

- Use `MockDatagenSource` for reproducible data generation
- Implement `LogReader` trait correctly for benchmark sources
- Report comprehensive throughput metrics: avg, p90, p95, p99
- Generate SVG visualizations using `plotters` crate
- Support YAML-based configuration for flexibility
- Clean up all resources after benchmark completion
- Handle errors gracefully without panicking mid-benchmark
- Document benchmark parameters and expected outputs
- Run `cargo fmt` before committing

## 6. Forbidden Changes (Must Not)

- Remove or disable throughput metric collection
- Break benchmark reproducibility across runs
- Hardcode benchmark parameters without configuration support
- Skip visualization generation in standard benchmark runs
- Leave temporary benchmark data files uncommitted
- Break sink interface compatibility
- Remove latency percentile calculations
- Use non-deterministic data sources by default

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Build bench | `cargo build -p risingwave_bench` |
| Run sink bench | `cargo run -p risingwave_bench --bin sink-bench` |
| Unit tests | `cargo test -p risingwave_bench` |
| Check binary | `cargo check --bin sink-bench` |

## 8. Dependencies & Contracts

- Connector: `risingwave_connector` for sink implementations
- Stream: `risingwave_stream` with test features for executors
- Common: `risingwave_common` for data types and utilities
- Plotting: `plotters` with SVG backend for visualization
- Config: `serde_yaml` for benchmark configuration files
- CLI: `clap` for command-line argument parsing
- Async: `futures-async-stream` for async iteration
- Datagen: Built-in datagen source for test data

## 9. Overrides

None. Child directories may override specific rules.

## 10. Update Triggers

Regenerate this file when:
- New benchmark binaries or tools added
- Sink interface or API changes
- Performance metric collection updates
- Visualization format or library changes
- Benchmark configuration format modifications

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

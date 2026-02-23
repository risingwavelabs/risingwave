# AGENTS.md - Sink Benchmark Tests

## 1. Scope

Directory: `src/bench/sink_bench`

Sink throughput and latency benchmarking binary for RisingWave connector performance testing.

## 2. Purpose

Provides comprehensive sink performance measurement:
- Measure end-to-end sink throughput under various configurations
- Analyze latency percentiles (p50, p90, p95, p99) for sink operations
- Generate visualizations (SVG plots) of performance metrics
- Validate sink implementations under realistic data volumes
- Detect performance regressions in connector code

## 3. Structure

```
src/bench/sink_bench/
├── main.rs              # Benchmark binary: sink throughput testing
├── schema.yml           # Table schema configuration for test data
├── sink_option.yml      # Sink connector options configuration
└── AGENTS.md            # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `main.rs` | Sink benchmark binary with metrics collection and visualization |
| `schema.yml` | YAML schema definition for test tables |
| `sink_option.yml` | YAML configuration for sink parameters (batch size, parallelism) |

## 5. Edit Rules (Must)

- Use `MockDatagenSource` for reproducible, deterministic data generation
- Implement `LogReader` trait correctly for benchmark data sources
- Collect comprehensive metrics: throughput (rows/sec), latency percentiles
- Generate SVG visualizations using `plotters` crate
- Support YAML-based configuration for flexible benchmark parameters
- Clean up all resources (files, connections) after benchmark completion
- Handle errors gracefully without panicking mid-benchmark
- Document expected benchmark outputs and parameter ranges
- Run `cargo fmt` before committing
- Test with `./risedev build` before submitting changes

## 6. Forbidden Changes (Must Not)

- Remove throughput or latency metric collection
- Break benchmark reproducibility across identical runs
- Hardcode benchmark parameters without YAML config support
- Skip visualization generation in standard benchmark runs
- Leave temporary benchmark data files uncommitted
- Break sink trait interface compatibility
- Remove latency percentile calculations (p90, p95, p99)
- Use non-deterministic data sources by default
- Modify without testing with actual sink connectors

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Build bench | `cargo build -p risingwave_bench --bin sink-bench` |
| Run sink bench | `cargo run -p risingwave_bench --bin sink-bench` |
| Check binary | `cargo check --bin sink-bench -p risingwave_bench` |

## 8. Dependencies & Contracts

- `risingwave_connector`: Sink implementations under test
- `risingwave_stream`: Stream executor with test features
- `risingwave_common`: Data types and utilities
- `plotters`: SVG visualization generation
- `serde_yaml`: Configuration file parsing
- `clap`: CLI argument parsing
- `futures-async-stream`: Async iteration support
- Config contracts: schema.yml defines table structure, sink_option.yml defines sink behavior

## 9. Overrides

| Parent Rule | Override |
|-------------|----------|
| Test entry | Uses binary execution, not unit tests |

## 10. Update Triggers

Regenerate this file when:
- New benchmark metrics or visualizations added
- Sink interface changes requiring benchmark updates
- Configuration format (YAML) changes
- New sink connector types supported
- Performance measurement methodology changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `/home/k11/risingwave/src/bench/AGENTS.md`

# AGENTS.md - Execution Monitoring

## 1. Scope

Policies for the `src/stream/src/executor/monitor` directory, containing metrics collection and profiling statistics for streaming executors.

## 2. Purpose

The monitoring module provides observability infrastructure for the streaming engine:
- Prometheus-style metrics collection for executor performance
- Actor-level and operator-level metrics aggregation
- Profiling statistics for performance analysis
- Memory usage tracking for stateful operators

These metrics power the RisingWave dashboard and enable operators to diagnose performance issues, detect bottlenecks, and optimize resource allocation.

## 3. Structure

```
src/stream/src/executor/monitor/
├── mod.rs                    # Module exports and re-exports
├── streaming_stats.rs        # Prometheus metrics definitions for streaming
└── profiling_stats.rs        # Performance profiling counters
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `streaming_stats.rs` | Comprehensive metrics: row counts, barrier latency, cache hit rates, memory usage |
| `profiling_stats.rs` | CPU and execution time profiling for operator performance analysis |

## 5. Edit Rules (Must)

- Use label-guarded metrics to prevent cardinality explosion
- Update metrics atomically to avoid race conditions
- Add new metrics with appropriate help text and unit labels
- Follow Prometheus naming conventions (snake_case with units)
- Instrument critical paths without impacting performance significantly
- Use `Arc<StreamingMetrics>` for sharing across executors

## 6. Forbidden Changes (Must Not)

- Do not add high-cardinality labels without review (risk of metric explosion)
- Never use blocking operations in metrics collection
- Do not remove existing metrics without deprecation period
- Avoid string allocation in hot paths for metric labels
- Never bypass the metrics abstraction layer

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream monitor` |
| Metrics tests | `cargo test -p risingwave_stream streaming_stats` |

## 8. Dependencies & Contracts

- **Metrics registry**: Prometheus client with custom label guards
- **Integration**: Used by all executors via `prelude.rs`
- **Performance**: Metrics collection should add <1% overhead
- **Cardinality**: Labels must be bounded to prevent memory issues

## 9. Overrides

Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New metric types added
- Profiling infrastructure changes
- Prometheus client library updates
- New observability requirements added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/executor/AGENTS.md

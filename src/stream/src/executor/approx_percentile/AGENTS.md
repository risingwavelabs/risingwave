# AGENTS.md - Approximate Percentile Executor

## 1. Scope

Policies for the `src/stream/src/executor/approx_percentile` directory, containing approximate percentile aggregation executor implementations.

## 2. Purpose

This directory implements streaming approximate percentile:
- **LocalApproxPercentile**: Partial aggregation with t-digest
- **GlobalApproxPercentile**: Global merge of partial results
- **GlobalState**: State management for global aggregation
- **TDigest algorithm**: Memory-efficient approximate percentile

Approximate percentile provides bounded-error percentile calculations for large-scale streaming data with controlled memory usage.

## 3. Structure

```
src/stream/src/executor/approx_percentile/
├── mod.rs                    # Module exports
├── local.rs                  # Local partial percentile aggregation
├── global.rs                 # Global merge percentile aggregation
└── global_state.rs           # Global aggregation state management
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `local.rs` | `LocalApproxPercentileExecutor` for partial aggregation |
| `global.rs` | `GlobalApproxPercentileExecutor` for global merge |
| `global_state.rs` | `GlobalPercentileState` for state table management |
| `mod.rs` | Module organization and exports |

## 5. Edit Rules (Must)

- Use TDigest algorithm with configurable compression
- Implement two-phase aggregation: local then global
- Use `StateTable` for global aggregation state
- Handle group keys for partitioned percentile calculation
- Support multiple percentile values (e.g., 50th, 90th, 99th)
- Ensure deterministic results across retries
- Handle `Barrier` and `Checkpoint` for state persistence
- Run `cargo test -p risingwave_stream approx_percentile` after changes

## 6. Forbidden Changes (Must Not)

- Change TDigest accuracy guarantees without notice
- Remove two-phase aggregation pattern
- Use unbounded memory for percentile calculation
- Skip state persistence on checkpoint barriers
- Change result format without SQL standard compliance
- Remove group key support

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Approx percentile tests | `cargo test -p risingwave_stream approx_percentile` |
| Aggregation tests | `cargo test -p risingwave_stream agg` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |

## 8. Dependencies & Contracts

- **Parent module**: `executor/` provides `Execute` trait
- **TDigest**: `risingwave_expr::aggregate::tdigest` implementation
- **State tables**: Global executor uses `StateTable<S>`
- **Aggregation framework**: Part of streaming aggregation operators
- **Barriers**: State persistence on checkpoint barriers

## 9. Overrides

None. Follows parent AGENTS.md at `./src/stream/src/executor/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New percentile algorithm added
- TDigest implementation updated
- Global/local aggregation pattern changed
- State management modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/AGENTS.md

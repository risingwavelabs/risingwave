# AGENTS.md - Approx Percentile Builders

## 1. Scope

Policies for the `src/stream/src/from_proto/approx_percentile` directory, containing executor builders for approximate percentile aggregation operators.

## 2. Purpose

This directory implements the protobuf-to-executor conversion for approximate percentile:
- **LocalApproxPercentileBuilder**: Local partial aggregation executor
- **GlobalApproxPercentileBuilder**: Global merge aggregation executor
- **TDigest integration**: Approximate percentile using t-digest algorithm

Approximate percentile provides memory-efficient percentile calculations with bounded error for streaming aggregations.

## 3. Structure

```
src/stream/src/from_proto/approx_percentile/
├── mod.rs                    # Module exports
├── local.rs                  # LocalApproxPercentileBuilder
└── global.rs                 # GlobalApproxPercentileBuilder
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `local.rs` | Builder for local partial percentile aggregation |
| `global.rs` | Builder for global merge percentile aggregation |
| `mod.rs` | Subdirectory module organization |

## 5. Edit Rules (Must)

- Implement `ExecutorBuilder` trait for both local and global builders
- Configure TDigest parameters (compression, accuracy)
- Handle group keys for grouped percentile calculations
- Support multiple percentile values per aggregation
- Use `StateTable` for global merge state persistence
- Configure proper state table schemas for checkpoint/restore
- Run `cargo test -p risingwave_stream approx_percentile` after changes

## 6. Forbidden Changes (Must Not)

- Modify TDigest accuracy guarantees without documentation
- Remove local/global separation (required for distributed execution)
- Bypass state table for global aggregation state
- Change percentile result format without SQL compatibility check
- Remove approximate percentile support

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Approx percentile tests | `cargo test -p risingwave_stream approx_percentile` |
| Aggregation tests | `cargo test -p risingwave_stream agg` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |

## 8. Dependencies & Contracts

- **Parent module**: `from_proto/` provides `ExecutorBuilder` trait
- **Executor module**: `executor/approx_percentile/` implementations
- **TDigest**: `risingwave_expr::aggregate::tdigest` algorithm
- **State tables**: Global builder requires state table configuration
- **Aggregation**: Part of streaming aggregation framework

## 9. Overrides

None. Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/from_proto/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New approximate percentile variant added
- TDigest algorithm parameters changed
- Protobuf approx percentile node modified
- Local/global aggregation pattern updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/from_proto/AGENTS.md

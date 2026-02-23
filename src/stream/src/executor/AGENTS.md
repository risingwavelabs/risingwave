# AGENTS.md - RisingWave Streaming Executors

## 1. Scope

Policies for the `src/stream/src/executor` directory, containing 60+ streaming operator implementations for the RisingWave stream engine.

## 2. Purpose

This directory implements all streaming operators (executors) that process continuous data streams:
- Stateless operators (filter, project, expand, hop_window)
- Stateful operators (hash_agg, hash_join, top_n, over_window)
- Source/sink operators (source, sink, mview, backfill)
- Control flow operators (merge, dispatch, exchange, barrier_align)
- Watermark and event-time processing (watermark_filter, eowc)

All executors implement the `Execute` trait and communicate via `Message` types (StreamChunk, Barrier, Watermark).

## 3. Structure

```
src/stream/src/executor/
├── mod.rs                    # Core types: Message, Barrier, Watermark, StreamChunk, Execute trait
├── actor.rs                  # Actor execution loop and context
├── prelude.rs                # Common imports for executor implementations
├── integration_tests.rs      # Cross-executor integration tests
├── error.rs                  # Executor-specific error types
├── utils.rs                  # Shared executor utilities
├── aggregate/                # HashAgg, SimpleAgg, StatelessSimpleAgg
│   ├── hash_agg.rs
│   ├── simple_agg.rs
│   ├── agg_group.rs
│   ├── agg_state.rs
│   └── mod.rs
├── approx_percentile/        # Approximate percentile aggregations
├── backfill/                 # Historical data backfill operators
│   ├── arrangement_backfill.rs
│   ├── no_shuffle_backfill.rs
│   ├── cdc/                  # CDC backfill
│   │   ├── cdc_backfill.rs
│   │   ├── upstream_table/   # External table snapshot
│   │   └── mod.rs
│   └── snapshot_backfill/    # Snapshot-based backfill
│       ├── executor.rs
│       ├── consume_upstream/ # Upstream table consumption
│       └── mod.rs
├── dedup/                    # Append-only deduplication
├── dispatch.rs               # Output dispatching to downstream
├── dispatch/                 # Output mapping utilities
│   └── output_mapping.rs
├── eowc/                     # End-of-window computation
│   ├── eowc_gap_fill.rs
│   └── sort.rs
├── exchange/                 # Cross-node data shuffle
│   ├── input.rs
│   ├── output.rs
│   └── permit.rs
├── join/                     # Join state management
│   ├── hash_join.rs
│   ├── join_row_set.rs
│   └── mod.rs
├── lookup.rs                 # Lookup join implementation
├── lookup/                   # Lookup join utilities
│   ├── cache.rs
│   ├── impl_.rs
│   └── sides.rs
├── monitor/                  # Metrics and profiling
│   ├── streaming_stats.rs
│   └── profiling_stats.rs
├── mview/                    # Materialized view sink
│   ├── materialize.rs
│   └── mod.rs
├── over_window/              # Window function operators
│   ├── general.rs
│   ├── eowc.rs
│   ├── over_partition.rs
│   └── range_cache.rs
├── project/                  # Projection operators
│   ├── project_scalar.rs
│   ├── project_set.rs
│   └── materialized_exprs.rs
├── source/                   # Stream source ingestion
│   ├── source_executor.rs
│   ├── source_backfill_executor.rs
│   ├── fs_fetch_executor.rs
│   ├── iceberg_fetch_executor.rs
│   └── batch_source/         # Batch source executors
├── test_utils/               # Test utilities and mocks
│   ├── mock_source.rs
│   ├── agg_executor.rs
│   └── hash_join_executor.rs
├── top_n/                    # Streaming TopN variants
│   ├── top_n_plain.rs
│   ├── group_top_n.rs
│   ├── top_n_cache.rs
│   └── utils.rs
├── vector/                   # Vector index operations
│   ├── index_writer.rs
│   └── index_lookup_join.rs
├── watermark/                # Watermark generation
├── wrapper.rs                # Executor wrapper
└── wrapper/                  # Wrapper implementations
    ├── epoch_check.rs
    ├── schema_check.rs
    └── trace.rs
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | Core types: `Message`, `Barrier`, `Watermark`, `StreamChunk`, `Execute` trait |
| `actor.rs` | `ActorContext` shared across all executors in an actor |
| `prelude.rs` | Standard imports for new executor implementations |
| `integration_tests.rs` | Cross-executor integration tests |
| `error.rs` | `StreamExecutorError` and `StreamExecutorResult` types |
| `hash_join.rs` | Reference implementation for stateful operators |
| `hash_agg.rs` | Reference implementation for aggregation operators |
| `wrapper.rs` | Executor wrapper for metrics and validation |

## 5. Edit Rules (Must)

- Run `cargo test -p risingwave_stream` after any executor modification
- Use `prelude.rs` imports when implementing new executors
- Implement the `Execute` trait for all new executors: `fn execute(self: Box<Self>) -> BoxedMessageStream`
- Add `from_proto/` converter when adding new executor types (see `src/from_proto/`)
- Use `crate::consistency_error!` for non-fatal consistency violations (logs only)
- Use `crate::consistency_panic!` for fatal consistency violations (panics in strict mode)
- Access config via `crate::CONFIG` task-local, never directly access `StreamingConfig`
- Handle all `BarrierKind` variants: `Initial`, `Checkpoint`, `Barrier`, `SyncCheckpoint`
- Propagate watermarks correctly in stateful operators using `BufferedWatermarks`
- Use `StateTable` for all stateful operations requiring checkpoint/restore
- Register new executors in `from_proto/mod.rs` dispatcher
- Use `#[try_stream]` attribute from `futures_async_stream` for async stream generation
- Prefer `expect_test` for executor output validation

## 6. Forbidden Changes (Must Not)

- Modify `Message` enum variants without updating all pattern matches across the codebase
- Remove or bypass `consistency_error!`/`consistency_panic!` macros
- Access `StreamingConfig` directly; always use `crate::CONFIG` task-local
- Use `unsafe` blocks without explicit justification in comments
- Break the barrier protocol contract in stateful operators (must commit on checkpoint barrier)
- Use blocking operations in async executor contexts
- Skip watermark propagation in operators that process time-based data
- Modify state table schemas without migration path
- Use `Expression::eval` directly (use `NonStrictExpression::eval_infallible` per clippy.toml)
- Change `ExecutorInfo` fields without updating all executor constructors

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream` |
| Executor-specific | `cargo test -p risingwave_stream <executor_name>` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |
| Hash join tests | `cargo test -p risingwave_stream hash_join` |
| Aggregation tests | `cargo test -p risingwave_stream agg` |
| TopN tests | `cargo test -p risingwave_stream top_n` |

## 8. Dependencies & Contracts

- **Within crate**: Executors use `common/table/` for state, `task/` for actor context
- **Parent module**: `from_proto/` constructs executors from protobuf plans
- **State management**: All state changes must use `StateTable` for checkpoint/restore
- **Consistency**: `consistency::insane()` and `consistency::enable_strict_consistency()` gates
- **Task-local**: `crate::CONFIG` must be set before executing operators
- **Metrics**: Use `StreamingMetrics` from `prelude.rs` for observability

## 9. Overrides

Follows parent AGENTS.md at `./src/stream/src/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New executor type added to `src/executor/`
- New subdirectory created for executor category
- `Execute` trait or `Message` types modified
- Barrier protocol changes
- Consistency checking mechanisms modified
- `prelude.rs` imports changed

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/AGENTS.md

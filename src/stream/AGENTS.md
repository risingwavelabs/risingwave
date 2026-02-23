# AGENTS.md - RisingWave Stream Engine

## 1. Scope

Policies for the `risingwave_stream` crate containing the stream compute engine. This crate implements the core streaming execution engine for continuous SQL query processing.

**Policy Inheritance:**
This file inherits rules from `/home/k11/risingwave/AGENTS.md` (root).
When working in this directory:
1. Read THIS file first for directory-specific rules
2. Then read PARENT files up to root for inherited rules
3. Child rules override parent rules (nearest-wins)
4. If code changes conflict with these docs, update AGENTS.md to match reality

## 2. Purpose

The stream engine executes continuous streaming queries using an actor-based architecture. It processes unbounded data streams through a directed graph of operators (executors), handling:
- Incremental view maintenance via streaming operators (aggregations, joins, windowing)
- Barrier-based distributed snapshot isolation for consistency
- Watermark-driven event time processing
- State management with spill-to-disk capabilities

## 3. Structure

```
src/stream/
├── src/
│   ├── lib.rs              # Crate root, global config access
│   ├── error.rs            # StreamError types and scoring
│   ├── executor/           # Streaming operators/executors
│   │   ├── mod.rs          # Core types: Message, Barrier, Watermark
│   │   ├── actor.rs        # Actor execution loop
│   │   ├── aggregate/      # Streaming aggregations (hash_agg, simple_agg)
│   │   ├── backfill/       # Historical data backfill
│   │   ├── exchange/       # Data shuffle between actors
│   │   ├── hash_join.rs    # Streaming hash join
│   │   ├── mview/          # Materialized view sink
│   │   ├── over_window/    # Window functions
│   │   ├── sink.rs         # External sink output
│   │   ├── source/         # Stream source input
│   │   ├── top_n/          # Streaming TopN
│   │   └── watermark/      # Watermark generation/propagation
│   ├── task/               # Actor lifecycle management
│   │   ├── stream_manager.rs
│   │   └── barrier_manager/
│   ├── from_proto/         # Protobuf deserialization
│   ├── common/             # Shared utilities (state tables, metrics)
│   └── cache/              # Managed LRU cache
├── tests/                  # Integration tests
├── benches/                # Performance benchmarks
├── spill_test/             # Spill-to-disk testing subcrate
└── clippy.toml             # Lint configuration
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/executor/mod.rs` | Core streaming types: `Message`, `Barrier`, `Watermark`, `StreamChunk` |
| `src/executor/actor.rs` | Actor execution loop and message processing |
| `src/task/stream_manager.rs` | Actor lifecycle management and scheduling |
| `src/task/barrier_manager/` | Distributed barrier coordination |
| `src/error.rs` | Error types with root-cause scoring |
| `clippy.toml` | Disallows strict expression evaluation in streaming context |
| `src/executor/prelude.rs` | Common imports for executor implementations |

## 5. Edit Rules (Must)

- Use `expect_test` for executor tests; prefer `tests/` over inline unit tests
- Run `cargo test -p risingwave_stream` before submitting executor changes
- Update `clippy.toml` when adding new expression evaluation patterns
- Use `NonStrictExpression::eval_infallible` for expression evaluation (enforced by clippy)
- Add executor tests using `check_until_pending` pattern for async streams
- Propagate watermarks correctly in new operators
- Handle both `Checkpoint` and `BarrierKind::Initial` barriers in stateful operators

## 6. Forbidden Changes (Must Not)

- Use `Expression::eval` or `Expression::eval_row` directly (use non-strict variants)
- Use `Iterator::zip` (use `Itertools::zip_eq` instead)
- Modify barrier injection behavior without coordination with meta node
- Remove or alter consistency checking macros (`consistency_error!`, `consistency_panic!`)
- Disable backpressure mechanisms without performance justification
- Change epoch semantics without migration plan

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |
| Spill tests | `cargo test -p spill_test` |
| Benchmarks | `cargo bench -p risingwave_stream` |
| Hash agg bench | `cargo bench -p risingwave_stream --bench stream_hash_agg` |
| Hash join bench | `cargo bench -p risingwave_stream --bench stream_hash_join_rt` |

## 8. Dependencies & Contracts

- **Internal**: risingwave_common, risingwave_storage, risingwave_expr, risingwave_connector, risingwave_pb
- **Async runtime**: tokio (via madsim-tokio for simulation)
- **Stream processing**: futures-async-stream for generator-based streams
- **Metrics**: prometheus client with label-guarded metrics
- **State backend**: risingwave_storage (Hummock LSM-Tree)
- **Protocol**: Protobuf-based plan nodes from meta node
- **Consistency**: Barrier-driven epoch-based consistency

## 9. Overrides

None. Follows parent repository rules.

## 10. Update Triggers

Regenerate this file when:
- New executor type added to `src/executor/`
- Barrier protocol changes
- State table interface changes
- New clippy restrictions added to `clippy.toml`
- Test framework patterns change (expect_test usage)

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

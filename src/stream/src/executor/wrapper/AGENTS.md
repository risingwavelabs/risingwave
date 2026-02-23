# AGENTS.md - Executor Wrappers

## 1. Scope

Policies for the `src/stream/src/executor/wrapper` directory, containing executor wrapper implementations for validation, metrics, and debugging.

## 2. Purpose

The wrapper module provides composable middleware for executors:
- Schema validation to catch type mismatches early
- Epoch checking for barrier ordering invariants
- Update checking for data integrity validation
- Tracing for performance profiling and debugging
- Stream node metrics collection for observability

Wrappers are applied automatically by `WrapperExecutor` to provide cross-cutting concerns without polluting executor implementations.

## 3. Structure

```
src/stream/src/executor/wrapper/
├── epoch_check.rs            # Validates epoch monotonicity and ordering
├── epoch_provide.rs          # Provides epoch context to downstream
├── schema_check.rs           # Validates output schema conformance
├── stream_node_metrics.rs    # Operator-level metrics collection
├── trace.rs                  # Await-tree tracing for debugging
└── update_check.rs           # Validates update semantics in debug builds
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `epoch_check.rs` | Validates barrier epoch ordering and detects anomalies |
| `schema_check.rs` | Verifies output chunks match declared schema |
| `trace.rs` | Instruments executors with await-tree tracing spans |
| `stream_node_metrics.rs` | Collects per-operator performance metrics |
| `update_check.rs` | Debug-only validation of update/insert/delete semantics |
| `epoch_provide.rs` | Provides epoch context for downstream consumption |

## 5. Edit Rules (Must)

- Add new wrappers to `WrapperExecutor::wrap()` method
- Use debug assertions for expensive checks (`update_check`)
- Ensure wrappers compose correctly (order matters)
- Keep wrapper overhead minimal in release builds
- Instrument with await-tree as the outermost wrapper
- Handle all message types (Chunk, Barrier, Watermark) consistently

## 6. Forbidden Changes (Must Not)

- Do not disable schema checking in release builds
- Never skip epoch validation in production
- Do not add expensive checks to non-debug wrappers
- Avoid breaking wrapper composition order
- Never swallow errors in validation wrappers

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream wrapper` |
| Validation | Test via integration with wrapped executors |

## 8. Dependencies & Contracts

- **Wrapper order**: Schema -> Epoch -> Trace -> Metrics -> AwaitTree
- **Performance**: Debug wrappers only in debug builds
- **Metrics**: Integrated with `monitor::StreamingMetrics`
- **Tracing**: Uses `await_tree` for async instrumentation

## 9. Overrides

Follows parent AGENTS.md at `./src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New wrapper types added
- Wrapper composition order changes
- Validation logic updates
- New metrics or tracing requirements

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/AGENTS.md

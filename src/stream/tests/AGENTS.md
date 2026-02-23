# AGENTS.md - RisingWave Stream Engine Integration Tests

## 1. Scope

Integration tests for the `risingwave_stream` crate. This directory contains executor-level integration tests using snapshot testing to verify streaming operator behavior.

## 2. Purpose

These tests validate streaming executor correctness through black-box testing:

- Snapshot-based output verification using `expect_test`
- Executor input/output behavior testing without mocking internals
- Stateful operator testing with barriers and checkpoints
- Watermark propagation validation
- Aggregation, windowing, and join correctness

## 3. Structure

```
src/stream/tests/
└── integration_tests/
    ├── main.rs                 # Test entry point, module declarations
    ├── snapshot.rs             # Snapshot testing utilities (check_until_pending)
    ├── hash_agg.rs             # Streaming hash aggregation tests
    ├── over_window.rs          # Over window function tests
    ├── eowc_over_window.rs     # Emit-on-window-close over window tests
    ├── hop_window.rs           # Hop (sliding) window tests
    ├── materialized_exprs.rs   # Materialized expression tests
    ├── project_set.rs          # Project set (unnest) tests
    └── integration_test.toml   # RiseDev task definitions
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `main.rs` | Test entry point with module declarations and shared prelude |
| `snapshot.rs` | `check_until_pending()` and `SnapshotOptions` for snapshot testing |
| `integration_test.toml` | RiseDev task for applying test snapshots (`dasit` alias) |
| Test modules | Individual executor integration tests (one per executor type) |

## 5. Edit Rules (Must)

- Use `expect_test::expect!` or `expect_test::expect_file!` for all assertions
- Follow the `check_until_pending` pattern for async stream testing
- Import test utilities from `crate::prelude` (re-exports `risingwave_stream::executor::test_utils::prelude::*`)
- Add `risingwave_expr_impl::enable!()` to main.rs if adding new expression-dependent tests
- Run tests with `UPDATE_EXPECT=1` when creating new test cases
- Include barrier messages in test inputs for stateful operators
- Use `MemoryStateStore` for stateful executor tests

## 6. Forbidden Changes (Must Not)

- Modify `check_until_pending` behavior without reviewing all test snapshots
- Remove the `risingwave_expr_impl::enable!()` macro call from main.rs
- Use `tokio::test` without the executor test utilities from `risingwave_stream::executor::test_utils`
- Commit failing snapshots without running `UPDATE_EXPECT=1`
- Bypass snapshot testing for executor output verification

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All integration tests | `cargo test -p risingwave_stream --test integration_tests` |
| Single test file | `cargo test -p risingwave_stream --test integration_tests hash_agg` |
| Update snapshots | `UPDATE_EXPECT=1 cargo test -p risingwave_stream --test integration_tests` |
| Using risedev | `./risedev dasit` (alias for apply stream integration test) |
| With nextest | `cargo nextest run -p risingwave_stream --test integration_tests` |

## 8. Dependencies & Contracts

- **Test framework**: `expect_test` for snapshot assertions
- **Async runtime**: `tokio::test` with `madsim-tokio` for simulation support
- **Executor testing**: `risingwave_stream::executor::test_utils` for mocks and utilities
- **State store**: `MemoryStateStore` for isolated test state
- **Expression framework**: `risingwave_expr_impl::enable!()` for aggregate functions
- **Snapshot format**: Inline expect blocks or external `.snap` files

## 9. Overrides

- Test location: Prefer `tests/integration_tests/` over inline `#[cfg(test)]` modules per parent crate policy
- Snapshot workflow: Use `UPDATE_EXPECT=1` environment variable instead of direct file editing
- State management: Tests must manually send barriers for stateful operators (no automatic barrier injection)

## 10. Update Triggers

Regenerate this file when:
- New executor type requires integration tests
- `check_until_pending` or `SnapshotOptions` API changes
- New test utilities added to `snapshot.rs`
- RiseDev task definitions modified in `integration_test.toml`
- Test pattern or snapshot format conventions change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/AGENTS.md

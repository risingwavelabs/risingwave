# AGENTS.md - Stream Integration Tests

## 1. Scope

Policies for the `src/stream/tests/integration_tests` directory, containing executor-level integration tests using snapshot testing.

## 2. Purpose

This directory provides black-box integration testing for streaming executors:
- **Snapshot testing**: Output verification using `expect_test`
- **Executor behavior**: Input/output testing without internal mocking
- **Stateful operators**: Barrier and checkpoint handling tests
- **Watermark propagation**: Event-time processing validation

Integration tests validate end-to-end executor correctness through message stream verification.

## 3. Structure

```
src/stream/tests/integration_tests/
├── main.rs                   # Test entry point with module declarations
├── snapshot.rs               # Snapshot testing utilities
├── hash_agg.rs               # Streaming hash aggregation tests
├── over_window.rs            # Over window function tests
├── eowc_over_window.rs       # Emit-on-window-close tests
├── hop_window.rs             # Hop/sliding window tests
├── project_set.rs            # Project set (unnest) tests
├── materialized_exprs.rs     # Materialized expression tests
└── prelude.rs                # Shared test imports
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `main.rs` | Test modules declaration, `risingwave_expr_impl::enable!()` |
| `snapshot.rs` | `check_until_pending()` for async stream testing |
| `hash_agg.rs` | Hash aggregation snapshot tests |
| `over_window.rs` | Window function tests |
| `prelude.rs` | Re-exports from `risingwave_stream::executor::test_utils` |

## 5. Edit Rules (Must)

- Use `expect_test::expect!` or `expect_file!` for all assertions
- Follow `check_until_pending` pattern for async stream testing
- Import from `crate::prelude` for test utilities
- Include barrier messages for stateful operator tests
- Use `MemoryStateStore` for stateful executor tests
- Run with `UPDATE_EXPECT=1` when creating new test cases
- Add `risingwave_expr_impl::enable!()` for expression tests
- Keep tests deterministic and reproducible

## 6. Forbidden Changes (Must Not)

- Remove `risingwave_expr_impl::enable!()` from main.rs
- Use `tokio::test` without executor test utilities
- Commit failing snapshots without `UPDATE_EXPECT=1`
- Bypass snapshot testing for output verification
- Modify `check_until_pending` without reviewing all snapshots
- Use non-deterministic test data

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| All integration tests | `cargo test -p risingwave_stream --test integration_tests` |
| Single test file | `cargo test -p risingwave_stream --test integration_tests hash_agg` |
| Update snapshots | `UPDATE_EXPECT=1 cargo test -p risingwave_stream --test integration_tests` |
| With nextest | `cargo nextest run -p risingwave_stream --test integration_tests` |

## 8. Dependencies & Contracts

- **Test framework**: `expect_test` for snapshot assertions
- **Async runtime**: `tokio::test` with `madsim-tokio` support
- **Executor testing**: `risingwave_stream::executor::test_utils`
- **State store**: `MemoryStateStore` for isolated tests
- **Expressions**: `risingwave_expr_impl::enable!()` for aggregates

## 9. Overrides

- Test location: Prefer this directory over inline `#[cfg(test)]` per parent policy
- Snapshot workflow: Use `UPDATE_EXPECT=1` instead of manual file editing
- State management: Tests manually send barriers (no automatic injection)

## 10. Update Triggers

Regenerate this file when:
- New executor integration tests added
- `check_until_pending` API changes
- New test utilities added
- Test pattern conventions change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/tests/AGENTS.md

# AGENTS.md - Test Utilities

## 1. Scope

Policies for the `src/stream/src/executor/test_utils` directory, containing testing utilities and mocks for stream executor tests.

## 2. Purpose

The test_utils module provides infrastructure for executor testing:
- Mock sources for controlled input injection
- Executor test helpers for common assertions
- Mock aggregation and join executors for integration testing
- Test expression builders

These utilities enable isolated unit testing of executors without requiring full cluster setup.

## 3. Structure

```
src/stream/src/executor/test_utils/
├── mod.rs                    # Module exports and StreamExecutorTestExt trait
├── mock_source.rs            # MockSource for test input generation
├── agg_executor.rs           # Mock aggregation executor builders
├── hash_join_executor.rs     # Mock hash join executor builders
└── top_n_executor.rs         # Mock TopN executor builders
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `StreamExecutorTestExt` trait with assertion helpers for testing async streams |
| `mock_source.rs` | `MockSource` and `MessageSender` for controlled test input |
| `agg_executor.rs` | Builder functions for creating aggregation executors in tests |
| `hash_join_executor.rs` | Builder functions for creating hash join executors in tests |

## 5. Edit Rules (Must)

- Use `MockSource` for all executor unit tests requiring input control
- Implement `StreamExecutorTestExt` for custom test stream types
- Use `expect_test` patterns for output verification
- Keep mock executors synchronized with real executor interfaces
- Document test helper functions with usage examples
- Use `test_epoch` for consistent epoch values in tests

## 6. Forbidden Changes (Must Not)

- Do not modify test utilities without updating dependent tests
- Never break `MockSource` API without migration path
- Do not use real sources in unit tests (use mocks instead)
- Avoid test utilities that depend on external services
- Never skip cleanup in test helpers that allocate resources

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | Tests using these utilities are in executor modules |
| Mock source | `cargo test -p risingwave_stream mock_source` |
| Test utils | `cargo test -p risingwave_stream test_utils` |

## 8. Dependencies & Contracts

- **Testing**: Designed for `cargo test` execution
- **Isolation**: No external dependencies (no real storage/network)
- **Performance**: Optimized for fast unit test execution
- **Compatibility**: Maintains API stability for test suites

## 9. Overrides

Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New test utilities added
- Mock executor interfaces changed
- Test assertion patterns updated
- New executor types require mock builders

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/executor/AGENTS.md

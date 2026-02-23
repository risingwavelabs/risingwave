# AGENTS.md - Sync Point Crate

## 1. Scope

Policies for the `sync-point` crate providing test synchronization primitives for deterministic testing.

## 2. Purpose

The sync-point crate provides:
- **sync_point! Macro**: Named synchronization points in code
- **Barrier Synchronization**: Wait for specific conditions in tests
- **Deterministic Testing**: Enable reproducible concurrent test execution
- **Failure Injection**: Coordinate fault injection with execution state

This enables deterministic testing of concurrent and distributed systems.

## 3. Structure

```
src/utils/sync-point/
├── Cargo.toml                    # Crate manifest
└── src/
    └── lib.rs                    # sync_point macro and implementation
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `sync_point!`, `SyncPoint`, barrier implementation |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Feature-gate all sync-point code (`sync_point` feature)
- Zero overhead when feature is disabled (compile to no-op)
- Use spin-locks or lightweight synchronization
- Support both sync and async contexts
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Enable sync-points by default in production builds
- Add significant overhead when disabled
- Block indefinitely without timeout
- Use in hot production code paths
- Remove without updating all tests using `sync_point!`

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p sync-point --features sync_point` |
| Without feature | `cargo test -p sync-point` (tests no-op behavior) |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **futures-util**: Async synchronization
- **spin**: Spin-lock synchronization
- **tokio**: Async runtime for tests

Contracts:
- When `sync_point` feature is disabled, macro expands to empty statement
- Named sync points are globally registered
- Timeouts prevent indefinite blocking
- Thread-safe for concurrent access

## 9. Overrides

Overrides parent rules:

| Parent Rule | Override |
|-------------|----------|
| "Add unit tests for new public functions" | Tests require `--features sync_point` |

## 10. Update Triggers

Regenerate this file when:
- Sync point registration mechanism changes
- New synchronization primitive added
- Feature gate name changed
- Testing integration modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/utils/AGENTS.md

# AGENTS.md - Futures Util Crate

## 1. Scope

Policies for the `rw_futures_util` crate providing future and stream utility extensions.

## 2. Purpose

The futures util crate provides:
- **Stream Extensions**: Additional combinators for async streams
- **BufferedWithFence**: Batching with fence/barrier semantics
- **Pausable Streams**: Streams that can be paused and resumed
- **Utility Functions**: Common async patterns

This complements the standard `futures` crate with RisingWave-specific utilities.

## 3. Structure

```
src/utils/futures_util/
├── Cargo.toml                    # Crate manifest
└── src/
    ├── lib.rs                    # Module exports
    ├── buffered_with_fence.rs    # Batching with fence support
    ├── misc.rs                   # Miscellaneous utilities
    └── pausable.rs               # Pausable stream wrapper
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Public exports, trait extensions |
| `src/buffered_with_fence.rs` | `BufferedWithFence` stream combinator |
| `src/pausable.rs` | `Pausable` stream wrapper |
| `src/misc.rs` | Helper functions and traits |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Implement `Stream` trait correctly (poll semantics)
- Use `pin-project-lite` for struct projection
- Ensure combinators are cancellation-safe
- Add unit tests for all stream combinators
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Break `Stream` contract (poll after None, etc.)
- Add blocking operations in async combinators
- Use `unsafe` without proper Pin projection
- Ignore wakeups from inner streams
- Break cancellation safety guarantees

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p rw_futures_util` |
| Stream tests | `cargo test -p rw_futures_util stream` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **futures**: Core async traits and utilities
- **pin-project-lite**: Pin projection macros

Contracts:
- All combinators respect backpressure
- Streams are fused-friendly (handle poll after completion)
- Pausable streams properly handle pending items during pause
- Buffered operations have bounded memory usage

## 9. Overrides

None. Follows parent `src/utils/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New stream combinator added
- Async runtime integration changes
- Pin projection patterns updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/utils/AGENTS.md

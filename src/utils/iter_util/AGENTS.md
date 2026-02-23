# AGENTS.md - Iter Util Crate

## 1. Scope

Policies for the `rw_iter_util` crate providing iterator utility extensions.

## 2. Purpose

The iter util crate provides:
- **Iterator Extensions**: Additional combinators for standard iterators
- **Chunking Utilities**: Group iterators by size or predicate
- **Peeking Helpers**: Extended lookahead beyond single element
- **Conversion Utilities**: Iterator to collection helpers

This complements `itertools` with RisingWave-specific patterns.

## 3. Structure

```
src/utils/iter_util/
├── Cargo.toml                    # Crate manifest
└── src/
    └── lib.rs                    # Iterator extensions and utilities
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `IterExt` trait and implementations |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Implement lazy evaluation (no eager collection)
- Preserve iterator fusion semantics
- Add `#[inline]` for small adapter functions
- Test with various iterator types (fuse, once, empty)
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Break lazy evaluation (eager collection without explicit naming)
- Consume iterator without documenting side effects
- Use `unsafe` without justification
- Ignore fused iterator semantics

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p rw_iter_util` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **itertools**: Base iterator utilities

Contracts:
- All extensions are lazy (no intermediate allocation)
- Iterator fusion is preserved
- Double-ended iterator support where applicable
- Exact size hints where computable

## 9. Overrides

None. Follows parent `src/utils/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New iterator extension added
- itertools API changes
- Performance optimization patterns change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/utils/AGENTS.md

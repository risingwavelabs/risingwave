# AGENTS.md - Custom Lints

## 1. Scope

Policies for the lints directory, covering custom Rust lints powered by cargo-dylint for enforcing code quality and best practices.

## 2. Purpose

The lints module provides custom Rust lints that enforce RisingWave-specific coding standards and best practices. Using cargo-dylint, these lints integrate with the Rust compiler to provide early detection of problematic patterns that standard Clippy lints may not catch.

## 3. Structure

```
lints/
├── src/                      # Lint implementations
│   ├── lib.rs               # Lint registration and setup
│   ├── format_error.rs      # Error formatting lint
│   └── utils/               # Lint utility functions
├── ui/                      # UI test files for lints
│   └── (test cases for each lint)
├── .cargo/                  # Cargo configuration
├── Cargo.toml              # Lint crate manifest
├── Cargo.lock              # Dependency lock file
├── rust-toolchain          # Rust toolchain version
└── README.md               # Documentation
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Lint registration and metadata |
| `src/format_error.rs` | Error message formatting lint |
| `src/utils/` | Shared lint utilities |
| `ui/` | UI tests for lint validation |
| `rust-toolchain` | Specific Rust version for lints |
| `Cargo.toml` | Package and example definitions |

## 5. Edit Rules (Must)

- Follow clippy_utils patterns for AST manipulation
- Document each lint with clear description and examples
- Add UI tests for new lints in `ui/` directory
- Register new lints in `src/lib.rs`
- Use the specified rust-toolchain version
- Run `cargo test` before committing lint changes
- Follow existing lint patterns for consistency
- Provide helpful error messages with suggestions

## 6. Forbidden Changes (Must Not)

- Modify rust-toolchain without compatibility verification
- Remove lints without deprecation period
- Add lints with high false positive rates
- Bypass the dylint registration system
- Commit target/ directory contents
- Add lints that duplicate standard Clippy functionality

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test` |
| Lint check | `cargo dylint --all` (from repo root) |
| UI tests | `cargo test --examples` |
| Install dylint | `cargo install cargo-dylint dylint-link` |

## 8. Dependencies & Contracts

- Rust nightly toolchain (specific version in rust-toolchain)
- cargo-dylint and dylint-link
- clippy_utils (from rust-lang/rust-clippy)
- rustc internal APIs
- RisingWave source code for lint target

## 9. Overrides

Inherits from `/home/k11/risingwave/AGENTS.md`:
- Override: Edit Rules - Lint-specific development requirements
- Override: Test Entry - cargo-dylint testing workflow
- Override: Dependencies - Rust compiler internals

## 10. Update Triggers

Regenerate this file when:
- New lint categories are added
- rust-toolchain version changes
- cargo-dylint integration changes
- Lint testing framework changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

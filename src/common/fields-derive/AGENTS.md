# AGENTS.md - Fields Derive Crate

## 1. Scope

Policies for the `risingwave-fields-derive` procedural macro crate providing derive macros for field introspection.

## 2. Purpose

The fields-derive crate provides:
- **Fields Derive Macro**: Generate field name constants and introspection methods
- **Field Metadata**: Compile-time access to struct field names and types
- **Reflection Support**: Enable generic code that operates on struct fields

This enables type-safe field access patterns used in configuration, serialization, and query processing.

## 3. Structure

```
src/common/fields-derive/
├── Cargo.toml                    # Proc-macro crate manifest
└── src/
    ├── lib.rs                    # Proc-macro entry points
    └── gen/                      # Code generation modules
        └── ...
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Proc-macro definitions and public API |
| `src/gen/` | Code generation helpers and templates |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Use `proc-macro2` for token manipulation (not `proc_macro` directly)
- Add comprehensive test cases in `tests/` for new derive functionality
- Handle all struct types (named, unnamed, unit) and enums
- Provide meaningful error messages using `syn::Error`
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Change generated API without updating all call sites
- Use `panic!` in proc-macro code (use `syn::Error` and `emit_error`)
- Break compatibility with existing struct definitions
- Generate non-deterministic output (must be reproducible)
- Modify `Cargo.toml` to add non-proc-macro dependencies carelessly
- Forget to update documentation when changing macro behavior

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Proc-macro tests | `cargo test -p risingwave-fields-derive` |
| Compile tests | `cargo test -p risingwave-fields-derive --test compile` |
| Expansion tests | `cargo expand -p risingwave-fields-derive --test test_name` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **proc-macro2**: Token manipulation and Span handling
- **quote**: Quasi-quotation for code generation
- **syn**: Parsing Rust syntax into structured types
- **expect-test**: Snapshot testing for macro output
- **indoc**: Indented string literals for test code
- **prettyplease**: Code formatting for test output

Contracts:
- Macros generate valid, compilable Rust code
- Generated code respects visibility modifiers
- Error spans point to correct source locations
- Output is deterministic given the same input

## 9. Overrides

Overrides parent rules:

| Parent Rule | Override |
|-------------|----------|
| "Add unit tests for new public functions" | Tests use `expect-test` for macro expansion snapshots |

## 10. Update Triggers

Regenerate this file when:
- New derive macro added
- Code generation patterns change
- syn/proc-macro2 dependencies upgraded
- New struct/enum type support added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/common/AGENTS.md

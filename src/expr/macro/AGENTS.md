# AGENTS.md - Expression Macro Crate

## 1. Scope

Policies for the `risingwave_expr_macro` crate providing procedural macros for expression function definition.

## 2. Purpose

The expression macro crate provides:
- **#[function] Macro**: Simplified function registration and implementation
- **#[aggregate] Macro**: Aggregate function boilerplate reduction
- **Type Derivation**: Automatic input/output type handling
- **Documentation Generation**: Auto-generate function documentation

This reduces boilerplate for the 200+ functions in `risingwave_expr_impl`.

## 3. Structure

```
src/expr/macro/
├── Cargo.toml                    # Proc-macro crate manifest
└── src/
    ├── lib.rs                    # Proc-macro entry points
    ├── context.rs                # Function context handling
    ├── gen.rs                    # Code generation helpers
    ├── parse.rs                  # Attribute parsing
    ├── types.rs                  # Type handling utilities
    └── utils.rs                  # Helper functions
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `#[function]`, `#[aggregate]` macro exports |
| `src/gen.rs` | Code generation for function boilerplate |
| `src/parse.rs` | Parsing macro attributes |
| `src/types.rs` | Type conversion and inference |
| `src/context.rs` | Evaluation context integration |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Use `syn` version 2.x for parsing
- Support all valid Rust function signatures
- Generate deterministic output for the same input
- Provide helpful error messages with correct spans
- Test with `risingwave_expr_impl` functions
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Break existing `#[function]` usages in `risingwave_expr_impl`
- Use `panic!` in proc-macro code (emit proper errors)
- Generate code requiring unstable features without feature gates
- Remove support for existing function patterns
- Change generated API without updating documentation
- Add heavy dependencies that slow compilation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Proc-macro tests | `cargo test -p risingwave_expr_macro` |
| Compile tests | `cargo test -p risingwave_expr_macro --test compile` |
| With expr_impl | `cargo test -p risingwave_expr_impl` (tests macros indirectly) |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **proc-macro2**: Token manipulation
- **quote**: Code generation
- **syn**: Rust syntax parsing (v2.x)
- **proc-macro-error**: Error handling
- **itertools**: Iterator utilities

Contracts:
- `#[function]` generates `#[linkme::distributed_slice]` registration
- Handles NULL inputs via `Option` wrapping automatically
- Generates both `eval()` and `eval_row()` implementations
- Supports type-generic functions with constraints
- Preserves documentation comments on generated items

## 9. Overrides

Overrides parent rules:

| Parent Rule | Override |
|-------------|----------|
| "Add unit tests for new public functions" | Tests via `risingwave_expr_impl` |

## 10. Update Triggers

Regenerate this file when:
- New expression macro added
- Code generation patterns change
- syn v2 API changes
- Function registration mechanism changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

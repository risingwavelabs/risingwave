# AGENTS.md - Frontend Proc-Macro Crate

## 1. Scope

Policies for the `src/frontend/macro/` procedural macro crate providing compiler extensions for the RisingWave frontend.

## 2. Purpose

The frontend macro crate implements procedural macros used by the SQL frontend for code generation and derive macros. These macros automate boilerplate generation for plan nodes, expressions, and other frontend structures.

## 3. Structure

```
src/frontend/macro/
└── src/
    └── lib.rs                    # Proc-macro implementations
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Macro definitions and implementations |
| `Cargo.toml` | Proc-macro crate configuration |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Use `syn` for parsing Rust syntax
- Use `quote` for code generation
- Test macro expansions compile correctly
- Document macro usage in `lib.rs` doc comments
- Add compile-time error messages for invalid usage

## 6. Forbidden Changes (Must Not)

- Generate code that doesn't compile without clear errors
- Change macro signatures without updating all usages
- Remove macro functionality without migration path
- Use unsafe code in proc-macros without justification

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Macro expansion | `cargo test -p risingwave_frontend` (tests usage) |
| Compile tests | `cargo build -p risingwave_frontend_macro` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- `proc-macro2`: Token manipulation
- `syn`: Rust syntax parsing
- `quote`: Code generation
- Output: Generated code consumed by `risingwave_frontend`
- Must maintain compatibility with frontend crate

## 9. Overrides

None. Inherits rules from parent `src/frontend/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New procedural macro added
- Macro expansion logic changes
- Frontend code generation patterns updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/frontend/AGENTS.md

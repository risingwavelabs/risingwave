# AGENTS.md - Connector WithOptions Proc-Macro

## 1. Scope

Policies for the `src/connector/with_options/` procedural macro crate that generates connector option parsing code.

## 2. Purpose

The with_options crate provides a derive macro (`#[derive(WithOptions)]`) that automatically generates parsing and validation code for connector configuration options. It processes struct fields annotated with connector-specific attributes to produce standardized option handling.

## 3. Structure

```
src/connector/with_options/
└── src/
    └── lib.rs                    # Proc-macro implementation
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `WithOptions` derive macro implementation |
| `Cargo.toml` | Proc-macro crate configuration |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Use `syn` for Rust code parsing in macros
- Use `quote` for code generation
- Add test cases in `src/connector/` for macro behavior
- Document new attributes in macro doc comments

## 6. Forbidden Changes (Must Not)

- Change existing attribute names without backward compatibility
- Remove supported attribute types without migration path
- Generate invalid Rust code that fails compilation
- Modify macro without testing in connector crate

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Macro compile tests | `cargo test -p risingwave_connector` (tests macro usage) |
| Clippy check | `./risedev c` |
| Generate options | `./risedev generate-with-options` |

## 8. Dependencies & Contracts

- `proc-macro2`: Token stream manipulation
- `syn`: Rust syntax parsing
- `quote`: Code generation
- Output: Generates `WithOptions` trait implementations
- Consumed by: `risingwave_connector` crate

## 9. Overrides

None. Inherits rules from parent `src/connector/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New connector option attributes added
- Macro expansion logic changes
- Options generation process modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/connector/AGENTS.md

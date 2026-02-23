# AGENTS.md - Common Proc Macro Crate

## 1. Scope

Policies for the `risingwave_common_proc_macro` crate providing procedural macros for common patterns.

## 2. Purpose

The common proc-macro crate provides:
- **Config Derive**: `#[derive(Config)]` for configuration structs
- **Session Config**: `#[session_config]` for session variable definitions
- **Estimate Size Derive**: `#[derive(EstimateSize)]` for memory size estimation
- **Serde Prefix**: `#[serde_prefix_all]` for field name transformations

This reduces boilerplate and ensures consistent patterns across the codebase.

## 3. Structure

```
src/common/proc_macro/
├── Cargo.toml                    # Proc-macro crate manifest
└── src/
    ├── lib.rs                    # Proc-macro entry points
    ├── config.rs                 # #[derive(Config)] implementation
    ├── config_doc.rs             # Configuration documentation generation
    ├── estimate_size.rs          # #[derive(EstimateSize)] implementation
    ├── serde_prefix_all.rs       # #[serde_prefix_all] attribute
    └── session_config.rs         # #[session_config] attribute
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Proc-macro exports and documentation |
| `src/config.rs` | Configuration struct derive macro |
| `src/estimate_size.rs` | Memory size estimation derive |
| `src/session_config.rs` | Session variable configuration |
| `src/serde_prefix_all.rs` | Serde field prefix attribute |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Use `proc-macro-error` for error handling (not panics)
- Test generated code compiles in `tests/` directory
- Support both named and unnamed struct fields where applicable
- Provide clear error messages with correct span information
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Change macro output without checking all usage sites
- Use `panic!` in proc-macro code (use `emit_error`)
- Remove support for existing struct field patterns
- Generate code that requires unstable features without gating
- Add heavy dependencies that slow compile times
- Break backward compatibility without migration path

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Proc-macro tests | `cargo test -p risingwave_common_proc_macro` |
| Compile tests | `cargo test -p risingwave_common_proc_macro --test compile` |
| UI tests | `cargo test -p risingwave_common_proc_macro --test ui` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **proc-macro2**: Token manipulation
- **quote**: Code generation quasi-quoting
- **syn**: Rust syntax parsing (version 1.x)
- **bae**: Attribute parsing helper
- **proc-macro-error**: Error handling framework
- **itertools**: Iterator utilities

Contracts:
- Macros generate valid, idiomatic Rust code
- Generated code compiles without additional dependencies
- Error messages point to correct source locations
- Macros are deterministic (same input produces same output)

## 9. Overrides

Overrides parent rules:

| Parent Rule | Override |
|-------------|----------|
| "Run cargo fmt" | Generated code is not formatted (formatted by caller) |

## 10. Update Triggers

Regenerate this file when:
- New proc-macro added
- syn/proc-macro2 dependencies upgraded
- Code generation patterns change
- New derive macro functionality added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/common/AGENTS.md

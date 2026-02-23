# AGENTS.md - Prost Helpers Crate

## 1. Scope

Policies for the `prost-helpers` crate providing procedural macros for protobuf code generation assistance.

## 2. Purpose

The prost-helpers crate provides:
- **#[AnyPB] Macro**: Add type-erased protobuf accessors
- **#[Version] Macro**: Generate LATEST version constants
- **Field Getters**: Generate convenient accessor methods

This assists the prost code generation in `risingwave_pb`.

## 3. Structure

```
src/prost/helpers/
├── Cargo.toml                    # Proc-macro crate manifest
└── src/
    ├── lib.rs                    # Proc-macro exports
    └── generate.rs               # Code generation utilities
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `AnyPB`, `Version` derive macros |
| `src/generate.rs` | Helper generation logic |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Use `syn` v2 for parsing
- Generate deterministic output
- Handle errors gracefully with proper spans
- Test with generated protobuf code
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Break generated code compatibility
- Use `panic!` in proc-macro code
- Generate code requiring unstable features
- Remove support for existing protobuf patterns

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Proc-macro tests | `cargo test -p prost-helpers` |
| With risingwave_pb | `cargo build -p risingwave_pb` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **proc-macro2**: Token manipulation
- **quote**: Code generation
- **syn**: Syntax parsing (v2)

Contracts:
- Generated code compiles with prost-generated types
- Macros are deterministic
- Output is compatible with `risingwave_pb` patterns

## 9. Overrides

None. Follows parent `src/prost/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New protobuf helper macro added
- prost/tonic version upgraded
- Code generation patterns changed

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/prost/AGENTS.md

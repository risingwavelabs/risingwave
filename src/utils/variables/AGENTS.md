# AGENTS.md - Variables Crate

## 1. Scope

Policies for the `risingwave_variables` crate providing server global variables and version information.

## 2. Purpose

The variables crate provides:
- **Version Constants**: Server version, PG compatibility version
- **Git Metadata**: Commit hash, build time information
- **Feature Flags**: Compile-time feature availability
- **Build Information**: Target platform, Rust version

This enables runtime introspection of build configuration.

## 3. Structure

```
src/utils/variables/
├── Cargo.toml                    # Crate manifest
└── src/
    └── lib.rs                    # Version and build constants
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `RW_VERSION`, `PG_VERSION`, build metadata constants |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Use `chrono` for build timestamp generation
- Keep version format compatible with semantic versioning
- Include all relevant build-time metadata
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Change version format without updating parsers
- Remove version constants used by clients
- Hardcode values that should be build-time generated
- Break PostgreSQL version compatibility claims

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_variables` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **chrono**: Build timestamp generation

Contracts:
- Version constants are available at compile time
- PG_VERSION is compatible with PostgreSQL wire protocol
- Build metadata includes git commit when available

## 9. Overrides

None. Follows parent `src/utils/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New build metadata added
- Version format changed
- PostgreSQL compatibility version updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/utils/AGENTS.md

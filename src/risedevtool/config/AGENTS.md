# AGENTS.md - RiseDev Config Crate

## 1. Scope

Policies for the `risedev-config` crate providing interactive configuration for RiseDev development environment.

## 2. Purpose

The risedev-config crate provides:
- **Interactive Configurator**: TUI for selecting RiseDev components
- **Profile Generation**: Create risedev.yml from user selections
- **Component Detection**: Auto-detect available services
- **Dependency Resolution**: Ensure required dependencies

This simplifies onboarding for new RisingWave developers.

## 3. Structure

```
src/risedevtool/config/
├── Cargo.toml                    # Crate manifest
└── src/
    └── main.rs                   # Interactive configuration TUI
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/main.rs` | Interactive prompt flow, component selection |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Use `dialoguer` for interactive prompts
- Provide sensible defaults for all options
- Validate component compatibility
- Test TUI on common terminal emulators
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Remove components without deprecation notice
- Break existing profile generation format
- Add hardcoded paths or credentials
- Require components that are optional

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Build test | `cargo build -p risedev-config` |
| Run config | `cargo run -p risedev-config` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **dialoguer**: Interactive TUI prompts
- **console**: Terminal control
- **enum-iterator**: Component enumeration
- **fs-err**: Filesystem operations
- **itertools**: Iterator utilities

Contracts:
- Generates valid risedev.yml syntax
- All selected components are available
- Default profile works without modification

## 9. Overrides

None. Follows parent `src/risedevtool/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New component type added
- Profile format changed
- Interactive flow modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/risedevtool/AGENTS.md

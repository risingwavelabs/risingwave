# AGENTS.md - src/cmd Binary Executables

## 1. Scope

Policies for the cmd crate containing binary entry points for all RisingWave node types.

## 2. Purpose

Provides unified entry points and main function macros for RisingWave components (compute, meta, frontend, compactor, ctl). This crate aggregates dependencies from other workspace crates and exports ready-to-use binary targets.

## 3. Structure

```
src/cmd/
├── Cargo.toml         # Package manifest with binary definitions
├── src/
│   ├── lib.rs         # Entry point functions and main! macro
│   └── bin/
│       └── ctl.rs     # risectl binary source
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Defines `main!` macro and entry functions for all node types |
| `src/bin/ctl.rs` | risectl command-line tool binary |
| `Cargo.toml` | Defines `risectl` binary target and workspace dependencies |

## 5. Edit Rules (Must)

- Use the `main!` macro for all new binary entry points
- Add new entry function in `lib.rs` following the pattern: `pub fn <name>(opts: <OptsType>) -> !`
- Initialize logger with `init_risingwave_logger(LoggerSettings::from_opts(&opts))` for services
- Call `main_okk` wrapper for async runtime initialization
- Enable required executor/expr crates with `<crate>::enable!()` in lib.rs
- Keep binary files in `src/bin/` directory, lib code in `src/lib.rs`
- Add workspace dependency in Cargo.toml for any new node type integration

## 6. Forbidden Changes (Must Not)

- Do not bypass the `main!` macro for binary entry points
- Do not add business logic to this crate (delegate to specific component crates)
- Do not add `test = true` to binary definitions (keep `test = false`)
- Do not hardcode component versions or metadata in entry points
- Do not remove `risingwave_batch_executors::enable!()` or `risingwave_expr_impl::enable!()` calls

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Build check | `cargo build -p risingwave_cmd` |
| Binary build | `cargo build --bin risectl` |
| Clippy check | `cargo clippy -p risingwave_cmd` |

## 8. Dependencies & Contracts

- Depends on: risingwave_compute, risingwave_meta_node, risingwave_frontend, risingwave_compactor, risingwave_ctl
- Uses tikv-jemallocator for memory allocation
- Links with workspace-config for build-time features (rw-static-link, rw-dynamic-link, fips)
- Tokio runtime initialized via `main_okk` wrapper
- All Opts types must implement `clap::Parser`

## 9. Overrides

None. Inherits all rules from parent AGENTS.md.

## 10. Update Triggers

Regenerate this file when:
- New binary target added to Cargo.toml
- New entry point function added to lib.rs
- Component crate dependencies change
- Build features (rw-static-link, fips) are modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

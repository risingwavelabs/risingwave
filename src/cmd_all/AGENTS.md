# AGENTS.md - cmd_all Crate

## 1. Scope

The all-in-one binary crate for RisingWave. Provides a unified `risingwave` binary that can run as any component (compute, meta, frontend, compactor, ctl) or in bundled modes (standalone, single-node, playground).

## 2. Purpose

This crate produces the main `risingwave` binary executable used for all deployment scenarios:
- **Individual components**: Run as separate processes in distributed deployments
- **Single-node mode**: User-friendly default for local development and small deployments
- **Standalone mode**: Production-ready bundled deployment exposing node-level options
- **Playground mode**: Shortcut for in-memory single-node (`--in-memory`)

## 3. Structure

```
cmd_all/
├── build.rs                  # Build script for git version info (vergen)
├── src/
│   ├── bin/risingwave.rs     # Main binary entrypoint with component dispatch
│   ├── lib.rs                # Library exports
│   ├── standalone.rs         # Standalone mode implementation (multi-service process)
│   ├── single_node.rs        # Single-node mode (high-level user options)
│   └── common.rs             # Shared utilities
└── scripts/                  # E2E demo scripts for standalone mode
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/bin/risingwave.rs` | Main binary with clap CLI, component dispatch, and multicall support |
| `src/standalone.rs` | Standalone mode: runs meta/compute/frontend/compactor in one process with separate runtimes |
| `src/single_node.rs` | Single-node mode: high-level CLI mapping to standalone with sensible defaults |
| `build.rs` | Generates git SHA for version info at build time |
| `Cargo.toml` | Defines the `risingwave` binary and feature flags |

## 5. Edit Rules (Must)

- Keep component dispatch in `risingwave.rs` synchronized with `Component` enum
- Maintain backward compatibility for CLI arguments (aliases in `Component::aliases()`)
- Single-node mode must continue to map to standalone mode internally
- Run `./risedev c` before submitting changes
- Update clap parser tests when adding new CLI arguments
- Write all code comments in English

## 6. Forbidden Changes (Must Not)

- Remove or rename existing component subcommands (compute, meta, frontend, compactor, ctl)
- Break multicall invocation style (`./meta-node` vs `./risingwave meta`)
- Remove playground mode (used for quickstart tutorials)
- Modify git version logic without testing binary version output
- Bypass the standalone-to-single-node mapping layer
- Change default subcommand from single-node mode

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_cmd_all` |
| CLI tests | Built into `src/bin/risingwave.rs` as `#[cfg(test)]` |
| Standalone e2e | `./src/cmd_all/scripts/e2e-full-standalone-demo.sh` |
| Build binary | `./risedev build` or `cargo build -p risingwave_cmd_all --release` |

## 8. Dependencies & Contracts

- **Entry interface**: `risingwave [COMPONENT] [OPTS]` or multicall via symlinks
- **Components**: compute, meta, frontend, compactor, ctl, playground, standalone, single-node
- **Allocator**: tikv-jemallocator with unprefixed malloc
- **Runtime**: Separate tokio runtimes per service in standalone mode for isolation
- **Shutdown**: Graceful shutdown via `CancellationToken` in reverse start order
- **Feature flags**: `rw-static-link`, `all-connectors`, `udf`, `fips`, `datafusion`

## 9. Overrides

| Parent Rule | Override |
|-------------|----------|
| None | None - follows root AGENTS.md policies |

## 10. Update Triggers

Regenerate this file when:
- New component type added to `Component` enum
- CLI argument structure changes significantly
- Deployment mode behavior changes (standalone/single-node)
- Build script or feature flags modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

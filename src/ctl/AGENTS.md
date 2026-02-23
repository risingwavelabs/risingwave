# AGENTS.md - Control/CLI Utilities Crate

## 1. Scope

Policies for the `risingwave_ctl` crate - risectl DevOps CLI tool for cluster management.

## 2. Purpose

Provides the `risectl` command-line tool for internal RisingWave cluster operations. Enables administrators to inspect state, trigger maintenance operations, manage Hummock storage, and perform scaling operations on running clusters.

## 3. Structure

```
src/ctl/
├── src/
│   ├── lib.rs              # CLI entry point and command dispatch
│   ├── cmd_impl/           # Command implementations
│   │   ├── compute.rs      # Compute node commands
│   │   ├── hummock.rs      # Hummock storage commands
│   │   ├── table.rs        # Table operations
│   │   ├── meta.rs         # Meta service commands
│   │   ├── scale.rs        # Scaling operations
│   │   ├── bench.rs        # Benchmark commands
│   │   ├── await_tree.rs   # Async debugging
│   │   ├── profile.rs      # Profiling commands
│   │   ├── throttle.rs     # Throttling controls
│   │   └── test.rs         # Test commands
│   └── common/             # Common utilities
│       ├── context.rs      # CLI context
│       ├── meta_service.rs # Meta service access
│       └── hummock_service.rs # Hummock access
└── Cargo.toml
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | CliOpts, Commands enum, start() entry |
| `src/cmd_impl/hummock.rs` | Hummock storage administration |
| `src/cmd_impl/meta.rs` | Meta cluster operations |
| `src/cmd_impl/scale.rs` | Scaling and cordon operations |
| `src/common/context.rs` | CtlContext for connection management |

## 5. Edit Rules (Must)

- Use `clap` derive macros for CLI structure
- Implement commands in `cmd_impl` submodule
- Use `CtlContext` for service connections
- Read `RW_META_ADDR` and `RW_HUMMOCK_URL` env vars
- Support dry-run mode for destructive operations
- Add `--yes` flag for non-interactive confirmation
- Properly clean up context with `try_close()`
- Run `cargo fmt` before committing

## 6. Forbidden Changes (Must Not)

- Remove environment variable configuration
- Break backward compatibility of CLI commands
- Remove dry-run support for destructive ops
- Bypass confirmation for dangerous operations
- Hardcode cluster addresses
- Skip context cleanup on exit

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_ctl` |
| Build ctl | `cargo build -p risingwave_ctl` |
| Run ctl | `./target/debug/risectl --help` |
| Integration | Test against running cluster |

## 8. Dependencies & Contracts

- CLI: `clap` for argument parsing
- Storage: `risingwave_storage`, `risingwave_hummock_sdk`
- Meta: `risingwave_meta`, `risingwave_rpc_client`
- Stream: `risingwave_stream` for executor access
- Table: `comfy-table` for output formatting
- Diagnose: `rw-diagnose-tools` for analysis
- Async: `tokio` (madsim-tokio)

## 9. Overrides

None. Child directories may override specific rules.

## 10. Update Triggers

Regenerate this file when:
- New command categories added
- CLI structure changes
- Environment variable modifications
- Context management updates

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

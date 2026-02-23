# AGENTS.md - Meta Node Binary

## 1. Scope

Policies for the `src/meta/node/` directory containing the meta service node binary and server initialization.

## 2. Purpose

The meta node crate provides the executable entry point for the RisingWave meta service. It handles command-line parsing, service initialization, gRPC server startup, and coordination with etcd for leader election and distributed locking.

## 3. Structure

```
src/meta/node/
└── src/
    ├── lib.rs                    # MetaNodeOpts and initialization
    └── server.rs                 # gRPC server and service startup
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | CLI options (`MetaNodeOpts`), node initialization |
| `src/server.rs` | gRPC server setup, service registration, shutdown |
| `Cargo.toml` | Binary crate dependencies |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Add new CLI options to `MetaNodeOpts` with clap attributes
- Document configuration options in help text
- Initialize services in correct dependency order
- Implement graceful shutdown handlers
- Add metrics for server lifecycle events

## 6. Forbidden Changes (Must Not)

- Remove required CLI options without migration path
- Change default ports without documentation update
- Skip etcd election in production configurations
- Remove graceful shutdown handling
- Break backward compatibility in `MetaNodeOpts`

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Node tests | `cargo test -p risingwave_meta_node` |
| Integration | `cargo test -p risingwave_meta` (uses node) |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- `clap`: Command-line argument parsing
- `tonic`: gRPC server framework
- `etcd-client`: Distributed coordination
- Internal: `risingwave_meta`, `risingwave_pb`
- gRPC services: Defined in `risingwave_pb`, implemented in `risingwave_meta`

## 9. Overrides

None. Inherits rules from parent `src/meta/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New CLI option added
- Service initialization order changed
- gRPC server configuration modified
- Leader election mechanism updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/meta/AGENTS.md

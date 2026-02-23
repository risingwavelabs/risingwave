# AGENTS.md - src/utils

## 1. Scope

Policies for the utility crates directory containing independent, reusable components used across the RisingWave codebase.

## 2. Purpose

The utils directory contains 10 standalone utility crates providing shared functionality for:
- Data structure extensions (delta_btree_map)
- Async runtime utilities (futures_util, runtime)
- Iterator helpers (iter_util)
- PostgreSQL wire protocol (pgwire)
- System resource monitoring (resource_util)
- Testing primitives (sync-point)
- Server metadata (variables)
- Build configuration (workspace-config)
- External service adapters (openai_embedding_service)

## 3. Structure

```
src/utils/
├── delta_btree_map/          # BTreeMap wrapper with delta/snapshot semantics
│   └── src/lib.rs
├── futures_util/             # Future/stream extensions (crate: rw_futures_util)
│   └── src/
│       ├── lib.rs
│       ├── buffered_with_fence.rs
│       ├── misc.rs
│       └── pausable.rs
├── iter_util/                # Iterator extensions (crate: rw_iter_util)
│   └── src/lib.rs
├── openai_embedding_service/ # OpenAI-compatible embedding HTTP service
│   └── src/
│       ├── lib.rs
│       └── bin/test_client.rs
├── pgwire/                   # PostgreSQL wire protocol implementation
│   └── src/
│       ├── lib.rs
│       ├── pg_protocol.rs
│       ├── pg_server.rs
│       ├── pg_message.rs
│       ├── ldap_auth.rs
│       └── ...
├── resource_util/            # System resource detection (crate: rw_resource_util)
│   └── src/lib.rs
├── runtime/                  # Runtime initialization (crate: risingwave_rt)
│   └── src/
│       ├── lib.rs
│       ├── logger.rs
│       ├── deadlock.rs
│       └── panic_hook.rs
├── sync-point/               # Test synchronization primitives
│   └── src/lib.rs
├── variables/                # Server global variables (crate: risingwave_variables)
│   └── src/lib.rs
└── workspace-config/         # Build-time configuration
    ├── Cargo.toml
    ├── src/lib.rs
    └── README.md
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `*/Cargo.toml` | Per-crate manifest with workspace inheritance |
| `*/src/lib.rs` | Crate root module |
| `pgwire/src/pg_protocol.rs` | Core PostgreSQL protocol handling |
| `runtime/src/logger.rs` | Logging initialization with OpenTelemetry |
| `workspace-config/README.md` | Linking and feature configuration docs |

## 5. Edit Rules (Must)

- Each utility crate must remain independent (no circular dependencies with src/*)
- Use workspace dependency inheritance: `workspace = true` in Cargo.toml
- Crate naming: prefer `rw_*` prefix or `risingwave_*` for public APIs
- Add inline documentation for all public traits and types
- For pgwire changes: test with actual PostgreSQL client (psql, jdbc)
- For runtime changes: verify signal handling (SIGINT, SIGTERM) behavior
- For workspace-config: do NOT add workspace-hack dependency

## 6. Forbidden Changes (Must Not)

- Do not introduce dependencies from utils crates to src/meta, src/stream, src/batch, src/frontend, src/storage
- Do not modify workspace-config dependencies without understanding feature unification impact
- Do not disable sync-point feature without checking all tests using `sync_point!` macro
- Do not add blocking operations in futures_util stream combinators
- Do not break PostgreSQL wire protocol compatibility (startup, authentication, query flow)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Single crate test | `cargo test -p <crate-name>` |
| pgwire tests | `cargo test -p pgwire` |
| sync-point tests | `cargo test -p sync-point --features sync_point` |
| All utils tests | `cargo test -p rw_futures_util -p rw_iter_util -p pgwire -p sync-point -p rw_resource_util` |

## 8. Dependencies & Contracts

- All crates use workspace Rust edition (2021)
- External dependencies: tokio, futures, tracing, serde (workspace-managed)
- pgwire: depends on risingwave_common, risingwave_sqlparser
- runtime: depends on risingwave_common, risingwave_variables
- workspace-config: must only be depended by risingwave_cmd crates
- sync-point: feature-gated (`sync_point` feature must be enabled for use)

## 9. Overrides

None. Follows root AGENTS.md rules for code formatting, clippy, and testing.

## 10. Update Triggers

Regenerate this file when:
- New utility crate added to src/utils/
- Crate renamed or merged
- workspace-config linking features modified
- New pgwire message type added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

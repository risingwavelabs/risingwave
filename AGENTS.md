# AGENTS.md - RisingWave Repository Root

## 1. Scope

Repository-wide policies applicable to all directories unless explicitly overridden.

**IMPORTANT FOR CODING AGENTS:**
- **ALWAYS read the AGENTS.md in your current working directory AND all parent directories up to the repository root**
- Policies are hierarchical: child directory rules override parent rules (nearest-wins)
- If your code changes conflict with AGENTS.md descriptions, you MUST update the AGENTS.md files along the path to reflect the new reality
- AGENTS.md is a living document - keep it synchronized with the actual codebase

## 2. Purpose

RisingWave is a Postgres-compatible streaming database that offers the simplest and most cost-effective way to process, analyze, and manage real-time event data — with built-in support for the Apache Iceberg™ open table format.

This repository contains all source code, tests, documentation, and tooling for the RisingWave streaming database system.

### Component Overview

RisingWave components are developed in Rust and organized into crates:

- **`config`** - Default configurations for servers
- **`prost`** - Generated protobuf Rust code (gRPC and message definitions)
- **`stream`** - Stream compute engine for continuous queries
- **`batch`** - Batch compute engine for ad-hoc queries
- **`frontend`** - SQL planner, binder, and optimizer
- **`storage`** - Cloud-native storage engine (Hummock LSM-tree)
- **`meta`** - Cluster metadata and coordination
- **`utils`** - Independent utility crates
- **`cmd`** - Binary executables
- **`cmd_all`** - All-in-one `risingwave` binary
- **`risedevtool`** - Developer tooling and local cluster management
- **`connector`** - Source and sink connectors
- **`object_store`** - Storage abstraction (S3, GCS, Azure, etc.)
- **`common`** - Shared types and utilities
- **`expr`** - Expression evaluation framework
- **`sqlparser`** - SQL parser (forked from sqlparser-rs)

## 3. Structure

```
risingwave/
├── src/                    # Source code crates
│   ├── config/             # Default server configurations
│   ├── prost/              # Generated protobuf Rust code
│   ├── stream/             # Stream compute engine
│   ├── batch/              # Batch compute engine
│   ├── frontend/           # SQL planner & scheduler
│   ├── storage/            # Cloud-native storage engine
│   ├── meta/               # Metadata & cluster management
│   ├── utils/              # Independent utility crates
│   ├── cmd/                # Binary executables
│   ├── cmd_all/            # All-in-one binary
│   ├── connector/          # Source/sink connectors
│   ├── object_store/       # Storage abstraction
│   ├── common/             # Shared types
│   ├── expr/               # Expression framework
│   ├── sqlparser/          # SQL parser
│   └── risedevtool/        # Developer tooling
├── e2e_test/               # End-to-end SQL logic tests
├── docs/                   # Documentation
├── proto/                  # Protobuf definitions
├── ci/                     # CI/CD configurations
├── docker/                 # Docker configurations
├── dashboard/              # Web UI (Next.js)
├── java/                   # Java components
└── grafana/                # Grafana dashboards
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `Cargo.toml` | Workspace manifest |
| `risedev.yml` | Developer environment configuration |
| `Makefile.toml` | Task runner definitions |
| `AGENTS.md` | This file - agent policies and coding guidelines |
| `./risedev` | Developer tool script (build, test, run) |

## 5. Edit Rules (Must)

### Code Quality
- Write all code comments in English
- Write simple, easy-to-read and easy-to-maintain code
- Run `cargo fmt` before committing Rust code
- Follow existing code patterns in the target module
- Pass `./risedev c` (clippy) before submitting changes
- Add unit tests for new public functions

### Test Updates
- Update planner tests with `./risedev do-apply-planner-test` when query plans change
- Update parser tests with `./risedev update-parser-test` when syntax changes
- Run tests before committing changes to test expectations

### Documentation Sync
**CRITICAL:** When modifying code, check if the changes affect AGENTS.md descriptions:
- If you add/modify test commands → Update Test Entry sections
- If you change architecture → Update Structure sections
- If you modify dependencies → Update Dependencies sections
- If you change build requirements → update Edit Rules sections

### Build & Development Workflow
- Use `./risedev b` to build the project
- Use `./risedev c` to check code with clippy and formatting
- Use `./risedev d` to run a RisingWave instance via tmux (builds if necessary)
- Use `./risedev k` to stop the RisingWave instance
- Use `./risedev psql -c "<query>"` to run SQL queries (requires running instance)
- Use `./risedev slt './path/to/e2e-test-file.slt'` to run end-to-end tests
- Logs are written to `.risingwave/log/` folder

## 6. Forbidden Changes (Must Not)

- Modify protobuf definitions without regenerating prost code
- Delete or modify files in `src/prost/` manually (auto-generated)
- Bypass safety checks with `unsafe` blocks without explicit justification
- Commit secrets, credentials, or API keys
- Modify `./e2e_test/` test expectations without running tests
- Break backward compatibility without migration path
- Submit code that doesn't pass `./risedev c`

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p <crate>` |
| All unit tests | `./risedev test` |
| Parser tests | `./risedev update-parser-test` |
| Planner tests | `./risedev run-planner-test [name]` |
| E2E tests | `./risedev slt './e2e_test/**/*.slt'` |
| Clippy check | `./risedev c` |
| Build check | `./risedev b` |
| Run dev cluster | `./risedev d` |
| Stop dev cluster | `./risedev k` |

### Sandbox Escalation

When sandboxing is enabled, these commands need escalation because they bind or connect to local TCP sockets:
- `./risedev d` and `./risedev p` (uses local ports and tmux sockets)
- `./risedev psql ...` or direct `psql -h localhost -p 4566 ...` (local TCP connection)
- `./risedev slt './path/to/e2e-test-file.slt'` (connects via psql protocol)
- Any health checks or custom SQL clients

## 8. Dependencies & Contracts

- Rust edition 2021
- Workspace-based Cargo structure
- Generated code in `src/prost/` from `proto/` definitions
- External: tokio, tonic, etcd-client, arrow, sqlparser
- Protocol: gRPC for internal communication
- Storage: S3/GCS/Azure/OSS compatible
- Build tool: cargo-make (via Makefile.toml)
- Dev tool: RiseDev (./risedev script)

## 9. Overrides

None. Child directories may override specific rules.

**Override Rules for Child Directories:**
- To override a parent rule, document it in the Overrides section
- Include: "Override parent rule", "Override Reason", "Scope of override"
- Child rules always take precedence (nearest-wins policy)

## 10. Update Triggers

Regenerate this file when:
- Build system changes (risedev commands modified)
- New crate added to workspace
- Protobuf generation process changes
- Testing framework updated
- Architecture changes affecting the structure diagram
- New major components added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.1
- Parent: None (root policy)
- Last Updated: 2025-02-23

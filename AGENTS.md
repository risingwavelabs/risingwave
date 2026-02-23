# AGENTS.md - RisingWave Repository Root

## 1. Scope

Repository-wide policies applicable to all directories unless explicitly overridden.

## 2. Purpose

RisingWave is a Postgres-compatible streaming database with Apache Iceberg support. This repository contains all source code, tests, documentation, and tooling for the RisingWave streaming database system.

## 3. Structure

```
risingwave/
├── src/               # Source code crates
│   ├── config/        # Default server configurations
│   ├── prost/         # Generated protobuf Rust code
│   ├── stream/        # Stream compute engine
│   ├── batch/         # Batch compute engine
│   ├── frontend/      # SQL planner & scheduler
│   ├── storage/       # Cloud-native storage engine
│   ├── meta/          # Metadata & cluster management
│   ├── utils/         # Independent utility crates
│   ├── cmd/           # Binary executables
│   └── risedevtool/   # Developer tooling
├── e2e_test/          # End-to-end SQL logic tests
├── docs/              # Documentation
└── proto/             # Protobuf definitions
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `Cargo.toml` | Workspace manifest |
| `risedev.yml` | Developer environment config |
| `Makefile.toml` | Task runner definitions |
| `AGENTS.md` | This file - agent policies |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Follow existing code patterns in the target module
- Pass `./risedev c` (clippy) before submitting changes
- Add unit tests for new public functions
- Update planner tests with `./risedev do-apply-planner-test` when query plans change
- Update parser tests with `./risedev update-parser-test` when syntax changes

## 6. Forbidden Changes (Must Not)

- Modify protobuf definitions without regenerating prost code
- Delete or modify files in `src/prost/` manually (auto-generated)
- Bypass safety checks with `unsafe` blocks without explicit justification
- Commit secrets, credentials, or API keys
- Modify `./e2e_test/` test expectations without running tests
- Break backward compatibility without migration path

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

## 8. Dependencies & Contracts

- Rust edition 2021
- Workspace-based Cargo structure
- Generated code in `src/prost/` from `proto/` definitions
- External: tokio, tonic, etcd-client, arrow, sqlparser
- Protocol: gRPC for internal communication
- Storage: S3/GCS/Azure/OSS compatible

## 9. Overrides

None. Child directories may override specific rules.

## 10. Update Triggers

Regenerate this file when:
- Build system changes (risedev commands modified)
- New crate added to workspace
- Protobuf generation process changes
- Testing framework updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: None (root policy)

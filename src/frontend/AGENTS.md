# AGENTS.md - RisingWave Frontend

## 1. Scope

Policies for `/home/k11/risingwave/src/frontend` - the SQL query processing layer of RisingWave. This crate handles SQL parsing, binding, optimization, and query planning for both batch and streaming queries.

**Policy Inheritance:**
This file inherits rules from `/home/k11/risingwave/AGENTS.md` (root).
When working in this directory:
1. Read THIS file first for directory-specific rules
2. Then read PARENT files up to root for inherited rules
3. Child rules override parent rules (nearest-wins)
4. If code changes conflict with these docs, update AGENTS.md to match reality

## 2. Purpose

The frontend crate is the stateless SQL processing layer that:
- Parses SQL via `risingwave_sqlparser`
- Binds AST nodes to catalog objects (tables, sources, views)
- Optimizes logical plans through heuristic and cost-based rules
- Generates physical execution plans for batch and stream engines
- Manages sessions and handles DDL/DML statements

## 3. Structure

```
src/frontend/
├── Cargo.toml                # Crate manifest with datafusion feature
├── macro/                    # Procedural macros for frontend
│   ├── Cargo.toml
│   └── src/lib.rs
├── planner_test/             # Golden-file planner tests
│   ├── Cargo.toml
│   └── tests/
│       └── testdata/
│           ├── input/*.yaml  # Test input files
│           └── output/*.plan # Expected plan output
└── src/
    ├── lib.rs                # Frontend node entry point
    ├── session.rs            # Session management
    ├── error.rs              # Top-level error types (RwError)
    ├── meta_client.rs        # Meta service client
    ├── metrics_reader.rs     # Prometheus metrics integration
    ├── telemetry.rs          # Telemetry reporting
    ├── health_service.rs     # Health check endpoints
    ├── catalog/              # Catalog management
    │   ├── catalog_service.rs
    │   ├── root_catalog.rs   # Global catalog view
    │   ├── schema_catalog.rs
    │   ├── table_catalog.rs
    │   └── ...
    ├── binder/               # SQL binding (AST -> Bound AST)
    │   ├── mod.rs
    │   ├── query.rs
    │   ├── expr.rs
    │   └── ...
    ├── planner/              # Logical planning (Bound AST -> LogicalPlan)
    │   ├── mod.rs
    │   ├── query.rs
    │   ├── select.rs
    │   └── ...
    ├── optimizer/            # Query optimization
    │   ├── mod.rs
    │   ├── logical_optimization.rs
    │   ├── optimizer_context.rs
    │   ├── plan_node/        # Plan node definitions (100+ nodes)
    │   ├── rule/             # Optimization rules (80+ rules)
    │   └── property/         # Plan properties (distribution, order)
    ├── handler/              # SQL statement handlers
    │   ├── mod.rs
    │   ├── query.rs
    │   ├── create_*.rs
    │   ├── alter_*.rs
    │   └── ...
    ├── scheduler/            # Query scheduling
    ├── stream_fragmenter/    # Stream plan fragmentation
    ├── user/                 # User management
    └── utils/                # Utility functions
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Frontend node entry and startup |
| `src/session.rs` | Session management and configuration |
| `src/error.rs` | Top-level error type (`RwError`, `ErrorCode`) |
| `src/binder/mod.rs` | SQL binding logic |
| `src/planner/mod.rs` | Logical plan construction |
| `src/optimizer/mod.rs` | Query optimization entry |
| `src/optimizer/plan_node/mod.rs` | Plan node type definitions |
| `src/optimizer/rule/mod.rs` | Optimization rule registry |
| `src/handler/mod.rs` | SQL statement dispatch |
| `planner_test/tests/testdata/input/*.yaml` | Planner test inputs |
| `Cargo.toml` | Crate manifest |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Follow existing patterns in binder/optimizer/handler modules
- Pass `./risedev c` (clippy) before submitting changes
- Add unit tests for new optimization rules in `optimizer/rule/`
- Add planner tests for new SQL features in `planner_test/tests/testdata/input/`
- Update planner tests with `./risedev do-apply-planner-test` when plans change
- Use `RwError` as the error type; add variants to `ErrorCode` if needed
- Maintain backward compatibility for existing SQL syntax

## 6. Forbidden Changes (Must Not)

- Modify planner test output files manually (auto-generated)
- Delete or modify `src/prost/` files (regenerate from proto)
- Add new plan nodes without corresponding `ToStream`/`ToBatch` implementations
- Break existing SQL parsing without parser test updates
- Modify test expectations in `planner_test/` without running tests
- Use `unsafe` blocks without explicit safety justification
- Break query plan determinism (plans must be stable across runs)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_frontend` |
| Planner tests | `./risedev run-planner-test [name]` |
| Update planner tests | `./risedev do-apply-planner-test` |
| Single planner test | `cargo test -p risingwave_planner_test --test planner_test_runner -- [test_name]` |
| Parser tests | `./risedev update-parser-test` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- Rust edition 2021
- Depends on: `risingwave_common`, `risingwave_sqlparser`, `risingwave_pb`, `risingwave_batch`, `risingwave_connector`
- Optional feature: `datafusion` for DataFusion integration
- Internal crate: `risingwave_frontend_macro` for procedural macros
- Planner test crate: `risingwave_planner_test` for golden file testing
- Protocol: gRPC to meta service and compute nodes
- Plan serialization: Protobuf via `ToProtobuf` trait

## 9. Overrides

None. Inherits all rules from repository root AGENTS.md.

## 10. Update Triggers

Regenerate this file when:
- New plan node type added
- New optimization rule category added
- Test framework changes (planner_test structure)
- Major binder/optimizer refactoring
- New handler modules for SQL statements

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

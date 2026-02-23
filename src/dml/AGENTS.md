# AGENTS.md - DML Operations Crate

## 1. Scope

Policies for the `risingwave_dml` crate - Data Manipulation Language operations.

## 2. Purpose

Implements DML (Data Manipulation Language) operations including INSERT, UPDATE, DELETE handling. Provides the DML manager for coordinating data modifications across tables with transaction channel support for streaming DML operations.

## 3. Structure

```
src/dml/
├── src/
│   ├── lib.rs            # Module exports
│   ├── dml_manager.rs    # DML operation coordination
│   ├── table.rs          # Table-level DML operations
│   ├── txn_channel.rs    # Transaction channel for streaming DML
│   └── error.rs          # DML-specific errors
└── Cargo.toml
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/dml_manager.rs` | Central DML operation manager |
| `src/table.rs` | Table-level DML handling |
| `src/txn_channel.rs` | Transaction channel for DML streams |
| `src/error.rs` | DML error definitions |

## 5. Edit Rules (Must)

- Use `DmlManager` for coordinating all DML operations
- Implement proper error propagation via `dml::error`
- Follow streaming DML patterns with `txn_channel`
- Use async/await for DML operations
- Ensure thread safety with `parking_lot` primitives
- Add unit tests for new DML operations
- Run `cargo fmt` before committing

## 6. Forbidden Changes (Must Not)

- Bypass DmlManager for direct table modifications
- Remove transaction channel safety guarantees
- Break DML operation atomicity
- Modify public API without updating dependents
- Use blocking operations in async DML paths

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_dml` |
| DML integration | Test via `risingwave_stream` tests |

## 8. Dependencies & Contracts

- Core: `risingwave_common` for types and utilities
- Async: `tokio` (madsim-tokio), `futures`, `futures-async-stream`
- Sync: `parking_lot` for synchronization
- Used by: `risingwave_stream`, `risingwave_batch`

## 9. Overrides

None. Child directories may override specific rules.

## 10. Update Triggers

Regenerate this file when:
- New DML operation types added
- Transaction channel implementation changes
- DML manager API modifications

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

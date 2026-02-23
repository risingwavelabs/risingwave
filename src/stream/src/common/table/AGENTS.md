# AGENTS.md - State Tables

## 1. Scope

Policies for the `src/stream/src/common/table` directory, containing state table abstractions for checkpoint/restore operations in streaming operators.

## 2. Purpose

This directory provides the core state table interface:
- **StateTable**: Main abstraction for operator state persistence
- **Checkpoint/restore**: Barrier-driven state persistence to storage backend
- **Test utilities**: Mock state tables for unit testing executors

State tables wrap the storage layer to provide epoch-isolated state operations with automatic checkpointing on barriers.

## 3. Structure

```
src/stream/src/common/table/
├── mod.rs                    # Module exports
├── state_table.rs            # Core StateTable implementation
├── test_state_table.rs       # Test-specific state table variants
├── test_storage_table.rs     # Storage table test utilities
└── test_utils.rs             # Common test utilities for state tables
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `state_table.rs` | `StateTable<S>` with insert/delete/scan operations |
| `test_state_table.rs` | `TestStateTable` for executor unit tests |
| `test_storage_table.rs` | Storage-backed test table implementations |
| `test_utils.rs` | Helper functions for state table testing |

## 5. Edit Rules (Must)

- Ensure all state changes respect the current epoch barrier
- Use `StateTable::commit()` on checkpoint barriers
- Handle vnode bitmap changes for scaling operations
- Test with both `MemoryStateStore` and `Hummock` backends
- Maintain correct primary key semantics for state lookups
- Use `StateTable::insert`, `delete`, `update` for state modifications
- Call `init_epoch()` before first state operation
- Run `cargo test -p risingwave_stream state_table` after modifications

## 6. Forbidden Changes (Must Not)

- Modify state table checkpoint semantics without updating all operators
- Remove epoch isolation from state operations
- Change primary key encoding without migration path
- Bypass barrier-driven commit for immediate persistence
- Remove test utilities used by other crates
- Use blocking operations in state table methods

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| State table tests | `cargo test -p risingwave_stream state_table` |
| Storage table tests | `cargo test -p risingwave_stream storage_table` |
| Test utilities | `cargo test -p risingwave_stream test_utils` |

## 8. Dependencies & Contracts

- **Storage backend**: `risingwave_storage::StateStore` (Hummock)
- **Within crate**: Used by all stateful executors via `common/`
- **Epoch protocol**: State changes isolated per barrier epoch
- **Vnode mapping**: Consistent hashing for distributed state
- **Primary keys**: User-defined + vnode prefix for distribution

## 9. Overrides

None. Follows parent AGENTS.md at `./src/stream/src/common/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- StateTable interface changes (new methods)
- Checkpoint protocol modifications
- Primary key encoding changes
- New storage backend integration

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/common/AGENTS.md

# AGENTS.md - Estimate Size Crate

## 1. Scope

Policies for the `risingwave_common_estimate_size` crate providing memory size estimation traits and implementations.

## 2. Purpose

The estimate size crate provides:
- **EstimateSize Trait**: Unified interface for memory size estimation across data structures
- **Collection Implementations**: Size estimation for Vec, HashMap, BTreeMap, etc.
- **Primitive Wrappers**: Size estimation for scalar types and external crate types
- **Derive Macro Support**: `#[derive(EstimateSize)]` via `risingwave_common_proc_macro`

This enables accurate memory accounting for cache management, memory limits, and resource scheduling.

## 3. Structure

```
src/common/estimate_size/
├── Cargo.toml                    # Crate manifest
└── src/
    ├── lib.rs                    # EstimateSize trait and impls
    └── collections/              # Collection-specific implementations
        └── vec.rs                # Vec size estimation details
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `EstimateSize` trait, `ZeroHeapSize` marker, primitive impls |
| `src/collections/` | Size estimation for standard library collections |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Implement `EstimateSize` for all new collection types in the codebase
- Account for both stack and heap allocations in size calculations
- Use `mem::size_of_val` for stack size, traverse heap allocations
- Test size estimates against actual allocator measurements when possible
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Return zero for types with heap allocations (use `ZeroHeapSize` marker explicitly)
- Ignore capacity vs. length distinction in collection sizing
- Break backward compatibility of `EstimateSize` trait interface
- Remove implementations for types used in cache/memory tracking
- Use `std::mem::size_of` instead of `size_of_val` for dynamically sized types
- Forget to update estimates when adding fields to sized structs

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_common_estimate_size` |
| Trait tests | `cargo test -p risingwave_common_estimate_size estimate` |
| Collection tests | `cargo test -p risingwave_common_estimate_size collections` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **educe**: Derive macro support for Debug, Clone, etc.
- **bytes**: Size estimation for `Bytes` type
- **rust_decimal**: Size estimation for `Decimal` type
- **serde_json**: Size estimation for JSON values
- **risingwave_common_proc_macro**: `EstimateSize` derive macro

Contracts:
- `estimate_heap_size()` returns heap-allocated bytes only (not stack)
- `estimate_size()` returns total memory footprint (stack + heap)
- Implementations must be deterministic (no randomness, no I/O)
- Size estimates are approximate; accuracy depends on allocator behavior

## 9. Overrides

None. Follows parent `src/common/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New external crate types need size estimation
- Memory estimation accuracy requirements change
- New collection types added to standard library integration
- Derive macro functionality extended

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/common/AGENTS.md

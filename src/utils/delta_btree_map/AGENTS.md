# AGENTS.md - Delta BTreeMap Crate

## 1. Scope

Policies for the `delta_btree_map` crate providing a BTreeMap wrapper with delta/snapshot semantics.

## 2. Purpose

The delta btree map crate provides:
- **DeltaBTreeMap**: BTreeMap with efficient snapshot isolation
- **Change Tracking**: Track insertions, deletions, and updates
- **Snapshot Views**: Read-consistent views without copying
- **Merge Operations**: Efficiently apply deltas to base maps

This enables incremental view maintenance and materialization in streaming.

## 3. Structure

```
src/utils/delta_btree_map/
├── Cargo.toml                    # Crate manifest
└── src/
    └── lib.rs                    # DeltaBTreeMap implementation
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `DeltaBTreeMap`, `Delta`, `Snapshot` types |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Maintain O(log n) operations for lookups and modifications
- Minimize memory overhead for delta tracking
- Implement `EstimateSize` for memory accounting
- Test correctness against standard BTreeMap behavior
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Break snapshot isolation guarantees
- Introduce O(n) operations in hot paths
- Remove delta compaction without replacement
- Break ordering invariants of underlying BTreeMap
- Add unsafe code without thorough review

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p delta_btree_map` |
| Property tests | `cargo test -p delta_btree_map --release` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **educe**: Derive macros for Debug, Clone, etc.
- **enum-as-inner**: Enum variant accessors

Contracts:
- Snapshots are immutable and read-consistent
- Deltas are applied atomically
- Iterators respect snapshot boundaries
- Memory usage is proportional to delta size, not base size

## 9. Overrides

None. Follows parent `src/utils/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New delta operation type added
- Snapshot semantics changed
- Memory optimization strategy modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/utils/AGENTS.md

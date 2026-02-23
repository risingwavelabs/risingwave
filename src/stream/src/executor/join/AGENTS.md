# AGENTS.md - Streaming Hash Join Implementation

## 1. Scope

Policies for the `src/stream/src/executor/join` directory, containing the streaming hash join state management and core join algorithms for the RisingWave stream engine.

## 2. Purpose

This directory implements the stateful hash join operator for streaming SQL queries:
- Hash join state management with LRU caching (`JoinHashMap`)
- Join entry state tracking with match degrees (`JoinEntryState`)
- Support for all SQL join types: Inner, Left/Right/Full Outer, Left/Right Semi, Left/Right Anti
- AsOf join support for temporal/inequality joins
- Degree tracking for outer join null propagation
- Hybrid row storage (`JoinRowSet`) optimizing small vs large join keys
- Stream chunk building for join output (`JoinStreamChunkBuilder`)

The hash join maintains state in `StateTable` for fault tolerance and supports incremental view maintenance through barrier-based checkpoints.

## 3. Structure

```
src/stream/src/executor/join/
├── mod.rs                    # Join type definitions, JoinType/SideType constants, helper functions
├── hash_join.rs              # Core JoinHashMap, JoinEntryState, state management logic
├── join_row_set.rs           # Hybrid Vec/BTreeMap storage for join rows
├── row.rs                    # JoinRow with degree, JoinEncoding trait, EncodedJoinRow
└── builder.rs                # JoinStreamChunkBuilder, JoinChunkBuilder for output construction
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | Join type primitives: `JoinType::Inner/LeftOuter/...`, `SideType::Left/Right`, helper predicates |
| `hash_join.rs` | `JoinHashMap<K, S, E>` - hash table with LRU cache, `JoinEntryState<E>` - per-key state, degree tracking |
| `join_row_set.rs` | `JoinRowSet<K, V>` - switches from Vec to BTreeMap at MAX_VEC_SIZE (4) elements |
| `row.rs` | `JoinRow<R>` - row with match degree, `JoinEncoding` trait, `MemoryEncoding`/`CpuEncoding` |
| `builder.rs` | `JoinStreamChunkBuilder` - constructs output chunks, `JoinChunkBuilder` - type-safe const generic wrapper |

## 5. Edit Rules (Must)

- Run `cargo test -p risingwave_stream hash_join` after any join modification
- Maintain join degree correctness: degree must equal matched row count for outer joins
- Use `JoinEncoding` trait for row serialization in cache; prefer `MemoryEncoding` for compactness
- Handle `JoinEntryError::Occupied` and `JoinEntryError::Remove` with proper error context
- Update both state table and degree table atomically in `insert`/`delete` operations
- Use `consistency_error!` for non-fatal state mismatches (logs only in non-strict mode)
- Use `consistency_panic!` for fatal invariant violations (panics in strict consistency mode)
- Ensure `JoinRowSet` maintains ordering guarantees when using BTreeMap mode
- Test AsOf join inequality index operations when modifying `JoinEntryState`
- Call `post_process()` on output chunks to eliminate adjacent no-op updates
- Respect `need_degree_table` logic: degrees needed for Full/Left/Right Outer and Semi/Anti joins

## 6. Forbidden Changes (Must Not)

- Remove degree tracking from outer joins (required for NULL propagation)
- Modify `MAX_VEC_SIZE` in `join_row_set.rs` without performance benchmarking
- Bypass `JoinEncoding` trait and directly serialize rows in cache
- Change join type constants without updating all const-generic `JoinChunkBuilder` usages
- Break 1:1 correspondence between state table rows and degree table rows
- Remove `JoinHashMapMetrics` tracking or disable cache hit/miss instrumentation
- Use non-ordered collection for `inequality_index` (AsOf joins require ordering)
- Skip `update_watermark` propagation to state tables
- Modify barrier commit order between state and degree tables
- Remove `enable_strict_consistency()` guards from consistency checks

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Hash join tests | `cargo test -p risingwave_stream hash_join` |
| Join entry state | `cargo test -p risingwave_stream test_managed_join_state` |
| Join row set | `cargo test -p risingwave_stream test_join_row_set` |
| AsOf join tests | `cargo test -p risingwave_stream as_of` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |
| All join tests | `cargo test -p risingwave_stream join` |

## 8. Dependencies & Contracts

- **Parent module**: `executor/` provides `Message`, `Barrier`, `StreamChunk`, `Execute` trait
- **State storage**: Uses `StateTable<S>` from `common/table/` for checkpoint/restore
- **Cache**: Integrates with `ManagedLruCache` from `cache/` for memory management
- **Metrics**: Reports to `StreamingMetrics` for join lookup counts and cache miss rates
- **Consistency**: Uses `enable_strict_consistency()` for state validation strictness
- **Encoding**: `JoinEncoding` trait abstracts row serialization (`CompactedRow` vs `OwnedRow`)

## 9. Overrides

Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New join type added (extends `JoinType` constants)
- AsOf join inequality types modified
- Join encoding strategies changed
- `JoinHashMap` state management logic refactored
- Degree tracking semantics modified
- `JoinRowSet` storage strategy changed
- New join builder patterns introduced

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/executor/AGENTS.md

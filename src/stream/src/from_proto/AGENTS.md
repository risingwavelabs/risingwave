# AGENTS.md - Executor Factory (from_proto)

## 1. Scope

Policies for the `src/stream/src/from_proto` directory, implementing the executor factory pattern for converting protobuf plan nodes into runtime executor instances.

## 2. Purpose

This directory implements the bridge between the frontend's physical plan representation and the streaming runtime executors. Each file contains a builder that deserializes protobuf nodes and constructs the corresponding executor with proper state table configuration, metrics, and operator parameters.

## 3. Structure

```
src/stream/src/from_proto/
├── mod.rs                    # Central dispatcher and ExecutorBuilder trait
├── agg_common.rs             # Shared aggregation builder utilities
├── append_only_dedup.rs      # Append-only deduplication executor builder
├── asof_join.rs              # As-of join executor builder
├── barrier_recv.rs           # Barrier receiver executor builder
├── batch_query.rs            # Batch query executor builder
├── cdc_filter.rs             # CDC filter executor builder
├── changelog.rs              # Change log executor builder
├── dml.rs                    # DML executor builder
├── dynamic_filter.rs         # Dynamic filter executor builder
├── eowc_gap_fill.rs          # EOWC gap fill executor builder
├── eowc_over_window.rs       # EOWC over window executor builder
├── expand.rs                 # Expand executor builder
├── filter.rs                 # Filter executor builder
├── gap_fill.rs               # Gap fill executor builder
├── group_top_n.rs            # Group TopN executor builder
├── hash_agg.rs               # Hash aggregation executor builder
├── hash_join.rs              # Hash join executor builder
├── hop_window.rs             # Hop window executor builder
├── locality_provider.rs      # Locality provider executor builder
├── lookup.rs                 # Lookup executor builder
├── lookup_union.rs           # Lookup union executor builder
├── materialized_exprs.rs     # Materialized expressions executor builder
├── merge.rs                  # Merge executor builder
├── mview.rs                  # Materialized view sink builder
├── no_op.rs                  # No-op executor builder
├── now.rs                    # Now executor builder
├── over_window.rs            # Over window executor builder
├── project.rs                # Project executor builder
├── project_set.rs            # Project set executor builder
├── row_id_gen.rs             # Row ID generator executor builder
├── row_merge.rs              # Row merge executor builder
├── simple_agg.rs             # Simple aggregation executor builder
├── sink.rs                   # Sink executor builder
├── sort.rs                   # Sort executor builder
├── source_backfill.rs        # Source backfill executor builder
├── stateless_simple_agg.rs   # Stateless simple aggregation builder
├── stream_cdc_scan.rs        # CDC scan executor builder
├── stream_scan.rs            # Stream scan executor builder
├── sync_log_store.rs         # Sync log store executor builder
├── temporal_join.rs          # Temporal join executor builder
├── top_n.rs                  # TopN executor builder
├── union.rs                  # Union executor builder
├── upstream_sink_union.rs    # Upstream sink union builder
├── values.rs                 # Values executor builder
├── vector_index_lookup_join.rs  # Vector index lookup join builder
├── vector_index_write.rs     # Vector index write executor builder
├── watermark_filter.rs       # Watermark filter executor builder
├── source/                   # Source-related builders
│   ├── mod.rs                # Source utilities and helpers
│   ├── trad_source.rs        # Traditional source executor builder
│   └── fs_fetch.rs           # Filesystem fetch executor builder
└── approx_percentile/        # Approximate percentile builders
    ├── mod.rs                # Subdirectory module exports
    ├── global.rs             # Global approximate percentile builder
    └── local.rs              # Local approximate percentile builder
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `ExecutorBuilder` trait definition and `create_executor()` dispatcher macro |
| `hash_join.rs` | Complex builder with degree tables and join state configuration |
| `hash_agg.rs` | Aggregation builder with group key and state table setup |
| `source/mod.rs` | Shared source connector configuration utilities |
| `agg_common.rs` | Common aggregation logic for simple_agg, hash_agg, etc. |
| `sink.rs` | Sink executor with connector dispatch |

## 5. Edit Rules (Must)

- Implement `ExecutorBuilder` trait for new executor types
- Register new executors in `mod.rs` `build_executor!` macro dispatch table
- Add `NodeBody::Xxx => XxxExecutorBuilder` entry matching protobuf variant
- Use `ExecutorParams` for accessing inputs, actor context, and operator info
- Handle state table creation via `StateTableBuilder` for stateful operators
- Import from `executor/prelude.rs` for common types
- Ensure protobuf field getters validate required fields
- Use `#[derive(Clone)]` on builder configs only when deep-clone safe
- Update both `NodeBody` pattern match and builder type import in `mod.rs`

## 6. Forbidden Changes (Must Not)

- Modify the `ExecutorBuilder` trait signature without updating all 45+ implementations
- Remove or skip entries from the `build_executor!` dispatch macro
- Use direct protobuf access instead of provided `ExecutorParams` helpers
- Create executors without proper state table configuration for stateful operators
- Bypass the dispatcher by calling builder constructors directly outside tests
- Modify `NodeBody` enum without regenerating protobuf code first
- Use `unwrap()` on protobuf fields without proper error handling

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream from_proto` |
| Specific builder | `cargo test -p risingwave_stream hash_join` |
| All stream tests | `cargo test -p risingwave_stream` |

## 8. Dependencies & Contracts

- **Builder trait**: `ExecutorBuilder::new_boxed_executor()` returns `StreamResult<Executor>`
- **Protobuf source**: `risingwave_pb::stream_plan::stream_node::NodeBody` enum variants
- **State store**: All builders receive `impl StateStore` for state table construction
- **Executor params**: Input executors, actor context, vnode bitmap via `ExecutorParams`
- **Dependencies**: `from_proto/` -> `executor/`, `common/table/`, `task/`

## 9. Overrides

None. Follows parent AGENTS.md at `./src/stream/src/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New executor type added with corresponding builder
- `ExecutorBuilder` trait signature changes
- `NodeBody` protobuf enum variants change
- New subdirectory added for complex executor families
- Dispatcher macro pattern changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/AGENTS.md

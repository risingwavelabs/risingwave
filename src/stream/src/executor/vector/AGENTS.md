# AGENTS.md - Vector Operations

## 1. Scope

Policies for the `src/stream/src/executor/vector` directory, containing vector index operations for approximate nearest neighbor search.

## 2. Purpose

The vector module provides streaming support for vector similarity search:
- Vector index write operations for incremental index building
- Vector index lookup join for similarity-based joins
- Integration with vector search infrastructure

These executors enable real-time vector search use cases such as semantic search, recommendation systems, and RAG (Retrieval-Augmented Generation) pipelines.

## 3. Structure

```
src/stream/src/executor/vector/
├── mod.rs                    # Module exports
├── index_writer.rs           # VectorIndexWriteExecutor for index updates
└── index_lookup_join.rs      # VectorIndexLookupJoinExecutor for similarity joins
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `index_writer.rs` | Executor for writing vector embeddings to indexes incrementally |
| `index_lookup_join.rs` | Executor for joining streams with vector index similarity search |

## 5. Edit Rules (Must)

- Handle vector dimension validation before index operations
- Ensure proper error handling for malformed vector data
- Support both INSERT and DELETE operations for index maintenance
- Use appropriate distance metrics (cosine, euclidean, dot product)
- Handle barrier commits for index persistence
- Validate vector column types match index configuration

## 6. Forbidden Changes (Must Not)

- Do not modify vector index schemas without migration plans
- Never bypass dimension validation for vector inputs
- Do not use blocking vector index operations in async contexts
- Avoid unbounded memory growth for vector buffering
- Never skip error handling for vector parsing failures

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream vector` |
| Vector index tests | `cargo test -p risingwave_stream index_writer` |
| Lookup join tests | `cargo test -p risingwave_stream index_lookup_join` |

## 8. Dependencies & Contracts

- **Index backend**: Integration with RisingWave vector index storage
- **Input**: Vector columns as fixed-dimension float arrays
- **Operations**: INSERT for adding vectors, DELETE for removal
- **Similarity**: Configurable distance metrics per index

## 9. Overrides

Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New vector index types added
- Vector similarity algorithms updated
- New vector operations (UPDATE, UPSERT) supported
- Vector dimension constraints changed

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/executor/AGENTS.md

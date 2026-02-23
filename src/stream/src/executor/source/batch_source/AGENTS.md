# AGENTS.md - Batch Source Executors

## 1. Scope

Policies for `src/stream/src/executor/source/batch_source` - batch source executors for refreshable and file-based data sources in RisingWave's streaming engine.

## 2. Purpose

This directory implements batch-oriented source executors for processing discrete data sets:

- **File source processing**: List and fetch files from object storage (S3, GCS, Azure, POSIX FS)
- **Iceberg integration**: List and read Iceberg table snapshots with schema evolution support
- **Adbc Snowflake**: Batch data extraction from Snowflake via ADBC protocol
- **Feature gating**: Compile-time feature flags to reduce binary size and build time
- **Two-phase architecture**: List executors discover files, fetch executors read content

These executors enable RisingWave to ingest batch data alongside streaming sources.

## 3. Structure

```
src/stream/src/executor/source/batch_source/
├── mod.rs                              # Module exports and feature-gating macro
├── batch_posix_fs_list.rs              # POSIX filesystem file listing
├── batch_posix_fs_fetch.rs             # POSIX filesystem file content reading
├── batch_iceberg_list.rs               # Iceberg table snapshot file listing
├── batch_iceberg_fetch.rs              # Iceberg data file reading
├── batch_adbc_snowflake_list.rs        # Snowflake table listing (gated)
└── batch_adbc_snowflake_fetch.rs       # Snowflake data fetching (gated)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `feature_gated_executor_mod!` macro for compile-time feature selection |
| `batch_posix_fs_list.rs` | `BatchPosixFsListExecutor` for local filesystem discovery |
| `batch_posix_fs_fetch.rs` | `BatchPosixFsFetchExecutor` for reading local files |
| `batch_iceberg_list.rs` | `BatchIcebergListExecutor` for Iceberg snapshot scanning |
| `batch_iceberg_fetch.rs` | `BatchIcebergFetchExecutor` for reading Iceberg data files |
| `batch_adbc_snowflake_*.rs` | Snowflake batch source (requires `source-adbc_snowflake` feature) |

## 5. Edit Rules (Must)

- Use `feature_gated_executor_mod!` macro for new source types with heavy dependencies
- Implement `Execute` trait: `fn execute(self: Box<Self>) -> BoxedMessageStream`
- Handle `Barrier` messages correctly for checkpoint/restore
- Use `StreamSourceCore` for state management and split tracking
- Apply rate limiting via `apply_rate_limit` for backpressure control
- Support schema evolution where applicable (especially Iceberg)
- Implement proper error handling with retry logic for transient failures
- Update `mod.rs` exports when adding new batch source types
- Run `cargo test -p risingwave_stream batch_source` after modifications
- Test with and without feature flags (`--features source-<name>`)

## 6. Forbidden Changes (Must Not)

- Remove feature gating for heavy dependencies without performance review
- Use blocking IO in async executor contexts
- Skip barrier handling in batch source executors
- Remove support for existing source types without deprecation period
- Break schema evolution support for Iceberg sources
- Use `Expression::eval` directly (use `NonStrictExpression::eval_infallible`)
- Bypass rate limiting in file reading operations
- Ignore split assignment changes from meta service

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Batch source tests | `cargo test -p risingwave_stream batch_source` |
| POSIX FS tests | `cargo test -p risingwave_stream posix_fs` |
| Iceberg tests | `cargo test -p risingwave_stream iceberg` |
| Feature-gated tests | `cargo test -p risingwave_stream --features source-adbc_snowflake` |
| Source executor tests | `cargo test -p risingwave_stream source_executor` |

## 8. Dependencies & Contracts

- **Source core**: `StreamSourceCore<S>` for state management
- **Connector**: `risingwave_connector` for source implementations
- **Feature flags**: `source-adbc_snowflake`, `source-iceberg` for conditional compilation
- **State store**: `StateTable` for split and offset persistence
- **Barrier protocol**: Must handle all `BarrierKind` variants
- **Actor context**: `ActorContextRef` for metrics and progress reporting

## 9. Overrides

None. Follows parent AGENTS.md at `./src/stream/src/executor/source/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New batch source type added
- Feature gating mechanism changed
- Source core interface updated
- New file format or storage backend supported
- Split assignment protocol changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/source/AGENTS.md

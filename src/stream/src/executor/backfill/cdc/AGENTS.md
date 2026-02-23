# AGENTS.md - CDC Backfill

## 1. Scope

Policies for the `src/stream/src/executor/backfill/cdc` directory, containing Change Data Capture backfill executors for external database synchronization.

## 2. Purpose

This directory implements CDC backfill for external databases:
- **CdcBackfillExecutor**: Single-threaded CDC backfill from MySQL, Postgres, etc.
- **ParallelizedCdcBackfillExecutor**: Parallel CDC backfill (v2) for improved throughput
- **State management**: CDC backfill state persistence (v1 and v2 schemas)
- **External table interface**: Reading snapshots and CDC events from upstream

CDC backfill synchronizes historical snapshot data with real-time CDC events for materialized views on external tables.

## 3. Structure

```
src/stream/src/executor/backfill/cdc/
├── mod.rs                    # Module exports
├── cdc_backfill.rs           # Single-threaded CDC backfill executor
├── cdc_backill_v2.rs         # Parallelized CDC backfill executor (v2)
├── state.rs                  # CDC backfill state (v1)
├── state_v2.rs               # CDC backfill state (v2)
└── upstream_table/           # External table reading interface
    ├── mod.rs                # Module exports
    ├── external.rs           # ExternalStorageTable interface
    └── snapshot.rs           # Snapshot reading from external tables
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `cdc_backfill.rs` | `CdcBackfillExecutor` for initial CDC backfill |
| `cdc_backill_v2.rs` | `ParallelizedCdcBackfillExecutor` for parallel backfill |
| `state.rs` | `CdcBackfillState` persistence (v1 schema) |
| `state_v2.rs` | `CdcBackfillState` persistence (v2 schema) |
| `upstream_table/external.rs` | `ExternalStorageTable` for upstream DB access |

## 5. Edit Rules (Must)

- Handle `CdcOffset` correctly - parse and serialize consistently
- Use `ExternalStorageTable` for snapshot and CDC event reading
- Support both state schema versions during migrations
- Call `mark_cdc_chunk()` for upstream data deduplication
- Report progress via `CreateMviewProgressReporter`
- Use rate limiting for snapshot reads
- Handle all `BarrierKind` variants for state persistence
- Run `cargo test -p risingwave_stream cdc_backfill` after modifications

## 6. Forbidden Changes (Must Not)

- Modify `CdcOffset` serialization without migration path
- Remove `backfill_finished` state checks
- Bypass rate limiting in external table reads
- Use blocking IO in CDC external table readers
- Change state table primary key structure without migration
- Skip progress reporting during backfill

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| CDC backfill tests | `cargo test -p risingwave_stream cdc_backfill` |
| CDC tests | `cargo test -p risingwave_stream cdc` |
| Backfill tests | `cargo test -p risingwave_stream backfill` |

## 8. Dependencies & Contracts

- **Parent module**: `executor/backfill/` provides utilities
- **CDC connector**: `risingwave_connector::source::cdc` for database access
- **State versions**: Support v1 and v2 state schemas
- **External tables**: MySQL, Postgres, MongoDB, SQL Server, Citus
- **Metrics**: `CdcBackfillMetrics` for observability

## 9. Overrides

None. Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/executor/backfill/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New CDC source type added
- CDC backfill state schema changes (v3)
- Parallelization strategy modified
- External table interface updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/executor/backfill/AGENTS.md

# AGENTS.md - CDC Upstream Table Interface

## 1. Scope

Policies for `src/stream/src/executor/backfill/cdc/upstream_table` - the external table interface for CDC backfill operations, enabling snapshot reads and CDC event consumption from external databases.

## 2. Purpose

This directory provides the abstraction layer for CDC backfill to interact with external databases:

- **External table abstraction**: `ExternalStorageTable` represents upstream database tables
- **Snapshot reading**: Reads historical table snapshots for initial backfill
- **CDC event consumption**: Receives real-time change events from upstream
- **Metadata mapping**: Maps external table schemas to RisingWave internal representations
- **Connection management**: Manages connections to MySQL, Postgres, MongoDB, SQL Server, Citus

The upstream table interface enables RisingWave to synchronize data from external OLTP databases for materialized views.

## 3. Structure

```
src/stream/src/executor/backfill/cdc/upstream_table/
├── mod.rs                # Module exports: ExternalStorageTable
├── external.rs           # ExternalStorageTable struct and implementation
└── snapshot.rs           # Snapshot reading from external tables
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | Module exports, re-exports `ExternalStorageTable` |
| `external.rs` | `ExternalStorageTable` struct with schema, PK info, and reader access |
| `snapshot.rs` | `UpstreamTable` trait for snapshot iteration and CDC offset management |

## 5. Edit Rules (Must)

- Maintain `ExternalStorageTable` fields for schema, PK indices, and connection config
- Use `ExternalTableReader` from `risingwave_connector` for database access
- Handle `CdcOffset` correctly for MySQL (binlog position), Postgres (LSN), etc.
- Support schema evolution detection where possible
- Use `SchemaTableName` for qualified table references (`schema.table`)
- Implement rate limiting for snapshot reads via `RateLimiter`
- Handle connection failures with exponential backoff retry
- Update `snapshot.rs` when adding new CDC source types
- Run `cargo test -p risingwave_stream cdc_backfill` after modifications

## 6. Forbidden Changes (Must Not)

- Modify `CdcOffset` structure without migration path
- Remove `ExternalStorageTable` fields without updating all constructors
- Change snapshot iteration semantics without considering recovery
- Bypass rate limiting in external table reads
- Use blocking IO in async snapshot readers
- Remove support for existing CDC database types without deprecation
- Skip schema validation for external tables
- Break backward compatibility in `ExternalTableConfig`

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| CDC backfill tests | `cargo test -p risingwave_stream cdc_backfill` |
| Upstream table tests | `cargo test -p risingwave_stream upstream_table` |
| External table tests | `cargo test -p risingwave_stream external_table` |
| Snapshot tests | `cargo test -p risingwave_stream snapshot` |
| Integration tests | `cargo test -p risingwave_stream --test integration_tests` |

## 8. Dependencies & Contracts

- **CDC connector**: `risingwave_connector::source::cdc::external` for database readers
- **CdcOffset**: Database-specific position tracking (MySQL binlog, Postgres LSN)
- **ExternalTableConfig**: Connection parameters and authentication
- **SchemaTableName**: Qualified table name with schema and table components
- **State store**: For persisting backfill progress
- **Rate limiting**: `risingwave_common_rate_limit::RateLimiter` for backpressure

## 9. Overrides

None. Follows parent AGENTS.md at `./src/stream/src/executor/backfill/cdc/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New CDC database type added (e.g., Oracle, SQL Server enhancements)
- External table interface changes
- CdcOffset format modifications
- New connection parameters required
- Snapshot reading semantics updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/backfill/cdc/AGENTS.md

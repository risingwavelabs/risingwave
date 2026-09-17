# Iceberg streaming updates

`streaming_updates = 'true'` opts a non-shared Iceberg `CREATE SOURCE` into
Insert/Delete ingestion. Without this option, the existing append-only source
behavior is unchanged.

```sql
CREATE SOURCE iceberg_updates
WITH (
    connector = 'iceberg',
    streaming_updates = 'true',
    catalog.type = 'rest',
    catalog.uri = 'https://catalog.example.com',
    database.name = 'analytics',
    table.name = 'events'
);
```

Supply the catalog, storage, and authentication options required by your
deployment. Do not specify a schema or `PRIMARY KEY`: both are inferred from the
Iceberg table and its writer contract.

## Prerequisites and rollout

This mode is intended only for tables produced by a compatible RisingWave
PK-index writer. It is not a general CDC reader for arbitrary Iceberg writers.
It supports V2 file-scoped Parquet position deletes and V3 deletion vectors, not
equality deletes.

The table must advertise the supported versioned writer contract and the actual,
ordered sink key. Every consumed snapshot must carry its data/compaction marker.
Unmarked old tables and snapshots fail closed. Setting properties manually does
not make an incompatible writer safe.

Automatic writer-contract publication and coordinated writer rollout are separate
work. They are not enabled by this source option. In particular, all writers must
guarantee that a compaction commit contains no ordinary logical changes. The
current guard rejects mixed commits; that guard alone is not a writer liveness
guarantee.

## Schema and ordering

- The source uses the complete stored sink key, including nullable fields and
  stored hidden keys. It does not generate a new serial row ID.
- The source catalog exposes only stored logical fields. Reader-generated
  `_iceberg_file_path`, `_iceberg_file_pos`, and `_iceberg_sequence_number`
  cannot be referenced in SQL, including qualified names, filters, groups, and
  joins. Batch delete processing may use these fields internally without
  exposing them in the catalog or query result.
- List binds the bootstrap snapshot once and checkpoints its binding and task
  progress. Recovery does not rebind to the latest snapshot.
- For each subsequent snapshot, parallel Deletes complete at a globally
  committed checkpoint before Inserts are issued. New files' same-commit
  deleted positions only filter Inserts; they never generate Deletes.
- Incremental pure compaction emits no rows and does not reread row artifacts.
  Bootstrap still imports the visible rows of a compaction snapshot.

Results converge to the Iceberg table contents. Intermediate logical states are
visible; this is not a business transaction log or an atomic snapshot refresh.

## Restrictions

Retain snapshots, manifests, data files, and delete artifacts long enough for
lagging jobs and recovery to reread them. Missing history, rollback, changed
table identity/schema/key, unsupported artifacts, and resource-budget violations
produce errors rather than a silent resnapshot.

This initial opt-in does not support `CREATE TABLE`, shared sources, refresh,
`INCLUDE` columns, source watermarks, source rate limiting, or streaming
time-travel queries. Changing `streaming_updates` with `ALTER` is rejected.
Create a new source to change modes. Existing append-only, batch, and refresh
behavior for sources without the option remains unchanged.

Multi-actor Hummock/network fault testing, real coordinated-compactor integration,
and performance acceptance remain separate from the local planner, reader, and
checkpoint regression tests.

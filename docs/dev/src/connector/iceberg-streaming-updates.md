# Iceberg streaming updates

`streaming_updates = 'true'` opts a non-shared Iceberg source or table into
Insert/Delete ingestion. Without this option, the existing append-only source
behavior is unchanged.

```sql
CREATE TABLE iceberg_updates (PRIMARY KEY (id))
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
deployment. The schema is inferred from Iceberg. Specify the logical `PRIMARY KEY`
in SQL, as for other retracting sources. It must uniquely identify the rows written
by the PK-index sink.

## Supported tables

This mode is intended only for tables produced by a RisingWave
PK-index writer. It is not a general CDC reader for arbitrary Iceberg writers.
It supports V2 file-scoped Parquet position deletes and V3 deletion vectors, not
equality deletes.

Setting `streaming_updates = 'true'` asserts that the table was written by the
RisingWave PK-index sink. Iceberg snapshot `operation` is authoritative:
`replace` is a pure compaction, while `append`, `overwrite`, and `delete` represent
logical changes. Pure compaction uses `replace`; epochs combining compaction and
ordinary writes keep `overwrite`. Logical changes are read from file differences,
including retractions of visible rows from removed files.

## Schema and ordering

- The source uses the SQL primary key, including stored hidden columns when
  explicitly selected as key fields. It does not generate a new serial row ID.
- The source catalog exposes only stored logical fields. Reader-generated
  `_iceberg_file_path`, `_iceberg_file_pos`, and `_iceberg_sequence_number`
  cannot be referenced in SQL.
- List binds the bootstrap snapshot once and checkpoints its binding and task
  progress. Recovery does not rebind to the latest snapshot.
- List dispatches bounded task pages from a lazy manifest scan, without
  rebuilding the snapshot plan or waiting for Fetch between pages. It retains
  the active scan across pages and checkpoints the last dispatched task.
  Recovery reconstructs the deterministic scan and skips its dispatched prefix.
  Fetch removes each task at EOF, checkpointing its retirement together with its output.
- Before switching from Delete to Insert, or applying a snapshot, List
  asynchronously checks that the committed Fetch queue is empty. The checkpoint
  must cover the phase's last task dispatch; an older empty queue is not proof of
  completion. This preserves Delete-before-Insert across actors without dedicated
  task reports through Meta. Phase transitions depend on checkpoint commit latency,
  but waiting never blocks ordinary barriers. New files' same-commit deleted
  positions only filter Inserts; they never generate Deletes.
- Incremental pure compaction emits no rows and does not reread row artifacts.
  Bootstrap still imports the visible rows of a compaction snapshot.

Results converge to the Iceberg table contents. Intermediate logical states are
visible; this is not a business transaction log or an atomic snapshot refresh.

## Restrictions

Retain snapshots, manifests, data files, and delete artifacts long enough for
lagging jobs and recovery to reread them. Missing history, rollback, changed
table identity/schema, and unsupported artifacts
produce errors rather than a silent resnapshot.

`streaming.developer.iceberg_fetch_batch_size` controls both the number of tasks
per List page and the number of pending tasks loaded per Fetch batch. Pagination
does not bound snapshot metadata memory usage: each phase indexes parent files
and delete descriptors and decodes one current data manifest at a time.

This initial opt-in does not support shared sources, refresh,
`INCLUDE` columns, source watermarks, source rate limiting, or streaming
time-travel queries. Changing `streaming_updates` with `ALTER` is rejected.
Create a new source to change modes. Existing append-only, batch, and refresh
behavior for sources without the option remains unchanged.

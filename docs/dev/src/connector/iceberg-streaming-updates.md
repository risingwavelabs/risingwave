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
logical changes. The sink commits compaction as `replace`, and compaction epochs
never carry ordinary writes, so a `replace` snapshot contains no logical change.
Logical changes are read from file differences, including retractions of visible
rows from removed files.

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
- List plans all pending snapshots up to the next compaction as one net change
  from the last applied snapshot to the last snapshot of the range. Each range
  has a Delete phase followed by an Insert phase. A file added and removed
  within the range produces no task, and repeated updates of one key produce at
  most one Delete and one Insert. Intermediate snapshots of a range are never
  materialized. A `replace` snapshot always forms its own range, because a
  compaction contains no logical change and is applied by moving the baseline.
- Fetch keeps its tasks in a state table keyed by `(route, seq)` and
  distributed by `route`. List assigns `seq` in dispatch order, and Fetch reads
  the pending tasks of one route in `seq` order.
- A phase whose tasks fit in one page of at most
  `streaming.developer.iceberg_update_ordered_max_tasks` tasks uses the ordered
  route: all its tasks go to the single Fetch actor that owns that route. That
  actor already applies Delete before Insert and one snapshot range after
  another, so List starts the next phase immediately, without waiting for a
  checkpoint.
- A larger phase fans out: each task gets its own route, spreading the phase
  across Fetch actors. Before starting the next phase, List asynchronously
  checks that the committed Fetch queue is empty. The checkpoint must cover the
  phase's last task dispatch; an older empty queue is not proof of completion.
  A fan-out phase also waits for outstanding ordered tasks to drain first. This
  preserves Delete-before-Insert across actors without dedicated task reports
  through Meta. Only fan-out phase transitions depend on checkpoint commit
  latency, and waiting never blocks ordinary barriers.
- New files' same-commit deleted positions only filter Inserts; they never
  generate Deletes.
- Incremental pure compaction emits no rows and does not reread row artifacts.
  Bootstrap still imports the visible rows of a compaction snapshot.

Results converge to the Iceberg table contents. Intermediate logical states
between a range's Delete and Insert phases are visible; this is not a business
transaction log or an atomic snapshot refresh.

## Restrictions

Retain snapshots, manifests, data files, and delete artifacts long enough for
lagging jobs and recovery to reread them. Missing history, rollback, changed
table identity/schema, and unsupported artifacts
produce errors rather than a silent resnapshot.

`streaming.developer.iceberg_fetch_batch_size` controls both the number of tasks
per List page and the number of pending tasks loaded per Fetch batch.
`streaming.developer.iceberg_update_ordered_max_tasks` (default 16) is the
largest phase sent to the ordered route; `0` fans out every phase. Raising it
trades Fetch parallelism for fewer checkpoint waits. When List finds no new work, it waits `streaming.developer.iceberg_list_interval_sec` before polling the
catalog again.

Planning cost follows the size of the change, not the size of the table.
Bootstrap enumerates the whole snapshot: it indexes all delete descriptors, then
decodes one data manifest at a time. Incremental phases diff the manifest lists
of the range's base and target snapshots by manifest path and never read a
manifest present in both lists for the diff itself:

- Insert reads only the added data and delete manifests.
- Delete reads the removed and added manifests. It also reads an unchanged data
  manifest only when a changed delete artifact may apply to one of its files,
  pruned by partition summary, and an unchanged delete manifest only to find the
  parent delete artifact of a removed file.

Pagination does not bound the metadata held by one phase: the descriptors of the
changed delete manifests are indexed in memory.

This initial opt-in does not support shared sources, refresh,
`INCLUDE` columns, source watermarks, source rate limiting, or streaming
time-travel queries. Changing `streaming_updates` with `ALTER` is rejected.
Create a new source to change modes. Existing append-only, batch, and refresh
behavior for sources without the option remains unchanged.

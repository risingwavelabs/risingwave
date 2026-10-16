# Iceberg E2E Tests

## Prerequisites

- Java 17 or 21 (required by Spark 4.0)
- [Poetry](https://python-poetry.org/) (Python package manager)

## Running Tests Locally

### 1. Start RisingWave with Minio

```bash
./risedev k; ./risedev clean-data && ./risedev d for-ctl && ./risedev mc mb -p hummock-minio/icebergdata
```

Always restart with this full sequence: `for-ctl` uses an in-memory meta store, so a restart against stale minio data panics the meta node with `Data directory is already used by another cluster`.

### 2. Start Spark Connect Server

```bash
cd e2e_test/iceberg
bash start_spark_connect_server.sh
```

This downloads Spark if needed, resolves Maven packages, and starts the server on port 15002.

If a long-running server fails writes with `Could not find any valid local directory for s3ablock`, macOS cleaned its /tmp buffer dir (and pid file, so the stop script no longer works). Kill it by port and restart:

```bash
lsof -ti :15002 | xargs kill
bash start_spark_connect_server.sh
```

### 3. Run Tests

In `e2e_test/iceberg` (`poetry install --quiet` on first run):

```bash
# single test case
poetry run python main.py -t ./test_case/no_partition_append_only.toml
# all test cases
poetry run python main.py
```

Pure SLT tests, from the repo root:

```bash
./risedev slt './e2e_test/iceberg/test_case/pure_slt/*.slt'
```

`iceberg_engine.slt` needs the Java connector libs (`ENABLE_BUILD_RW_CONNECTOR=true` in `risedev-components.user.env`); without them, `CREATE TABLE ... ENGINE = iceberg` fails with `failed to read connector libs`.

### 4. Cleanup

```bash
# from e2e_test/iceberg
./spark-*/sbin/stop-connect-server.sh
# from the repo root
cd ../.. && ./risedev k
```

## CI

CI runs `ci/scripts/e2e-iceberg-test.sh`. The main suite uses the `ci-iceberg-test` profile, which needs an external postgres for the meta store and prebuilt connector libs downloaded from CI artifacts. The steps above are the local equivalent.

### Multi-batch compaction rounds

CI runs `compaction_round.slt` and `pk_index_compaction_round.slt` first in the
`ci-iceberg-compaction-round` profile, with one compactor worker and a one-plan
admission budget. They check that scheduled compaction rewrites all three partitions
across multiple tasks without further sink commits, excludes earlier batches' output,
and drains. The PK-index test also checks updates, deletes, and reinsertion after
compaction. Both tests require this profile: with a larger admission budget, PK-index
compaction can combine several partition rewrites into one snapshot.

The PK-index test also covers `ALTER TABLE ... CONNECTOR WITH (...)` for compaction
scheduling and maintenance options, including activation without new writes.
It also reattaches a table with existing snapshots and verifies that reconciliation
finishes without new ingestion after the independent job becomes ready.
Alter the table, not its internal sink. Online changes to `commit_checkpoint_interval`
and `compaction.write_parquet_*` are currently rejected for PK-index sinks because
their running writers do not reload these options.
The ordinary-sink knobs `compaction.max_snapshots_num` and
`compaction.delete_files_count_threshold` are also rejected by this ALTER path;
PK-index compaction uses `compaction.delete_ratio_percent` for delete selection.

These tests only need MinIO and the connector libs. Start with fresh RiseDev data,
as the profile uses an in-memory meta store:

```sh
./risedev d ci-iceberg-compaction-round
./risedev slt './e2e_test/iceberg/compaction_round.slt'
./risedev slt './e2e_test/iceberg/pk_index_compaction_round.slt'
./risedev k
```

### PK-index compaction regressions

The pure SLT suite includes `iceberg_pk_index/compaction_*.slt`. These use V2
Parquet position deletes and read directly from Iceberg to check compaction,
deletion, and reinsertion. They need MinIO and connector libs, but not Spark.

Simulation coverage uses V3 deletion vectors and injects failures before the
independent job's End barrier and after its durable commit. Only the compactor
rewrite is simulated; resolver, checkpoint, commit, and recovery paths are real:

```sh
./risedev sit-test iceberg_pk_index
```

## Key Config Files

| File | Description |
|------|-------------|
| `e2e_test/commands/common.sh` | Spark/Iceberg versions and `SPARK_PACKAGES` |
| `e2e_test/iceberg/pyproject.toml` | Python (pyspark) dependencies |
| `e2e_test/iceberg/config.ini` | Spark connect URL and RisingWave connection |
| `e2e_test/iceberg/start_spark_connect_server.sh` | Spark server startup script |

## Test Case Format

- **`.toml` files** in `test_case/` — Orchestrated by `main.py`. Uses Spark to create Iceberg tables, RisingWave SLT for sink/source operations, and Spark for result verification.
- **`.slt` files** in `test_case/pure_slt/` — Pure SQL logic tests, run directly via `risedev slt`.

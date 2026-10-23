# Iceberg streaming updates integration smoke test

Exercises the full RisingWave PK-index sink → Iceberg → streaming source path.
The Python standard-library fixture prepares an empty table and verifies catalog
metadata. The SLT runs the real writer and checks SQL results, then removes its
SQL objects and external table.

## Prerequisites

Start these services before running the test:

- A RisingWave cluster containing streaming-update support and PK-index sinks.
  Its compute nodes need access to the catalog and object storage.
- A Lakekeeper REST catalog with a configured warehouse, reachable without
  authentication by the fixture.
- S3-compatible storage accessible with explicit test credentials.
- Python 3 and the tools required by `./risedev slt`.

Use a disposable test environment. No personal RiseDev profile is required.
The runner does not start, stop, or reset services. For a RiseDev-managed cluster,
start an appropriate profile first so `./risedev slt` can load its connection
configuration. Additional SQLLogicTest connection flags can be passed to `run.sh`.

## Run

Set the endpoints, warehouse and credentials for your test environment:

```sh
export ICEBERG_TEST_CATALOG_URI='http://<catalog-host>:8181/catalog'
export ICEBERG_TEST_WAREHOUSE='<warehouse-name>'
export ICEBERG_TEST_S3_ENDPOINT='http://<storage-host>:9000'
export ICEBERG_TEST_S3_ACCESS_KEY='<test-access-key>'
export ICEBERG_TEST_S3_SECRET_KEY='<test-secret-key>'
bash integration_tests/iceberg-streaming-updates/run.sh
```

The runner works from any working directory when invoked by its path. By default
it runs V2 and V3 sequentially, creating a fresh `rw_update_e2e_<uuid>` table for
each. Set `ICEBERG_TEST_FORMAT_VERSION=2` or `3` to run only that format.
`ICEBERG_TEST_PYTHON` optionally selects the Python executable (default: `python3`).
The test uses S3 region `us-east-1` and path-style access.

The SLT uses fixed SQL object names. Do not run concurrent instances against the
same database; use a clean, dedicated test database for retries after failure.

## Coverage

The real PK-index sink writes its data/delete artifacts and `overwrite` snapshots
through the native `rest_rust` catalog. SQL declares the ingestion primary key.

The test checks rows and aggregates through both `CREATE SOURCE` and `CREATE TABLE`
ingestion at parallelism 2, covering bootstrap, update, delete, and same-key
reinsertion. It also checks V2 position-delete / V3 deletion-vector file formats
and the absence of equality deletes.

Compaction and snapshot expiration are disabled. This smoke test does not cover
compaction or fault recovery.

## Cleanup and CI

Successful runs remove their SQL objects and request deletion of the unique
external table. On failure, preserve the generated table name and inspect the
cluster logs before cleaning only that run's objects. The runner prints the
format and unique table name but never credentials. It stops on the first failure
and preserves the failed run's resources for diagnosis. Do not use a broad
`clean-data` or `slt-clean` against an existing development environment.

This test is not yet wired into integration CI. Moving it here does not add CI
coverage; a future CI job must provision the services above and call `run.sh`.

# Streaming update source, real writer smoke test

Start `./risedev d silver`. Run from the repository root, once for each format:

```sh
export ICEBERG_TEST_CATALOG_URI=http://127.0.0.1:8181/catalog
export ICEBERG_TEST_WAREHOUSE=risingwave-warehouse
export ICEBERG_TEST_S3_ENDPOINT=http://127.0.0.1:9301
# Public local-development credentials from the risedev MinIO profile.
export ICEBERG_TEST_S3_ACCESS_KEY=hummockadmin
export ICEBERG_TEST_S3_SECRET_KEY=hummockadmin
export ICEBERG_TEST_PYTHON="$(command -v python3)"
for version in 2 3; do
  export ICEBERG_TEST_FORMAT_VERSION="$version"
  export ICEBERG_TEST_TABLE="rw_update_e2e_$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
  ./risedev slt -u root -j 1 e2e_test/iceberg/manual/streaming_updates.slt || break
done
```

The fixture creates a **new empty** Iceberg table with the versioned contract.
It uses only Python's standard library; the absolute interpreter path avoids
the generic E2E Python wrapper's unrelated connector dependencies.
The native `rest_rust` catalog avoids requiring the Java connector distribution.
The real PK-index sink validates its actual key and writes every data/delete
artifact and snapshot marker. No historical marker is rewritten. This fixture
does not implement automatic contract publication or certify writer rollout.

The test uses two independent streaming jobs at parallelism 2 and checks full
rows and grouped aggregates after bootstrap, update, delete, and same-key
reinsertion. Compaction and snapshot expiration are explicitly disabled.
There is no compaction or recovery acceptance claim.

Successful runs remove their SQL objects and request deletion of the unique
external table. On failure, preserve the generated table name and inspect the
cluster logs before cleaning only that run's objects. Do not use a broad
`clean-data` or `slt-clean` against an existing development environment.

This manual path is not selected by `e2e-iceberg-test.sh`'s `pure_slt` glob.
It requires Lakekeeper and test-only contract preparation, so it is not yet a
production-rollout CI acceptance test.

## Local validation

On 2026-09-17, both V2 and V3 passed on `silver` with real Hummock state,
MinIO, Lakekeeper, and the PK-index writer (about 20 seconds per format).
The assertions also confirmed Parquet position deletes for V2, Puffin DVs for
V3, no equality deletes, and at least four marked data snapshots.

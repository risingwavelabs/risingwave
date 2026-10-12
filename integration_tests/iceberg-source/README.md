# How to run the test

Run following command to run the test:

```bash
cd python
poetry update
poetry run python main.py
```

# How to override risingwave image version:

```bash
export RW_IMAGE=<your version>
```

## REST namespace location regression

The `rest` case also tests `default_table_location_from_namespace` against the
existing `tabulario/iceberg-rest:0.6.0` service and MinIO, with no mock catalog.
It covers Java REST, native Rust REST, and REST with vended credentials.

For each runtime, it checks the table location returned by the real catalog and
reads back a row written by a RisingWave sink. Cases cover the default-disabled
behavior, namespace location with trailing slashes, explicit warehouse precedence,
and missing/empty namespace locations.

The namespace uses a custom location different from the catalog warehouse.
Tabulario's default location is warehouse-based, so removing the client-side
fallback makes the derived-location assertion fail even though table creation
still succeeds. This tests the compatibility behavior without pretending that
Tabulario reproduces Glue's missing-location error.

The test uses unique namespaces and a unique RisingWave schema and cleans them
up on completion or failure. It runs in the existing `iceberg-source` integration
CI job. Use an `RW_IMAGE` containing the fix when running it locally.

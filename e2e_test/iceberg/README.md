# Iceberg E2E Tests

## Prerequisites

- Java 17 or 21 (required by Spark 4.0)
- [Poetry](https://python-poetry.org/) (Python package manager)

## Running Tests Locally

### 1. Start RisingWave with Minio

```bash
./risedev d for-ctl
```

### 2. Create Minio Bucket

```bash
./risedev mc mb -p hummock-minio/icebergdata
```

### 3. Start Spark Connect Server

```bash
cd e2e_test/iceberg
bash start_spark_connect_server.sh
```

This downloads Spark if needed, resolves Maven packages, and starts the server on port 15002.

### 4. Install Python Dependencies

```bash
cd e2e_test/iceberg
poetry install --quiet
```

### 5. Run Tests

Run a single test case (in `e2e_test/iceberg`):

```bash
poetry run python main.py -t ./test_case/no_partition_append_only.toml
```

Run all test cases (in `e2e_test/iceberg`):

```bash
poetry run python main.py
```

Run pure SLT tests (from repo root):

```bash
./risedev slt './e2e_test/iceberg/test_case/pure_slt/*.slt'
```

### 6. Cleanup

```bash
cd e2e_test/iceberg
./spark-*/sbin/stop-connect-server.sh
./risedev k
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

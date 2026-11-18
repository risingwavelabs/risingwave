# Manual live Redshift sink tests

These tests access a real Amazon Redshift database and, in S3 mode, a real S3
bucket. They are **disabled by default and are not scheduled by any CI pipeline**.

- `run.py` does nothing without `--run-live` and refuses to run under `CI`,
  `BUILDKITE`, or `GITHUB_ACTIONS`.
- Every executable SLT record requires the `redshift-live` label. Ordinary SLT
  globs skip those records. Only the manual runner supplies that label.
- `.coverageignore` documents this intentional exclusion; it does not itself
  control execution. No CI workflow, job, or CI label enables these tests.

## Prerequisites

- A RisingWave instance built from the checkout, with four-way streaming
  parallelism available, the Java connector libraries, and a configured secret
  store. Start your normal development profile with `./risedev d`.
- Python 3.10+, `rustc`, and the Python packages below. Run from the repository
  root. The runner uses `./risedev slt` and does not start or stop RisingWave.
- A reachable Redshift database and an existing disposable schema. The test user
  needs permission to create/drop/read/write tables there and create temporary
  tables. Use a JDBC URL without credentials or query parameters.
- For S3 mode: a dedicated prefix in a bucket in the **same region** as Redshift.
  Grant bucket listing restricted to that prefix and object read/write/delete,
  multipart-abort, and multipart-part-list permissions within it. The supplied
  access key pair is used by both RisingWave and Redshift COPY. Temporary
  credentials requiring a session token and IAM-role authorization are not
  covered by this harness. If using SSE-KMS, grant the corresponding KMS access.

```bash
python3 -m venv .context/redshift-e2e-venv
.context/redshift-e2e-venv/bin/pip install -r e2e_test/sink/redshift_manual/requirements.txt
cp e2e_test/sink/redshift_manual/redshift.env.example .context/redshift-test.env
chmod 600 .context/redshift-test.env
```

Fill in the private environment file, then explicitly run:

```bash
.context/redshift-e2e-venv/bin/python e2e_test/sink/redshift_manual/run.py \
  --run-live --env-file .context/redshift-test.env --mode both \
  --host 127.0.0.1 --port 4566 --database dev
```

Use `--mode jdbc` to run without S3, or `--mode s3` to test S3 staging and COPY.
Exported environment variables also work; omit `--env-file` in that case. The
file parser accepts plain or quoted assignments without evaluating shell code.

## Coverage

- Both modes: four parallel sink writers, 256 initial rows, updates, deletes,
  reinsertion (172 final rows), CDC cleanup, and 128 append-only rows.
- S3 mode: manifest consumption, unique `epoch_counter_actor` identifiers across
  four writers, and a counter exceeding 9. Checks read retained staging files,
  so they do not race the periodic merge deleting CDC rows.
- SQL regressions: legacy/new/padded IDs, counter/epoch/actor digit boundaries,
  unsigned limits, updates/deletes/reinsertion, empty batches, complete-tuple
  watermarks, and late-row cleanup boundaries. These nine cases run using both
  direct SQL inserts and S3 manifest COPY.
- JDBC mode additionally runs 18 watermark-column name collision cases,
  composite-key coverage, and schema-qualified table coverage (29 cases total).

The SQL tests compile the pure name/merge formatters directly from the current
Rust source into a temporary executable. This tests production SQL without
duplicating it in fixtures. Fixture transactions are rolled back after each
case, including on failure. The live sink SLT exercises the running binary;
rebuild it after changing the connector.

Each run creates a unique RisingWave schema, Redshift table names, and S3
subprefix. The runner attempts cleanup on success, failure, and interruption;
it never clears the configured bucket/prefix or existing schema. Staged data
files are explicitly deleted by the harness after checking manifest cleanup.
S3 versioned buckets may retain noncurrent object versions according to their
lifecycle configuration.

Credentials are supplied through environment variables and RisingWave secrets,
not SLT substitutions or command-line arguments. The runner redacts configured
values from its output and removes temporary generated files on exit. Keep
private environment files and server logs out of commits; server-side JDBC
debug logs can contain COPY credentials. No real endpoint, account, bucket,
credential, or test output belongs in this directory.

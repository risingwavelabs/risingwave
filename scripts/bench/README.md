# Object-store diagnostics

`risectl bench object-store` is the long-term diagnostic using RisingWave's
object-store client. It produces JSON on stdout and a terminal summary on stderr.

`object_store_benchmark.py` is an independent S3 diagnostic for environments that
cannot yet upgrade risectl or a RisingWave image. It requires Python 3.10+ with SSL
support and the pinned boto3 dependencies. It never invokes risectl or Kubernetes.
The source lives here for review and version tracking. Customers can receive the
script and dependencies directly without cloning or building RisingWave.

## Run the standalone script

Use an existing bucket and the usual AWS credential environment/provider chain:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r object_store_benchmark.requirements.txt
.venv/bin/python object_store_benchmark.py \
  --endpoint https://minio.example:9000 --bucket benchmark-bucket \
  --ca-cert /path/to/ca.pem --profile all --output result.json
```

The default profile is `compactor`. `basic` covers complete multipart uploads,
random ranged/block GETs, complete prefix LIST scans and batched DELETEs. `all`
also runs the compactor comparisons and fast-reader interference experiments.
See `--help` for workload sizes, budgets, pauses and timeouts.

## Deliver to an offline environment

Prepare the script and requirement file from the same reviewed commit. On a
connected machine, download the dependencies:

```sh
python3 -m pip download --only-binary=:all: \
  -r object_store_benchmark.requirements.txt -d wheels
```

Send those two source files and `wheels/`, recording the commit and checksums.
On the target machine, install with `--no-index --find-links ./wheels` instead of
the online pip command above. Wheels, credentials and benchmark results are local
delivery artifacts and should not be committed to this directory.

## Measurement and maintenance

Each long-range reader consumes one live `StreamingBody`; only the short-GET
comparison downloads a bounded chunk before consuming it from memory. Deliberate
pauses and resume waits are separate. Both tools use run-owned prefixes, and do
not create buckets or apply automatic storage-health thresholds.

The script uses boto3, with its own client version and JSON schema. Its numerical
baseline differs from risectl/WARP. Cache state and server backpressure require
external verification. Workload semantics should be kept consistent in both
implementations, with regression tests for changes to those semantics.

Run `python3 -m unittest discover -s scripts/bench -p 'test_*.py'` from the repository
root. CI runs these tests on Python 3.10 and 3.13. Actual MinIO/AWS validation is
separate from those unit tests. Interrupted blocking reads may wait for the socket
timeout before cleanup; process kills or failed multipart aborts can require
server-side lifecycle cleanup.

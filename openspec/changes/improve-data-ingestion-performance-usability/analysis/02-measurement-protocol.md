# Ingestion Measurement Protocol

This protocol standardizes how ingestion benchmark scenarios are executed and recorded so results are comparable across runs.

## 1. Run Structure

For each scenario ID in `01-benchmark-matrix.md`:

1. Environment preparation
2. Warm-up window
3. Measurement window (primary sample)
4. Repeated runs for variance tracking
5. Post-run validation and artifact capture

## 2. Environment Metadata (Required)

Record these fields for every run:

- Git commit SHA and branch
- Cluster profile (node counts and roles)
- Source connector type and connector configuration snapshot
- Data generator profile (event size distribution, key cardinality, schema version)
- Relevant system/session parameters affecting source parallelism and buffering
- Run start/end timestamp (UTC)

## 3. Timing Rules

- Warm-up: 5 minutes minimum (or until rates stabilize, whichever is longer)
- Measurement window: 15 minutes minimum for sustained scenarios
- Burst scenarios: at least 3 burst cycles per run
- Repeats: minimum 3 runs per scenario

## 4. Metrics Collection Rules

Collect at fixed interval (recommended 10s):

- Throughput: records/sec, bytes/sec
- Latency: p50/p95/p99 end-to-end
- Resource utilization: CPU %, memory RSS/working set, network throughput, disk I/O
- Backpressure and lag indicators: source lag, processing queue depth, checkpoint/barrier pressure indicators
- Error counters: parse/decode errors, retries, timeouts, connector disconnect/reconnect events

## 5. Data Quality and Consistency Checks

A run is invalid if any condition is met:

- Missing core metrics for more than 10% of measurement intervals
- Workload generator misconfiguration or incomplete scenario execution
- Cluster instability unrelated to scenario (e.g., unrelated service crash)

For invalid runs:

- Mark run as discarded
- Record discard reason
- Re-run scenario with same configuration

## 6. Output Artifacts

Each run MUST produce:

- `run-summary.md`: scenario ID, key metrics, pass/fail summary
- `metrics.csv` (or equivalent structured export)
- `env.json`: environment and config snapshot
- `events.log`: notable events (retries, failures, topology/parameter changes)

## 7. Aggregation and Comparison

For each scenario:

- Compute mean/median and p95 spread across valid repeats
- Report variance coefficient for throughput and p99 latency
- Highlight outliers and annotate likely causes
- Provide an explicit confidence level for the scenario baseline

## 8. Confidence Levels

- High: >=3 valid runs, low variance, no unresolved anomalies
- Medium: >=2 valid runs, moderate variance or minor anomalies
- Low: insufficient runs or unresolved anomalies affecting interpretation

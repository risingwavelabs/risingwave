# Ingestion Benchmark Matrix

This matrix defines the baseline workload coverage for ingestion optimization across connector categories and data-shape complexity.

## Connector Categories

- Message queue: Kafka / Pulsar / Kinesis style connectors
- CDC: MySQL / PostgreSQL CDC connectors
- Object storage / file-based: S3 / GCS style connectors
- API / queue adapters: webhook-like and generic queue sources

## Scenario Matrix

| Scenario ID | Traffic Pattern | Data Shape | Connector Coverage | Scale Profile | Primary Objective |
| --- | --- | --- | --- | --- | --- |
| P1 | Sustained high-throughput | Fixed schema, medium payload | Message queue + CDC | 30 min steady run | Max sustained ingest rate, stability under constant load |
| P2 | Bursty traffic | Fixed schema, mixed payload sizes | Message queue + object storage | 5-min bursts with 2-min idle gaps | Burst absorption, recovery time, lag growth/decay |
| P3 | Schema-change events | Add/remove/rename fields mid-stream | CDC + message queue | 3 schema transitions per run | Parse/decode resilience, schema adaptation overhead |
| P4 | Mixed event sizes | Small + large records with skewed distribution | Message queue + API/queue adapters | 70/20/10 size distribution | Throughput fairness, memory pressure, backpressure onset |
| P5 | Skewed partition activity | Hot partitions + cold partitions | Message queue | 80/20 key skew | Split balancing and per-partition lag behavior |
| P6 | Failure-and-recovery | Intermittent upstream/network failures | CDC + message queue | Inject 3 fault windows | Recovery latency, duplicate risk, operator intervention effort |

## Common Measurement Dimensions

Each matrix entry MUST capture:

- Throughput: records/sec and bytes/sec at source ingest boundary
- End-to-end latency: p50, p95, p99 from source event time to stream processing acknowledgment boundary
- Resource use: CPU, memory, network, storage I/O per node role
- Backpressure signals: upstream lag, source pending queue depth, barrier/checkpoint-related pressure where available
- Error profile: parse/decode failures, transient connector retries, hard failures

## Success Criteria Baseline

- Every connector category is represented in at least one sustained and one non-sustained scenario.
- Every scenario includes explicit target load, expected steady state, and stop criteria.
- Every scenario defines pass/fail signals for throughput stability, latency bounds, and failure tolerance.

## Notes

- This matrix is intentionally implementation-agnostic so it can be reused for follow-up optimization changes.
- Scenario IDs are referenced by downstream artifacts (protocol, bottleneck report, prioritization backlog).

# Ingestion Bottleneck Taxonomy and Evidence Format

This taxonomy provides a consistent classification for ingestion bottlenecks and a standard evidence format for reporting.

## 1. Bottleneck Categories

### C1. Connector I/O Bottlenecks
Symptoms:
- Source lag grows while downstream CPU is under-utilized
- Frequent connector retries, throttling, or partition fetch stalls

Primary evidence:
- Connector fetch/poll throughput vs configured limits
- Retry/error counters and reconnect frequency
- Upstream service latency/availability signals

### C2. Decode/Parse Bottlenecks
Symptoms:
- High CPU spent in parsing/deserialization path
- Throughput drops with complex schemas or larger payloads
- Elevated parse/decode error rates

Primary evidence:
- Parser/decode stage CPU time and throughput
- Error distribution by payload/schema variant
- Throughput sensitivity to schema complexity and event size

### C3. Scheduling/Backpressure Bottlenecks
Symptoms:
- Queue depth grows despite steady source input
- Periodic latency spikes aligned with scheduling or checkpoint pressure

Primary evidence:
- Queue depth and lag trend over time
- Checkpoint/barrier timing and correlation with latency spikes
- Executor saturation indicators and per-operator throughput imbalance

### C4. Downstream State Interaction Bottlenecks
Symptoms:
- Ingestion path slows when state access/write amplification increases
- Increased tail latency under state-heavy operators

Primary evidence:
- State write/read throughput and amplification signals
- Storage-related latency and compaction/checkpoint interaction indicators
- Throughput drop under state-intensive workload variants

## 2. Reporting Evidence Format

Each bottleneck finding MUST include the following fields:

- `scenario_id`: Benchmark scenario reference (e.g., P1, P2)
- `category`: One of C1/C2/C3/C4
- `symptom_summary`: Brief statement of observed behavior
- `impact`: Quantified effect (throughput loss, latency increase, lag growth)
- `primary_metrics`: Key metrics supporting the conclusion
- `supporting_context`: Config/environment notes needed for interpretation
- `confidence`: High/Medium/Low
- `suspected_root_cause`: Most likely root cause hypothesis
- `candidate_actions`: 2-5 practical optimization directions

## 3. Confidence Guidance

- High: Multiple correlated signals and repeatable reproduction
- Medium: Signals are directionally aligned but with moderate noise
- Low: Incomplete evidence or confounded by unrelated instability

## 4. Example Finding Skeleton

```yaml
scenario_id: P2
category: C3
symptom_summary: Queue depth grows rapidly during bursts and drains slowly.
impact:
  throughput_loss_pct: 18
  p99_latency_increase_ms: 430
primary_metrics:
  - source_queue_depth
  - end_to_end_latency_p99
  - checkpoint_interval_duration
confidence: Medium
suspected_root_cause: Executor scheduling pressure under burst synchronization.
candidate_actions:
  - Tune source parallelism strategy bounds
  - Reduce per-batch parse overhead in hot path
  - Revisit queue sizing/backpressure thresholds
```

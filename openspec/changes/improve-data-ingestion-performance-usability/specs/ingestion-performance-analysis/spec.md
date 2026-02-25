## ADDED Requirements

### Requirement: Ingestion benchmark matrix definition
The ingestion optimization workflow MUST define a benchmark matrix that covers representative ingestion patterns, including sustained high-throughput, bursty traffic, and schema-change events.

#### Scenario: Benchmark matrix is prepared before analysis runs
- **WHEN** a team starts an ingestion optimization cycle
- **THEN** the team MUST produce a benchmark matrix document with workload types, connector categories, and data-shape coverage
- **AND** each benchmark entry MUST define success metrics and resource dimensions to capture

### Requirement: Repeatable ingestion measurement protocol
The ingestion optimization workflow MUST execute each benchmark scenario using a repeatable measurement protocol so results can be compared across runs and environments.

#### Scenario: Team executes benchmark runs
- **WHEN** benchmark scenarios are executed for performance analysis
- **THEN** each scenario MUST record warm-up duration, measurement window, run count, and environment metadata
- **AND** the workflow MUST capture throughput, end-to-end latency, and resource utilization for each run

### Requirement: Bottleneck attribution output
The ingestion optimization workflow MUST produce a bottleneck attribution output for each benchmark scenario.

#### Scenario: Bottleneck report generation
- **WHEN** benchmark data is collected
- **THEN** the analysis MUST classify dominant bottlenecks into connector I/O, decode/parse, scheduling/backpressure, or downstream-state interaction categories
- **AND** each bottleneck finding MUST include supporting metrics and confidence notes

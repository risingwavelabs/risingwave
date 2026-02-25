## Context

Data ingestion in RisingWave crosses multiple layers: connector runtimes (`src/connector/`), stream/source execution (`src/stream/`), metadata/control paths, and operator-facing workflows in the dashboard and docs. Current optimization work is mostly local to individual components, which makes it hard to compare bottlenecks, explain tradeoffs, and prioritize work that improves both throughput and user experience.

This change focuses on building a repeatable optimization framework before implementation-heavy fixes. The framework should let teams answer three questions consistently:
1. Where is ingestion capacity lost?
2. Which user-facing frictions most often block successful ingestion setup/tuning?
3. Which optimization items should be done first for maximum impact/risk balance?

## Goals / Non-Goals

**Goals:**
- Define a common ingestion benchmark matrix (workload types, data formats, connector classes, scale levels) and measurement protocol.
- Define a bottleneck classification model for ingestion (connector I/O, decode/parse, source scheduling, backpressure, state interactions).
- Define usability evaluation criteria for ingestion setup and operations (time-to-first-success, config error rate, troubleshooting effort, tuning discoverability).
- Define a prioritization model that scores candidate improvements by expected impact, engineering cost, risk, and rollout complexity.
- Produce artifacts that can be reused in future ingestion optimization cycles.

**Non-Goals:**
- Implementing all identified optimization fixes in this change.
- Redesigning the full connector architecture.
- Introducing breaking API/protocol changes for connectors.
- Replacing existing observability stack or dashboard architecture.

## Decisions

### Decision 1: Use a two-track analysis design (performance + usability) with shared prioritization

- **Choice:** Maintain two analysis tracks in parallel:
  - Performance track: throughput/latency/resource efficiency and bottleneck localization.
  - Usability track: configuration clarity, diagnostics quality, and tuning ergonomics.
  Both feed a single prioritization output.
- **Rationale:** Ingestion incidents are often a mix of system limits and operator friction. Isolating only one side tends to incorrectly rank improvements.
- **Alternatives considered:**
  - Performance-only analysis first: simpler but misses high-impact UX fixes that reduce failed ingestion setups.
  - Usability-only analysis first: improves experience but can hide hard throughput ceilings.

### Decision 2: Standardize measurement via scenario matrix and fixed run protocol

- **Choice:** Define a scenario matrix covering representative workloads (steady high-throughput, bursty ingestion, schema evolution, mixed event sizes) and run each scenario with a fixed protocol (warm-up, measurement window, repeat count, environment notes).
- **Rationale:** Comparable measurements are required to avoid anecdotal optimization decisions.
- **Alternatives considered:**
  - Ad-hoc per-team benchmarks: lower setup effort but weak comparability.
  - Single synthetic benchmark: easy to run but poor real-world coverage.

### Decision 3: Use capability-level outputs to bridge analysis and execution

- **Choice:** Express outputs as capability-scoped findings and prioritized follow-up items, so specs and tasks can map directly to concrete implementation changes.
- **Rationale:** This keeps the change actionable and reduces translation loss between analysis and implementation.
- **Alternatives considered:**
  - Narrative-only report: informative but hard to operationalize.
  - Issue-list only: actionable but lacks rationale and repeatability.

### Decision 4: Prioritize with a transparent weighted score

- **Choice:** Score candidate improvements using explicit dimensions:
  - Performance gain potential
  - Usability gain potential
  - Engineering effort
  - Operational risk
  - Validation complexity
- **Rationale:** A visible scoring model supports cross-team alignment and faster planning decisions.
- **Alternatives considered:**
  - Pure expert judgment: fast but inconsistent across reviewers.
  - Strict ROI-only model: may underweight reliability and operator burden.

## Risks / Trade-offs

- **[Risk] Benchmark scenarios under-represent production diversity** → **Mitigation:** include connector and workload diversity review before finalizing matrix; iterate scenarios after first pass.
- **[Risk] Analysis overhead delays optimization delivery** → **Mitigation:** time-box analysis phases and require interim “quick-win” candidates after each phase.
- **[Risk] Prioritization score becomes contentious across teams** → **Mitigation:** publish scoring rubric and allow calibration review with domain owners.
- **[Risk] Usability assessment is too subjective** → **Mitigation:** define concrete evaluation signals (time-to-success, retries, common error patterns, operator steps).
- **[Trade-off] Standardized protocol increases upfront cost** → **Benefit:** enables reliable comparison and better long-term decision quality.

## Migration Plan

- Phase 1: Define ingestion scenario matrix, metrics dictionary, and usability evaluation checklist.
- Phase 2: Run baseline measurements and workflow walkthroughs; produce initial bottleneck/usability findings.
- Phase 3: Generate prioritized optimization backlog with scoring and recommended sequence.
- Phase 4: Hand off top-priority items into implementation-focused changes/specs/tasks.

Rollback strategy: This change introduces process/documentation artifacts only; rollback means reverting generated analysis/prioritization artifacts if they are found invalid.

## Open Questions

- Which ingestion scenarios should be mandatory for CI-style regression checks versus periodic deep benchmarks?
- How should we segment connector categories for fair cross-connector comparison (message queue, CDC, object storage, etc.)?
- What minimum telemetry additions (if any) are needed to localize bottlenecks consistently?
- Should prioritization weights be globally fixed or tuned per release objective (throughput-first vs operator-efficiency-first)?

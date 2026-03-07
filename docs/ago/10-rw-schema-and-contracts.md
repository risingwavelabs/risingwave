# AGO Kickoff: RW Schema And Contracts

## Boundary
- RW owns ingestion, state derivation, trigger rules, intent production, and observability projections.
- Control plane owns claim/lease, execution orchestration, retries/compensation, policy, and side-effect commits.

## Minimal Schema v0
- `signals_raw`
- `world_state`
- `task_intents`
- `task_claims`
- `task_results`
- `task_state` (derived, read-only)
- `agent_events`
- `control_commands`

## Contract Fields (must-have)
- Causality: `trace_id`, `cause_event_id`, `intent_id`, `side_effect_id`
- Delivery semantics: `dedupe_key`, `policy_version`, `payload_version`
- Timing: `event_time`, `ingest_time`, `deadline_at`, terminal timestamps

## Ownership
- RW writes intents and derived views.
- Control plane writes claims/results.
- Human/manual flow writes control commands only.

## SLO v0
- Detect p95 (`event_time -> intent emit`) <= 30s.
- Execute p95 (`intent emit -> terminal result`) <= 5m.
- Silent loss = 0 under failure injection suite.
- Replay intent-set consistency >= 99% for fixed-seed input.

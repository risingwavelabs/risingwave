# AGO Kickoff: Harness Blueprint

## Objective
Provide a pre-implementation harness that validates boundaries, correctness, replayability, and failure behavior.

## Required Components
- Contract freeze checks (schema + ownership + semantics)
- Replay contract (`trace_id` and `intent_id` explain path)
- Mock harness:
  - mock_event_producer
  - mock_executor
  - mock_tool_adapter
  - mock_human_gate
- Failure injection matrix:
  - duplicate/reorder/late/burst
  - worker crash/timeout
  - side-effect success but writeback failure
  - RW recovery/watermark stall
  - schema drift/dead-letter growth
- Observability baseline:
  - lag/backlog/retry/dlq/duplicate_suppressed/orphan/stuck/state_divergence

## Acceptance
Harness is considered ready only when replay, injection, and observability checks are all executable and documented.

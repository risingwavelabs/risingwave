# AGO Kickoff: Goal And Principles

Status: Implementation frozen until Gates 0-4 all passed.

## Problem
We are building a RisingWave-centered self-organizing agent operating system, not a single demo pipeline.

Target loop:
`facts -> signals -> intents -> claim/execution -> writeback -> new state`

## Goals
1. Complete observe -> decide -> act within action windows.
2. Make decisions auditable, replayable, and explainable.
3. Support multi-agent collaboration on a shared world model.
4. Keep the core scenario-agnostic (smart-cafe is only the first integration).

## Principles
- RW-centered, data-first: all trigger/decision/outcome paths must be visible in structured data.
- Self-organization via shared world model: no hidden per-agent private truth.
- RW = intelligence plane, control plane = execution plane.
- At-least-once + idempotent by default.
- Minimal reusable core before scenario-specific adapters.

## Non-goals
- No full workflow engine in RW.
- No millisecond hard-real-time scheduler.
- No demo-only architecture shortcuts.
- No direct external side-effect commit from RW.

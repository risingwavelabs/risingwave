# AGO Kickoff: Gates And Collaboration

Blueprint entrypoint: `notes/agent-system-on-rw.md`

## Gate Policy
No implementation starts until Gates 0-4 all pass.

## Gates (DoR)
- Gate-0: Problem/goals/non-goals frozen.
- Gate-1: Schema/contracts/ownership frozen.
- Gate-2: Replay and mock harness ready.
- Gate-3: Failure injection and observability baseline ready.
- Gate-4: Safety/policy/rollback boundaries frozen.

## Collaboration Model
- Keep architecture decisions in docs first, then implementation.
- Every gate status update must include evidence links.
- No hidden glue logic outside contracts.

## Forbidden Patterns
- Bypassing intents/results contracts for direct imperative agent calls.
- Writing production feature code before gate completion.
- Scenario-specific core schema that cannot generalize.

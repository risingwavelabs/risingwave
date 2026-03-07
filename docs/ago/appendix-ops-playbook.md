# AGO Kickoff: Ops Playbook (Appendix)

## Triage Order
1. Dashboard signals (lag/backlog/error classes)
2. Log evidence in matching time window
3. Responsibility split (RW vs control plane vs external dependency)

## Required IDs
All diagnosis paths must preserve and query by:
- `trace_id`
- `intent_id`
- `task_id`
- `worker_id`/`executor_id`
- `policy_version`

## Incident Output Template
- Symptom
- Evidence (with timestamps)
- Root cause classification
- Immediate mitigation
- Follow-up action and owner

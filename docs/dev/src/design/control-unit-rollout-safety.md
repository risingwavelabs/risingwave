# Control Unit Rollout Safety

<!-- toc -->

## Purpose

This document defines the rollout ladder, safety guardrails, rollback contract, and canary-readiness expectations for a candidate-capable `control_unit`.

These rules apply to every admitted `single_knob`, `family`, or `bundle`, including any `parameterized_control` that enters the inventory through the gated `other` registry.

## Objective ordering and hard boundaries

Every rollout decision must preserve the approved priority order:

1. stability
2. latency
3. resource usage

Additional hard boundaries:

- fixed-budget tuning only;
- never rely on RAM enlargement as the optimization path;
- never rely on reserved-RAM-ratio adjustment as the optimization path; and
- never batch multiple unrelated changes together.

## Rollout ladder

A `control_unit` must earn each stage before moving up.

| Stage | Name | Required evidence | Exit condition |
| --- | --- | --- | --- |
| 0 | Offline recommendation only | Inventory entry, explicit objective, bounded hypothesis | Reviewer agrees that the unit is worth shadow evaluation |
| 1 | Shadow evaluation | Observability grade A/B/C assigned, explicit confounders, and proposed rollback contract | Signals show the unit can be watched without action |
| 2 | Canary autotuning | Observability grade A, bounded blast radius, and low-cost rollback path | Canary can be enabled for a narrow scope with a named owner |
| 3 | Guarded online tuning | Verified rollback contract, cooldown window, and auto-disable triggers | Canary results remain stable across representative workloads |
| 4 | Policy-driven continuous tuning | Repeated safe execution with stable evidence and drift checks | The unit no longer needs stage-specific exceptions |

No `control_unit` may skip a lower stage.

## Safety guardrails

| Guardrail | Requirement |
| --- | --- |
| Fixed-budget boundary | Proposed actions must stay inside the existing CPU, memory, disk, and network budget envelope |
| No RAM enlargement strategy | A unit is immediately blocked if the proposed benefit depends on adding RAM |
| No reserved-ratio strategy | A unit is immediately blocked if it depends on changing reserved RAM ratio |
| One change at a time | Apply one `control_unit` at a time unless it is a validated `bundle` |
| Explicit scope | Declare whether the change is scoped to session, tenant, node, or cluster |
| Observation window | Wait long enough to observe steady-state and stability signals before judging success |
| Cooldown window | Allow time for backlog, barrier, cache, or storage effects to settle before the next action |
| Auto-disable triggers | Stop progression when stability regression, latency-tail regression, resource-pressure breach, or alert/log anomaly occurs |

## Rollback contract

Every candidate-capable `control_unit` must define a rollback contract before canary use.

### Required fields

- prior value snapshot;
- expected benefit signal;
- rollback trigger list;
- rollback scope;
- rollback validation signal;
- observation window; and
- cooldown window.

### Minimum rollback triggers

The rollback contract must cover at least:

- stability regression;
- latency-tail regression;
- resource-pressure breach; and
- alert or log anomaly.

### Contract template

```yaml
rollback_contract:
  canonical_key: <control_unit key>
  unit_kind: single_knob | family | bundle
  prior_value_snapshot: <captured before change>
  expected_benefit_signal:
    - <metric or panel anchor>
  rollback_triggers:
    - stability regression
    - latency-tail regression
    - resource-pressure breach
    - alert or log anomaly
  rollback_scope: session | tenant | node | cluster
  rollback_validation_signal:
    - <metric, panel, or alert recovery anchor>
  observation_window: <must be long enough to observe steady-state>
  cooldown_window: <must be long enough to avoid overlapping decisions>
```

A rollback contract is not complete if it only describes the revert action without the validation signal proving recovery.

## Canary-readiness expectations

A `control_unit` is canary-ready only when all items below are true:

1. observability grade is **A**;
2. the unit passes the `parameterized_control` gate when sourced from `other`;
3. scope and blast radius are explicitly bounded;
4. the rollback contract is fully populated and reviewable;
5. the cooldown and observation windows are justified;
6. the unit does not depend on RAM enlargement or reserved-ratio changes; and
7. the owning reviewer can explain why the chosen `single_knob`, `family`, or `bundle` shape is the safest control surface.

## Tabletop scenarios to preserve

Future verification should at least rehearse these scenarios:

1. barrier/checkpoint tuning causes latency regression;
2. compaction-related tuning increases IO pressure;
3. a join/cache bundle creates a stability regression under mixed workload; and
4. a parallelism change improves throughput but violates stability guardrails.

Each scenario should identify the trigger, rollback scope, and validation signal before the change is allowed to proceed past shadow evaluation.

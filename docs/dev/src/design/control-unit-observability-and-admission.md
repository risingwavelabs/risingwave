# Control Unit Observability and Admission

<!-- toc -->

## Purpose

This document defines the score-based observability rubric and candidate admission rules for a `control_unit`. It applies equally to `single_knob`, `family`, and `bundle` units, including gated `parameterized_control` entries from the `other` registry.

Candidate admission remains narrower than inventory coverage:

- the inventory keeps broad knowledge coverage;
- observability grade A/B/C decides evidence maturity; and
- admission decides whether a `control_unit` is safe enough for future autotuning workflows.

## Observability rubric

Each `control_unit` is scored across five dimensions. Every dimension uses the same scale:

- `0` — absent or unusable
- `1` — partial, indirect, or weak
- `2` — strong, direct, and repeatable

| Dimension | What it asks | Typical evidence |
| --- | --- | --- |
| Discoverability | Can operators consistently find the relevant signal? | Stable dashboards, panels, metric names, or runbooks |
| Attribution | Can signal movement be connected to this `control_unit` instead of background noise? | Controlled comparisons, family-level interaction notes, or known confounder handling |
| Sensitivity | Does a meaningful control change produce detectable movement? | Clear before/after metrics or anchored workload probes |
| Guardrail visibility | Will regressions surface through metrics, logs, or alerts? | Stability, latency-tail, or resource-pressure indicators |
| Rollback visibility | After revert, can recovery be confirmed? | Reversion signal, cooldown validation, or alert recovery evidence |

Every graded entry should carry:

- exact dashboard, panel, or metric anchors when available;
- log or alert anchors when available;
- confounder or interaction notes;
- a reviewer confidence note; and
- field-level evidence states (`verified | inferred | unknown`).

## Grade thresholds

| Grade | Rule | Meaning |
| --- | --- | --- |
| A | Total score >= 8, with attribution >= 1, guardrail visibility >= 1, and rollback visibility >= 1 | Ready to support candidate review |
| B | Total score 4-7, or gating dimensions are present but weak | Useful knowledge layer, incomplete candidate evidence |
| C | Total score <= 3, or attribution or rollback visibility is absent | Important to record, but too weak for candidate admission |

Observability grade A/B/C must be assigned to the `control_unit` as a whole, not to an isolated field without context.

Reviewers should speak explicitly in terms of grade A, grade B, and grade C so the admission bar stays stable across exemplar families.

## Calibration rules

The rubric is not considered stable until the first exemplar batch is calibrated.

1. Double-score the first exemplar batch with two reviewers.
2. If reviewers repeatedly disagree on the same `control_unit`, add a rubric clarification note before calling the grade model stable.
3. Prefer family-level scoring when interactions dominate member-wise behavior.
4. Do not convert an `inferred` anchor into grade A evidence without a concrete operational or repository anchor.

## Candidate admission classes

Candidate admission applies to `control_unit` entries, not just raw parameters.

| Class | Meaning | Typical next step |
| --- | --- | --- |
| `eligible_now` | Meets the admission bar now | Can enter offline recommendation or shadow workflows |
| `eligible_with_guardrails` | Promising, but requires extra rollout constraints | Can advance only with explicit guardrails and reviewer approval |
| `knowledge_only` | Useful to catalog, but not ready for candidate review | Keep in the inventory and improve evidence later |
| `blocked_by_observability` | Signals exist but do not yet support stable admission | Improve anchors, attribution, or rollback visibility |
| `blocked_by_risk` | Behavior is too risky or blast radius is too large | Narrow scope or redesign the unit |
| `blocked_by_budget_constraint` | The unit violates fixed-budget tuning constraints | Exclude until a budget-safe framing exists |
| `blocked_by_interaction_complexity` | Coupling is too strong for safe control | Reframe as a family or bundle, or keep knowledge-only |

## Admission requirements

A `control_unit` may enter the future autotuning candidate layer only if all of the following are true:

1. observability grade is **A**;
2. directional effect is clear enough to support action selection;
3. blast radius is bounded and documented;
4. rollback is cheap and verified through a rollback contract;
5. the action respects fixed-budget constraints;
6. stability > latency > resource usage remains explicit; and
7. interaction complexity is acceptable for the chosen `single_knob`, `family`, or `bundle` shape.

A `family` or `bundle` may be admitted even when no individual `single_knob` member is safe alone.

## Minimum exemplar set

Before the rubric is widened, the review set must freeze at least one reviewed `control_unit` from each area below:

- barrier / checkpoint
- parallelism
- compaction / storage
- cache or join behavior
- spill or batch memory gating

This minimum exemplar set ensures that admission rules and observability grade A/B/C boundaries are calibrated against different kinds of operating behavior.

## Shared review checklist

Each candidate review should answer the following:

1. Is the unit a valid `parameterized_control` or a supported config/system/session surface?
2. Is the unit best modeled as a `single_knob`, `family`, or `bundle`?
3. Are the current anchors strong enough for observability grade A/B/C?
4. Are confounders and interaction notes documented at the same granularity as the control decision?
5. Is the rollback contract explicit enough to prove recovery after revert?
6. Does the candidate stay inside the fixed-budget boundary without using RAM enlargement or reserved-RAM-ratio changes?

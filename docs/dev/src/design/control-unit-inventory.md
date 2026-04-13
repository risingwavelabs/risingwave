# Control Unit Inventory

<!-- toc -->

## Purpose

This document freezes the inventory model for future cloud autotuning readiness work in RisingWave. The canonical planning object is a `control_unit`, not an isolated parameter, so the inventory can describe safe single knobs as well as interacting families and bundles.

This inventory foundation preserves the approved hard constraints:

- stability > latency > resource usage
- fixed-budget tuning only
- no RAM enlargement strategy
- no reserved-RAM-ratio strategy
- `other` control surfaces stay behind the `parameterized_control` gate
- candidate admission applies to a `control_unit`, not just a raw parameter

## Canonical source matrix

The inventory spans every control surface that can materially change system behavior.

| Control surface | Canonical collection path | Canonical key shape | Coverage requirement | Notes |
| --- | --- | --- | --- | --- |
| Config / node config | `src/config/docs.md`, `src/config/example.toml`, and the `src/common/src/config/` Rust config structs | `config.<family>[.<path>]` | Include every node top-level family and all developer-facing leaves | The inventory must keep top-level families even when only some leaves are initially deep-reviewed |
| System params | `src/common/src/system_param/`, Meta controllers, and `ALTER SYSTEM`-backed surfaces | `system_param.<name>` | Include cluster-wide mutable controls | Use the externally referenced system parameter name as the stable key |
| Session params | `src/common/src/session_config/`, frontend session handling, and `SET`/`RESET` surfaces | `session_param.<name>` | Include per-session controls with material system impact | Session-only SQL ergonomics may remain knowledge-only if system impact is weak |
| Gated other controls | Explicit review registry only | `other.<module>.<name>` | Include only entries that satisfy the `parameterized_control` gate | This lane prevents the `other` bucket from becoming an unbounded catch-all |

## The `parameterized_control` gate for `other` surfaces

A behavior-changing surface may enter the inventory as `other` only if it is a `parameterized_control`:

1. it has a stable name or externally referenceable key;
2. it has an identifiable owner or module;
3. it has a defined mutation or application path;
4. it materially changes system behavior; and
5. it is not merely an internal heuristic or derived implementation detail.

Anything that does not pass the gate must be classified as exactly one of:

- `derived_runtime_policy` — contextual explanation only
- `internal_heuristic_or_implementation_detail` — excluded from the inventory

## The `control_unit` ontology

Future autotuning cannot assume that every meaningful behavior change is a one-knob decision. The inventory therefore uses three `control_unit` kinds.

| `unit_kind` | Meaning | When to use it | Typical example shape |
| --- | --- | --- | --- |
| `single_knob` | One parameter is independently meaningful and safely tunable | The parameter has clear directional effect, bounded blast radius, and standalone rollback discipline | One system parameter or one config leaf |
| `family` | Related controls must be reasoned about together even if some members can change independently | The controls share attribution, guardrails, or operating envelopes | A set of parallelism knobs or a storage maintenance family |
| `bundle` | Multiple controls must be admitted, rolled out, and rolled back together | The unit is unsafe or misleading when split into member-wise decisions | A coupled join or cache tuning package |

Every `control_unit` entry records:

- `canonical_key`
- `unit_kind`
- `control_surface_type`
- `member_parameters`
- `owning_module`
- `scope`
- `mutability`
- `default_value_summary`
- `allowed_range`
- `impact_dimensions`
- `positive_effects`
- `risks`
- `observability_evidence`
- `attribution_notes`
- `candidate_status`
- `non_admission_reason`
- `safe_operating_range`
- `rollback_trigger`
- `blast_radius`
- `cooldown_window`
- `tuning_preconditions`

The schema artifact in [`control-unit-schema.yaml`](./control-unit-schema.yaml) is the machine-readable source of truth for the field layout.

## Coverage rules

### Developer parameters remain in scope

Developer-facing parameters are part of the inventory even when they are too risky or opaque for early candidate admission. They may stay `knowledge_only`, `blocked_by_observability`, or `blocked_by_risk`, but they are still part of the catalog.

### Node top-level config families remain in scope

Every node top-level family must appear in the inventory matrix even before deep grading is complete. The inventory may defer per-leaf deep evidence, but it must keep the family visible so later reviewers do not silently skip operator-relevant areas.

## Precedence and deduplication rules

Inventory collection must not create multiple competing `control_unit` entries for the same behavioral decision.

1. **One canonical unit per behavioral lever.** If a config default, a system parameter override, and a session override influence the same behavior, keep one canonical `control_unit` and record the member surfaces in `member_parameters`.
2. **Prefer the externally operated name for the key.** Use the name the operator or controller would set first; store alternative paths as aliases or evidence notes.
3. **Model interaction, not duplication.** If a set of knobs is only understandable together, promote it from duplicated singles to a `family` or `bundle`.
4. **Keep the knowledge layer broader than the candidate layer.** Inventory inclusion is not blocked by missing observability grade A/B/C evidence.

## Evidence-state rules

Every extracted fact or judgment should carry an evidence state so the docs can stay honest in a brownfield codebase.

| Evidence state | Meaning | Allowed use |
| --- | --- | --- |
| `verified` | Directly supported by current repo evidence, runtime docs, or an anchored operational artifact | Safe for schema fields, grade decisions, and rollback contract inputs |
| `inferred` | Strongly suggested by code structure, naming, or prior operating knowledge, but not yet anchored end-to-end | Acceptable for planning notes, but should not alone justify candidate admission |
| `unknown` | Not yet proven or not yet reviewed | Must block precise claims and should trigger follow-up evidence work |

Recommended usage rules:

- record evidence state at the field or judgment level, not just once per document;
- never upgrade `inferred` to `verified` without a new anchor;
- never treat `unknown` fields as compatible with a production-ready rollback contract.

## Relationship to downstream admission and safety work

This inventory is the broad knowledge layer. The downstream observability rubric determines observability grade A/B/C, and the rollout-safety artifact defines the rollback contract that candidate-capable `control_unit` entries must satisfy.

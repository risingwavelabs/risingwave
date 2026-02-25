## Context

RisingWave already supports `SECRET ...` references for source/sink/connection options, and those objects persist secret references plus `object_dependency` edges so secret lifecycle operations remain safe. UDFs currently do not have the same mechanism: `CREATE FUNCTION` only persists plain function metadata, with no secret reference field and no function-to-secret dependency rows.

For the requested change, we need two things together:
1. Users can use catalog secrets instead of plaintext for sensitive UDF parameters.
2. Secret and function DDL operations enforce dependency rules consistently.

The design must cover frontend SQL handling, catalog/meta persistence, dependency tracking, and execution-time secret resolution without exposing plaintext in metadata.

## Goals / Non-Goals

**Goals:**
- Support secret-backed UDF parameters in `CREATE FUNCTION` using SQL syntax that matches existing secret reference patterns.
- Persist structured secret references for functions (secret id + ref type), not plaintext values.
- Record function-to-secret dependency edges in `object_dependency` so `DROP SECRET` / `DROP FUNCTION` behave safely and deterministically.
- Keep secret values out of stored catalog data and user-visible metadata where possible.
- Ensure query execution can resolve secret values when building/running UDF executors.

**Non-Goals:**
- Redesigning UDF runtime protocol (Flight/Javascript/Python/Wasm invocation model).
- Introducing a new `ALTER FUNCTION ... SET OPTIONS` syntax in this change.
- Supporting cross-database secret references.
- Refactoring all existing connector secret paths.

## Decisions

### Decision 1: Reuse `WITH (...)` option syntax and allow secret refs for UDF parameter keys

- **Choice:** Extend `CreateFunctionWithOptions` handling so UDF options can include `SECRET` values, e.g. `WITH (api_token = SECRET my_schema.my_secret)`, while preserving existing boolean options like `always_retry_on_network_error`, `async`, and `batch`.
- **Rationale:** The parser and option model already support `SqlOptionValue::SecretRef`; reusing this keeps SQL consistent with connector semantics and avoids introducing a new top-level clause.
- **Alternatives considered:**
  - New `SECRETS (...)` clause for functions: clearer separation, but requires new grammar surface and duplicated parsing/validation.
  - Placeholder substitution inside `AS`/`USING LINK` strings: easier to type, but harder to validate and easier to leak.

### Decision 2: Add explicit function secret reference fields in catalog/model

- **Choice:** Add a `secret_refs` map to function catalog protobuf/model path (same shape used by other objects), and add function model persistence for secret refs (nullable field, default empty).
- **Rationale:** Structured secret references are required both for dependency graph generation and for runtime resolution. Encoding them in generic string options is fragile and difficult to evolve.
- **Alternatives considered:**
  - Store secret reference mapping in the existing function `options` property map: low migration cost, but weak typing and poor interoperability.
  - Resolve secrets immediately and persist plaintext: unacceptable security properties.

### Decision 3: Create function-to-secret `object_dependency` edges at function creation

- **Choice:** During `create_function`, collect referenced secret ids and insert `object_dependency` rows (`oid = secret_id`, `used_by = function_id`) in the same transaction as function creation.
- **Rationale:** Existing drop checks (`check_object_refer_for_drop`) already enforce lifecycle safety based on `object_dependency`; adding function edges makes secret/function behavior consistent with source/sink/connection.
- **Alternatives considered:**
  - Ad-hoc dependency checks in `drop_secret` only: duplicates logic and risks drift.
  - Dependency checks only in frontend: unsafe because backend remains source of truth.

### Decision 4: Resolve secret names at DDL time, resolve secret values at execution build time

- **Choice:**
  - **DDL time (frontend):** Resolve secret names to ids and privilege-check via existing catalog lookup paths.
  - **Execution build time (expr runtime path):** Resolve secret values from `LocalSecretManager` using stored `secret_refs`, then construct runtime UDF options.
- **Rationale:** We need early validation for correctness/permissions, but we should avoid embedding plaintext into durable catalog data. Runtime resolution aligns with existing secret manager behavior.
- **Alternatives considered:**
  - Resolve values in frontend and embed plaintext into expression protobuf: simpler but increases leak surface (plan/log/diagnostic paths).
  - Resolve from meta on every UDF call: higher latency and stronger runtime coupling.

### Decision 5: Keep dependency semantics strict and explicit

- **Choice:** Keep default restrictive behavior when a secret is referenced by functions; operations that would break dependencies fail with dependent-object guidance. `DROP FUNCTION` continues to clean up dependency rows via object deletion cascade.
- **Rationale:** This matches existing dependency contract and prevents silent runtime breakage.
- **Alternatives considered:**
  - Auto-detach function secret refs when dropping secret: unsafe implicit behavior.
  - Auto-drop dependent functions on secret drop by default: too destructive.

## Risks / Trade-offs

- **[Risk] Proto/model evolution causes mixed-version incompatibility** → **Mitigation:** use additive fields, nullable DB column, and keep old behavior for functions without secret refs.
- **[Risk] Secret-backed option key semantics are ambiguous** → **Mitigation:** define a strict validation rule for accepted keys and include precise error messages for unknown/unsupported keys.
- **[Risk] Secret value leakage through logs/errors** → **Mitigation:** never log resolved secret content; only log option keys and secret object identifiers/names where necessary.
- **[Risk] Dependency drift if future function-alter paths update refs** → **Mitigation:** centralize dependency upsert/diff logic and require transactional updates when secret refs change.
- **[Trade-off] Additional catalog fields and migration increase implementation scope** → **Benefit:** consistent, auditable lifecycle handling and safer UDF secret usage.

## Migration Plan

1. Add additive protobuf fields for function secret refs and wire them through frontend/meta conversions.
2. Add nullable function secret-ref storage in meta model/migration; existing rows default to empty refs.
3. Implement frontend `CREATE FUNCTION` option parsing/validation for secret refs and DDL-time name-to-id resolution.
4. Implement meta `create_function` dependency insertion and notification wiring.
5. Implement runtime/expression path secret resolution using stored refs.
6. Add integration tests for:
   - create function with secret refs,
   - drop secret with dependent function (restrict failure),
   - drop function then drop secret (success),
   - privilege errors for unauthorized secret reference.

Rollback strategy: disable secret-backed UDF option acceptance in frontend while retaining additive schema fields; existing functions without refs remain unaffected.

## Open Questions

- Which UDF option keys are allowed to be secret-backed in v1 (all custom keys vs a validated subset by language/runtime)?
- Should `SHOW CREATE FUNCTION` render secret refs in `SECRET schema.name` form for debuggability, and what masking policy should apply?
- Do we need immediate support for secret refs in aggregate/table UDF metadata paths, or only scalar/external UDF first?
- Should we gate this behavior behind a system parameter until full cross-component tests are complete?

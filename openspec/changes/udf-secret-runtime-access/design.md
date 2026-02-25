## Context

The previous UDF secret work resolves secret references (`secret_id` + `ref_as`) into runtime option key/value pairs during UDF build. However, embedded runtime implementations (Python/JavaScript) currently do not consume `BuildOptions.options`, so user code cannot reliably access those values at execution time.

This change is cross-cutting across `expr/core` (runtime build) and `expr/impl` (language runtimes). It must preserve existing behavior for UDFs without secret-backed options and avoid persisting plaintext values in catalog metadata.

## Goals / Non-Goals

**Goals:**
- Define a stable runtime contract so embedded UDF code can access resolved `WITH (...)` options.
- Expose secret-backed options to Python and JavaScript runtimes in a deterministic way.
- Keep behavior unchanged for functions without secret-backed custom options.
- Add end-to-end tests that verify runtime access and regression behavior.

**Non-Goals:**
- Redesigning external/remote Flight UDF protocol to transmit per-function options.
- Changing SQL syntax or relaxing validation to allow plaintext custom options.
- Storing secret plaintext in any persistent catalog field.

## Decisions

### Decision 1: Expose options through a runtime global variable contract

- **Choice:** Inject a language-specific prelude so user code can read resolved options from `RW_UDF_OPTIONS`.
  - Python: inject read-only mapping assignment for `RW_UDF_OPTIONS`.
  - JavaScript: inject `globalThis.RW_UDF_OPTIONS = Object.freeze({...})`.
- **Rationale:** `BuildOptions.options` already carries resolved values at runtime build time. A global contract is the smallest cross-cutting change that does not alter SQL or catalog schemas.
- **Alternatives considered:**
  - New function parameter syntax for options: larger SQL/runtime contract change.
  - Environment variables: process-global scope is unsafe for per-function values.
  - Runtime API changes in upstream `arrow-udf-runtime`: larger dependency and compatibility impact.

### Decision 2: Inject prelude only when options are non-empty

- **Choice:** Leave runtime body unchanged when `opts.options` is empty.
- **Rationale:** Preserves behavior for legacy UDFs and reduces risk of unintended runtime side effects.
- **Alternatives considered:**
  - Always inject empty object: simpler implementation, but unnecessary behavior change.

### Decision 3: Keep secret resolution point unchanged

- **Choice:** Continue resolving secret refs through `LocalSecretManager` in `expr/core` build path, then pass resolved map to runtime implementation.
- **Rationale:** Keeps plaintext secret values ephemeral and local to execution-time runtime construction.
- **Alternatives considered:**
  - Resolve in language implementations directly from `secret_refs`: duplicates logic across runtimes.

## Risks / Trade-offs

- **[Risk] Prelude injection may interact with legacy body-compat fallback** → **Mitigation:** apply injection in a way that remains valid for both direct and fallback parsing paths.
- **[Risk] Runtime global name conflicts with user code** → **Mitigation:** use a clearly reserved framework name `RW_UDF_OPTIONS` and document it.
- **[Risk] Secret values become visible to user code by design** → **Mitigation:** scope is explicit to function author/executor; still avoid persistence/logging of plaintext.
- **[Trade-off] External UDF link runtimes remain unsupported for this contract initially** → **Benefit:** avoid protocol changes and keep this iteration focused.

## Migration Plan

1. Add runtime helper utilities in `expr/impl` for rendering language prelude from `BuildOptions.options`.
2. Update Python and JavaScript UDF builders to inject the prelude before registering function bodies.
3. Keep non-`WITH SECRET` UDF behavior unchanged.
4. Add/extend e2e tests to validate runtime access for both Python and JavaScript UDFs.
5. Run targeted checks/tests (`cargo check`, related parser/frontend/expr tests, udf e2e where available).

Rollback: disable prelude injection while preserving prior secret-ref parsing/storage behavior.

## Open Questions

- Should external Flight UDFs receive options in a future protocol extension?
- Do we need masking rules for `SHOW CREATE FUNCTION` if option names imply sensitive semantics?

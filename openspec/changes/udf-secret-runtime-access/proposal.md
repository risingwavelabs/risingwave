## Why

`CREATE FUNCTION ... WITH (...)` already accepts secret references, but today runtime implementations do not consistently expose those resolved values to user UDF code. Users cannot reliably consume secret-backed options at execution time, which makes the feature incomplete for real authentication/token use cases.

## What Changes

- Define a runtime contract for UDF secret-backed options so resolved values are available during function execution.
- Inject resolved `WITH (...)` options into embedded Python and JavaScript UDF runtimes in a deterministic, read-only global variable.
- Keep existing behavior for functions without secret-backed options unchanged.
- Add tests that verify secret-backed options are available at runtime and that non-secret custom option values remain rejected.

## Capabilities

### New Capabilities
- `udf-secret-runtime-option-access`: Expose resolved secret-backed function options to runtime UDF code (Python and JavaScript) via a stable runtime contract.

### Modified Capabilities
- None.

## Impact

- `src/expr/core` and `src/expr/impl`: runtime build path and per-language UDF builder behavior.
- `e2e_test/udf`: regression coverage for secret option runtime access.
- User-facing UDF behavior: users can access secret-backed `WITH` options from function code without storing plaintext in catalog metadata.

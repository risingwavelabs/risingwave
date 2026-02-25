## Why

RisingWave users currently need to place sensitive values directly in UDF parameters, which increases credential exposure risk and makes rotation workflows harder to manage. We also need explicit function-to-secret dependency handling so secret lifecycle operations cannot silently break existing UDFs.

## What Changes

- Add support for binding UDF parameters to catalog secrets instead of plaintext literals during function definition and updates.
- Validate referenced secrets during UDF DDL so invalid or unauthorized references fail early.
- Track function-to-secret dependencies in metadata whenever a UDF references one or more secrets.
- Enforce dependency rules for secret and function DDL (for example, reject destructive secret operations when referenced by live UDFs unless dependencies are removed first).
- Provide clear dependency-related error messages so operators can resolve blocking relationships safely.

## Capabilities

### New Capabilities
- `udf-secret-parameter-binding`: Allow UDF definitions to use secret references for sensitive parameters with validation at DDL time.
- `udf-secret-dependency-management`: Maintain and enforce dependency relationships between UDF objects and secrets across create, alter, and drop operations.

### Modified Capabilities
- None.

## Impact

- `src/frontend/`: UDF SQL parsing, binding, privilege checks, and DDL validation logic for secret-backed parameters.
- `src/meta/`: Metadata persistence and lifecycle checks for function-secret dependency edges.
- `src/catalog/` and related protobuf metadata contracts: New or extended schema fields to represent secret references and dependencies.
- User-facing SQL behavior: Safer secret usage in UDFs, plus deterministic DDL behavior when secrets and functions depend on each other.

## 1. SQL and frontend option binding

- [x] 1.1 Extend `CREATE FUNCTION ... WITH (...)` handling to accept `SECRET <schema>.<name>` option values alongside existing boolean UDF options
- [x] 1.2 Resolve secret names to secret ids during function DDL and return clear not-found / permission-denied errors
- [x] 1.3 Add frontend validation that secret-backed UDF options are persisted as secret refs instead of plaintext option values

## 2. Catalog schema and metadata plumbing

- [x] 2.1 Add additive protobuf/catalog fields for function secret references and regenerate related Rust protobuf bindings
- [x] 2.2 Extend function catalog/model conversions (frontend catalog, meta model/controller) to round-trip secret refs
- [x] 2.3 Add meta migration/model update for function secret-ref storage with backward-compatible defaults for existing functions

## 3. Function-secret dependency enforcement

- [x] 3.1 Update meta create-function transaction to insert one `object_dependency` edge per referenced secret
- [x] 3.2 Ensure dependency insertion and function creation are atomic in one transaction and rollback together on failure
- [x] 3.3 Verify drop-secret restriction path reports dependent functions using existing dependency checks

## 4. Runtime secret resolution for UDF execution

- [x] 4.1 Extend UDF runtime metadata path to carry function secret refs to execution build sites
- [x] 4.2 Resolve secret refs through `LocalSecretManager` when constructing UDF runtime options, without persisting/logging plaintext
- [x] 4.3 Add guards so functions without secret refs preserve existing execution behavior

## 5. Validation and regression tests

- [x] 5.1 Add SQL/frontend tests for `CREATE FUNCTION` with valid, missing, and unauthorized secret references
- [x] 5.2 Add meta/controller tests covering function-secret dependency creation and cleanup on function drop
- [x] 5.3 Add integration/e2e tests for `DROP SECRET` failure with dependent functions and success after dropping the dependent function

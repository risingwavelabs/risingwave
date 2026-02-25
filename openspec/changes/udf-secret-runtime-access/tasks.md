## 1. Runtime option exposure contract

- [x] 1.1 Add a shared helper in `expr/impl` to render language-safe `RW_UDF_OPTIONS` prelude from `BuildOptions.options`
- [x] 1.2 Keep prelude injection disabled when options map is empty to preserve legacy behavior

## 2. Python and JavaScript runtime integration

- [x] 2.1 Update Python UDF builder to prepend the runtime-options prelude before registering function/aggregate bodies
- [x] 2.2 Update JavaScript UDF builder to prepend the runtime-options prelude for both direct and compatibility fallback body paths
- [x] 2.3 Ensure options injection remains compatible with scalar/table/aggregate UDF creation paths

## 3. Validation and regression coverage

- [x] 3.1 Add/extend UDF e2e tests proving Python UDFs can read secret-backed options at runtime
- [x] 3.2 Add/extend UDF e2e tests proving JavaScript UDFs can read secret-backed options at runtime
- [x] 3.3 Keep regression assertions for rejecting plaintext custom `WITH (...)` option values

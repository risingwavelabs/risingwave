// Secret References in Function Parameters - Implementation Guide
//
// This document describes how secret references work when used as function parameters.
//
// # Overview
//
// RisingWave supports passing secrets as parameters to functions (both built-in and user-defined).
// This allows UDFs and functions to access sensitive credentials without exposing them in query text
// or logs. The implementation follows the same security model as WITH options.
//
// # Syntax
//
// ```sql
// -- Use secret as TEXT (default)
// SELECT my_function(data, secret api_key);
//
// -- Use secret as FILE (writes secret to temp file, passes file path)
// SELECT my_function(data, secret db_password AS FILE);
//
// -- Multiple secrets
// SELECT concat(secret username, ':', secret password);
// ```
//
// # Execution Flow
//
// 1. **Parser (sqlparser)**
//    - Recognizes `secret <name> [AS FILE]` in function arguments
//    - Creates `FunctionArgExpr::SecretRef(SecretRefValue)` AST node
//
// 2. **Binder (frontend)**
//    - Creates `SecretRefExpr` expression node
//    - For UDFs: Resolves secret name via catalog lookup to get `PbSecretRef`
//    - For builtin functions: Same resolution process
//    - Stores secret references in a map: `{arg_index → PbSecretRef}`
//    - Replaces original argument with placeholder (empty string literal)
//    - Attaches `secret_refs` map to function expression
//
// 3. **Serialization (protobuf)**
//    - `UserDefinedFunction` and `FunctionCall` carry `secret_refs: map<uint32, PbSecretRef>`
//    - Prevents secrets from appearing in logical plans
//
// 4. **Runtime (expr/core)**
//    - Deserializes `secret_refs` map from protobuf
//    - During evaluation:
//      a. Fetches secret value from `LocalSecretManager`
//      b. Converts to appropriate type (TEXT or FILE)
//      c. Injects secret value into argument column
//      d. Proceeds with function execution
//
// # Security Model
//
// - **Access Control**: Uses same privilege checks as WITH options
// - **Audit Trail**: Secret usage is not logged in query plans
// - **Type Safety**: Secrets are stored as VARCHAR internally
// - **Scope**: Secrets are only valid as direct function arguments
//
// # File References (AS FILE)
//
// When `secret <name> AS FILE` is used:
// - Secret value is written to a temporary file in the secret manager's directory
// - File path (string) is passed to the function instead of the secret value
// - File is cleaned up when the secret is removed
// - Use case: External tools/libraries that require file input
//
// # Limitations
//
// 1. Only VARCHAR secrets are currently supported
// 2. Secrets must be used as direct arguments (not in expressions like CASE/COALESCE)
// 3. Secrets cannot appear in SELECT list directly (must be in function call)
// 4. SQL UDFs cannot access secrets (requires compiled language like javascript/python)
//
// # Example: UDF with Secret Parameter
//
// ```sql
// CREATE SECRET api_key WITH (backend = 'meta') AS 'my_api_key_xyz';
//
// -- UDF that makes HTTP request to external API
// CREATE FUNCTION fetch_user_data(user_id INT, secret token VARCHAR)
//   RETURNS JSONB
//   LANGUAGE javascript
// AS $$
//   const https = require('https');
//   const options = {
//     headers: { 'Authorization': `Bearer ${token}` }
//   };
//   return await https.get(`https://api.example.com/users/${user_id}`, options);
// $$;
//
// -- Usage
// SELECT fetch_user_data(42, secret api_key);
// ```
//
// # Key Files
//
// - **Parser**: `src/sqlparser/src/ast/mod.rs` - FunctionArgExpr::SecretRef
// - **Parser**: `src/sqlparser/src/parser.rs` - parse_function_args
// - **Binder**: `src/frontend/src/binder/expr/function/mod.rs` - bind_function
// - **Binder**: `src/frontend/src/binder/expr/function/builtin_scalar.rs` - builtin handling
// - **Expr**: `src/frontend/src/expr/secret_ref.rs` - SecretRefExpr type
// - **Expr**: `src/frontend/src/expr/function_call.rs` - FunctionCall with secret_refs
// - **Expr**: `src/frontend/src/expr/user_defined_function.rs` - UDF with secret_refs
// - **Proto**: `proto/expr.proto` - FunctionCall, UserDefinedFunction
// - **Runtime**: `src/expr/core/src/expr/build.rs` - Builtin secret injection
// - **Runtime**: `src/expr/core/src/expr/expr_udf.rs` - UDF secret injection
// - **Tests**: `e2e_test/ddl/secret_function_param*.slt` - E2E tests
//
// # Related Features
//
// - Secret management: `src/common/secret/src/secret_manager.rs`
// - WITH options: `src/connector/src/with_options.rs` (reference implementation)
// - Privilege checks: `src/frontend/src/handler/privilege.rs`

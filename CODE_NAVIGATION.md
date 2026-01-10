# Secret References in Function Parameters - Code Navigation Guide

## Quick Reference to Key Code Locations

### Parser Layer

**SQL Syntax Recognition**
- File: `src/sqlparser/src/ast/mod.rs`
- Key Type: `FunctionArgExpr::SecretRef(SecretRefValue)`
- Lines: ~50
- Handles: Parsing `secret <name> [AS FILE]`

**Function Argument Parsing**
- File: `src/sqlparser/src/parser.rs`
- Function: `parse_function_args()`
- Recognizes secret references in function argument lists

**AST Definition**
- File: `src/sqlparser/src/ast/value.rs`
- Type: `SecretRefValue`
- Contains: `secret_name: ObjectName`, `ref_as: SecretRefAsType`

### Frontend Expression Layer

**Secret Reference Expression Type**
- File: `src/frontend/src/expr/secret_ref.rs` (NEW)
- Type: `SecretRefExpr`
- Key Method: `try_to_expr_proto()` → Returns error (resolved at bind time)
- Returns: `DataType::Varchar`

**Expression Integration**
- File: `src/frontend/src/expr/mod.rs`
- Macro: `ExprImpl` includes `SecretRefExpr` variant
- Lines: See ExprImpl definition

**Function Call with Secrets**
- File: `src/frontend/src/expr/function_call.rs`
- Field: `pub(super) secret_refs: BTreeMap<u32, PbSecretRef>`
- Methods:
  - `try_to_expr_proto()` → Serializes secret_refs to protobuf
  - `from_expr_proto()` → Deserializes (sets default map)

**UDF with Secrets**
- File: `src/frontend/src/expr/user_defined_function.rs`
- Field: `pub secret_refs: BTreeMap<u32, PbSecretRef>`
- Constructor: `UserDefinedFunction::new()` initializes to Default
- Serialization: `try_to_expr_proto()` includes secret_refs

**Expression Traversal**
- Visitor: `src/frontend/src/expr/expr_visitor.rs`
  - Hook: `visit_secret_ref(&mut self, secret_ref: SecretRefExpr) -> ExprImpl`
  
- Rewriter: `src/frontend/src/expr/expr_rewriter.rs`
  - Hook: `rewrite_secret_ref(&mut self, secret_ref: SecretRefExpr) -> ExprImpl`
  
- Mutator: `src/frontend/src/expr/expr_mutator.rs`
  - Hook: `visit_secret_ref(&mut self, secret_ref: SecretRefExpr)`

### Binder (Where Magic Happens)

**Main Entry Point**
- File: `src/frontend/src/binder/expr/function/mod.rs`
- Function: `bind_function()`
- Lines: ~460-470 for UDF secret resolution

**UDF Secret Resolution**
```rust
// File: src/frontend/src/binder/expr/function/mod.rs
// Lines: 432-462
if let ExprImpl::SecretRefExpr(secret_expr) = arg {
    // 1. Resolve secret name
    let (schema, name) = Binder::resolve_schema_qualified_name(db_name, ...)?;
    
    // 2. Catalog lookup
    let (secret_catalog, _) = self.catalog.get_secret_by_name(...)?;
    
    // 3. Convert reference type
    let ref_as = match secret_expr.secret_ref.ref_as {
        SecretRefAsType::Text => PbRefAsType::Text,
        SecretRefAsType::File => PbRefAsType::File,
    };
    
    // 4. Create PbSecretRef
    let pb = PbSecretRef { secret_id: secret_catalog.id, ref_as };
    
    // 5. Store in map
    secret_refs.insert(idx as u32, pb);
    
    // 6. Replace with placeholder
    resolved_args.push(ExprImpl::literal_varchar(String::new()));
}
```

**Builtin Function Secret Resolution**
- File: `src/frontend/src/binder/expr/function/builtin_scalar.rs`
- Function: `bind_builtin_scalar_function()`
- Lines: 37-77
- Same resolution logic, applied before handler execution
- Attaches `secret_refs` to resulting `FunctionCall`

### Protobuf Schema

**Message Definitions**
- File: `proto/expr.proto`

**FunctionCall Message**
```protobuf
message FunctionCall {
  repeated ExprNode children = 1;
  map<uint32, secret.SecretRef> secret_refs = 2;
}
```

**UserDefinedFunction Message**
```protobuf
message UserDefinedFunction {
  repeated ExprNode children = 1;
  string name = 2;
  // ... other fields ...
  map<uint32, secret.SecretRef> secret_refs = 15;
}
```

### Runtime (Where Secrets Are Injected)

**Builtin Function Build**
- File: `src/expr/core/src/expr/build.rs`
- Struct: `FuncCallBuilder`
- Method: `build_boxed()`
- Lines: ~190-215

**Key Steps**:
```rust
// For each secret in func_call.secret_refs:
let value = LocalSecretManager::global()
    .fill_secret(secret_ref.clone())?;

let lit = LiteralExpression::new(
    DataType::Varchar,
    Some(ScalarImpl::Utf8(value.into()))
);

children[idx] = lit.boxed();
```

**UDF Secret Injection**
- File: `src/expr/core/src/expr/expr_udf.rs`
- Struct: `UserDefinedFunction`
- Method: `eval()`
- Lines: ~68-84

**Key Steps**:
```rust
// In eval(), before calling eval_inner():
for (idx, secret_ref) in &self.secret_refs {
    let value = LocalSecretManager::global()
        .fill_secret(secret_ref.clone())?;
    
    let mut builder = DataType::Varchar.create_array_builder(num_rows);
    for _ in 0..num_rows {
        let scalar_value: ScalarImpl = value.clone().into();
        builder.append(Some(scalar_value));
    }
    
    columns[*idx] = builder.finish().into_ref();
}
```

**UDF Build from Protobuf**
- File: `src/expr/core/src/expr/expr_udf.rs`
- Trait: `Build for UserDefinedFunction`
- Method: `build()`
- Lines: ~240-252

**Deserialization**:
```rust
let secret_refs_map = udf.secret_refs.clone();
// ... build UDF ...
secret_refs: secret_refs_map
```

### Secret Manager (Core Infrastructure)

**Secret Value Retrieval**
- File: `src/common/secret/src/secret_manager.rs`
- Struct: `LocalSecretManager`
- Method: `fill_secret(PbSecretRef) -> SecretResult<String>`
- Lines: ~130-145

**Supports**:
- TEXT: Returns secret value as string
- FILE: Writes to temp file, returns file path

### Tests

**Builtin Function Tests**
- File: `e2e_test/ddl/secret_function_param.slt`
- Tests: 7+ scenarios
- Covers: length(), concat(), substring(), CASE, etc.

**UDF Tests**
- File: `e2e_test/ddl/secret_udf_param.slt`
- Tests: Syntax demonstration
- Covers: UDF parameter passing patterns

**Error Cases**
- File: `e2e_test/ddl/secret_function_param_errors.slt`
- Tests: 4+ error scenarios
- Covers: Nonexistent secrets, syntax errors, invalid contexts

## Code Flow Diagrams

### Full Parse-to-Runtime Flow

```
User Query: SELECT f(x, secret api_key)
    ↓
PARSE LAYER
├─ sqlparser::Parser::parse_expression()
│  └─ parse_function_args()
│     └─ Creates FunctionArgExpr::SecretRef("api_key", Text)
    ↓
BIND LAYER
├─ binder::bind_function()
│  ├─ bind_function_expr_arg(SecretRefExpr)
│  │  └─ Creates ExprImpl::SecretRefExpr
│  ├─ For UDFs:
│  │  ├─ Binder::resolve_schema_qualified_name()
│  │  ├─ catalog.get_secret_by_name()
│  │  ├─ Convert to PbSecretRef
│  │  ├─ secret_refs.insert(idx, pb)
│  │  └─ Replace arg with Literal("")
│  │  └─ UserDefinedFunction::new() with secret_refs
│  │
│  ├─ For Builtins:
│  │  ├─ bind_builtin_scalar_function()
│  │  ├─ (Same secret resolution)
│  │  └─ FunctionCall with secret_refs
    ↓
SERIALIZE
├─ try_to_expr_proto()
│  ├─ Serialize args (without secrets)
│  └─ Serialize secret_refs map
    ↓
PLAN -> PROTOBUF
├─ UserDefinedFunction protobuf
│  ├─ children: [...]
│  └─ secret_refs: {0 → PbSecretRef}
    ↓
RUNTIME BUILD
├─ expr/core/build.rs::FuncCallBuilder
│  ├─ Read secret_refs from protobuf
│  ├─ For each secret:
│  │  ├─ LocalSecretManager::fill_secret()
│  │  ├─ Create LiteralExpression
│  │  └─ Replace child
│  └─ build_func()
    ↓
EXECUTION
├─ Expression::eval()
│  ├─ For UDFs:
│  │  ├─ Inject secrets into columns
│  │  └─ Call eval_inner()
│  └─ Return result
```

## Debugging Tips

### Checking if Secret Was Resolved
1. Look at `UserDefinedFunction::secret_refs` size
2. If non-empty, secret was resolved
3. Check `secret_id` in map matches catalog

### Tracing Secret Value
1. Break in `LocalSecretManager::fill_secret()`
2. Check `PbSecretRef::secret_id`
3. Verify it exists in secret store
4. Check `ref_as_type` for TEXT vs FILE behavior

### Common Issues
- **"SecretRefExpr should be resolved"**: Secret didn't get resolved in binder
- **"Secret not found"**: Catalog lookup failed, check name/schema
- **Empty string in argument**: Placeholder not replaced with actual value
  - Check if runtime injection code was reached
  - Verify `secret_refs` map was deserialized

## Related Files (Reference Implementations)

**WITH Options Secret Handling**
- File: `src/connector/src/with_options.rs`
- Function: `resolve_secret_ref_in_with_options()`
- Pattern: Same approach as function parameters

**Privilege Checking**
- File: `src/frontend/src/handler/privilege.rs`
- Used: During catalog lookups in binder

**Catalog Access**
- File: `src/frontend/src/catalog/`
- Method: `get_secret_by_name()`
- Returns: `(SecretCatalog, ...)` with privilege checks

---

**Navigation Tip**: Use the file paths and line numbers above to jump directly to implementations in your editor.

**Last Updated**: 2025-12-26

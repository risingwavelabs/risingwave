# AGENTS.md - Expression Core Crate

## 1. Scope

Policies for the `risingwave_expr` crate providing the expression evaluation framework core.

## 2. Purpose

The expression core crate provides:
- **Expression Traits**: `Expression`, `AggregateFunction`, `TableFunction` interfaces
- **Function Registry**: Dynamic function lookup and dispatch
- **Expression Builder**: Construction from protobuf plans
- **Context Management**: Evaluation context (time zone, session variables)
- **Signature System**: Function overload resolution and type checking

This enables type-safe, extensible expression evaluation for SQL queries.

## 3. Structure

```
src/expr/core/
├── Cargo.toml                    # Crate manifest
└── src/
    ├── lib.rs                    # Module exports and core traits
    ├── expr/                     # Expression trait and implementations
    │   ├── mod.rs                # Expression trait definition
    │   └── context.rs            # Evaluation context
    ├── aggregate/                # Aggregate function framework
    │   ├── mod.rs                # AggregateFunction trait
    │   └── build.rs              # Aggregate builder
    ├── table_function/           # Table-returning functions
    │   └── mod.rs
    ├── window_function/          # Window function framework
    │   └── mod.rs
    ├── scalar/                   # Scalar expression types
    │   └── mod.rs
    ├── sig/                      # Function signatures
    │   ├── mod.rs                # Signature definitions
    │   └── func.rs               # Function metadata
    ├── expr_context.rs           # Expression evaluation context
    ├── error.rs                  # ExpressionError types
    └── codegen.rs                # Code generation utilities
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Trait exports, `ExprError`, `BoxedExpression` |
| `src/expr/mod.rs` | Core `Expression` trait with `eval()` and `eval_row()` |
| `src/aggregate/mod.rs` | `AggregateFunction` trait, state management |
| `src/sig/mod.rs` | `FuncSign`, `DataTypeName`, signature matching |
| `src/expr_context.rs` | `ExprContext` with time zone, session info |
| `src/error.rs` | Expression evaluation error types |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Implement both `eval()` (batch) and `eval_row()` (single row) for expressions
- Use `linkme` for function registration via distributed slices
- Add function signatures to `sig/` for new functions
- Update `FuncSign` when adding function variants
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Change `Expression` trait without updating all 200+ implementations
- Remove `eval_row()` optimization (used in critical paths)
- Break function signature backward compatibility
- Add blocking operations in expression evaluation
- Use `unsafe` without justification and safety comments
- Modify aggregate state serialization format without migration

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_expr` |
| Trait tests | `cargo test -p risingwave_expr expr` |
| Signature tests | `cargo test -p risingwave_expr sig` |
| Aggregate tests | `cargo test -p risingwave_expr aggregate` |
| Window tests | `cargo test -p risingwave_expr window` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **risingwave_common**: Data types, arrays, error types
- **risingwave_common_estimate_size**: Memory size estimation
- **risingwave_pb**: Expression protobuf definitions
- **risingwave_expr_macro**: Expression derive macros
- **linkme**: Distributed function registration
- **async-trait**: Async expression evaluation
- **futures-async-stream**: Stream-based table functions
- **downcast-rs**: Expression type downcasting

Contracts:
- Expressions are pure functions (no side effects)
- `eval()` processes `DataChunk` batches for efficiency
- `eval_row()` optimizes single-row evaluation
- Functions are registered at link time via `#[linkme::distributed_slice]`
- Signature matching follows PostgreSQL precedence rules

## 9. Overrides

None. Follows parent `src/expr/AGENTS.md` if it exists.

## 10. Update Triggers

Regenerate this file when:
- New expression trait or trait method added
- Function registration mechanism changed
- Signature system modified
- Expression context structure changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

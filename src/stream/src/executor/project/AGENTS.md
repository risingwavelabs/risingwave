# AGENTS.md - Projection Operators

## 1. Scope

Policies for the `src/stream/src/executor/project` directory, containing projection operators for expression evaluation and result transformation.

## 2. Purpose

The project module implements SQL projection operations:
- Scalar expression evaluation (SELECT expressions)
- Project set operations (UNNEST, generate_series)
- Materialized expressions for incremental view maintenance

These operators transform input rows by evaluating expressions, expanding arrays, and computing derived columns for downstream consumption.

## 3. Structure

```
src/stream/src/executor/project/
├── mod.rs                    # Module exports
├── project_scalar.rs         # Scalar projection with expression evaluation
├── project_set.rs            # Set-returning function projection
└── materialized_exprs.rs     # Materialized expression executor
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `project_scalar.rs` | Evaluates scalar expressions on input rows, producing one output row per input |
| `project_set.rs` | Evaluates set-returning functions, producing zero or more rows per input |
| `materialized_exprs.rs` | Maintains materialized expressions with change streaming |

## 5. Edit Rules (Must)

- Use `NonStrictExpression::eval_infallible` for expression evaluation (per clippy.toml)
- Handle errors gracefully without panicking in production
- Support watermark derivation through expressions
- Implement efficient batch evaluation for performance
- Handle NULL values correctly in all expression contexts
- Propagate visibility bitmaps through projections

## 6. Forbidden Changes (Must Not)

- Do not use `Expression::eval` directly (violates clippy.toml)
- Never panic on expression evaluation errors in production
- Do not modify watermark semantics without planner coordination
- Avoid memory allocation in tight evaluation loops
- Never skip visibility handling for projected rows

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream project` |
| Scalar projection | `cargo test -p risingwave_stream project_scalar` |
| Project set | `cargo test -p risingwave_stream project_set` |

## 8. Dependencies & Contracts

- **Expressions**: `risingwave_expr` for expression evaluation
- **Policy**: Non-strict evaluation required (clippy.toml enforced)
- **Watermark**: Expression-based watermark derivation supported
- **Performance**: Batch evaluation preferred over row-by-row

## 9. Overrides

Follows parent AGENTS.md at `/home/k11/risingwave/src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New projection types added
- Expression evaluation interface changes
- Set-returning function support expanded
- Watermark derivation rules updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/stream/src/executor/AGENTS.md

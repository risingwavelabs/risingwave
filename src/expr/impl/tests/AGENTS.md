# AGENTS.md - Expression Implementation Tests

## 1. Scope

Policies for `./src/expr/impl/tests` - unit tests for RisingWave expression function signatures and registry validation.

## 2. Purpose

The expression test suite validates:
- Function signature registry consistency
- No duplicate function signatures with conflicting return types
- Proper function dispatch and resolution
- CAST function signature correctness
- Aggregate and scalar function registration

These tests ensure the function signature map used by the frontend for function resolution is valid and unambiguous.

## 3. Structure

```
src/expr/impl/tests/
├── sig.rs                    # Function signature registry tests
└── AGENTS.md                 # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `sig.rs` | Tests for `FUNCTION_REGISTRY` and function signature validation |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Use `risingwave_expr_impl::enable!()` to register all functions before testing
- Use snapshot testing with `expect_test` for stable output comparison
- Test for duplicate signatures that would confuse function resolution
- Sort results for deterministic test output
- Document expected duplicates (e.g., polymorphic CAST functions) in comments
- Pass `./risedev c` (clippy) before submitting changes
- Update snapshot expectations when function signatures intentionally change

## 6. Forbidden Changes (Must Not)

- Remove function signature validation tests
- Skip deprecated function handling in tests
- Add non-deterministic assertions
- Modify snapshot expectations without understanding the change
- Remove `risingwave_expr_impl::enable!()` call (required for registration)
- Ignore duplicate signature warnings without investigation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run signature tests | `cargo test -p risingwave_expr_impl --test sig` |
| Update snapshots | `UPDATE_EXPECT=1 cargo test -p risingwave_expr_impl --test sig` |
| All expr impl tests | `cargo test -p risingwave_expr_impl` |

## 8. Dependencies & Contracts

- `risingwave_expr`: Function signature registry (`FUNCTION_REGISTRY`)
- `risingwave_expr_impl`: Function implementations and registration
- `expect_test`: Snapshot testing for stable output comparison
- `itertools`: Iterator utilities for test data processing
- Function registration: Requires `risingwave_expr_impl::enable!()` macro
- Signature format: `FuncSign` with `name`, `inputs_type`, `ret_type`, `deprecated`

## 9. Overrides

None. Inherits all rules from parent `./src/expr/impl/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New function signature validation tests added
- Function registration mechanism changes
- Snapshot testing approach modified
- New function categories requiring signature validation
- FUNCTION_REGISTRY API changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/expr/impl/AGENTS.md

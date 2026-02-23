# AGENTS.md - Expression Implementation Crate

## 1. Scope

Policies for the `risingwave_expr_impl` crate providing concrete expression and function implementations.

## 2. Purpose

The expression implementation crate provides:
- **Scalar Functions**: 200+ SQL functions (math, string, date, JSON, etc.)
- **Aggregate Functions**: COUNT, SUM, AVG, MIN, MAX, ARRAY_AGG, etc.
- **Table Functions**: UNNEST, generate_series, etc.
- **Window Functions**: ROW_NUMBER, RANK, LAG, LEAD, etc.
- **UDF Support**: External function runtime (WASM, Python, Remote)

This is the main function implementation crate for RisingWave SQL.

## 3. Structure

```
src/expr/impl/
├── Cargo.toml                    # Crate manifest
├── benches/                      # Criterion benchmarks
└── src/
    ├── lib.rs                    # Module exports and registrations
    ├── scalar/                   # Scalar function implementations
    │   ├── mod.rs                # Registration module
    │   ├── arithmetic.rs         # Math functions
    │   ├── string.rs             # String functions
    │   ├── time.rs               # Date/time functions
    │   ├── jsonb.rs              # JSONB functions
    │   └── ...                   # 20+ function categories
    ├── aggregate/                # Aggregate function implementations
    │   ├── mod.rs                # Registration
    │   ├── count.rs              # COUNT implementation
    │   ├── sum.rs                # SUM implementation
    │   └── ...
    ├── table_function/           # Table function implementations
    │   ├── mod.rs
    │   └── unnest.rs
    ├── window_function/          # Window function implementations
    │   ├── mod.rs
    │   └── row_number.rs
    └── udf/                      # User-defined function support
        ├── mod.rs
        ├── wasm.rs               # WASM UDF runtime
        ├── python.rs             # Python UDF runtime
        └── external.rs           # External process UDF
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Module declarations, `#[linkme::distributed_slice]` registrations |
| `src/scalar/mod.rs` | Scalar function exports, `#[function]` macro usage |
| `src/aggregate/mod.rs` | Aggregate function exports |
| `src/udf/mod.rs` | UDF runtime initialization |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Register new functions with `#[function]` macro from `risingwave_expr_macro`
- Implement both `eval()` and `eval_row()` for scalar functions
- Use property-based testing for numeric functions (check edge cases)
- Add benchmarks for performance-critical functions in `benches/`
- Handle NULL inputs correctly following SQL semantics
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Remove function implementations without deprecation cycle
- Change function semantics without updating planner tests
- Ignore overflow/underflow in numeric functions (handle explicitly)
- Return incorrect NULL handling (use `Option` properly)
- Add non-deterministic functions without marking them
- Break UDF runtime ABI without versioning
- Use `unsafe` without justification and safety comments

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_expr_impl` |
| Scalar tests | `cargo test -p risingwave_expr_impl scalar` |
| Aggregate tests | `cargo test -p risingwave_expr_impl aggregate` |
| UDF tests | `cargo test -p risingwave_expr_impl udf --features udf` |
| Benchmarks | `cargo bench -p risingwave_expr_impl` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **risingwave_expr**: Core expression traits
- **risingwave_common**: Data types, arrays
- **risingwave_pb**: Function protobuf definitions
- **risingwave_sqlparser**: SQL parser for function names
- **arrow-udf-runtime**: UDF execution (optional, `udf` feature)
- **fancy-regex**: Regular expression functions
- **chrono-tz**: Time zone support
- **jsonbb**: JSONB operations

Contracts:
- Functions are registered via `#[linkme::distributed_slice(EXPRS)]`
- Scalar functions handle NULL inputs per SQL standard
- Aggregate functions maintain state in `AggregateState` struct
- UDFs are sandboxed and have timeouts

## 9. Overrides

None. Follows parent `src/expr/AGENTS.md` if it exists.

## 10. Update Triggers

Regenerate this file when:
- New function category added
- UDF runtime architecture changed
- Function registration mechanism modified
- New benchmark harness added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

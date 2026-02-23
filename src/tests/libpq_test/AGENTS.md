# AGENTS.md - libpq Compatibility Tests

## 1. Scope

Directory: `src/tests/libpq_test`
Crate: `risingwave_libpq_test`

Compatibility tests for RisingWave with PostgreSQL libpq C client library.

## 2. Purpose

Validates RisingWave compatibility with the PostgreSQL C client library:
- Test libpq connection establishment and authentication
- Verify SQL execution via libpq API
- Validate result set handling and data type mappings
- Test asynchronous query processing (async API)
- Ensure binary format support for data transfer
- Verify notification and LISTEN/NOTIFY behavior

## 3. Structure

```
src/tests/libpq_test/
├── src/
│   └── main.rs         # libpq test runner and C FFI tests
├── Cargo.toml          # Dependencies: pq-sys (libpq bindings), tokio
└── AGENTS.md           # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/main.rs` | libpq FFI test cases and C API validation |
| `Cargo.toml` | Build configuration with libpq linking |

## 5. Edit Rules (Must)

- Use `pq-sys` crate for safe Rust bindings to libpq
- Test both synchronous and asynchronous libpq APIs
- Cover connection lifecycle: connect, query, fetch, close
- Verify error handling and status code translations
- Test binary result format for numeric types
- Handle memory management correctly (PQclear, PQfinish)
- Document test cases mapping to libpq functions
- Require libpq development headers for compilation
- Run `cargo fmt` before committing
- Test on systems with PostgreSQL client libraries installed

## 6. Forbidden Changes (Must Not)

- Use raw FFI without `pq-sys` safe wrappers
- Skip memory cleanup (PQclear result sets, close connections)
- Hardcode connection strings with credentials
- Remove tests for core libpq functionality
- Skip async API testing
- Modify without testing on clean environment

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run tests | `cargo run -p risingwave_libpq_test` |
| Build check | `cargo check -p risingwave_libpq_test` |
| With connection string | `cargo run -p risingwave_libpq_test -- "host=localhost port=4566 dbname=dev user=root"` |

## 8. Dependencies & Contracts

- `pq-sys`: Low-level FFI bindings to libpq
- System dependency: `libpq-dev` or `postgresql-libs`
- Requires running RisingWave cluster accepting libpq connections
- Uses PostgreSQL wire protocol on default port 4566
- Link-time dependency on libpq shared library

## 9. Overrides

| Parent Rule | Override |
|-------------|----------|
| Test entry | Requires system libpq library installed |

## 10. Update Triggers

Regenerate this file when:
- New libpq API surface tested
- PostgreSQL protocol compatibility changes
- pq-sys crate version updates
- libpq linking or build configuration changes
- New async API patterns introduced

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `./AGENTS.md`

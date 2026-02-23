# AGENTS.md - E2E Extended Mode Tests

## 1. Scope

Directory: `src/tests/e2e_extended_mode`
Crate: `risingwave_e2e_extended_mode_test`

End-to-end tests for RisingWave extended query mode (prepared statements) compatibility.

## 2. Purpose

Validates extended query protocol support for PostgreSQL compatibility:
- Test prepared statement parsing and execution
- Validate parameter binding for all data types
- Verify portal management and cursor behavior
- Test extended query mode in batch and streaming contexts
- Ensure compatibility with PostgreSQL JDBC and libpq drivers

## 3. Structure

```
src/tests/e2e_extended_mode/
├── src/
│   ├── lib.rs          # Test utilities and shared setup
│   ├── main.rs         # CLI entry point
│   ├── opts.rs         # Command-line options parsing
│   └── test.rs         # Extended mode test implementations
├── Cargo.toml          # Dependencies: frontend, pgwire, test utilities
└── AGENTS.md           # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/test.rs` | Test cases for prepared statements and parameter binding |
| `src/opts.rs` | CLI options for test configuration (host, port, database) |
| `src/main.rs` | Test runner entry point with connection setup |
| `src/lib.rs` | Shared utilities for test database connections |

## 5. Edit Rules (Must)

- Test all PostgreSQL extended query protocol phases: Parse, Bind, Execute, Close
- Cover all RisingWave data types in parameter binding tests
- Use `tokio-postgres` for protocol-level testing
- Include tests for error handling in extended mode
- Document each test case purpose and protocol interaction
- Clean up prepared statements and portals after tests
- Test both text and binary parameter formats
- Run `cargo fmt` before committing
- Ensure tests work against running RisingWave cluster

## 6. Forbidden Changes (Must Not)

- Skip testing any core extended query protocol phase
- Remove test coverage for data type parameter binding
- Hardcode connection credentials (use environment or CLI options)
- Leave prepared statements or portals open after test completion
- Modify without testing against actual RisingWave deployment
- Break PostgreSQL protocol compatibility

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run tests | `cargo run -p risingwave_e2e_extended_mode_test` |
| With options | `cargo run -p risingwave_e2e_extended_mode_test -- --host localhost --port 4566` |
| Build check | `cargo check -p risingwave_e2e_extended_mode_test` |

## 8. Dependencies & Contracts

- `tokio-postgres`: Async PostgreSQL client for protocol testing
- `tokio`: Async runtime for test execution
- `clap`: CLI argument parsing
- Requires running RisingWave cluster (frontend accessible)
- Uses standard PostgreSQL protocol on port 4566 (default)
- Tests depend on `pgwire` crate implementation

## 9. Overrides

None. Follows parent test policies from `src/tests/` hierarchy.

## 10. Update Triggers

Regenerate this file when:
- New extended query protocol features added
- New data types requiring parameter binding tests
- pgwire implementation changes
- PostgreSQL protocol compatibility updates
- Test framework or utilities modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `/home/k11/risingwave/AGENTS.md`

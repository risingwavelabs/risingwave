# AGENTS.md - MySQL Compatibility Tests

## 1. Scope

Directory: `src/tests/mysql_test`
Crate: `risingwave_mysql_test`

Compatibility tests for MySQL protocol support and MySQL client library integration.

## 2. Purpose

Validates RisingWave MySQL protocol compatibility:
- Test MySQL wire protocol implementation
- Verify compatibility with `mysql_async` Rust client
- Validate MySQL authentication methods
- Test SQL compatibility for MySQL-specific syntax
- Ensure proper type mapping between MySQL and RisingWave types
- Validate prepared statements via MySQL protocol

## 3. Structure

```
src/tests/mysql_test/
├── src/
│   └── lib.rs                      # Library exports and MySQL test utilities
├── tests/
│   └── mysql_async_connection_pool.rs # Async connection pool tests
├── Cargo.toml                      # Dependencies: mysql_async, tokio
└── AGENTS.md                       # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `tests/mysql_async_connection_pool.rs` | MySQL async client connection pool tests |
| `src/lib.rs` | Test utilities and MySQL client setup helpers |

## 5. Edit Rules (Must)

- Use `mysql_async` crate for MySQL client testing
- Test connection pooling and connection lifecycle
- Verify authentication handshake and protocol negotiation
- Cover MySQL-specific SQL syntax support
- Test prepared statements and parameter binding
- Validate result set metadata and row parsing
- Handle MySQL error packet parsing correctly
- Document MySQL-specific behavior differences from PostgreSQL
- Run `cargo fmt` before committing
- Test against RisingWave MySQL-compatible frontend port

## 6. Forbidden Changes (Must Not)

- Remove MySQL protocol compatibility tests
- Hardcode credentials in connection strings
- Skip connection pool stress testing
- Remove tests for MySQL-specific syntax
- Modify without testing actual MySQL client connectivity
- Break MySQL wire protocol compatibility

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run tests | `cargo test -p risingwave_mysql_test` |
| Specific test | `cargo test -p risingwave_mysql_test connection_pool` |
| Build check | `cargo check -p risingwave_mysql_test` |

## 8. Dependencies & Contracts

- `mysql_async`: Pure Rust MySQL client library
- `tokio`: Async runtime for test execution
- Requires RisingWave MySQL protocol frontend enabled
- Uses MySQL default port 4566 (shared with PostgreSQL protocol)
- Protocol detection based on initial client packet

## 9. Overrides

None. Follows parent test policies from `src/tests/` hierarchy.

## 10. Update Triggers

Regenerate this file when:
- MySQL protocol implementation changes
- New MySQL client library features tested
- mysql_async crate version updates
- MySQL-specific SQL syntax support added
- Authentication method changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `./AGENTS.md`

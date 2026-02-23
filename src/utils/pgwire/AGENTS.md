# AGENTS.md - PGWire Crate

## 1. Scope

Policies for the `pgwire` crate providing PostgreSQL wire protocol implementation.

## 2. Purpose

The pgwire crate provides:
- **Protocol Handler**: PostgreSQL wire protocol v3 implementation
- **Authentication**: MD5, SCRAM-SHA-256, LDAP, JWT authentication
- **Query Processing**: Parse, Bind, Execute message handling
- **SSL/TLS**: Encrypted connection support
- **Extended Protocol**: Prepared statements, portals, parameters

This enables RisingWave to accept connections from standard PostgreSQL clients.

## 3. Structure

```
src/utils/pgwire/
├── Cargo.toml                    # Crate manifest
├── src/
│   ├── lib.rs                    # Module exports
│   ├── pg_protocol.rs            # Core protocol state machine
│   ├── pg_server.rs              # TCP server and connection handling
│   ├── pg_message.rs             # Message serialization/deserialization
│   ├── pg_response.rs            # Query response formatting
│   ├── pg_field_descriptor.rs    # Row description metadata
│   ├── pg_extended.rs            # Extended query protocol
│   ├── ldap_auth.rs              # LDAP authentication
│   ├── net.rs                    # Network utilities
│   ├── error.rs                  # Protocol error types
│   ├── error_or_notice.rs        # Error/notice formatting
│   ├── memory_manager.rs         # Connection memory limits
│   └── types.rs                  # PostgreSQL type mappings
└── tests/                        # Integration tests
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Public exports, `PgProtocol`, `PgServer` |
| `src/pg_protocol.rs` | Protocol state machine, message dispatch |
| `src/pg_server.rs` | `PgServer`, connection acceptor |
| `src/pg_message.rs` | Frontend/backend message types |
| `src/ldap_auth.rs` | LDAP authentication implementation |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Test with actual PostgreSQL clients (psql, JDBC, psycopg2)
- Follow PostgreSQL message format specifications exactly
- Support both text and binary format for all types
- Implement proper error code mapping to SQLSTATE
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Break PostgreSQL wire protocol compatibility
- Change authentication flow without security review
- Remove support for standard PostgreSQL message types
- Hardcode credentials or certificates
- Ignore SSL/TLS certificate validation
- Break prepared statement or portal semantics

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p pgwire` |
| Protocol tests | `cargo test -p pgwire protocol` |
| Auth tests | `cargo test -p pgwire auth` |
| Integration | `cargo test -p pgwire --test integration` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **tokio**: Async runtime and TCP
- **tokio-openssl**: TLS support
- **openssl**: SSL/TLS implementation
- **rustls**: Alternative TLS implementation
- **ldap3**: LDAP authentication client
- **jsonwebtoken**: JWT authentication
- **postgres-types**: PostgreSQL type system
- **byteorder**: Network byte order handling

Contracts:
- Compatible with PostgreSQL wire protocol v3
- Supports SSL negotiation per RFC 6594
- Authentication follows PostgreSQL handshake sequence
- Error responses include SQLSTATE codes
- Type format codes: 0=text, 1=binary

## 9. Overrides

None. Follows parent `src/utils/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New PostgreSQL message type support added
- Authentication mechanism changed
- TLS configuration options modified
- Protocol version support changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/utils/AGENTS.md

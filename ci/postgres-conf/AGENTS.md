# AGENTS.md - PostgreSQL Test Configuration

## 1. Scope

Policies for the ci/postgres-conf directory, covering PostgreSQL test configurations for CDC (Change Data Capture) and integration testing.

## 2. Purpose

The postgres-conf module provides PostgreSQL configuration and initialization scripts for testing RisingWave's PostgreSQL CDC source and interoperability features. It includes SSL/TLS setup for secure connection testing and custom user configurations.

## 3. Structure

```
ci/postgres-conf/
├── 00-setup-ssl-user.sh      # SSL user creation and pg_hba.conf modification
└── entrypoint-wrapper.sh     # Docker entrypoint wrapper for SSL cert generation
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `00-setup-ssl-user.sh` | Creates ssl_user with password authentication and adds pg_hba.conf rules to enforce SSL for this user |
| `entrypoint-wrapper.sh` | Generates self-signed SSL certificates and configures PostgreSQL for TLS testing |

## 5. Edit Rules (Must)

- Document all PostgreSQL configuration changes
- Ensure SSL certificates are generated with proper permissions (600 for keys)
- Test certificate generation in Alpine-based PostgreSQL images
- Verify pg_hba.conf rules are correctly ordered (more specific rules first)
- Keep certificate validity period reasonable (365 days default)
- Document all test users and their intended use cases
- Ensure scripts are POSIX-compliant where possible
- Test changes with both Debian and Alpine PostgreSQL images

## 6. Forbidden Changes (Must Not)

- Remove SSL/TLS configuration without replacement
- Use weak cipher suites or short key lengths
- Hardcode production credentials in test scripts
- Modify default PostgreSSL behavior without documentation
- Remove the ssl_user enforcement rules
- Break compatibility with docker-compose PostgreSQL services
- Commit actual SSL certificates or private keys

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Certificate generation | Run entrypoint-wrapper.sh in container |
| User setup | Run 00-setup-ssl-user.sh against PostgreSQL |
| SSL connection | psql with sslmode=require |
| Integration | docker-compose up with postgres service |

## 8. Dependencies & Contracts

- PostgreSQL 12+ (Alpine or Debian based images)
- OpenSSL for certificate generation
- docker-compose for service orchestration
- Proper pg_hba.conf syntax for access control

## 9. Overrides

Inherits from `./ci/AGENTS.md`:
- Override: Edit Rules - PostgreSQL-specific security requirements

## 10. Update Triggers

Regenerate this file when:
- New PostgreSQL test configurations are added
- SSL/TLS requirements change
- CDC test scenarios expand
- PostgreSQL image base changes (Alpine/Debian)

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./ci/AGENTS.md

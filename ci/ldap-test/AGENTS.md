# AGENTS.md - LDAP Test Configuration

## 1. Scope

Policies for the ci/ldap-test directory, covering LDAP authentication integration testing configurations and TLS certificate generation.

## 2. Purpose

The ldap-test module provides OpenLDAP configuration and TLS certificate infrastructure for testing RisingWave's LDAP authentication feature. It supports both Simple Bind and Search+Bind authentication modes with optional mutual TLS.

## 3. Structure

```
ci/ldap-test/
├── ldif/
│   └── 01-users.ldif           # LDAP directory entries (test users)
├── certs/                      # Generated TLS certificates (gitignored)
├── setup-ldap-certs.sh         # Certificate generation script
├── README.md                   # Comprehensive test documentation
└── .gitignore                  # Excludes generated certificates
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `setup-ldap-certs.sh` | Generates CA, server, and client certificates with OpenSSL |
| `ldif/01-users.ldif` | Defines test LDAP users and organizational units |
| `README.md` | Complete guide for LDAP test setup and execution |
| `.gitignore` | Excludes generated certs/ directory |

## 5. Edit Rules (Must)

- Generate 4096-bit RSA keys with SHA256 signatures
- Include proper Subject Alternative Names (SAN) for server certs
- Set appropriate key usage and extended key usage attributes
- Pre-generate DH parameters to avoid long CI delays
- Document all certificate attributes and validity periods
- Keep client certificate CN descriptive (RisingWave Client)
- Ensure certificate permissions are restrictive (600 for keys)
- Verify certificate chain in generation script

## 6. Forbidden Changes (Must Not)

- Commit generated certificates to repository
- Use weak key sizes or deprecated algorithms
- Remove certificate verification steps
- Hardcode passwords in LDIF files (use {SSHA} placeholder)
- Break mutual TLS compatibility
- Remove SAN entries from server certificates
- Use short certificate validity in production configs

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Cert generation | `./setup-ldap-certs.sh` |
| Cert verification | `openssl x509 -in certs/ca.crt -noout -text` |
| LDAP connection | `ldapwhoami -x -H ldap://ldap-server:389 -ZZ` |
| Full test | `ci/scripts/e2e-ldap-test.sh` |

## 8. Dependencies & Contracts

- OpenLDAP server with TLS support
- OpenSSL for certificate generation
- ldap-utils package for testing
- Environment variables: LDAPTLS_CACERT, LDAPTLS_REQCERT
- docker-compose ldap-server service
- RisingWave LDAP authentication configs

## 9. Overrides

Inherits from `./ci/AGENTS.md`:
- Override: Edit Rules - TLS certificate security requirements

## 10. Update Triggers

Regenerate this file when:
- LDAP authentication modes change
- TLS certificate requirements evolve
- New test scenarios are added
- Certificate generation process changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./ci/AGENTS.md

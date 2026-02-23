# AGENTS.md - LDAP Test Data

## 1. Scope

Policies for the ci/ldap-test/ldif directory, containing LDAP Data Interchange Format (LDIF) files for LDAP integration testing.

## 2. Purpose

The ldif directory contains LDAP directory entries used to populate test LDAP servers during RisingWave's LDAP authentication integration tests. These files define test users, groups, and organizational structures for validating LDAP connectivity and authentication flows.

## 3. Structure

```
ldif/
├── 01-users.ldif           # Test user and group definitions
├── 02-groups.ldif          # Additional group definitions (optional)
└── 99-test-data.ldif       # Test scenario specific data
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `01-users.ldif` | Test users, groups, and organizational units |
| `02-groups.ldif` | Extended group membership definitions |
| `99-test-data.ldif` | Scenario-specific test entries |

## 5. Edit Rules (Must)

- Follow LDIF format specifications (RFC 2849)
- Use sequential numbering for load order (01-, 02-, etc.)
- Include proper DN (Distinguished Name) definitions
- Use test-only credentials (never real passwords)
- Define all required LDAP object classes
- Add comments explaining test scenarios
- Validate LDIF syntax before committing
- Keep test data minimal but representative
- Use consistent naming conventions
- Include organizational unit structure
- Define group memberships for authorization tests

## 6. Forbidden Changes (Must Not)

- Use real production credentials
- Add sensitive organizational data
- Break LDIF syntax (will fail LDAP load)
- Remove required object classes
- Create circular group memberships
- Add entries without proper schema
- Skip DN validation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Syntax check | `ldapadd -n -f 01-users.ldif` (dry run) |
| Integration | Run LDAP server and load data |
| E2E test | `./risedev slt './e2e_test/**/*ldap*.slt'` |
| Search test | `ldapsearch -x -b dc=example,dc=com` |

## 8. Dependencies & Contracts

- LDAP server (OpenLDAP, 389-ds, etc.)
- LDIF format compliance
- RisingWave LDAP authentication configuration
- Test environment variables for bind credentials
- LDAP schema definitions

## 9. Overrides

Inherits from `./ci/ldap-test/AGENTS.md`:
- Override: LDAP-specific test data requirements

## 10. Update Triggers

Regenerate this file when:
- LDAP schema changes
- New authentication scenarios are added
- Test user requirements change
- New LDAP attributes are needed

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./ci/ldap-test/AGENTS.md

# AGENTS.md - Vault Test Configuration

## 1. Scope

Policies for the ci/vault directory, covering HashiCorp Vault configurations for secret management integration testing.

## 2. Purpose

The vault module provides HashiCorp Vault configuration for testing RisingWave's secret management features. It enables testing of credential storage, dynamic secrets, and encryption capabilities through Vault integration.

## 3. Structure

```
ci/vault/
└── (empty - placeholder for future Vault configurations)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| (placeholder) | Vault server configuration and initialization scripts |

## 5. Edit Rules (Must)

- Document all Vault policies and ACLs
- Use dev mode only for local testing
- Configure proper PKI infrastructure for production tests
- Implement automatic unsealing for CI environments
- Document required Vault version compatibility
- Test with official Vault Docker images
- Include initialization and unseal scripts
- Document all secret engines used

## 6. Forbidden Changes (Must Not)

- Commit unseal keys or root tokens
- Use production Vault configurations in CI
- Disable audit logging
- Remove mTLS configuration without replacement
- Hardcode secrets in configuration files
- Use persistent storage in CI tests without cleanup
- Break KV version compatibility

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Vault status | `vault status` |
| Secret write | `vault kv put secret/test key=value` |
| Secret read | `vault kv get secret/test` |
| Integration | docker-compose with RisingWave Vault integration |

## 8. Dependencies & Contracts

- HashiCorp Vault 1.10+
- Vault Docker image (dev or server mode)
- AppRole or token authentication for RisingWave
- docker-compose service orchestration
- RisingWave secret management features

## 9. Overrides

Inherits from `/home/k11/risingwave/ci/AGENTS.md`:
- Override: Edit Rules - Vault security requirements

## 10. Update Triggers

Regenerate this file when:
- Vault integration features are added
- Secret management requirements change
- PKI or encryption configurations evolve
- Authentication methods expand

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/ci/AGENTS.md

# AGENTS.md - Deployment Configurations

## 1. Scope

Policies for the deploy directory, covering deployment configurations for various platforms and environments.

## 2. Purpose

The deploy module provides deployment configurations and templates for deploying RisingWave across different environments including Kubernetes, cloud platforms, and on-premise infrastructure.

## 3. Structure

```
deploy/
└── (deployment templates and configurations)
```

Note: This directory appears to be a placeholder for deployment configurations. Active deployment configurations may be maintained in separate repositories (e.g., risingwave-operator for Kubernetes).

## 4. Key Files

| File | Purpose |
|------|---------|
| (Directory is currently empty) | Reserved for future deployment configs |

## 5. Edit Rules (Must)

- Follow infrastructure-as-code best practices
- Document all configuration parameters
- Include examples for common deployment scenarios
- Version control all deployment changes
- Test deployments in staging before production
- Include resource requirement documentation

## 6. Forbidden Changes (Must Not)

- Commit production credentials or secrets
- Remove deployment templates without deprecation notice
- Make breaking changes to existing deployment patterns
- Add unverified third-party dependencies
- Modify production deployment configs without testing

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Template validation | Use respective platform tools |
| Deployment test | Deploy to test environment |
| Configuration lint | Run linters for IaC files |

## 8. Dependencies & Contracts

- Target deployment platform APIs
- Container orchestration systems
- Cloud provider SDKs
- Infrastructure tooling (Terraform, Helm, etc.)

## 9. Overrides

Inherits from `./AGENTS.md`:
- No overrides currently defined

## 10. Update Triggers

Regenerate this file when:
- Deployment templates are added
- Infrastructure patterns change
- New deployment targets are supported

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

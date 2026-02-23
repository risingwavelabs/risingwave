# AGENTS.md - Bot Deployment

## 1. Scope

Policies for the slack-oncall-bot/deploy directory, covering deployment configurations and infrastructure definitions for the Slack on-call bot.

## 2. Purpose

The deploy directory contains Kubernetes manifests, deployment scripts, and infrastructure-as-code configurations for deploying the Slack on-call bot to production and staging environments. It ensures consistent, reproducible deployments across different clusters.

## 3. Structure

```
deploy/
├── k8s/                       # Kubernetes manifests
│   ├── deployment.yaml       # Main bot deployment
│   ├── service.yaml          # Service definition
│   ├── configmap.yaml        # Configuration data
│   ├── secret.yaml.example   # Secret template (example only)
│   └── ingress.yaml          # Ingress configuration (if applicable)
├── scripts/                   # Deployment scripts
│   ├── deploy.sh             # Main deployment script
│   ├── rollback.sh           # Rollback procedures
│   └── verify.sh             # Post-deployment verification
├── helm/                      # Helm chart (optional)
│   ├── Chart.yaml
│   ├── values.yaml
│   └── templates/
└── AGENTS.md                  # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `k8s/deployment.yaml` | Bot pod deployment specification |
| `k8s/service.yaml` | Kubernetes service for bot |
| `k8s/configmap.yaml` | Non-sensitive configuration |
| `scripts/deploy.sh` | Automated deployment script |
| `scripts/rollback.sh` | Rollback procedures |
| `secret.yaml.example` | Template for required secrets |

## 5. Edit Rules (Must)

- Use declarative configurations (no imperative commands)
- Document all environment variables required
- Include resource limits and requests
- Use specific image tags (never `latest` in production)
- Document required secrets and ConfigMaps
- Include health checks and readiness probes
- Test deployments in staging before production
- Version all configuration changes

## 6. Forbidden Changes (Must Not)

- Commit actual secret values (only examples/templates)
- Use `latest` tag for production deployments
- Remove resource limits without justification
- Hardcode environment-specific values
- Deploy without verification steps
- Remove rollback capabilities
- Commit credentials or tokens

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Validate YAML | `kubectl apply --dry-run=client -f k8s/` |
| Kustomize build | `kustomize build k8s/` |
| Helm lint | `helm lint helm/` |
| Helm template | `helm template bot ./helm` |
| Deploy to staging | `./scripts/deploy.sh staging` |
| Verify deployment | `./scripts/verify.sh` |

## 8. Dependencies & Contracts

- Kubernetes 1.24+
- kubectl CLI
- Docker registry access
- Slack App credentials (stored as secrets)
- PagerDuty API access (if integrated)
- Ingress controller (if exposed externally)
- Monitoring and logging stack

## 9. Overrides

Inherits from `./slack-oncall-bot/AGENTS.md`:
- Override: Edit Rules - Deployment-specific security requirements
- Override: Test Entry - Kubernetes-specific validation

## 10. Update Triggers

Regenerate this file when:
- New deployment targets are added
- Kubernetes manifest structure changes
- Helm chart is introduced or modified
- Deployment process changes significantly
- New infrastructure requirements are added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./slack-oncall-bot/AGENTS.md

# AGENTS.md - Slack Bot Deployment

## 1. Scope

Policies for the `slack-oncall-bot-python/deploy` directory, covering deployment configurations and infrastructure for the Slack on-call bot.

## 2. Purpose

The Deploy module contains deployment configurations, infrastructure definitions, and deployment scripts for the Slack on-call bot. This includes Kubernetes manifests, Docker configurations, and environment-specific deployment templates.

## 3. Structure

```
slack-oncall-bot-python/deploy/
└── (Kubernetes manifests, Docker files)
```

## 4. Key Files

| File/Directory | Purpose |
|----------------|---------|
| Kubernetes manifests | Deployment, Service, ConfigMap definitions |
| Docker files | Container build configurations |
| Environment configs | Per-environment configuration files |

## 5. Edit Rules (Must)

- Use environment variables for configuration injection
- Document resource requirements (CPU, memory)
- Include health check endpoints
- Configure proper logging and monitoring
- Use secrets management for sensitive data
- Version pin base Docker images
- Include restart policies and resource limits

## 6. Forbidden Changes (Must Not)

- Hardcode secrets in configuration files
- Remove resource limits without justification
- Use latest tag for Docker base images
- Skip health check configurations
- Commit environment-specific secrets
- Remove monitoring or logging configurations

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Kubernetes validate | `kubectl apply --dry-run=client -f deploy/` |
| Docker build | `docker build -f deploy/Dockerfile .` |
| Helm lint | `helm lint deploy/` (if using Helm) |
| YAML lint | `yamllint deploy/` |

## 8. Dependencies & Contracts

- Kubernetes cluster access
- Docker or container runtime
- Container registry credentials
- Environment-specific configurations
- Secrets management system
- Monitoring and alerting integrations

## 9. Overrides

Inherits from `./slack-oncall-bot-python/AGENTS.md`:
- Override: Edit Rules - Deployment-specific requirements
- Override: Test Entry - Deployment validation commands

## 10. Update Triggers

Regenerate this file when:
- Deployment configurations are added
- Infrastructure requirements change
- New environment targets are added
- Deployment tooling changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./slack-oncall-bot-python/AGENTS.md

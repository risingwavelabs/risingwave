# AGENTS.md - Standalone Deployment

## 1. Scope

Policies for the standalone directory, covering standalone deployment configurations for monitoring and lightweight RisingWave instances.

## 2. Purpose

The standalone module provides minimal configuration files for running RisingWave in standalone mode with integrated monitoring. It includes configurations for Prometheus metrics collection and Grafana dashboards.

## 3. Structure

```
standalone/
├── grafana.ini          # Grafana server configuration
└── prometheus.yml       # Prometheus scraping configuration
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `grafana.ini` | Grafana server settings and feature flags |
| `prometheus.yml` | Prometheus scrape targets and retention |

## 5. Edit Rules (Must)

- Document configuration changes with comments
- Use relative paths for portability
- Ensure configurations work with docker-compose setup
- Test configuration changes before committing
- Follow Grafana and Prometheus configuration best practices
- Include default credentials warnings where applicable

## 6. Forbidden Changes (Must Not)

- Hardcode production credentials
- Remove default security settings
- Change scrape intervals without performance consideration
- Remove essential monitoring targets
- Break compatibility with docker-compose setup

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Config validation | `promtool check config prometheus.yml` |
| Grafana config | `grafana-server -config grafana.ini` (dry-run) |
| Integration | Test with docker-compose up |

## 8. Dependencies & Contracts

- Prometheus 2.x
- Grafana 8.x+
- Docker Compose (for integrated deployment)
- RisingWave metrics endpoints

## 9. Overrides

Inherits from `/home/k11/risingwave/AGENTS.md`:
- No overrides currently defined

## 10. Update Triggers

Regenerate this file when:
- Monitoring stack versions change
- New scrape targets are added
- Configuration structure changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

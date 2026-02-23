# AGENTS.md - Docker Dashboards

## 1. Scope

Policies for the docker/dashboards directory, covering Grafana dashboard JSON definitions for RisingWave monitoring.

## 2. Purpose

The dashboards directory contains pre-built Grafana dashboard JSON files that provide comprehensive monitoring and observability for RisingWave clusters deployed via Docker. These dashboards visualize metrics for developers and operators to understand system health and performance.

## 3. Structure

```
dashboards/
├── risingwave-dev-dashboard.json      # Developer-focused detailed dashboard
├── risingwave-user-dashboard.json     # User-friendly overview dashboard
└── AGENTS.md                          # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `risingwave-dev-dashboard.json` | Detailed metrics for developers and debugging |
| `risingwave-user-dashboard.json` | High-level overview for operators and users |

## 5. Edit Rules (Must)

- Source dashboards from grafana/ directory (do not edit directly)
- Ensure dashboards use portable datasource variables
- Document dashboard refresh intervals and retention
- Test dashboards in local Grafana before updating
- Version control dashboard changes
- Include clear panel titles and descriptions
- Use consistent color schemes across panels
- Document any custom queries or calculations

## 6. Forbidden Changes (Must Not)

- Edit JSON files directly (modify source in grafana/ instead)
- Hardcode datasource UIDs or names
- Remove panels without checking dependencies
- Add queries that only work with specific versions
- Commit dashboards with broken queries
- Remove backward compatibility without notice

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| JSON validation | `python -c "import json; json.load(open('risingwave-dev-dashboard.json'))"` |
| Import test | Import to local Grafana instance |
| Query validation | Verify all PromQL queries |
| Visual check | Review in Grafana UI |

## 8. Dependencies & Contracts

- Grafana 8.x or later
- Prometheus datasource
- RisingWave metrics exposition
- Dashboard schema version compatibility
- Docker Compose volume mounts

## 9. Overrides

Inherits from `/home/k11/risingwave/docker/AGENTS.md`:
- Override: Edit Rules - Dashboard files are generated from source
- Override: Test Entry - Grafana-specific validation required

## 10. Update Triggers

Regenerate this file when:
- New dashboard types are added
- Dashboard generation process changes
- Grafana version requirements change
- Dashboard structure conventions change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/docker/AGENTS.md

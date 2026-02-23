# AGENTS.md - Grafana Dashboards

## 1. Scope

Policies for the grafana directory, covering Grafana dashboard definitions, generation tools, and monitoring configurations.

## 2. Purpose

The grafana module provides code-generated Grafana dashboards for monitoring RisingWave clusters. Dashboards are defined as Python code using grafanalib and generated to JSON for deployment. This approach enables version-controlled, reproducible dashboard definitions.

## 3. Structure

```
grafana/
├── risingwave-dev-dashboard.dashboard.py      # Developer dashboard source
├── risingwave-dev-dashboard.gen.json          # Generated developer dashboard
├── risingwave-dev-dashboard.json              # Final developer dashboard
├── risingwave-user-dashboard.dashboard.py     # User dashboard source
├── risingwave-user-dashboard.gen.json         # Generated user dashboard
├── risingwave-user-dashboard.json             # Final user dashboard
├── risingwave-traces.json                     # Distributed tracing dashboard
├── dashboard/                                 # Dashboard components
│   └── (reusable dashboard widgets)
├── generate.sh                                # Dashboard generation script
├── update.sh                                  # Live dashboard update script
├── requirements.txt                           # Python dependencies
└── README.md                                  # Documentation
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `*.dashboard.py` | Python source for dashboard definitions |
| `*.gen.json` | Generated JSON from grafanalib |
| `*-dashboard.json` | Final dashboard for import |
| `generate.sh` | Main dashboard generation script |
| `update.sh` | Update running Grafana instance |
| `requirements.txt` | Python package dependencies |

## 5. Edit Rules (Must)

- Edit only `.dashboard.py` files, never `.json` files directly
- Run `./generate.sh` after modifying dashboard sources
- Commit both source `.py` and generated `.json` files
- Document panel purposes with clear titles and descriptions
- Use consistent color schemes for related metrics
- Include units for all graph axes
- Add alerts for critical metrics where appropriate
- Test dashboard in local Grafana before committing

## 6. Forbidden Changes (Must Not)

- Edit generated `.json` files directly (changes will be lost)
- Remove panels without checking dependencies
- Hardcode Grafana UIDs or datasource names
- Add panels without proper legends or units
- Commit without running generate.sh
- Remove backward compatibility for existing dashboards

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Generate | `./generate.sh` |
| Validate | Import JSON to Grafana test instance |
| Update live | `./update.sh` (to localhost:3001) |
| Python syntax | `python -m py_compile *.py` |

## 8. Dependencies & Contracts

- Python 3.x
- grafanalib
- jsonmerge
- jq (command-line JSON processor)
- Grafana 8.x or later
- Prometheus datasource

## 9. Overrides

Inherits from `./AGENTS.md`:
- Override: Edit Rules - Dashboard source files only
- Override: Test Entry - Grafana-specific validation

## 10. Update Triggers

Regenerate this file when:
- Dashboard generation workflow changes
- New dashboard types are added
- Grafana version requirements change
- Multi-cluster support features change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

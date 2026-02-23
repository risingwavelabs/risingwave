# AGENTS.md - Dashboard Components

## 1. Scope

Policies for the grafana/dashboard directory, covering Python-based Grafana dashboard component definitions and generation utilities.

## 2. Purpose

The dashboard directory contains Python modules that define reusable Grafana dashboard components using grafanalib. These components generate the JSON dashboard definitions, enabling version-controlled, programmatic dashboard creation with consistent styling and behavior.

## 3. Structure

```
dashboard/
├── common.py                  # Shared utilities and base classes
├── dev/                       # Developer dashboard components
│   ├── backup_manager.py
│   ├── batch.py
│   ├── cluster_alerts.py
│   ├── cluster_errors.py
│   ├── cluster_essential.py
│   ├── compaction.py
│   ├── grpc_meta.py
│   ├── hummock_*.py          # Hummock storage metrics
│   ├── iceberg_*.py          # Iceberg connector metrics
│   ├── kafka_metrics.py
│   ├── kinesis_metrics.py
│   ├── memory_manager.py
│   ├── network_connection.py
│   ├── object_storage.py
│   ├── refresh_manager.py
│   ├── sink_metrics.py
│   ├── source_general.py
│   ├── streaming_*.py        # Streaming engine metrics
│   ├── sync_logstore_metrics.py
│   ├── udf.py
│   └── vector_search.py
├── user/                      # User dashboard components
│   ├── actor_info.py
│   ├── batch.py
│   ├── cpu.py
│   ├── memory.py
│   ├── network.py
│   ├── overview.py
│   ├── storage.py
│   └── streaming.py
└── AGENTS.md                  # This file
```

## 4. Key Files

| File/Directory | Purpose |
|----------------|---------|
| `common.py` | Base layout classes, panel factories, and metric helpers |
| `dev/` | Detailed panels for developers (40+ metric categories) |
| `user/` | High-level panels for operators (8 overview categories) |

## 5. Edit Rules (Must)

- Edit only `.py` files, never generated `.json` files
- Use `common.py` classes for consistent panel layout
- Document metric purposes in panel descriptions
- Include proper units for all metrics
- Use consistent legend formats across similar panels
- Follow existing naming conventions for panel IDs
- Test dashboard generation after changes
- Run generate.sh after modifying any Python file

## 6. Forbidden Changes (Must Not)

- Edit generated JSON files directly
- Remove panels without checking dashboard references
- Hardcode datasource UIDs in panel definitions
- Add panels without units or legends
- Break the layout grid (24-column system)
- Remove common.py utilities without refactoring dependents
- Commit without running generate.sh

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Python syntax | `python -m py_compile dev/*.py user/*.py` |
| Generate dev | `python risingwave-dev-dashboard.dashboard.py` |
| Generate user | `python risingwave-user-dashboard.dashboard.py` |
| Full build | `./generate.sh` |
| Import test | Import JSON to Grafana test instance |
| Dashboard validation | `jsonschema` validation |

## 8. Dependencies & Contracts

- Python 3.x
- grafanalib (Grafana dashboard Python library)
- jsonmerge (JSON merging utilities)
- jq (command-line JSON processor)
- Grafana 8.x or later for generated dashboards
- Prometheus datasource for metrics
- 24-column grid layout system

## 9. Overrides

Inherits from `/home/k11/risingwave/grafana/AGENTS.md`:
- Override: Edit Rules - Component-specific generation workflow
- Override: Test Entry - Python component testing required

## 10. Update Triggers

Regenerate this file when:
- New dashboard component categories are added
- Common.py utility classes change
- Panel generation patterns change
- New metric types are supported
- Layout system changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/grafana/AGENTS.md

# AGENTS.md - User Grafana Dashboards

## 1. Scope

Policies for the grafana/dashboard/user directory, containing Python-generated Grafana dashboard definitions for RisingWave end users.

## 2. Purpose

The user directory contains Python modules that generate user-facing Grafana dashboards. These dashboards provide high-level visibility into database performance, query execution, resource utilization, and streaming job health from a user perspective, abstracting internal implementation details.

## 3. Structure

```
user/
├── __init__.py              # Python package marker
├── actor_info.py            # Actor/streaming task information
├── batch.py                 # Batch query performance
├── cpu.py                   # CPU utilization metrics
├── memory.py                # Memory usage overview
├── network.py               # Network throughput metrics
├── overview.py              # High-level database overview
├── storage.py               # Storage layer metrics
└── streaming.py             # Streaming job health
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `overview.py` | Main database health dashboard |
| `streaming.py` | Streaming job status and throughput |
| `batch.py` | Batch query performance metrics |
| `cpu.py` | CPU resource utilization |
| `memory.py` | Memory consumption trends |
| `storage.py` | Storage capacity and I/O |

## 5. Edit Rules (Must)

- Focus on user-relevant metrics only
- Use clear, non-technical panel titles
- Aggregate internal metrics appropriately
- Provide actionable insights, not raw data
- Use intuitive color schemes (green=good, red=bad)
- Include usage examples in descriptions
- Support multi-tenant filtering
- Keep dashboards focused and uncluttered
- Add helpful text panels with guidance
- Use percentage/rate views where helpful

## 6. Forbidden Changes (Must Not)

- Expose internal implementation details
- Add developer-specific metrics
- Use technical jargon in titles
- Overwhelm with too many panels
- Remove user-facing key metrics
- Break backward compatibility without notice

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Generate JSON | `cd grafana && ./generate.sh` |
| Validate JSON | Check syntax of generated files |
| User review | Validate with actual users |

## 8. Dependencies & Contracts

- Python 3.8+
- grafanalib or similar dashboard library
- Prometheus metrics from RisingWave
- Grafana 9+ for visualization
- User-focused metric naming conventions

## 9. Overrides

Inherits from `./grafana/AGENTS.md`:
- Override: User-facing dashboard design patterns

## 10. Update Triggers

Regenerate this file when:
- New user-facing metrics are added
- User feedback drives dashboard changes
- SLA/SLO monitoring requirements change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./grafana/AGENTS.md

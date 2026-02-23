# AGENTS.md - Developer Grafana Dashboards

## 1. Scope

Policies for the grafana/dashboard/dev directory, containing Python-generated Grafana dashboard definitions for RisingWave developers.

## 2. Purpose

The dev directory contains Python modules that generate comprehensive Grafana dashboards for monitoring RisingWave internal metrics. These dashboards provide deep visibility into streaming jobs, storage engine, compaction, barrier management, and other internal components used by RisingWave developers and operators.

## 3. Structure

```
dev/
├── __init__.py                      # Python package marker
├── backup_manager.py                # Backup system metrics
├── batch.py                         # Batch query execution
├── cluster_alerts.py                # Cluster alerting rules
├── cluster_errors.py                # Error rate tracking
├── cluster_essential.py             # Core cluster metrics
├── compaction.py                    # Storage compaction metrics
├── grpc_meta.py                     # Meta service gRPC metrics
├── hummock_manager.py               # Hummock storage manager
├── hummock_read.py                  # Storage read metrics
├── hummock_tiered_cache.py          # Tiered cache monitoring
├── hummock_write.py                 # Storage write metrics
├── iceberg_compaction_metrics.py    # Iceberg compaction
├── iceberg_metrics.py               # Iceberg connector metrics
├── kafka_metrics.py                 # Kafka source/sink metrics
├── kinesis_metrics.py               # AWS Kinesis metrics
├── memory_manager.py                # Memory management
├── network_connection.py            # Network layer metrics
├── object_storage.py                # S3/object store metrics
├── refresh_manager.py               # Materialized view refresh
├── sink_metrics.py                  # Data sink metrics
├── source_general.py                # General source metrics
├── streaming_backfill.py            # Streaming backfill jobs
├── streaming_barrier.py             # Barrier lifecycle metrics
├── streaming_cdc.py                 # CDC connector metrics
├── streaming_common.py              # Shared streaming panels
├── streaming_fragments.py           # Fragment-level metrics
├── streaming_metadata.py            # Metadata service metrics
├── streaming_operators_by_operator.py  # Per-operator metrics
├── streaming_operators_overview.py  # Operator overview
├── streaming_relations.py           # Relation-level metrics
├── sync_logstore_metrics.py         # Log store metrics
├── system_params.py                 # System parameters
├── udf.py                           # User-defined functions
└── vector_search.py                 # Vector search metrics
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `cluster_essential.py` | Core cluster health dashboards |
| `streaming_*.py` | Streaming engine deep metrics |
| `hummock_*.py` | Storage engine internals |
| `compaction.py` | Compaction job monitoring |
| `streaming_barrier.py` | Barrier progress and lag |

## 5. Edit Rules (Must)

- Use grafanalib or similar Python Grafana libraries
- Define reusable panel and row functions
- Use consistent metric naming patterns
- Add descriptive panel titles and descriptions
- Include proper units and axis labels
- Use appropriate visualization types
- Add alerts for critical metrics
- Support template variables for filtering
- Follow existing dashboard organization patterns
- Document metric sources in comments

## 6. Forbidden Changes (Must Not)

- Break existing dashboard JSON generation
- Remove critical monitoring panels
- Use hardcoded values instead of variables
- Skip units on numeric panels
- Create panels without data source

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Generate JSON | `cd grafana && ./generate.sh` |
| Validate JSON | Check syntax of generated files |
| Import test | Import to Grafana and verify |

## 8. Dependencies & Contracts

- Python 3.8+
- grafanalib or similar dashboard library
- Prometheus metrics from RisingWave
- Grafana 9+ for dashboard features
- Consistent metric naming in RisingWave

## 9. Overrides

Inherits from `/home/k11/risingwave/grafana/AGENTS.md`:
- Override: Developer-focused dashboard patterns

## 10. Update Triggers

Regenerate this file when:
- New internal metrics are added
- Dashboard generation library changes
- New developer tooling is added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/grafana/AGENTS.md

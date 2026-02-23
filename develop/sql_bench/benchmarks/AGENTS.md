# AGENTS.md - SQL Benchmark Definitions

## 1. Scope

Policies for the develop/sql_bench/benchmarks directory, covering YAML-based SQL benchmark scenario definitions for RisingWave performance testing.

## 2. Purpose

The benchmarks module provides declarative benchmark configurations for measuring RisingWave query performance. It enables rapid performance validation of features like gap filling, window functions, and streaming SQL without the overhead of full nexmark benchmarks.

## 3. Structure

```
develop/sql_bench/benchmarks/
├── example.yaml                # Template benchmark with all options
├── gap_fill_eowc.yaml          # Gap fill with Emit On Window Close
└── gap_fill_streaming.yaml     # Gap fill streaming performance test
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `example.yaml` | Reference template showing all benchmark options |
| `gap_fill_eowc.yaml` | Tests GAP_FILL with EMIT ON WINDOW CLOSE semantics |
| `gap_fill_streaming.yaml` | Tests GAP_FILL in streaming scenarios |

## 5. Edit Rules (Must)

- Include clear benchmark_name describing the scenario
- Provide setup_sql for schema and initial data creation
- Use prepare_sql and conclude_sql for per-run isolation
- Include cleanup_sql to remove resources after benchmarking
- Set runs >= 3 for statistical significance
- Document the SQL being benchmarked with comments
- Use unique time ranges for append-only tables (NOW() based)
- Include appropriate pg_sleep for watermark advancement

## 6. Forbidden Changes (Must Not)

- Remove benchmark schema documentation
- Use hardcoded connection strings in YAML
- Create non-reproducible benchmarks
- Remove baseline comparison without justification
- Break YAML schema (required fields: benchmark_name, benchmark_sql, runs)
- Use DELETE on append-only tables
- Remove cleanup_sql (leaves resources behind)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| YAML validation | `python -c "import yaml; yaml.safe_load(open('file.yaml'))"` |
| Schema validation | `python main.py validate <name>` |
| Single run | `python main.py run <name> --runs 1` |
| Full benchmark | `python main.py run <name>` |
| Debug mode | `python main.py run <name> -d` |

## 8. Dependencies & Contracts

- Python 3.8+ with PyYAML
- Running RisingWave instance
- PostgreSQL-compatible connection (psycopg2)
- hyperfine for timing measurements
- YAML schema compliance

## 9. Overrides

Inherits from `./develop/sql_bench/AGENTS.md`:
- Override: Edit Rules - Benchmark-specific YAML requirements

## 10. Update Triggers

Regenerate this file when:
- New benchmark types are added
- YAML schema changes
- New SQL features require benchmarking
- Performance measurement methodology updates

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./develop/sql_bench/AGENTS.md

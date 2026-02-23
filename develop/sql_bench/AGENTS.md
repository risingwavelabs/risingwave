# AGENTS.md - SQL Benchmarks

## 1. Scope

Policies for the develop/sql_bench directory, covering SQL-based performance benchmarking tools and benchmark definitions.

## 2. Purpose

The sql_bench module provides a lightweight, YAML-based benchmarking framework for rapid performance validation of RisingWave features and optimizations. It uses hyperfine for statistical measurement and enables quick iteration without the overhead of the full nexmark benchmark suite.

## 3. Structure

```
sql_bench/
├── main.py                    # Benchmark runner and CLI
├── requirements.txt           # Python dependencies
├── README.md                  # Usage documentation
├── benchmarks/                # Benchmark configurations
│   ├── example.yaml          # Example benchmark template
│   ├── gap_fill_eowc.yaml    # Gap fill window function benchmark
│   └── gap_fill_streaming.yaml  # Streaming gap fill benchmark
└── AGENTS.md                  # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `main.py` | CLI tool for benchmark execution and management |
| `requirements.txt` | Python package dependencies (hyperfine, psycopg2) |
| `benchmarks/*.yaml` | Benchmark scenario definitions |
| `README.md` | User guide and benchmark authoring guide |

## 5. Edit Rules (Must)

- Document benchmark purpose and expected behavior
- Include baseline and benchmark SQL for comparison
- Set appropriate number of runs for statistical significance (minimum 3)
- Use clear, descriptive benchmark names
- Include setup and cleanup SQL for isolation
- Document database prerequisites and data requirements
- Test benchmarks locally before committing
- Update README.md when adding new benchmark categories

## 6. Forbidden Changes (Must Not)

- Remove existing benchmarks without archiving
- Add benchmarks without corresponding documentation
- Use hardcoded connection strings (use environment or CLI args)
- Commit benchmark results without context
- Add non-deterministic benchmarks
- Remove baseline comparisons without justification
- Break existing benchmark YAML schema

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Initialize benchmark | `python main.py init <name>` |
| Run benchmark | `python main.py run <name>` |
| Run with debug | `python main.py run <name> -d` |
| Validate YAML | `python -c "import yaml; yaml.safe_load(open('benchmarks/<name>.yaml'))"` |
| Full test | Run against local RisingWave instance |

## 8. Dependencies & Contracts

- Python 3.8+
- hyperfine (system dependency)
- psycopg2 or psycopg2-binary (PostgreSQL driver)
- PyYAML (YAML parsing)
- Running RisingWave instance (local or remote)
- PostgreSQL-compatible connection URL

## 9. Overrides

Inherits from `./develop/AGENTS.md`:
- Override: Test Entry - SQL benchmark specific commands
- Override: Edit Rules - Benchmark-specific documentation requirements

## 10. Update Triggers

Regenerate this file when:
- New benchmark types or categories are added
- Benchmark YAML schema changes
- CLI interface changes significantly
- New database backends are supported
- Performance measurement methodology changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./develop/AGENTS.md

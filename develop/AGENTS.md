# AGENTS.md - Development Tools

## 1. Scope

Policies for the develop directory, covering development environment configurations and benchmarking tools.

## 2. Purpose

The develop module provides specialized development tools and configurations, including Nix environment definitions and SQL benchmarking utilities for performance testing.

## 3. Structure

```
develop/
├── nix/                  # Nix development environment
│   └── (Nix expressions and lock files)
└── sql_bench/            # SQL benchmarking tools
    └── (Benchmark configurations and scripts)
```

## 4. Key Files

| File/Directory | Purpose |
|----------------|---------|
| `nix/` | Nix flake and development shell configuration |
| `sql_bench/` | SQL performance benchmarking suite |

## 5. Edit Rules (Must)

- Document all Nix dependencies and their purposes
- Include setup instructions for Nix-based development
- Version lock Nix dependencies for reproducibility
- Document benchmark methodology and metrics collected
- Ensure benchmarks are reproducible across runs
- Add benchmark results interpretation guidelines

## 6. Forbidden Changes (Must Not)

- Break Nix shell reproducibility
- Remove benchmark baselines without archiving
- Add non-deterministic benchmark tests
- Commit Nix build artifacts
- Modify benchmark queries without updating expected results

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Nix shell | `nix develop` |
| SQL benchmarks | Run benchmark scripts in sql_bench/ |
| Environment check | Verify tool availability in nix shell |

## 8. Dependencies & Contracts

- Nix package manager
- Nix flakes for reproducible builds
- Benchmark database connections
- Performance measurement tools
- Statistical analysis libraries

## 9. Overrides

Inherits from `./AGENTS.md`:
- Override: Test Entry - Nix-specific validation required

## 10. Update Triggers

Regenerate this file when:
- Nix configuration structure changes
- New benchmark categories are added
- Development environment tools change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

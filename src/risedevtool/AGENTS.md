# AGENTS.md - risedevtool Developer Tooling

## 1. Scope

Policies for `src/risedevtool` - the RiseDev developer tooling crate that manages local RisingWave development clusters.

## 2. Purpose

RiseDev is the unified developer tool for RisingWave that:
- Boots local development clusters with configurable service topologies
- Manages service dependencies (MinIO, Kafka, Postgres, etc.)
- Generates configuration files for monitoring (Prometheus, Grafana, Tempo)
- Orchestrates services via tmux sessions

## 3. Structure

```
risedevtool/
├── src/
│   ├── bin/
│   │   ├── risedev-dev.rs       # Main dev cluster entry point
│   │   └── risedev-docslt.rs    # Documentation SLT runner
│   ├── config/                  # YAML config expansion logic
│   │   ├── dollar_expander.rs   # ${var} variable expansion
│   │   ├── id_expander.rs       # ID reference expansion
│   │   ├── provide_expander.rs  # Dependency injection expansion
│   │   └── use_expander.rs      # Template application
│   ├── config_gen/              # Config file generators
│   │   ├── grafana_gen.rs
│   │   ├── prometheus_gen.rs
│   │   └── tempo_gen.rs
│   ├── task/                    # Service lifecycle tasks
│   │   ├── *_service.rs         # Individual service handlers
│   │   └── task_*_ready_check.rs # Health check tasks
│   ├── config.rs                # Main config expander
│   ├── service_config.rs        # Service configuration structs
│   └── lib.rs                   # Public API exports
├── schemas/                     # JSON schemas for risedev.yml
├── config/                      # Interactive configurator binary
└── *.toml                       # cargo-make task definitions
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `risedev.yml` | Main configuration file defining profiles and templates |
| `common.toml` | Base cargo-make environment and directory setup |
| `risedev-components.toml` | Component selection and configuration tasks |
| `src/config.rs` | YAML config expansion and deserialization |
| `src/service_config.rs` | Service configuration type definitions |
| `src/bin/risedev-dev.rs` | Main development cluster orchestrator |
| `schemas/risedev.json` | JSON schema for risedev.yml validation |

## 5. Edit Rules (Must)

- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Add unit tests for new config expanders in `src/config/*_expander.rs`
- Update JSON schemas in `schemas/` when adding new service config fields
- Follow existing service task pattern when adding new services
- Ensure new services implement both `Task` trait and ready-check logic
- Test profile expansion with `./risedev dev <profile>` locally
- Document new profile options in README.md

## 6. Forbidden Changes (Must Not)

- Modify `risedev.yml` structure without updating JSON schemas
- Delete or modify `risedev-components.user.env` manually (use `./risedev configure`)
- Hardcode ports or paths that should be configurable
- Bypass service dependency resolution in `provide_expander.rs`
- Add blocking operations in async task execution paths
- Commit hardcoded credentials or API keys
- Break backward compatibility for existing profile configurations

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risedev` |
| Config expander tests | `cargo test -p risedev config` |
| Clippy check | `./risedev c` |
| Dev cluster test | `./risedev d` (default profile) |

## 8. Dependencies & Contracts

- Uses `yaml-rust2` for YAML parsing
- Uses `serde_yaml` for config deserialization
- Uses `cargo-make` for build orchestration (external dependency)
- Uses `tmux` for process management (external system dependency)
- Config expansion: template -> use -> dollar -> id -> provide (fixed order)
- Service startup respects `TaskGroup` for parallelization
- Environment variables prefixed with `RISEDEV_` and `PREFIX_`

## 9. Overrides

None. Follows parent AGENTS.md rules from repository root.

## 10. Update Triggers

Regenerate this file when:
- New service type added to `ServiceConfig` enum
- Config expansion logic modified
- New cargo-make task files added
- Schema structure changes
- Service task pattern changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `./AGENTS.md`

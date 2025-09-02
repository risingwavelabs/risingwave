# GitHub Copilot Instructions for RisingWave

This file provides guidance to GitHub Copilot when working with code in this repository.

## Project Overview

RisingWave is a Postgres-compatible streaming database that offers the simplest and most cost-effective way to process, analyze, and manage real-time event data — with built-in support for the Apache Iceberg™ open table format.

## Code Architecture

RisingWave components are developed in Rust and split into several crates:

1. `config` - Default configurations for servers
2. `prost` - Generated protobuf rust code (grpc and message definitions)
3. `stream` - Stream compute engine
4. `batch` - Batch compute engine for queries against materialized views
5. `frontend` - SQL query planner and scheduler
6. `storage` - Cloud native storage engine
7. `meta` - Meta engine
8. `utils` - Independent util crates
9. `cmd` - All binaries, `cmd_all` contains the all-in-one binary `risingwave`
10. `risedevtool` - Developer tool for RisingWave

## Build, Run, Test

When implementing features or fixing bugs, use these commands:

- `./risedev b` - Build the project
- `./risedev c` - Check code follows rust-clippy rules and coding styles
- `./risedev d` - Run a RisingWave instance (builds if needed, runs in background)
- `./risedev k` - Stop a RisingWave instance started by `./risedev d`
- `./risedev psql -c "<your query>"` - Run SQL queries in RisingWave
- `./risedev slt './path/to/e2e-test-file.slt'` - Run end-to-end SLT tests
- Log files are in `.risingwave/log` folder - use `tail` to check them

### Testing

- Preferred testing format is SQLLogicTest (SLT)
- Tests are located in `./e2e_test` folder
- Integration and unit tests follow standard Rust/Tokio patterns
- The `./risedev` command is safe to run automatically
- Never run `git` mutation commands unless explicitly requested

## Coding Style

- Always write code comments in English
- Write simple, easy-to-read and maintainable code
- Use `cargo fmt` to format code when needed
- Follow existing code patterns and conventions in the repository

## Understanding RisingWave Macros

RisingWave uses many macros to simplify development:

- Use `cargo expand` to expand macros and understand generated code
- Setup `rust-analyzer` in your editor to expand macros interactively
- This is the preferred method for understanding code interactively

### Example: Using cargo expand
```bash
cargo expand -p risingwave_meta > meta.rs
cargo expand -p risingwave_expr > expr.rs
```

## Connector Development

RisingWave supports many connectors (sources and sinks). Key principles:

- **Environment-independent**: Easy to start cluster and run tests locally and in CI
- **Self-contained tests**: Straightforward to run individual test cases
- Don't use hard-coded configurations (e.g., `localhost:9092`)
- Don't write complex logic in `ci/scripts` - keep CI scripts thin

### Development Workflow
1. Use RiseDev to manage external systems (Kafka, MySQL, etc.)
2. Write profiles in `risedev.yml` or `risedev-profiles.user.yml`
3. Use `risedev d my-profile` to start cluster with external systems
4. Write e2e tests in SLT format using `system ok` commands for external interaction
5. Use environment variables for configuration (e.g., `${RISEDEV_KAFKA_WITH_OPTIONS_COMMON}`)

## Key Development Guidelines

1. **Minimal Changes**: Make the smallest possible changes to achieve goals
2. **Validation**: Always validate changes don't break existing behavior
3. **Testing**: Run existing linters, builds, and tests before making changes
4. **Documentation**: Update documentation when directly related to changes
5. **Ecosystem Tools**: Use existing tools (npm, cargo, etc.) to automate tasks

## Environment Setup

- RisingWave uses S3 as primary storage for high performance and fast recovery
- Supports elastic disk cache for performance optimization
- Native Apache Iceberg™ integration for open table format compatibility
- PostgreSQL wire protocol compatibility for seamless integration

## Common Use Cases

- Streaming analytics with sub-second data freshness
- Event-driven applications for monitoring and alerting
- Real-time data enrichment and delivery
- Feature engineering for machine learning models

---

For more detailed information, refer to:
- Project README.md
- Documentation in `docs/` directory
- Source code documentation in `src/README.md`
- Connector development guide in `docs/dev/src/connector/intro.md`
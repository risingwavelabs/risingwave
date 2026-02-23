# AGENTS.md - CI Configuration

## 1. Scope

Policies for the ci directory, covering continuous integration workflows, build configurations, and automation scripts.

## 2. Purpose

The CI module defines all continuous integration pipelines for RisingWave, including build workflows, testing automation, benchmark execution, and release processes. It ensures code quality and automates the validation of all changes.

## 3. Structure

```
ci/
├── workflows/            # GitHub Actions workflow definitions
│   ├── pull-request.yml  # PR validation workflow
│   ├── main-cron.yml     # Scheduled main branch tests
│   ├── main-cron-bisect.yml  # Bisection automation
│   ├── docker.yml        # Docker image builds
│   ├── integration-tests.yml  # Integration test orchestration
│   ├── gen-flamegraph.yml     # Performance profiling
│   ├── gen-flamegraph-cron.yml
│   ├── sqlsmith-snapshots.yml # Fuzzing test snapshots
│   └── ...
├── scripts/              # CI helper scripts
├── docs/                 # CI documentation
├── plugins/              # CI plugins and extensions
├── ldap-test/            # LDAP integration tests
├── mongodb/              # MongoDB test configurations
├── vault/                # HashiCorp Vault test configs
├── postgres-conf/        # PostgreSQL test configurations
├── rabbitmq-conf/        # RabbitMQ test configurations
├── redis-conf/           # Redis test configurations
├── build-ci-image.sh     # CI image build script
├── Dockerfile            # CI environment Docker image
├── docker-compose.yml    # CI services orchestration
├── license-header.txt    # License header template
├── rust-toolchain        # Rust version for CI
└── risedev-components.ci.*.env  # CI environment configs
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `workflows/pull-request.yml` | PR validation pipeline |
| `workflows/main-cron.yml` | Scheduled comprehensive tests |
| `Dockerfile` | CI runner environment image |
| `build-ci-image.sh` | CI image build script |
| `scripts/` | Helper scripts for CI operations |
| `risedev-components.ci.*.env` | Environment configurations |

## 5. Edit Rules (Must)

- Test all workflow changes in a fork before merging
- Document workflow dependencies and job relationships
- Use consistent naming conventions for workflows and jobs
- Ensure workflows fail fast on configuration errors
- Add timeouts to all jobs to prevent runaway processes
- Document required secrets and environment variables
- Follow GitHub Actions security best practices
- Update workflow documentation when adding new jobs

## 6. Forbidden Changes (Must Not)

- Modify production deployment workflows without approval
- Remove required status checks from PR workflow
- Add secrets to workflow files (use GitHub Secrets)
- Disable test workflows without documented justification
- Change Rust toolchain without compatibility verification
- Modify benchmark workflows that affect performance tracking
- Bypass required reviews for CI configuration changes

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Validate workflows | `act` or test in fork repository |
| Test scripts | Run scripts locally with test inputs |
| Integration | Trigger workflow_dispatch in test branch |
| Docker image | `docker build -f ci/Dockerfile .` |

## 8. Dependencies & Contracts

- GitHub Actions platform
- Docker and container registries
- Test databases (PostgreSQL, MongoDB, Redis, etc.)
- Cloud services (AWS S3, GCS, Azure)
- Secret management (GitHub Secrets)
- Artifact storage and caching

## 9. Overrides

Inherits from `/home/k11/risingwave/AGENTS.md`:
- Override: Test Entry - CI workflows require fork-based testing
- Override: Dependencies - CI-specific external services

## 10. Update Triggers

Regenerate this file when:
- New workflow categories are added
- CI architecture changes significantly
- New test infrastructure is introduced
- Workflow file organization changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

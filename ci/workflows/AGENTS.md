# AGENTS.md - CI Workflows

## 1. Scope

Policies for the ci/workflows directory, covering GitHub Actions workflow definitions for continuous integration pipelines.

## 2. Purpose

The workflows directory contains all GitHub Actions workflow definitions that orchestrate the RisingWave CI/CD pipeline. These workflows handle code validation, testing, benchmarking, and release automation for the entire project.

## 3. Structure

```
workflows/
├── pull-request.yml           # Main PR validation workflow
├── main-cron.yml              # Scheduled comprehensive testing
├── main-cron-bisect.yml       # Automated bisection for regressions
├── docker.yml                 # Docker image build and publish
├── integration-tests.yml      # Integration test orchestration
├── gen-flamegraph.yml         # Performance profiling generation
├── gen-flamegraph-cron.yml    # Scheduled profiling jobs
├── sqlsmith-snapshots.yml     # Fuzzing test snapshot management
└── AGENTS.md                  # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `pull-request.yml` | Validates PRs with build, test, and lint checks |
| `main-cron.yml` | Nightly comprehensive test suite execution |
| `main-cron-bisect.yml` | Automatic regression bisection |
| `docker.yml` | Multi-arch Docker image builds |
| `integration-tests.yml` | End-to-end integration test triggers |
| `gen-flamegraph*.yml` | Performance profiling and flamegraph generation |
| `sqlsmith-snapshots.yml` | SQLsmith fuzzing test management |

## 5. Edit Rules (Must)

- Test all workflow changes in a personal fork before submitting PRs
- Document job dependencies with clear visual diagrams in comments
- Use matrix builds for testing across multiple configurations
- Set appropriate timeouts on all jobs (default: 60 minutes)
- Use workflow concurrency controls to cancel outdated runs
- Document all required secrets and environment variables
- Use reusable workflows for common patterns
- Include explicit error handling and notification steps

## 6. Forbidden Changes (Must Not)

- Remove or disable required status checks without RFC approval
- Hardcode credentials, tokens, or secrets in workflow files
- Modify production deployment jobs without security review
- Add workflows that bypass branch protection rules
- Change benchmark workflows affecting historical tracking
- Remove test coverage without documented justification
- Use `pull_request_target` without explicit security review
- Disable workflow concurrency protection

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Validate syntax | `actionlint` or GitHub web editor |
| Test in fork | Push to fork and verify execution |
| Dry run | Use `act` tool for local validation |
| Check permissions | Review workflow token permissions |
| Matrix validation | Verify all matrix combinations |

## 8. Dependencies & Contracts

- GitHub Actions platform (ubuntu-latest, self-hosted runners)
- Docker Buildx for multi-platform builds
- AWS/GCP/Azure credentials for cloud testing
- Container registries (GHCR, Docker Hub)
- Test infrastructure (PostgreSQL, Redis, Kafka, etc.)
- Secret management via GitHub Secrets
- Artifact storage and caching (GitHub Cache)

## 9. Overrides

Inherits from `/home/k11/risingwave/ci/AGENTS.md`:
- Override: Test Entry - Workflows require fork-based testing
- Override: Edit Rules - Workflow-specific validation requirements

## 10. Update Triggers

Regenerate this file when:
- New workflow categories are added (e.g., security scanning)
- GitHub Actions version requirements change
- Workflow file naming conventions change
- New runner types or platforms are introduced
- Required status check configurations change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/ci/AGENTS.md

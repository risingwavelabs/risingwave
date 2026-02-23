# AGENTS.md - Buildkite Plugins

## 1. Scope

Policies for the ci/plugins directory, covering Buildkite plugins for CI pipeline enhancement and failure debugging.

## 2. Purpose

The plugins module provides custom Buildkite plugins that extend CI functionality. These plugins handle post-command operations like log collection on failures, swapfile management for resource-constrained builds, and artifact upload handling.

## 3. Structure

```
ci/plugins/
├── docker-compose-logs/        # Collect RisingWave logs on test failure
│   └── hooks/post-command      # Uploads docker-compose logs as artifacts
├── swapfile/                   # Swapfile management for builds
│   └── hooks/                  # Pre-command hooks for swap setup
├── upload-failure-logs/        # Standard failure log upload
│   └── hooks/                  # Post-command failure handling
└── upload-failure-logs-zipped/ # Compressed failure log upload
    └── hooks/                  # Post-command with compression
```

## 4. Key Files

| File/Directory | Purpose |
|---------------|---------|
| `docker-compose-logs/hooks/post-command` | Captures RisingWave service logs when integration tests fail |
| `swapfile/hooks/` | Manages swapfile creation for memory-intensive builds |
| `upload-failure-logs/hooks/` | Generic failure log upload handler |
| `upload-failure-logs-zipped/hooks/` | Compressed log upload for large outputs |

## 5. Edit Rules (Must)

- Use bash with strict mode (set -euo pipefail)
- Source environment variables from env_vars.sh
- Handle both standalone and distributed RisingWave deployments
- Clean up docker resources after log collection
- Document plugin dependencies and requirements
- Test plugins with BUILDKITE_COMMAND_EXIT_STATUS simulation
- Ensure artifacts are uploaded with descriptive names
- Handle missing containers gracefully

## 6. Forbidden Changes (Must Not)

- Remove error handling from hook scripts
- Hardcode paths that should be configurable
- Upload sensitive data in logs
- Leave docker resources uncleaned
- Break plugin interface compatibility
- Add long-running operations to post-command hooks
- Remove support for distributed RisingWave deployments

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Hook syntax | `bash -n hooks/post-command` |
| ShellCheck | `shellcheck hooks/post-command` |
| Local test | Set BUILDKITE_COMMAND_EXIT_STATUS=1 and run |
| Integration | Trigger Buildkite pipeline with failure case |

## 8. Dependencies & Contracts

- Buildkite Agent and environment variables
- docker-compose for service management
- buildkite-agent CLI for artifact upload
- env_vars.sh for configuration
- Python 3 for cleanup scripts

## 9. Overrides

Inherits from `/home/k11/risingwave/ci/AGENTS.md`:
- Override: Test Entry - Buildkite-specific testing required

## 10. Update Triggers

Regenerate this file when:
- New Buildkite plugins are added
- Hook interfaces change
- Artifact handling requirements evolve
- Docker compose service names change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/ci/AGENTS.md

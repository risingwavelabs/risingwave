# AGENTS.md - Docker Compose Logs Plugin

## 1. Scope

Policies for the ci/plugins/docker-compose-logs directory, containing Buildkite plugin for capturing Docker Compose logs in CI pipelines.

## 2. Purpose

The docker-compose-logs plugin provides hooks that automatically capture and archive Docker Compose service logs during CI pipeline execution. It helps debug failures by preserving logs from all running containers even if the build fails or times out.

## 3. Structure

```
docker-compose-logs/
└── hooks/                   # Buildkite plugin hooks
    ├── post-command         # Post-command hook for log collection
    └── environment          # Environment setup hook
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `hooks/post-command` | Collects logs after command execution |
| `hooks/environment` | Sets up plugin environment |

## 5. Edit Rules (Must)

- Follow Buildkite plugin hook conventions
- Use POSIX-compliant shell scripts
- Handle errors gracefully (don't fail the build)
- Support multiple docker-compose files
- Archive logs in a structured format
- Clean up temporary files after execution
- Use proper quoting for shell variables
- Add timestamps to log collection
- Compress logs before upload for large outputs
- Include service names in log filenames
- Handle containers that may not exist
- Support both docker-compose v1 and v2

## 6. Forbidden Changes (Must Not)

- Fail the build if log collection fails
- Modify the original docker-compose state
- Leave temporary files behind
- Use bash-specific features (keep POSIX compatible)
- Log sensitive information
- Collect logs from non-RisingWave containers
- Remove existing log files without archiving

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Syntax check | `shellcheck hooks/*` |
| Integration | Test in CI pipeline |
| Local test | `BUILDKITE_COMMAND_EXIT_STATUS=1 hooks/post-command` |

## 8. Dependencies & Contracts

- Buildkite agent environment
- Docker Compose CLI
- POSIX shell (/bin/sh)
- Write access to artifact directory
- buildkite-agent for artifact upload

## 9. Overrides

Inherits from `/home/k11/risingwave/ci/plugins/AGENTS.md`:
- Override: Buildkite plugin specific patterns

## 10. Update Triggers

Regenerate this file when:
- Plugin architecture changes
- Hook lifecycle changes
- Log collection requirements change
- Docker Compose version requirements change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/ci/plugins/AGENTS.md

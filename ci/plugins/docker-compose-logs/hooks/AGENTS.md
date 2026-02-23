# AGENTS.md - Docker Compose Logs Hooks

## 1. Scope

Policies for the docker-compose-logs/hooks directory, containing Buildkite plugin hooks.

## 2. Purpose

The hooks directory contains Buildkite plugin lifecycle hooks that execute at specific pipeline stages. The post-command hook collects Docker Compose logs when builds fail.

## 3. Structure

```
hooks/
└── post-command    # Post-command hook for log collection
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `post-command` | Collects and uploads logs on failure |

## 5. Edit Rules (Must)

- Use POSIX-compliant shell syntax
- Handle errors gracefully (don't fail the build)
- Check for container existence before logging
- Archive logs with descriptive names
- Clean up resources after execution
- Source environment from env_vars.sh
- Support both standalone and distributed deployments

## 6. Forbidden Changes (Must Not)

- Fail the build if log collection fails
- Use bash-specific features
- Leave temporary files behind
- Log sensitive information
- Modify docker-compose state

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Syntax check | `shellcheck post-command` |
| Local test | `BUILDKITE_COMMAND_EXIT_STATUS=1 ./post-command` |

## 8. Dependencies & Contracts

- Buildkite Agent
- Docker Compose
- bash shell

## 9. Overrides

Inherits from `/home/k11/risingwave/ci/plugins/docker-compose-logs/AGENTS.md`:
- Override: Hook-specific patterns

## 10. Update Triggers

Regenerate this file when:
- New hooks are added
- Hook lifecycle changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/ci/plugins/docker-compose-logs/AGENTS.md

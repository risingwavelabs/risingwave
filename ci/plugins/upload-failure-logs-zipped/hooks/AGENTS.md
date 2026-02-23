# AGENTS.md - Upload Failure Logs Zipped Hooks

## 1. Scope

Policies for the `ci/plugins/upload-failure-logs-zipped/hooks` directory, covering the post-command hook for compressed failure log uploads.

## 2. Purpose

The Upload Failure Logs Zipped Hooks directory contains a streamlined post-command script that collects and uploads compressed log archives when CI commands fail. This variant focuses exclusively on zip archive uploads without individual file uploads, providing a simplified alternative to the standard upload-failure-logs plugin.

## 3. Structure

```
hooks/
└── post-command                 # Post-command hook for zipped upload
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `post-command` | Bash script for zipped log collection and upload |

## 5. Edit Rules (Must)

- Use bash strict mode (`set -euo pipefail`)
- Check BUILDKITE_COMMAND_EXIT_STATUS before uploading
- Create zip archive of risedev-logs directory
- Handle missing log directories gracefully
- Support regress-test output collection
- Support connector-node.log upload
- Upload only zip archives (not individual files)
- Verify zip creation before upload attempt

## 6. Forbidden Changes (Must Not)

- Remove BUILDKITE_COMMAND_EXIT_STATUS check
- Add individual file uploads (use standard plugin)
- Hardcode paths without fallback handling
- Skip error handling for artifact upload
- Upload sensitive data in logs
- Remove zip-only upload behavior

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Syntax check | `bash -n post-command` |
| ShellCheck | `shellcheck post-command` |
| Local test | `BUILDKITE_COMMAND_EXIT_STATUS=1 bash post-command` |
| Zip verification | `unzip -l risedev-logs.zip` |

## 8. Dependencies & Contracts

- Buildkite Agent environment
- buildkite-agent CLI for artifact upload
- zip utility for compression
- Log directory: `.risingwave/log/`
- Regress test output: `src/tests/regress/output/results/`
- Connector node log: `./connector-node.log`

## 9. Overrides

Inherits from `/home/k11/risingwave/ci/plugins/upload-failure-logs-zipped/AGENTS.md` and `/home/k11/risingwave/ci/plugins/AGENTS.md`:
- Override: Hook-specific zip-only upload logic
- Override: Simplified log collection approach

## 10. Update Triggers

Regenerate this file when:
- Log collection logic changes
- New artifact types are added
- Hook interface changes
- Zip compression settings change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/ci/plugins/upload-failure-logs-zipped/AGENTS.md

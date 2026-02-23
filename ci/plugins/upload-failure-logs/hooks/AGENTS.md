# AGENTS.md - Upload Failure Logs Hooks

## 1. Scope

Policies for the `ci/plugins/upload-failure-logs/hooks` directory, covering the post-command hook implementation for CI failure log collection.

## 2. Purpose

The Upload Failure Logs Hooks directory contains the post-command bash script that executes after CI commands complete. When a build fails, it automatically collects RisingWave service logs, trims oversized files, creates zip archives, and uploads artifacts to Buildkite for debugging purposes.

## 3. Structure

```
hooks/
└── post-command                 # Post-command hook script
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `post-command` | Bash script for log collection and artifact upload |

## 5. Edit Rules (Must)

- Use bash strict mode (`set -euo pipefail`)
- Check BUILDKITE_COMMAND_EXIT_STATUS before processing
- Trim log files exceeding 200MB (keep tail)
- Handle missing log directories gracefully
- Upload both individual files and zip archives
- Support regress-test output collection
- Support connector-node.log upload
- Use descriptive artifact names
- Handle errors during artifact upload

## 6. Forbidden Changes (Must Not)

- Remove BUILDKITE_COMMAND_EXIT_STATUS check
- Remove log truncation logic for large files
- Hardcode paths without fallback handling
- Skip error handling for artifact operations
- Upload sensitive credentials or secrets
- Remove support for regress-test logs
- Remove zip archive creation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Syntax check | `bash -n post-command` |
| ShellCheck | `shellcheck post-command` |
| Local test | `BUILDKITE_COMMAND_EXIT_STATUS=1 bash post-command` |
| Dry run | Verify paths and commands without upload |

## 8. Dependencies & Contracts

- Buildkite Agent environment
- buildkite-agent CLI for artifact upload
- zip utility for compression
- find, wc, tail for file operations
- Log directory: `.risingwave/log/`
- Regress test output: `src/tests/regress/output/results/`
- Connector node log: `./connector-node.log`

## 9. Overrides

Inherits from `./ci/plugins/upload-failure-logs/AGENTS.md` and `./ci/plugins/AGENTS.md`:
- Override: Hook-specific implementation rules
- Override: Log truncation and upload logic

## 10. Update Triggers

Regenerate this file when:
- Log collection logic changes
- Artifact upload behavior changes
- Truncation thresholds change
- New log types are added
- Hook interface changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./ci/plugins/upload-failure-logs/AGENTS.md

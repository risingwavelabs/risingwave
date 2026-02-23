# AGENTS.md - Upload Failure Logs Plugin

## 1. Scope

Policies for the `ci/plugins/upload-failure-logs` directory, covering the Buildkite plugin for uploading failure logs.

## 2. Purpose

The Upload Failure Logs plugin provides post-command hooks that automatically collect and upload log artifacts when CI commands fail. It handles RisingWave service logs, regression test outputs, and connector node logs, with automatic truncation for large files.

## 3. Structure

```
ci/plugins/upload-failure-logs/
└── hooks/
    └── post-command          # Post-command hook for log collection
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `hooks/post-command` | Bash script for log collection and artifact upload |

## 5. Edit Rules (Must)

- Use bash strict mode (`set -euo pipefail`)
- Check BUILDKITE_COMMAND_EXIT_STATUS before uploading
- Trim logs exceeding 200MB before upload
- Handle missing log directories gracefully
- Upload both individual files and zip archives
- Support regress-test and connector-node logs

## 6. Forbidden Changes (Must Not)

- Remove exit status checks
- Hardcode paths without fallbacks
- Skip error handling for artifact upload
- Upload sensitive data in logs
- Remove log truncation logic

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Syntax check | `bash -n hooks/post-command` |
| ShellCheck | `shellcheck hooks/post-command` |
| Local test | `BUILDKITE_COMMAND_EXIT_STATUS=1 bash hooks/post-command` |

## 8. Dependencies & Contracts

- Buildkite Agent environment
- buildkite-agent CLI for artifact upload
- zip utility for compression
- find and wc for file operations
- Log directory structure: `.risingwave/log/`

## 9. Overrides

Inherits from `/home/k11/risingwave/ci/plugins/AGENTS.md`:
- Override: Edit Rules - Hook-specific requirements
- Override: Test Entry - Hook testing commands

## 10. Update Triggers

Regenerate this file when:
- Log collection logic changes
- New artifact types are added
- Truncation thresholds change
- Hook interface changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/ci/plugins/AGENTS.md

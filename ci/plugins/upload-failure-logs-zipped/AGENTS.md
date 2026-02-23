# AGENTS.md - Upload Failure Logs Zipped Plugin

## 1. Scope

Policies for the `ci/plugins/upload-failure-logs-zipped` directory, covering the Buildkite plugin for uploading compressed failure logs.

## 2. Purpose

The Upload Failure Logs Zipped plugin provides a simplified post-command hook that collects and uploads compressed log artifacts when CI commands fail. Unlike the standard upload-failure-logs plugin, this version focuses on zip-only uploads without individual file uploads.

## 3. Structure

```
ci/plugins/upload-failure-logs-zipped/
└── hooks/
    └── post-command          # Post-command hook for zipped log upload
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `hooks/post-command` | Bash script for log collection and zip upload |

## 5. Edit Rules (Must)

- Use bash strict mode (`set -euo pipefail`)
- Check BUILDKITE_COMMAND_EXIT_STATUS before uploading
- Create zip archive of log directory
- Handle missing log directories gracefully
- Support regress-test and connector-node logs
- Upload only zip archives (not individual files)

## 6. Forbidden Changes (Must Not)

- Remove exit status checks
- Add individual file uploads (use standard plugin for that)
- Hardcode paths without fallbacks
- Skip error handling for artifact upload
- Upload sensitive data in logs

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
- Log directory structure: `.risingwave/log/`
- Regress test output: `src/tests/regress/output/results/`

## 9. Overrides

Inherits from `/home/k11/risingwave/ci/plugins/AGENTS.md`:
- Override: Edit Rules - Hook-specific requirements
- Override: Test Entry - Hook testing commands

## 10. Update Triggers

Regenerate this file when:
- Log collection logic changes
- New artifact types are added
- Hook interface changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/ci/plugins/AGENTS.md

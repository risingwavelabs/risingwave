# AGENTS.md - Swapfile Plugin

## 1. Scope

Policies for the `ci/plugins/swapfile` directory, covering the Buildkite plugin for managing swapfiles in CI environments.

## 2. Purpose

The Swapfile plugin provides pre-command hooks that create and configure swapfiles for memory-intensive CI builds. It ensures 32GB of swap space is available for builds that may exceed physical memory limits, preventing OOM (Out of Memory) errors during compilation and testing.

## 3. Structure

```
ci/plugins/swapfile/
└── hooks/
    └── pre-command           # Pre-command hook for swapfile setup
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `hooks/pre-command` | Bash script for swapfile creation and activation |

## 5. Edit Rules (Must)

- Use bash strict mode (`set -euo pipefail`)
- Check if swapfile exists before creating
- Use sudo for privileged operations
- Verify swap activation with swapon -s
- Allocate 32GB swapfile with fallocate
- Set proper permissions (600) on swapfile

## 6. Forbidden Changes (Must Not)

- Remove swapfile existence check
- Skip permission setting on swapfile
- Remove swap verification
- Use inappropriate swapfile sizes
- Skip error handling for privileged operations

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Syntax check | `bash -n hooks/pre-command` |
| ShellCheck | `shellcheck hooks/pre-command` |
| Local test | `bash hooks/pre-command` (requires sudo) |

## 8. Dependencies & Contracts

- Linux environment with fallocate support
- sudo privileges for swap operations
- mkswap and swapon utilities
- 32GB free disk space for swapfile
- Buildkite Agent environment

## 9. Overrides

Inherits from `./ci/plugins/AGENTS.md`:
- Override: Edit Rules - Pre-command hook requirements
- Override: Test Entry - Hook testing commands

## 10. Update Triggers

Regenerate this file when:
- Swapfile size or configuration changes
- Swap creation logic changes
- Hook interface changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./ci/plugins/AGENTS.md

# AGENTS.md - Swapfile Hooks

## 1. Scope

Policies for the `ci/plugins/swapfile/hooks` directory, covering the pre-command hook for swapfile management in CI builds.

## 2. Purpose

The Swapfile Hooks directory contains the pre-command bash script that executes before CI commands run. It creates and activates a 32GB swapfile to prevent Out of Memory (OOM) errors during memory-intensive builds, particularly for Rust compilation and large-scale integration tests.

## 3. Structure

```
hooks/
└── pre-command                  # Pre-command hook for swapfile setup
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `pre-command` | Bash script for swapfile creation and activation |

## 5. Edit Rules (Must)

- Use bash strict mode (`set -euo pipefail`)
- Check if swapfile exists before creating (idempotent)
- Allocate 32GB using fallocate
- Set permissions to 600 (owner read/write only)
- Initialize swap with mkswap
- Activate swap with swapon
- Verify activation with swapon -s
- Use sudo for privileged operations
- Handle cases where swap already exists

## 6. Forbidden Changes (Must Not)

- Remove swapfile existence check
- Change swapfile size without build team approval
- Skip permission setting (security risk)
- Remove swap verification step
- Skip error handling for privileged operations
- Make operation non-idempotent

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Syntax check | `bash -n pre-command` |
| ShellCheck | `shellcheck pre-command` |
| Local test | `sudo bash pre-command` (requires root) |
| Verify swap | `swapon -s | grep /swapfile` |

## 8. Dependencies & Contracts

- Linux environment with fallocate support
- sudo privileges for swap operations
- mkswap and swapon utilities
- 32GB free disk space for swapfile
- Buildkite Agent environment
- Swapfile location: `/swapfile`

## 9. Overrides

Inherits from `/home/k11/risingwave/ci/plugins/swapfile/AGENTS.md` and `/home/k11/risingwave/ci/plugins/AGENTS.md`:
- Override: Pre-command hook swap management rules
- Override: Idempotent swap creation requirements

## 10. Update Triggers

Regenerate this file when:
- Swapfile size or configuration changes
- Swap creation logic changes
- Hook interface changes
- Swap verification method changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/ci/plugins/swapfile/AGENTS.md

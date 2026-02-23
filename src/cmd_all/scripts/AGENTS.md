# AGENTS.md - Deployment Scripts

## 1. Scope

Directory: `src/cmd_all/scripts`

Shell scripts for RisingWave deployment demonstrations and standalone mode testing.

## 2. Purpose

Provides ready-to-use deployment scripts for:
- Demonstrating RisingWave standalone mode capabilities
- Running end-to-end demos with full component startup
- Supporting development and testing workflows
- Showcasing production-like deployment patterns

## 3. Structure

```
src/cmd_all/scripts/
├── e2e-full-standalone-demo.sh    # Full E2E demo with standalone mode
├── standalone-demo-dev.sh         # Development-focused standalone demo
├── standalone-demo-full.sh        # Complete standalone deployment demo
└── AGENTS.md                      # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `e2e-full-standalone-demo.sh` | End-to-end demo: build, start standalone, run queries, stop |
| `standalone-demo-dev.sh` | Quick development demo with minimal configuration |
| `standalone-demo-full.sh` | Full-featured demo with all services enabled |

## 5. Edit Rules (Must)

- Write scripts in POSIX-compatible bash for portability
- Include proper error handling with `set -euo pipefail`
- Document script purpose and usage in header comments
- Use relative paths from repository root for reliability
- Clean up resources (processes, temp files) on exit
- Support both debug and release builds via environment variables
- Include informative echo statements for user feedback
- Make scripts executable: `chmod +x *.sh`
- Test scripts locally before committing
- Follow shell style: 2-space indentation, lowercase variables

## 6. Forbidden Changes (Must Not)

- Use bashisms incompatible with POSIX sh
- Hardcode absolute paths (use `$PWD` or relative paths)
- Leave RisingWave processes running after script completion
- Skip error handling on critical commands
- Include credentials or sensitive configuration
- Modify without testing on clean environment
- Remove existing demo scripts without replacement

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run E2E demo | `./src/cmd_all/scripts/e2e-full-standalone-demo.sh` |
| Run dev demo | `./src/cmd_all/scripts/standalone-demo-dev.sh` |
| Run full demo | `./src/cmd_all/scripts/standalone-demo-full.sh` |
| Syntax check | `bash -n src/cmd_all/scripts/*.sh` |

## 8. Dependencies & Contracts

- Requires: bash, cargo, tmux (for service management)
- Depends on `risingwave` binary from `risingwave_cmd_all` crate
- Assumes repository root as working directory
- Scripts manage RisingWave lifecycle: build -> start -> demo -> stop
- Uses default ports: meta (5690), frontend (4566), compute (5688), compactor (6660)

## 9. Overrides

| Parent Rule | Override |
|-------------|----------|
| Shell execution | These are shell scripts, not Rust code |

## 10. Update Triggers

Regenerate this file when:
- New deployment scripts added
- Standalone mode CLI changes
- Demo workflow modifications
- Port or service configuration changes
- New demo scenarios introduced

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `./src/cmd_all/AGENTS.md`

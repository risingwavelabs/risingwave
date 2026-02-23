# AGENTS.md - RiseDev Scripts

## 1. Scope

Directory: `src/risedevtool/scripts`

Support scripts for RiseDev developer tooling operations.

## 2. Purpose

Provides auxiliary shell scripts for RiseDev functionality:
- Download and install external dependencies (ADBC drivers)
- Support developer environment setup automation
- Handle component-specific installation tasks

## 3. Structure

```
src/risedevtool/scripts/
├── download_adbc_snowflake_driver.sh    # ADBC Snowflake driver installer
└── AGENTS.md                            # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `download_adbc_snowflake_driver.sh` | Downloads and installs Snowflake ADBC driver for testing |

## 5. Edit Rules (Must)

- Write scripts in POSIX-compatible bash for maximum portability
- Include error handling with `set -euo pipefail`
- Document purpose, usage, and dependencies in header comments
- Use checksum verification for downloaded binaries
- Support configurable installation directories via environment variables
- Clean up temporary files after installation
- Handle architecture detection (x86_64, aarch64) automatically
- Make scripts executable: `chmod +x *.sh`
- Test on both Linux and macOS when possible
- Follow shell style: 2-space indentation, descriptive variable names

## 6. Forbidden Changes (Must Not)

- Download untrusted binaries without checksum verification
- Hardcode absolute installation paths
- Skip error handling on network operations
- Require root privileges unnecessarily
- Include hardcoded credentials or API keys
- Modify without testing installation process
- Remove support for existing architectures

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Syntax check | `bash -n src/risedevtool/scripts/*.sh` |
| Run ADBC script | `./src/risedevtool/scripts/download_adbc_snowflake_driver.sh` |
| Check executable | `test -x src/risedevtool/scripts/*.sh` |

## 8. Dependencies & Contracts

- `curl`: For downloading driver packages
- `tar`/`unzip`: For archive extraction
- `sha256sum` or `shasum`: For checksum verification
- Environment: `$HOME/.local/lib` or configurable install path
- ADBC drivers used by connector components for external data sources
- Scripts are invoked by RiseDev tasks or manually by developers

## 9. Overrides

| Parent Rule | Override |
|-------------|----------|
| Shell execution | These are shell scripts, not Rust code |

## 10. Update Triggers

Regenerate this file when:
- New support scripts added
- Driver download URLs or versions change
- Installation procedures modified
- New architecture support added
- Checksum verification method changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: `./src/risedevtool/AGENTS.md`

# AGENTS.md - Utility Tools

## 1. Scope

Policies for the tools directory, covering utility scripts and helper tools for RisingWave development and operations.

## 2. Purpose

The tools module provides utility scripts and helper programs for various RisingWave development, testing, and operational tasks. This serves as a collection of miscellaneous utilities that support the development workflow.

## 3. Structure

```
tools/
└── (utility scripts and tools)
```

Note: This directory appears to be a placeholder or contains utility files that may be referenced from other locations. Additional tools may be located in `src/risedevtool/` and `java/tools/`.

## 4. Key Files

| File | Purpose |
|------|---------|
| (Directory is currently empty) | Reserved for general utilities |

## 5. Edit Rules (Must)

- Document tool purpose and usage in script headers
- Include usage examples in documentation
- Ensure tools are portable across platforms
- Add error handling for edge cases
- Version control all tool changes
- Test tools before committing

## 6. Forbidden Changes (Must Not)

- Remove tools without checking dependencies
- Add tools with hardcoded paths or credentials
- Commit binary artifacts or large data files
- Add tools that duplicate existing functionality
- Include malicious or harmful code

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Script validation | Run with test inputs |
| Integration | Test with actual RisingWave cluster |
| Lint | Run shellcheck for shell scripts |

## 8. Dependencies & Contracts

- Shell interpreters (bash, sh)
- Python runtime (for Python tools)
- RisingWave CLI tools
- External API clients

## 9. Overrides

Inherits from `./AGENTS.md`:
- No overrides currently defined

## 10. Update Triggers

Regenerate this file when:
- New utility categories are added
- Tool organization changes
- New dependencies are introduced

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

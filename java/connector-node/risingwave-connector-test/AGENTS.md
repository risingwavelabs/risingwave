# AGENTS.md - Connector Test

## 1. Scope

Policies for the `java/connector-node/risingwave-connector-test` directory, covering shared test utilities and infrastructure for connector testing.

## 2. Purpose

The Connector Test module provides shared test utilities, fixtures, and infrastructure components used across multiple connector test modules. It serves as a centralized location for test helpers that are reused by various source and sink connector tests, preventing code duplication.

## 3. Structure

```
risingwave-connector-test/
└── target/                      # Build artifacts directory
    (Minimal module - resources only)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `target/` | Build output and compiled test utilities |
| (No pom.xml) | Managed by parent as test resource |
| (No src/) | Test utilities provided via other means |

## 5. Edit Rules (Must)

- Maintain shared test configuration files
- Update common test data formats
- Document utility dependencies between tests
- Version control test schemas and fixtures
- Ensure thread-safety of shared test utilities

## 6. Forbidden Changes (Must Not)

- Add production source code to this module
- Remove shared utilities without migrating dependents
- Commit sensitive test credentials
- Break backward compatibility without notification

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Test utilities | Referenced by other test modules |
| Build | Managed via parent Maven project |

## 8. Dependencies & Contracts

- Maven 3.8+ (via parent)
- Java 11+ runtime
- Shared by risingwave-source-test and others
- Test scope dependencies only

## 9. Overrides

Inherits from `/home/k11/risingwave/java/connector-node/AGENTS.md`:
- Override: Shared test utilities only

## 10. Update Triggers

Regenerate this file when:
- Test utility structure changes
- New shared test components are added
- Module purpose expands

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/connector-node/AGENTS.md

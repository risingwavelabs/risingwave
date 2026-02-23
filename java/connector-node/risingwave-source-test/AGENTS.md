# AGENTS.md - Source Test

## 1. Scope

Policies for the `java/connector-node/risingwave-source-test` directory, covering shared test resources for source connector validation.

## 2. Purpose

The Source Test module provides test resources and infrastructure for validating source connector implementations. It serves as a container for test data, configurations, and fixtures used across multiple source connector test suites, including CDC and non-CDC source types.

## 3. Structure

```
risingwave-source-test/
└── target/                      # Build artifacts directory
    (Minimal module - test resources only)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `target/` | Build output directory |
| (No pom.xml) | Referenced as test resource by parent |
| (No src/) | Resources provided via test dependencies |

## 5. Edit Rules (Must)

- Maintain source test data schemas
- Update sample data for source validation
- Document test data formats and sources
- Version control test fixtures
- Ensure test data licensing compliance

## 6. Forbidden Changes (Must Not)

- Add production source code to this module
- Remove test fixtures without updating dependent tests
- Commit large binary test datasets
- Include sensitive data in test resources

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Test resources | Referenced by source connector tests |
| Build | Managed via parent Maven project |

## 8. Dependencies & Contracts

- Maven 3.8+ (via parent)
- Java 11+ runtime
- Shared test resources for source connectors
- Referenced by CDC and non-CDC source tests

## 9. Overrides

Inherits from `./java/connector-node/AGENTS.md`:
- Override: Source test resources only

## 10. Update Triggers

Regenerate this file when:
- Test resource structure changes
- New source connector test types are added
- Shared test infrastructure changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/connector-node/AGENTS.md

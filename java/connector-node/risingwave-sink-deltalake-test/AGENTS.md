# AGENTS.md - Delta Lake Sink Test

## 1. Scope

Policies for the `java/connector-node/risingwave-sink-deltalake-test` directory, covering integration tests for the Delta Lake sink connector.

## 2. Purpose

The Delta Lake Sink Test module provides integration testing infrastructure for the Delta Lake sink connector. It contains test configurations and resources for validating Delta Lake write operations, schema evolution, and time travel capabilities. Note: This module primarily serves as a test container; actual sink implementation resides in risingwave-sink-deltalake.

## 3. Structure

```
risingwave-sink-deltalake-test/
└── target/                      # Build and test artifacts
    (No source files - test resources only)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `target/` | Test output directory and compiled resources |
| (No pom.xml) | Referenced as test dependency by parent |

## 5. Edit Rules (Must)

- Maintain test resource files for Delta Lake validation
- Update test data for schema evolution scenarios
- Document test configuration parameters
- Follow Delta Lake protocol specifications in tests
- Clean up test artifacts after execution
- Version control test datasets for reproducibility

## 6. Forbidden Changes (Must Not)

- Add production source code to this test module
- Remove test resources without updating test cases
- Commit large binary test files directly
- Skip test cleanup operations
- Modify without updating parent module references

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Integration tests | Run via parent module or connector-node tests |
| Test resources | Referenced by risingwave-sink-deltalake tests |
| Build | Managed by parent Maven project |

## 8. Dependencies & Contracts

- Maven 3.8+ (via parent)
- Java 11+ runtime
- Delta Lake protocol compatibility
- Apache Spark (for test validation)
- Testcontainers for integration tests
- risingwave-sink-deltalake as test subject

## 9. Overrides

Inherits from `./java/connector-node/AGENTS.md`:
- Override: Test-only module, no production code

## 10. Update Triggers

Regenerate this file when:
- Test resource structure changes
- New Delta Lake test scenarios are added
- Integration test approach changes
- Parent module reference updates

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/connector-node/AGENTS.md

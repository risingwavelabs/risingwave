# AGENTS.md - CDC Source Test

## 1. Scope

Policies for the `java/connector-node/risingwave-source-cdc-test` directory, covering unit and integration tests for CDC (Change Data Capture) source connectors.

## 2. Purpose

The CDC Source Test module contains unit tests for CDC source connectors that cannot reside alongside the main source code due to circular dependency concerns. It validates Debezium-based CDC connectors for MySQL, PostgreSQL, and MongoDB, ensuring proper change event capture, deserialization, and validation logic.

## 3. Structure

```
risingwave-source-cdc-test/
├── pom.xml                      # Maven test module configuration
├── src/test/java/com/risingwave/connector/source/
│   ├── MySQLSourceTest.java     # MySQL CDC source tests
│   ├── PostgresSourceTest.java  # PostgreSQL CDC source tests
│   ├── MongoDbSourceTest.java   # MongoDB CDC source tests
│   ├── MongoDbValidatorTest.java # MongoDB validation tests
│   └── SourceTestClient.java    # Test client utilities
└── target/                      # Test build artifacts
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `MySQLSourceTest.java` | MySQL CDC connector unit tests |
| `PostgresSourceTest.java` | PostgreSQL CDC connector tests |
| `MongoDbSourceTest.java` | MongoDB CDC source tests |
| `MongoDbValidatorTest.java` | MongoDB connection validation tests |
| `SourceTestClient.java` | Shared test client utilities |
| `pom.xml` | Test dependencies including Testcontainers |

## 5. Edit Rules (Must)

- Add tests for new CDC source connectors
- Use Testcontainers for database test instances
- Test both snapshot and streaming phases
- Validate schema evolution scenarios
- Include error handling and recovery tests
- Test database-specific data type mappings
- Document test prerequisites and configurations
- Clean up test containers after execution
- Follow JUnit 4 testing conventions

## 6. Forbidden Changes (Must Not)

- Add production source code to this module
- Skip container cleanup in test teardown
- Hardcode database credentials in tests
- Remove tests for supported CDC connectors
- Break Testcontainers lifecycle management
- Skip error scenario validation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-source-cdc-test` |
| Single test | `mvn test -pl risingwave-source-cdc-test -Dtest=MySQLSourceTest` |
| Checkstyle | `mvn checkstyle:check -pl risingwave-source-cdc-test` |
| Integration | Via Testcontainers (auto-started) |

## 8. Dependencies & Contracts

- Maven 3.8+ for builds
- Java 11+ runtime
- JUnit 4 for test framework
- Testcontainers for database services
- Debezium connectors for CDC simulation
- risingwave-source-cdc as test subject
- risingwave-connector-service (test scope)
- HikariCP for test connection pooling
- Jackson for JSON test data

## 9. Overrides

Inherits from `./java/connector-node/AGENTS.md`:
- Override: Test-only module with circular dependency resolution
- Override: Testcontainers-based testing requirements

## 10. Update Triggers

Regenerate this file when:
- New CDC source tests are added
- Test infrastructure changes
- Supported database versions update
- Circular dependency resolution changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/connector-node/AGENTS.md

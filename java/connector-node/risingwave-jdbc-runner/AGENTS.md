# AGENTS.md - JDBC Runner

## 1. Scope

Policies for the `java/connector-node/risingwave-jdbc-runner` directory, covering the JDBC SQL execution utilities for cloud data warehouses.

## 2. Purpose

The JDBC Runner module provides specialized JDBC execution capabilities for cloud data warehouse sinks like Snowflake and Amazon Redshift. It handles SQL statement execution, connection management, and batch operations for external data warehouse integrations.

## 3. Structure

```
risingwave-jdbc-runner/
├── pom.xml                      # Maven module configuration
├── src/
│   ├── main/java/com/risingwave/runner/
│   │   └── JDBCSqlRunner.java   # Main SQL runner implementation
│   └── test/java/com/risingwave/runner/
│       └── JDBCSqlRunnerTest.java  # Unit tests
└── target/                      # Build artifacts
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `JDBCSqlRunner.java` | Core JDBC SQL execution runner |
| `JDBCSqlRunnerTest.java` | Unit tests for runner functionality |
| `pom.xml` | Dependencies for Snowflake and Redshift JDBC drivers |

## 5. Edit Rules (Must)

- Implement proper connection pooling for performance
- Handle SQL exceptions with detailed error messages
- Support parameterized queries to prevent injection
- Use appropriate batch sizes for cloud warehouses
- Implement connection retry logic for transient failures
- Add timeout configuration for long-running queries
- Support both Snowflake and Redshift JDBC drivers
- Document driver-specific configuration options
- Follow JDBC 4.2+ best practices

## 6. Forbidden Changes (Must Not)

- Remove support for existing warehouse drivers
- Skip connection pool configuration
- Use string concatenation for SQL (always use PreparedStatement)
- Hardcode credentials or connection strings
- Remove retry logic for cloud service failures
- Skip query timeout handling

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-jdbc-runner` |
| Checkstyle | `mvn checkstyle:check -pl risingwave-jdbc-runner` |
| Build | `mvn clean package -pl risingwave-jdbc-runner` |

## 8. Dependencies & Contracts

- Maven 3.8+ for builds
- Java 11+ runtime
- Snowflake JDBC driver 3.23+
- Amazon Redshift JDBC driver 2.1+
- risingwave-sink-jdbc for base functionality
- connector-api for integration
- HikariCP for connection pooling (in tests)

## 9. Overrides

Inherits from `/home/k11/risingwave/java/connector-node/AGENTS.md`:
- Override: Cloud warehouse-specific patterns
- Override: JDBC driver management requirements

## 10. Update Triggers

Regenerate this file when:
- New cloud warehouse drivers are added
- JDBC runner interface changes
- Connection pooling strategy changes
- Supported warehouse platforms expand

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/connector-node/AGENTS.md

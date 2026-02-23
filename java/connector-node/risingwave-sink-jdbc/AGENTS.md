# AGENTS.md - JDBC Sink Connector

## 1. Scope

Policies for the risingwave-sink-jdbc module, implementing JDBC-based sink connectors for databases.

## 2. Purpose

The JDBC sink module provides generic sink implementations for relational databases. It supports PostgreSQL, MySQL, SQL Server, Redshift, and Snowflake with dialect-specific optimizations and batch processing.

## 3. Structure

```
risingwave-sink-jdbc/
├── src/main/java/com/risingwave/connector/
│   ├── JDBCSink.java                   # Main JDBC sink implementation
│   ├── JDBCSinkConfig.java             # JDBC configuration
│   ├── SnowflakeJDBCSinkConfig.java    # Snowflake-specific config
│   ├── JDBCSinkFactory.java            # Factory for JDBC sinks
│   ├── JdbcUtils.java                  # JDBC utilities
│   ├── BatchAppendOnlyJDBCSink.java    # Append-only batch sink
│   └── jdbc/
│       ├── JdbcDialect.java            # Dialect interface
│       ├── JdbcDialectFactory.java     # Dialect factory
│       ├── SchemaTableName.java        # Schema/table naming
│       ├── PostgresDialect.java        # PostgreSQL dialect
│       ├── PostgresDialectFactory.java # PG dialect factory
│       ├── MySqlDialect.java           # MySQL dialect
│       ├── MySqlDialectFactory.java    # MySQL dialect factory
│       ├── SqlServerDialect.java       # SQL Server dialect
│       ├── SqlServerDialectFactory.java    # SQL Server factory
│       ├── RedShiftDialect.java        # Redshift dialect
│       ├── RedShiftDialectFactory.java # Redshift factory
│       ├── SnowflakeDialect.java       # Snowflake dialect
│       └── SnowflakeDialectFactory.java    # Snowflake factory
└── pom.xml
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `JDBCSink.java` | Main sink implementation |
| `JdbcDialect.java` | SQL dialect abstraction |
| `PostgresDialect.java` | PostgreSQL-specific SQL |
| `JDBCSinkConfig.java` | Configuration handling |
| `BatchAppendOnlyJDBCSink.java` | Optimized append-only sink |

## 5. Edit Rules (Must)

- Implement dialect-specific upsert logic
- Use prepared statements for all queries
- Support batch operations for performance
- Handle connection pooling properly
- Implement proper transaction handling
- Escape identifiers to prevent SQL injection
- Support schema evolution gracefully
- Add connection validation
- Handle network timeouts appropriately

## 6. Forbidden Changes (Must Not)

- Concatenate user input into SQL directly
- Remove support for existing dialects
- Break transaction atomicity
- Use blocking calls without timeouts
- Skip connection validation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-sink-jdbc` |
| Integration | Test with Testcontainers |

## 8. Dependencies & Contracts

- JDBC 4.2+ drivers
- Apache Commons Text for escaping
- PostgreSQL, MySQL, Redshift, Snowflake drivers
- Testcontainers for testing

## 9. Overrides

Inherits from `./java/connector-node/AGENTS.md`:
- Override: JDBC-specific patterns
- Override: SQL dialect implementation

## 10. Update Triggers

Regenerate this file when:
- New database dialects are added
- JDBC driver versions change
- Sink batching logic changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/connector-node/AGENTS.md

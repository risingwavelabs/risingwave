# AGENTS.md - Cassandra Sink Connector

## 1. Scope

Policies for the risingwave-sink-cassandra module, implementing Apache Cassandra sink connector.

## 2. Purpose

The Cassandra sink module provides a high-performance sink for writing data to Apache Cassandra and compatible databases like ScyllaDB. It supports batch writes, configurable consistency levels, and proper connection management.

## 3. Structure

```
risingwave-sink-cassandra/
└── src/main/java/com/risingwave/connector/
    ├── CassandraSink.java        # Main sink implementation
    ├── CassandraConfig.java      # Configuration handling
    ├── CassandraFactory.java     # Sink factory
    └── CassandraUtil.java        # Utility functions
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `CassandraSink.java` | Main sink implementation |
| `CassandraConfig.java` | Configuration parsing |
| `CassandraFactory.java` | Factory for creating sinks |
| `CassandraUtil.java` | Helper utilities |

## 5. Edit Rules (Must)

- Use DataStax Java driver 4.x
- Support configurable consistency levels
- Implement proper connection pooling
- Handle CQL statement preparation
- Support batch writes for performance
- Handle Cassandra-specific types
- Implement proper retry logic
- Support TLS/SSL connections

## 6. Forbidden Changes (Must Not)

- Remove support for existing Cassandra versions
- Break CQL compatibility
- Skip connection cleanup
- Use deprecated driver APIs
- Ignore query timeouts

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-sink-cassandra` |
| Integration | Test with Testcontainers Cassandra |

## 8. Dependencies & Contracts

- DataStax Java Driver 4.19.0
- Jackson for JSON processing
- Apache Commons Text

## 9. Overrides

Inherits from `./java/connector-node/AGENTS.md`:
- Override: Cassandra-specific patterns

## 10. Update Triggers

Regenerate this file when:
- Driver version upgrades
- New Cassandra features are added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/connector-node/AGENTS.md

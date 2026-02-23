# AGENTS.md - CDC Source Connector

## 1. Scope

Policies for the risingwave-source-cdc module, implementing Change Data Capture sources using Debezium.

## 2. Purpose

The CDC source module provides real-time change data capture from databases using Debezium. It supports PostgreSQL, MySQL, MongoDB, and SQL Server as sources. The module includes custom Debezium extensions for RisingWave-specific requirements.

## 3. Structure

```
risingwave-source-cdc/
├── src/main/java/
│   ├── com/risingwave/connector/cdc/debezium/
│   │   ├── converters/
│   │   │   └── DatetimeTypeConverter.java    # Date/time conversion
│   │   └── internal/
│   │       ├── DebeziumOffset.java           # Offset management
│   │       ├── DebeziumOffsetSerializer.java # Offset serialization
│   │       ├── ConfigurableOffsetBackingStore.java # Offset storage
│   │       ├── OpendalSchemaHistory.java     # Schema history
│   │       └── NoDataRecoverySnapshotter.java    # Snapshot control
│   └── io/debezium/
│       ├── connector/
│       │   ├── mysql/MySqlOffsetContext.java # MySQL offset handling
│       │   ├── binlog/
│       │   │   ├── BinlogOffsetContext.java  # Binlog offset
│       │   │   └── history/
│       │   │       └── BinlogHistoryRecordComparator.java
│       │   └── postgresql/
│       │       ├── PostgresOffsetContext.java    # PG offset
│       │       ├── PostgresStreamingChangeEventSource.java
│       │       ├── PostgresConnectorConfig.java
│       │       ├── PostgresSchema.java
│       │       └── connection/
│       │           ├── ReplicationStream.java
│       │           └── pgoutput/
│       │               └── PgOutputMessageDecoder.java
│       ├── embedded/
│       │   └── EmbeddedEngineChangeEventProxy.java
│       ├── pipeline/
│       │   └── ChangeEventSourceCoordinator.java
│       └── openlineage/
│           └── DebeziumOpenLineageEmitter.java
└── pom.xml
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `DebeziumOffset.java` | Manages CDC offset state |
| `PostgresOffsetContext.java` | PostgreSQL LSN tracking |
| `MySqlOffsetContext.java` | MySQL binlog position tracking |
| `DatetimeTypeConverter.java` | Type conversion for dates |
| `OpendalSchemaHistory.java` | Schema history storage |

## 5. Edit Rules (Must)

- Follow Debezium extension patterns for custom behavior
- Handle all database-specific edge cases
- Implement proper offset serialization
- Support exactly-once delivery semantics
- Document database version compatibility
- Add tests for each connector type
- Handle schema changes gracefully
- Use proper transaction boundary detection
- Implement restart recovery correctly

## 6. Forbidden Changes (Must Not)

- Modify Debezium core classes without justification
- Skip offset persistence
- Break existing CDC connector compatibility
- Remove support for existing database versions
- Use blocking calls in event processing

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-source-cdc` |
| CDC tests | Run `risingwave-source-cdc-test` module |
| Integration | Test with actual databases |

## 8. Dependencies & Contracts

- Debezium 2.x API
- MySQL binlog connector
- PostgreSQL JDBC driver
- MongoDB driver
- Jackson for serialization

## 9. Overrides

Inherits from `/home/k11/risingwave/java/connector-node/AGENTS.md`:
- Override: CDC-specific patterns
- Override: Debezium extension guidelines

## 10. Update Triggers

Regenerate this file when:
- Debezium version upgrades
- New CDC sources are added
- Offset management changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/connector-node/AGENTS.md

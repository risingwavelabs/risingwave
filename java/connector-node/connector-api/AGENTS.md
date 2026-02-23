# AGENTS.md - Connector API

## 1. Scope

Policies for the connector-api module, defining the core interfaces and contracts for all RisingWave connectors.

## 2. Purpose

The connector-api module provides the foundational interfaces that all connector implementations must follow. It defines contracts for sink writers, sink coordinators, deserializers, and CDC sources. This module ensures consistency across different connector implementations.

## 3. Structure

```
connector-api/
├── src/main/java/com/risingwave/connector/api/
│   ├── TableSchema.java            # Table schema definition
│   ├── ColumnDesc.java             # Column descriptor
│   ├── PkComparator.java           # Primary key comparator
│   ├── Monitor.java                # Monitoring interface
│   ├── sink/
│   │   ├── SinkFactory.java        # Factory for creating sinks
│   │   ├── SinkWriter.java         # Sink writer interface
│   │   ├── SinkWriterV1.java       # Legacy sink writer
│   │   ├── SinkWriterBase.java     # Base writer implementation
│   │   ├── SinkCoordinator.java    # Coordinator interface
│   │   ├── SinkRow.java            # Row abstraction
│   │   ├── ArraySinkRow.java       # Array-based row impl
│   │   ├── CommonSinkConfig.java   # Common configuration
│   │   ├── Deserializer.java       # Deserializer interface
│   │   ├── CloseableIterable.java  # Closeable iterator
│   │   └── TrivialCloseIterable.java   # Simple iterable
│   └── source/
│       ├── SourceTypeE.java        # Source type enum
│       ├── SourceConfig.java       # Source configuration
│       ├── SourceHandler.java      # Source handler interface
│       ├── CdcEngine.java          # CDC engine interface
│       └── CdcEngineRunner.java    # CDC runner interface
└── pom.xml                         # Maven configuration
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `SinkWriter.java` | Core interface for sink implementations |
| `SinkFactory.java` | Factory pattern for sink creation |
| `SinkRow.java` | Abstraction for data rows |
| `TableSchema.java` | Schema definition for tables |
| `CdcEngine.java` | Interface for CDC engines |

## 5. Edit Rules (Must)

- Maintain backward compatibility for all public interfaces
- Use Java 11+ features appropriately
- Document all interface methods with Javadoc
- Use Immutable data structures where possible
- Define clear contracts for null handling
- Version interface changes appropriately
- Keep interfaces focused and cohesive
- Use generics for type safety
- Define default methods for backward compatibility

## 6. Forbidden Changes (Must Not)

- Break backward compatibility without versioning
- Remove existing interface methods
- Add mandatory parameters to existing methods
- Change method signatures without deprecation
- Use implementation-specific types in interfaces
- Expose internal state through interfaces

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Compile check | `mvn compile -pl connector-api` |
| Unit tests | `mvn test -pl connector-api` |

## 8. Dependencies & Contracts

- Jackson for JSON processing
- SLF4J for logging contracts
- Protobuf for data serialization
- Java 11+ language features

## 9. Overrides

Inherits from `/home/k11/risingwave/java/connector-node/AGENTS.md`:
- Override: Interface definition patterns
- Override: API versioning rules

## 10. Update Triggers

Regenerate this file when:
- New connector interfaces are added
- Existing interfaces are modified
- API versioning strategy changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/connector-node/AGENTS.md

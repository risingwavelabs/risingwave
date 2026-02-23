# AGENTS.md - Connector Service

## 1. Scope

Policies for the risingwave-connector-service module, the main gRPC service implementation for RisingWave Connector Node.

## 2. Purpose

The connector service is the core gRPC server that handles all connector-related requests from RisingWave. It manages sink writers, sink coordinators, source validation, and CDC engine lifecycle. This service acts as the bridge between RisingWave's Rust core and Java-based connectors.

## 3. Structure

```
risingwave-connector-service/
├── src/main/java/com/risingwave/connector/
│   ├── ConnectorService.java           # Service entry point
│   ├── ConnectorServiceImpl.java       # gRPC service implementation
│   ├── SinkWriterStreamObserver.java   # Sink write request handler
│   ├── SinkCoordinatorStreamObserver.java  # Coordinator handler
│   ├── JniSinkWriterHandler.java       # JNI bridge for sink writers
│   ├── JniSinkCoordinatorHandler.java  # JNI bridge for coordinators
│   ├── SinkValidationHandler.java      # Sink config validation
│   ├── FileSink.java                   # File-based sink implementation
│   ├── FileSinkConfig.java             # File sink configuration
│   ├── FileSinkFactory.java            # File sink factory
│   ├── deserializer/
│   │   └── StreamChunkDeserializer.java    # Stream chunk parsing
│   └── source/
│       ├── SourceRequestHandler.java   # Source request router
│       ├── SourceValidateHandler.java  # Source validation logic
│       ├── JniSourceValidateHandler.java   # JNI validation bridge
│       ├── core/
│       │   ├── DbzCdcEngine.java       # Debezium CDC engine
│       │   ├── DbzCdcEngineRunner.java # CDC engine runner
│       │   ├── DbzChangeEventConsumer.java   # Change event processor
│       │   ├── DbzSourceHandler.java   # Source handler base
│       │   ├── JniDbzSourceHandler.java    # JNI source bridge
│       │   └── JniDbzSourceRegistry.java   # Source registry
│       └── common/
│           ├── DatabaseValidator.java  # Base validator
│           ├── PostgresValidator.java   # PostgreSQL validation
│           ├── MySqlValidator.java      # MySQL validation
│           ├── SqlServerValidator.java  # SQL Server validation
│           ├── MongoDbValidator.java    # MongoDB validation
│           ├── CitusValidator.java      # Citus validation
│           ├── DbzConnectorConfig.java  # Debezium configuration
│           └── DbzSourceUtils.java      # Source utilities
├── src/test/java/                      # Unit tests
└── pom.xml                             # Maven configuration
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `ConnectorServiceImpl.java` | Main gRPC service implementation |
| `SinkWriterStreamObserver.java` | Handles sink write streaming |
| `JniSinkWriterHandler.java` | JNI bridge for Rust-RisingWave integration |
| `DbzCdcEngine.java` | Debezium CDC engine wrapper |
| `PostgresValidator.java` | PostgreSQL source validation |
| `StreamChunkDeserializer.java` | Deserializes stream chunks from Rust |

## 5. Edit Rules (Must)

- Implement proper gRPC error handling with appropriate status codes
- Use try-with-resources for all AutoCloseable resources
- Validate all configuration parameters before processing
- Log all errors with appropriate context using SLF4J
- Implement graceful shutdown for all long-running operations
- Use connection pooling for database connections
- Handle backpressure from RisingWave with flow control
- Add metrics for all major operations (Prometheus)
- Document JNI method signatures in comments
- Follow existing patterns for sink/source handlers
- Use `CloseableIterable` for streaming data
- Implement proper retry logic with exponential backoff

## 6. Forbidden Changes (Must Not)

- Modify gRPC service definitions without updating protobuf
- Use blocking I/O on gRPC threads without proper executor
- Remove existing metrics or monitoring
- Break backward compatibility in JNI interfaces
- Skip validation for source/sink configurations
- Use System.out.println instead of proper logging
- Create memory leaks in JNI code (ensure proper cleanup)
- Ignore InterruptedException without proper handling

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-connector-service` |
| Integration | `cd python-client && python integration_tests.py` |
| Full build | `mvn clean package` |
| Spotless check | `mvn spotless:check -pl risingwave-connector-service` |

## 8. Dependencies & Contracts

- gRPC 1.58+ for service communication
- Debezium 2.x for CDC connectors
- connector-api for interface contracts
- Java binding for JNI integration
- Prometheus for metrics
- SLF4J for logging
- Jackson for JSON processing

## 9. Overrides

Inherits from `/home/k11/risingwave/java/connector-node/AGENTS.md`:
- Override: Service-specific patterns and JNI conventions
- Override: gRPC streaming handler patterns

## 10. Update Triggers

Regenerate this file when:
- gRPC service definitions change
- JNI interface contracts change
- CDC engine architecture changes
- New validator types are added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/connector-node/AGENTS.md

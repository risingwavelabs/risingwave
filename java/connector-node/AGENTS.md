# AGENTS.md - Connector Node

## 1. Scope

Policies for the java/connector-node directory, containing the RisingWave Connector Node service and its connectors.

## 2. Purpose

The connector-node module provides a gRPC-based service that enables RisingWave to connect with external data sources and sinks. It includes implementations for JDBC sinks, CDC sources (Debezium), Apache Iceberg, Elasticsearch, Cassandra, and more. The connector node acts as a bridge between RisingWave and external systems.

## 3. Structure

```
connector-node/
├── assembly/                        # Distribution packaging
├── connector-api/                   # Connector interface definitions
├── python-client/                   # Python client for testing
├── risingwave-connector-service/    # Core gRPC service
├── risingwave-connector-test/       # Test utilities
├── risingwave-jdbc-runner/          # JDBC execution utilities
├── risingwave-sink-cassandra/       # Cassandra sink connector
├── risingwave-sink-deltalake/       # Delta Lake sink
├── risingwave-sink-es-7/            # Elasticsearch 7.x sink
├── risingwave-sink-iceberg/         # Apache Iceberg sink
├── risingwave-sink-jdbc/            # Generic JDBC sink
├── risingwave-sink-mock-flink/      # Flink-compatible test sink
├── risingwave-source-cdc/           # CDC source (Debezium)
├── risingwave-source-cdc-test/      # CDC connector tests
├── risingwave-source-test/          # Source connector tests
├── s3-common/                       # S3/shared storage utilities
└── tracing/                         # Distributed tracing support
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `README.md` | Connector node documentation |
| `risingwave-connector-service/` | Main gRPC service implementation |
| `connector-api/` | Connector interface contracts |
| `risingwave-sink-jdbc/` | JDBC sink implementation |
| `risingwave-source-cdc/` | CDC source with Debezium |
| `assembly/` | Distribution tar.gz packaging |

## 5. Edit Rules (Must)

- Implement connector-api interfaces for new connectors
- Add integration tests for each connector type
- Document connector configuration options
- Use connection pooling for database connectors
- Handle backpressure from RisingWave properly
- Implement proper error handling with gRPC status codes
- Add metrics for connector operations
- Follow existing sink/source patterns
- Validate configuration parameters on initialization
- Support exactly-once semantics where applicable

## 6. Forbidden Changes (Must Not)

- Break connector API backward compatibility
- Remove existing connector implementations
- Add external dependencies without license review
- Skip integration tests for connector changes
- Use blocking I/O without proper thread management
- Hardcode credentials or secrets

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test` |
| Integration tests | `cd python-client && bash integration_tests.py` |
| Build package | `mvn clean package` |
| Docker build | `docker build -t connector-node .` |

## 8. Dependencies & Contracts

- Maven 3.8+ for builds
- Java 11+ runtime
- gRPC for service communication
- Debezium 2.x for CDC connectors
- JDBC 4.2+ for database sinks
- Apache Iceberg for lakehouse sink
- Elasticsearch 7.x client
- Cassandra driver
- Testcontainers for integration tests

## 9. Overrides

Inherits from `./java/AGENTS.md`:
- Override: Connector-specific development patterns
- Override: gRPC service implementation rules

## 10. Update Triggers

Regenerate this file when:
- New connector types are added
- Connector API changes
- gRPC service definitions change
- Build/packaging process changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/AGENTS.md

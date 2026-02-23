# AGENTS.md - Elasticsearch 7 Sink

## 1. Scope

Policies for the `java/connector-node/risingwave-sink-es-7` directory, covering the Elasticsearch 7.x and OpenSearch sink connector implementation.

## 2. Purpose

The Elasticsearch 7 Sink module provides a high-performance sink connector that writes data from RisingWave to Elasticsearch 7.x clusters and OpenSearch-compatible endpoints. It supports both Elasticsearch and OpenSearch clients, bulk ingestion with configurable batching, and primary key-based upsert operations.

## 3. Structure

```
risingwave-sink-es-7/
├── pom.xml                      # Maven module configuration
├── src/
│   ├── main/java/com/risingwave/connector/
│   │   ├── EsSink.java          # Main sink implementation
│   │   ├── EsSinkFactory.java   # Sink factory for instantiation
│   │   ├── BulkListener.java    # Bulk request callback handler
│   │   ├── BulkProcessorAdapter.java       # ES bulk processor wrapper
│   │   └── OpensearchBulkProcessorAdapter.java  # OpenSearch adapter
│   └── test/java/               # Unit tests with Testcontainers
└── target/                      # Build artifacts
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `EsSink.java` | Core sink implementation with bulk processing |
| `EsSinkFactory.java` | Factory for creating ES sink instances |
| `BulkProcessorAdapter.java` | Adapter for Elasticsearch bulk processor |
| `OpensearchBulkProcessorAdapter.java` | Adapter for OpenSearch bulk processor |
| `BulkListener.java` | Callback listener for bulk operation results |
| `pom.xml` | Dependencies for ES/OpenSearch clients |

## 5. Edit Rules (Must)

- Implement proper error handling with gRPC Status codes
- Support both Elasticsearch 7.x and OpenSearch clients
- Use bulk processor for efficient batch ingestion
- Handle backpressure from the bulk processor
- Validate index configuration on initialization
- Support primary key-based upsert operations
- Add comprehensive unit tests with Testcontainers
- Document all configuration parameters
- Follow connector-api interface contracts
- Use connection pooling for HTTP clients

## 6. Forbidden Changes (Must Not)

- Remove support for either ES or OpenSearch clients
- Break bulk processor configuration compatibility
- Skip error handling for failed bulk operations
- Hardcode index names or cluster endpoints
- Remove Testcontainers integration tests
- Use blocking I/O without proper timeout handling
- Break backward compatibility in sink configuration

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-sink-es-7` |
| Integration tests | Via Testcontainers in test sources |
| Checkstyle | `mvn checkstyle:check -pl risingwave-sink-es-7` |
| Build | `mvn clean package -pl risingwave-sink-es-7` |

## 8. Dependencies & Contracts

- Maven 3.8+ for builds
- Java 11+ runtime
- Elasticsearch 7.x REST high-level client
- OpenSearch REST high-level client
- Apache HttpClient for HTTP transport
- Jackson for JSON processing
- Testcontainers for integration tests
- connector-api for sink interface contracts

## 9. Overrides

Inherits from `./java/connector-node/AGENTS.md`:
- Override: Sink-specific implementation patterns
- Override: ES/OpenSearch client configurations

## 10. Update Triggers

Regenerate this file when:
- Sink implementation architecture changes
- New ES/OpenSearch client versions are adopted
- Bulk processing logic is modified
- Configuration parameters change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/connector-node/AGENTS.md

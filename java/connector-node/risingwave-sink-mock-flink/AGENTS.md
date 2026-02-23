# AGENTS.md - Mock Flink Sink

## 1. Scope

Policies for the `java/connector-node/risingwave-sink-mock-flink` directory, covering the Flink-compatible mock sink implementation for testing.

## 2. Purpose

The Mock Flink Sink module provides a test-only sink implementation that mimics Flink's sink interface for integration testing and development purposes. It enables testing of Flink-compatible connector patterns without requiring a full Flink runtime environment.

## 3. Structure

```
risingwave-sink-mock-flink/
├── risingwave-sink-mock-flink-common/      # Shared components and utilities
│   └── target/
├── risingwave-sink-mock-flink-http-sink/   # HTTP-based mock sink implementation
│   └── target/
├── risingwave-sink-mock-flink-runtime/     # Runtime components for mock execution
│   └── target/
└── (No parent pom.xml - referenced by assembly)
```

## 4. Key Files

| File/Directory | Purpose |
|----------------|---------|
| `risingwave-sink-mock-flink-common/` | Shared interfaces and utilities |
| `risingwave-sink-mock-flink-http-sink/` | HTTP sink implementation |
| `risingwave-sink-mock-flink-runtime/` | Runtime execution components |
| `assembly.xml` reference | Included in distribution assembly |

## 5. Edit Rules (Must)

- Implement Flink-compatible sink interfaces
- Provide HTTP endpoint for test data verification
- Support configurable sink behavior for testing
- Document mock behavior differences from real Flink
- Keep test-only dependencies separate from production
- Follow existing mock sink patterns
- Add unit tests for mock components

## 6. Forbidden Changes (Must Not)

- Use mock sink in production deployments
- Remove HTTP sink verification capability
- Add production dependencies to mock modules
- Break Flink interface compatibility
- Remove from assembly distribution without approval

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-sink-mock-flink-common,risingwave-sink-mock-flink-http-sink,risingwave-sink-mock-flink-runtime` |
| Build | `mvn clean package` (from parent) |
| Integration | Via connector-node python-client tests |

## 8. Dependencies & Contracts

- Maven 3.8+ for builds
- Java 11+ runtime
- Flink sink interfaces (compatible versions)
- HTTP server components for test endpoints
- connector-api for integration
- Referenced by assembly module for packaging

## 9. Overrides

Inherits from `/home/k11/risingwave/java/connector-node/AGENTS.md`:
- Override: Test-only module restrictions
- Override: Flink interface compatibility requirements

## 10. Update Triggers

Regenerate this file when:
- Mock sink architecture changes
- Flink version compatibility updates
- New mock sink types are added
- Assembly packaging changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/connector-node/AGENTS.md

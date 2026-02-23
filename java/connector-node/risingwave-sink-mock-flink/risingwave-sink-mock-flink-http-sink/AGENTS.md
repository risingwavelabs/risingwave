# AGENTS.md - Mock Flink HTTP Sink

## 1. Scope

Policies for the `risingwave-sink-mock-flink-http-sink` module, providing an HTTP-based mock sink implementation for testing and development purposes.

## 2. Purpose

The HTTP Sink module implements a mock Flink-compatible sink that exposes an HTTP endpoint for receiving and verifying test data. It enables integration testing of sink functionality without requiring external data stores, allowing developers to verify data output through HTTP requests. This sink is included in the assembly distribution for testing scenarios.

## 3. Structure

```
risingwave-sink-mock-flink-http-sink/
├── src/main/java/com/risingwave/mock/flink/http/
│   └── HttpFlinkMockSinkFactory.java       # HTTP sink factory implementation
├── src/test/java/                          # Unit tests
└── target/                                 # Build artifacts
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `HttpFlinkMockSinkFactory.java` | Factory for creating HTTP-based mock sinks |
| `target/classes/` | Compiled class output directory |
| Assembly reference | Included in distribution via assembly/assembly.xml |

## 5. Edit Rules (Must)

- Implement HTTP endpoint for test data reception
- Support configurable port and endpoint paths
- Provide clear HTTP response codes for test verification
- Document expected request/response formats
- Handle concurrent HTTP requests safely
- Implement proper resource cleanup
- Log all HTTP operations for debugging
- Follow existing sink factory patterns
- Support configurable batch sizes for testing
- Add health check endpoints

## 6. Forbidden Changes (Must Not)

- Use production authentication mechanisms
- Add persistent storage dependencies
- Remove HTTP endpoint verification capability
- Break Flink sink interface contracts
- Skip error handling for HTTP operations
- Hardcode network configuration
- Add production-only features
- Remove from assembly without approval

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-sink-mock-flink-http-sink` (from parent) |
| Build | `mvn clean package` (includes in assembly) |
| Integration | Via python-client integration_tests.py |
| HTTP endpoint test | Manual curl requests to exposed endpoints |

## 8. Dependencies & Contracts

- Java 11+ runtime
- HTTP server components (embedded)
- risingwave-sink-mock-flink-common for shared utilities
- risingwave-sink-mock-flink-runtime for execution context
- Flink sink interfaces for compatibility
- connector-api for integration
- Included in assembly distribution as test component

## 9. Overrides

Inherits from `/home/k11/risingwave/java/connector-node/risingwave-sink-mock-flink/AGENTS.md`:
- Override: HTTP-specific implementation rules
- Override: Test-only endpoint security guidelines
- Override: Assembly inclusion requirements

## 10. Update Triggers

Regenerate this file when:
- HTTP endpoint behavior changes
- New HTTP methods or paths are added
- Assembly packaging configuration changes
- Flink sink interface updates
- HTTP server component changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/connector-node/risingwave-sink-mock-flink/AGENTS.md

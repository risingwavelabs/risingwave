# AGENTS.md - Mock Flink Sink Common Module

## 1. Scope

Policies for the `risingwave-sink-mock-flink-common` module, containing shared components and utilities for the Flink-compatible mock sink implementation.

## 2. Purpose

The Common module provides shared interfaces, utilities, and adapter classes used by both the HTTP sink and runtime components. It defines the core abstractions that enable Flink-compatible sink behavior without requiring a full Flink runtime environment. This module serves as the foundation for mock sink implementations.

## 3. Structure

```
risingwave-sink-mock-flink-common/
├── src/main/java/com/risingwave/mock/flink/common/
│   ├── FlinkDynamicAdapterConfig.java      # Dynamic table configuration adapter
│   ├── FlinkDynamicUtil.java               # Flink dynamic table utilities
│   └── FlinkMockSinkFactory.java           # Factory for creating mock sinks
├── src/test/java/                          # Unit tests
└── target/                                 # Build artifacts
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `FlinkDynamicAdapterConfig.java` | Adapts RisingWave configurations to Flink dynamic table format |
| `FlinkDynamicUtil.java` | Utility methods for Flink dynamic table operations |
| `FlinkMockSinkFactory.java` | Factory pattern implementation for mock sink instances |
| `target/classes/` | Compiled class output directory |

## 5. Edit Rules (Must)

- Maintain Flink interface compatibility in all adapter classes
- Document mock behavior differences from real Flink in comments
- Keep dependencies minimal and test-only
- Follow factory pattern for sink creation
- Use immutable configuration objects
- Add Javadoc for all public APIs
- Run `mvn spotless:apply` before committing changes
- Add unit tests for utility classes
- Ensure thread-safety for shared components
- Validate configuration parameters in adapter constructors

## 6. Forbidden Changes (Must Not)

- Add production dependencies to this test-only module
- Break Flink interface compatibility
- Remove existing adapter methods without deprecation
- Use static mutable state in utility classes
- Hardcode configuration values
- Skip checkstyle validation
- Commit IDE-generated files or local configurations

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-sink-mock-flink-common` (from parent) |
| Build | Compiled as part of connector-node assembly |
| Integration | Via python-client integration_tests.py |

## 8. Dependencies & Contracts

- Java 11+ runtime
- Flink table API interfaces (compatible versions)
- connector-api for integration contracts
- No external production dependencies allowed
- Test-only scope for all dependencies
- Referenced by risingwave-sink-mock-flink-runtime and risingwave-sink-mock-flink-http-sink

## 9. Overrides

Inherits from `./java/connector-node/risingwave-sink-mock-flink/AGENTS.md`:
- Override: Test-only module restrictions
- Override: Flink interface compatibility requirements
- Override: Shared component maintenance rules

## 10. Update Triggers

Regenerate this file when:
- Mock sink architecture changes
- Flink version compatibility updates
- New shared components are added
- Factory pattern implementation changes
- Configuration adapter behavior changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/connector-node/risingwave-sink-mock-flink/AGENTS.md

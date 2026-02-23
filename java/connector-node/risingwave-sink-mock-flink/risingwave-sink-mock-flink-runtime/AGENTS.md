# AGENTS.md - Mock Flink Sink Runtime

## 1. Scope

Policies for the `risingwave-sink-mock-flink-runtime` module, providing runtime components and context implementations for the Flink-compatible mock sink execution.

## 2. Purpose

The Runtime module implements the execution environment and context classes required to run Flink-compatible sinks without a full Flink cluster. It provides mock implementations of Flink runtime interfaces including SinkWriterContext, ProcessingTimeService, and metric groups. This module enables testing of sink logic in a lightweight environment while maintaining API compatibility with Flink.

## 3. Structure

```
risingwave-sink-mock-flink-runtime/
├── src/main/java/com/risingwave/mock/flink/runtime/
│   ├── FlinkDynamicAdapterFactory.java     # Dynamic table adapter factory
│   ├── RowDataImpl.java                    # Row data implementation
│   ├── context/
│   │   ├── DynamicTableSinkContextImpl.java    # Dynamic table sink context
│   │   ├── MailBoxExecImpl.java               # Mailbox execution implementation
│   │   ├── SinkWriterContext.java             # Sink writer context (v1)
│   │   ├── SinkWriterContextV2.java           # Sink writer context (v2)
│   │   └── SinkWriterMetircGroupImpl.java     # Metrics group implementation
│   └── sinkwriter/
│       ├── AsyncSinkWriterImpl.java       # Async sink writer
│       ├── CommitRequestImpl.java         # Commit request handling
│       ├── SinkWriterImpl.java            # Sink writer implementation
│       └── SinkWriterV2Impl.java          # Sink writer v2 implementation
├── src/test/java/                          # Unit tests
└── target/                                 # Build artifacts
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `SinkWriterContext.java` | Mock implementation of Flink SinkWriterContext |
| `SinkWriterContextV2.java` | Version 2 context with enhanced features |
| `SinkWriterImpl.java` | Core sink writer implementation |
| `SinkWriterV2Impl.java` | Version 2 sink writer with new APIs |
| `AsyncSinkWriterImpl.java` | Asynchronous sink writer support |
| `RowDataImpl.java` | RowData interface implementation |
| `MailBoxExecImpl.java` | Mailbox-based execution model |

## 5. Edit Rules (Must)

- Implement all Flink runtime interface methods
- Provide no-op or mock implementations for unused features
- Document deviations from real Flink behavior
- Support both Sink V1 and Sink V2 APIs
- Implement proper lifecycle management (open/close)
- Handle processing time callbacks correctly
- Provide metric collection hooks
- Ensure thread-safety for concurrent operations
- Support checkpointing simulation
- Add comprehensive Javadoc for context classes

## 6. Forbidden Changes (Must Not)

- Remove support for Sink V1 APIs without deprecation
- Break Flink runtime interface contracts
- Add real Flink cluster dependencies
- Skip context lifecycle methods
- Use blocking operations without timeouts
- Remove metric group implementations
- Break backward compatibility in public APIs

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-sink-mock-flink-runtime` (from parent) |
| Build | Compiled as part of connector-node assembly |
| Integration | Via python-client integration_tests.py |
| Runtime test | Test sink execution with mock runtime |

## 8. Dependencies & Contracts

- Java 11+ runtime
- Flink runtime interfaces (Sink V1 and V2)
- risingwave-sink-mock-flink-common for shared utilities
- connector-api for integration contracts
- Testcontainers for integration testing (optional)
- Supports async and sync sink writer patterns

## 9. Overrides

Inherits from `/home/k11/risingwave/java/connector-node/risingwave-sink-mock-flink/AGENTS.md`:
- Override: Runtime context implementation rules
- Override: Sink V1/V2 compatibility requirements
- Override: Lifecycle management patterns

## 10. Update Triggers

Regenerate this file when:
- Flink runtime interface changes
- New sink writer APIs are added
- Context implementation patterns change
- Lifecycle management updates
- Sink V2 API evolution

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/connector-node/risingwave-sink-mock-flink/AGENTS.md

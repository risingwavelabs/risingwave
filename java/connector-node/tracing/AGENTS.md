# AGENTS.md - Tracing Support

## 1. Scope

Policies for the tracing module, providing distributed tracing integration for connectors.

## 2. Purpose

The tracing module provides SLF4J integration with distributed tracing capabilities. It allows connectors to emit trace information for debugging and monitoring in distributed environments.

## 3. Structure

```
tracing/
└── src/main/java/com/risingwave/tracing/
    ├── TracingSlf4jImpl.java           # SLF4J implementation
    ├── TracingSlf4jAdapter.java        # Adapter for tracing
    ├── TracingSlf4jLoggerFactory.java  # Logger factory
    └── TracingSlf4jServiceProvider.java    # Service provider
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `TracingSlf4jImpl.java` | Main SLF4J implementation |
| `TracingSlf4jAdapter.java` | Tracing adapter |
| `TracingSlf4jLoggerFactory.java` | Logger factory |
| `TracingSlf4jServiceProvider.java` | Service provider |

## 5. Edit Rules (Must)

- Follow SLF4J provider contract
- Support trace context propagation
- Handle async tracing properly
- Implement proper log levels
- Support MDC (Mapped Diagnostic Context)

## 6. Forbidden Changes (Must Not)

- Break SLF4J compatibility
- Remove tracing context handling
- Skip log level checks

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl tracing` |

## 8. Dependencies & Contracts

- SLF4J API
- Log4j Core
- Java Binding for JNI

## 9. Overrides

Inherits from `./java/connector-node/AGENTS.md`:
- Override: Tracing-specific patterns

## 10. Update Triggers

Regenerate this file when:
- Tracing framework changes
- SLF4J version upgrades

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/connector-node/AGENTS.md

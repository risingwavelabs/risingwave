# AGENTS.md - Java Components

## 1. Scope

Policies for the java directory, covering Java-based components including the Connector Node, Java bindings, and integration tools.

## 2. Purpose

The java module provides Java-based components for RisingWave integration. This includes the Connector Node for external data source/sink connectivity, Java language bindings for embedded use, and various utility tools for Java ecosystem integration.

## 3. Structure

```
java/
├── connector-node/              # Main connector service
│   ├── risingwave-connector-service/  # Core connector service
│   ├── connector-api/           # Connector API definitions
│   ├── risingwave-sink-jdbc/    # JDBC sink connector
│   ├── risingwave-sink-iceberg/ # Apache Iceberg sink
│   ├── risingwave-sink-deltalake/ # Delta Lake sink
│   ├── risingwave-sink-es-7/    # Elasticsearch sink
│   ├── risingwave-sink-cassandra/ # Cassandra sink
│   ├── risingwave-source-cdc/   # CDC source connector
│   ├── risingwave-source-cdc-test/  # CDC tests
│   ├── risingwave-jdbc-runner/  # JDBC execution utilities
│   ├── python-client/           # Python client for testing
│   ├── assembly/                # Distribution assembly
│   └── tracing/                 # Distributed tracing support
├── java-binding/                # Java native bindings
├── java-binding-benchmark/      # Binding performance tests
├── java-binding-integration-test/  # Integration tests
├── common-utils/                # Shared utility classes
├── tools/                       # Development tools
│   └── maven/                   # Maven configurations
├── proto/                       # Protocol buffer definitions
├── pom.xml                      # Maven parent POM
├── dev.md                       # Developer documentation
└── com_risingwave_java_binding_Binding.h  # JNI header
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `pom.xml` | Maven project configuration |
| `connector-node/README.md` | Connector Node documentation |
| `dev.md` | Developer setup guide |
| `tools/maven/checkstyle.xml` | Code style configuration |
| `rust-toolchain` | Rust version for JNI builds |

## 5. Edit Rules (Must)

- Run `mvn spotless:apply` before committing Java code
- Follow Google Java Format (AOSP style)
- Add unit tests for new connector implementations
- Document all public APIs with Javadoc
- Update checkstyle configuration if style rules change
- Ensure JNI code is properly memory-managed
- Run integration tests for connector changes
- Keep Maven dependencies up to date

## 6. Forbidden Changes (Must Not)

- Modify JNI bindings without memory safety review
- Remove connector implementations without deprecation
- Add external dependencies without license review
- Break backward compatibility in public APIs
- Commit build artifacts or target/ directories
- Modify generated protobuf code manually

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test` |
| Integration tests | `mvn integration-test` |
| Spotless check | `mvn spotless:check` |
| Build package | `mvn clean package` |
| Full build | `mvn clean package -DskipTests` |

## 8. Dependencies & Contracts

- Maven 3.8+
- Java 11+
- Rust toolchain (for JNI compilation)
- Protocol Buffers
- JDBC drivers for supported databases
- External systems (PostgreSQL, Kafka, Elasticsearch, etc.)
- gRPC for connector service communication

## 9. Overrides

Inherits from `./AGENTS.md`:
- Override: Edit Rules - Maven and Java-specific requirements
- Override: Test Entry - Maven-based testing workflow
- Override: Dependencies - Java ecosystem

## 10. Update Triggers

Regenerate this file when:
- Connector architecture changes
- New connector types are added
- Build system changes (Maven configurations)
- JNI interface changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

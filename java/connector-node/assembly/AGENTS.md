# AGENTS.md - Assembly

## 1. Scope

Policies for the `java/connector-node/assembly` directory, covering the Maven assembly configuration for connector node distribution packaging.

## 2. Purpose

The Assembly module defines the distribution packaging for the RisingWave Connector Node. It aggregates all connector implementations, service components, and dependencies into a deployable tar.gz archive with proper directory structure, startup scripts, and manifest configuration.

## 3. Structure

```
assembly/
├── pom.xml                      # Maven assembly plugin configuration
├── assembly.xml                 # Assembly descriptor for packaging
├── scripts/
│   └── start-service.sh         # Service startup script
└── target/                      # Build artifacts
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `pom.xml` | Maven assembly plugin configuration and dependencies |
| `assembly.xml` | Assembly descriptor defining package structure |
| `scripts/start-service.sh` | Bash startup script for connector service |
| `target/` | Build output including final tar.gz distribution |

## 5. Edit Rules (Must)

- Update assembly.xml when new connectors are added
- Maintain start-service.sh compatibility
- Include all required connector dependencies
- Set proper Main-Class in manifest
- Organize libs/ directory structure
- Include LICENSE and NOTICE files
- Validate assembly output before release
- Test startup script on target platforms
- Version the distribution package name

## 6. Forbidden Changes (Must Not)

- Remove existing connectors from assembly
- Break startup script argument handling
- Skip license file inclusion
- Hardcode version numbers in scripts
- Remove manifest classpath configuration
- Skip dependency verification

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Assembly build | `mvn clean package -pl assembly` |
| Verify contents | `tar -tzf target/risingwave-connector-*.tar.gz` |
| Test startup | `cd target && tar -xzf *.tar.gz && ./start-service.sh -h` |
| Full build | `mvn clean package` (from connector-node parent) |

## 8. Dependencies & Contracts

- Maven 3.8+ with assembly plugin 3.7+
- Java 11+ runtime
- Aggregates all connector modules:
  - risingwave-connector-service
  - risingwave-source-cdc
  - risingwave-sink-es-7
  - risingwave-sink-cassandra
  - risingwave-jdbc-runner
  - risingwave-sink-jdbc
  - risingwave-sink-iceberg
  - risingwave-sink-mock-flink-http-sink
  - s3-common
  - tracing

## 9. Overrides

Inherits from `./java/connector-node/AGENTS.md`:
- Override: Packaging and distribution rules
- Override: Assembly-specific build requirements

## 10. Update Triggers

Regenerate this file when:
- New connectors are added to distribution
- Assembly descriptor format changes
- Startup script modifications
- Package structure changes
- New dependency aggregation requirements

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/connector-node/AGENTS.md

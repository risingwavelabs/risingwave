# AGENTS.md - Iceberg Sink Connector

## 1. Scope

Policies for the risingwave-sink-iceberg module, implementing Apache Iceberg lakehouse sink connector.

## 2. Purpose

The Iceberg sink module provides a high-performance sink for writing data to Apache Iceberg tables. It supports multiple catalog implementations (Hive, REST, Glue), cloud storage (S3, GCS), and advanced features like partitioning and schema evolution.

## 3. Structure

```
risingwave-sink-iceberg/
├── src/main/java/
│   ├── com/risingwave/connector/
│   │   └── catalog/
│   │       ├── JniCatalogWrapper.java       # JNI catalog wrapper
│   │       ├── RESTObjectMapper.java        # REST catalog JSON
│   │       └── GlueCredentialProvider.java  # AWS Glue auth
│   └── org/apache/iceberg/
│       └── common/
│           ├── DynClasses.java              # Dynamic class loading
│           ├── DynConstructors.java         # Constructor access
│           ├── DynFields.java               # Field access
│           └── DynMethods.java              # Method access
└── pom.xml
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `JniCatalogWrapper.java` | JNI bridge for catalog operations |
| `RESTObjectMapper.java` | JSON handling for REST catalog |
| `GlueCredentialProvider.java` | AWS Glue credentials |
| `Dyn* classes` | Reflection utilities for Iceberg |

## 5. Edit Rules (Must)

- Support all Iceberg catalog types
- Handle schema evolution properly
- Implement proper partition pruning
- Support time-based partitioning
- Handle concurrent writes correctly
- Optimize for batch writes
- Support cloud storage authentication
- Handle large files with proper splitting
- Document supported Iceberg features

## 6. Forbidden Changes (Must Not)

- Break Iceberg format compatibility
- Remove support for existing catalog types
- Use reflection without fallback handling
- Skip schema validation
- Hardcode cloud credentials

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-sink-iceberg` |
| Integration | Test with MinIO/actual cloud storage |

## 8. Dependencies & Contracts

- Apache Iceberg 1.10.1
- Hadoop Common for storage
- AWS SDK for S3/Glue
- Google Cloud Storage
- PostgreSQL/MySQL for JDBC catalog

## 9. Overrides

Inherits from `./java/connector-node/AGENTS.md`:
- Override: Iceberg-specific patterns
- Override: Catalog integration patterns

## 10. Update Triggers

Regenerate this file when:
- Iceberg version upgrades
- New catalog types are added
- Cloud storage integration changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/connector-node/AGENTS.md

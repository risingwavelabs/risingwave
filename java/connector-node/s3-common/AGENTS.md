# AGENTS.md - S3 Common Utilities

## 1. Scope

Policies for the s3-common module, providing shared S3 and cloud storage utilities.

## 2. Purpose

The s3-common module provides shared utilities for interacting with S3-compatible storage services. It is used by Iceberg sink and other modules that require cloud storage access.

## 3. Structure

```
s3-common/
└── src/main/java/com/risingwave/connector/common/
    ├── S3Config.java     # S3 configuration
    └── S3Utils.java      # S3 utility functions
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `S3Config.java` | Configuration for S3 connections |
| `S3Utils.java` | Helper methods for S3 operations |

## 5. Edit Rules (Must)

- Support multiple S3-compatible services
- Handle authentication properly
- Support endpoint configuration
- Implement retry logic
- Handle region configuration
- Support path-style and virtual-hosted style

## 6. Forbidden Changes (Must Not)

- Hardcode credentials
- Remove support for S3-compatible services
- Skip endpoint validation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl s3-common` |

## 8. Dependencies & Contracts

- Hadoop Common 3.3.6
- Hadoop AWS
- Connector API

## 9. Overrides

Inherits from `/home/k11/risingwave/java/connector-node/AGENTS.md`:
- Override: Cloud storage patterns

## 10. Update Triggers

Regenerate this file when:
- New cloud providers are added
- Hadoop version upgrades

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/connector-node/AGENTS.md

# AGENTS.md - DeltaLake Sink Connector

## 1. Scope

Policies for the risingwave-sink-deltalake module, implementing Delta Lake sink connector.

## 2. Purpose

The Delta Lake sink module provides write capabilities to Delta Lake tables. It supports ACID transactions, schema evolution, and time travel features of the Delta Lake protocol.

## 3. Structure

```
risingwave-sink-deltalake/
└── target/           # Build output only (no source yet)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| (Module in development) | Future sink implementation |

## 5. Edit Rules (Must)

- Follow Delta Lake protocol specification
- Support concurrent writes with optimistic concurrency
- Handle schema evolution properly
- Implement proper transaction logs
- Support cloud storage backends
- Document supported Delta Lake versions

## 6. Forbidden Changes (Must Not)

- Break Delta Lake protocol compatibility
- Implement partial protocol support
- Skip transaction log validation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test -pl risingwave-sink-deltalake` |

## 8. Dependencies & Contracts

- Delta Lake libraries (TBD)
- Hadoop FileSystem API
- Cloud storage SDKs

## 9. Overrides

Inherits from `/home/k11/risingwave/java/connector-node/AGENTS.md`:
- Override: Delta Lake specific patterns

## 10. Update Triggers

Regenerate this file when:
- Module implementation begins
- Delta Lake version upgrades

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/connector-node/AGENTS.md

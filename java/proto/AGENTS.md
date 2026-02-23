# AGENTS.md - Java Protobuf

## 1. Scope

Policies for the java/proto directory, containing Java Protocol Buffer generated code from RisingWave proto definitions.

## 2. Purpose

The proto module contains Java classes generated from Protocol Buffer definitions shared between RisingWave's Rust backend and Java components. It provides the data structures and gRPC service interfaces used for communication between the connector node, Java bindings, and RisingWave core.

## 3. Structure

```
proto/
├── pom.xml                  # Maven configuration with protobuf plugin
├── target/                  # Build output with generated classes
│   └── generated-sources/   # Protobuf compiler output
└── (proto files are in ../../proto/)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `pom.xml` | Maven protobuf plugin configuration |
| `target/generated-sources/` | Generated Java protobuf classes |
| `../../proto/` | Source proto definitions (external) |

## 5. Edit Rules (Must)

- NEVER modify generated code manually
- Regenerate after proto definition changes
- Use `mvn clean compile` to regenerate
- Commit generated code when proto changes
- Follow protobuf best practices in source definitions
- Use proper protobuf package naming
- Keep proto definitions backward compatible
- Document breaking changes in proto definitions

## 6. Forbidden Changes (Must Not)

- Edit files in target/generated-sources/
- Modify generated Java classes manually
- Delete generated code without regenerating
- Add manual code to generated files
- Skip regeneration after proto changes

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Compile | `mvn clean compile` |
| Build | `mvn clean package` |
| Regenerate | Update proto files, then `mvn clean compile` |

## 8. Dependencies & Contracts

- Maven protobuf plugin
- Protocol Buffers 3.x
- gRPC Java for service definitions
- Source proto files in ../../proto/
- Used by all other Java modules

## 9. Overrides

Inherits from `/home/k11/risingwave/java/AGENTS.md`:
- Override: Code generation specific rules

## 10. Update Triggers

Regenerate this file when:
- Protobuf generation process changes
- Maven plugin versions change
- Proto source location changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/AGENTS.md

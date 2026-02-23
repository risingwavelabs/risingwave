# AGENTS.md - Common Utilities

## 1. Scope

Policies for the java/common-utils directory, containing shared utility classes used across RisingWave Java modules.

## 2. Purpose

The common-utils module provides shared utility classes, helper functions, and common data structures that are used by multiple RisingWave Java modules. It centralizes cross-cutting concerns like serialization, configuration parsing, logging utilities, and type conversions.

## 3. Structure

```
common-utils/
├── pom.xml                  # Maven module configuration
├── src/
│   └── main/
│       └── java/            # Java source files
│           └── com/risingwave/common/
│               └── (utility classes)
│   └── test/
│       └── java/            # Unit tests
└── target/                  # Build output
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `pom.xml` | Maven module configuration |
| `src/main/java/` | Shared utility classes |
| `src/test/java/` | Unit tests for utilities |

## 5. Edit Rules (Must)

- Keep utilities generic and reusable
- Document all public methods with JavaDoc
- Use null-safe operations and validate inputs
- Write unit tests for all utility functions
- Avoid external dependencies (keep module lightweight)
- Follow immutable design patterns where possible
- Use static methods for pure functions
- Provide both checked and unchecked exception variants
- Add `@Nullable`/`@NonNull` annotations
- Keep backward compatibility when modifying

## 6. Forbidden Changes (Must Not)

- Add module-specific logic (keep it generic)
- Introduce heavy external dependencies
- Break backward compatibility without deprecation
- Use System.out.println (use logging)
- Add mutable global state
- Skip unit tests for new utilities

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test` |
| Spotless check | `mvn spotless:check` |
| Build | `mvn clean package` |

## 8. Dependencies & Contracts

- Maven 3.8+
- Java 11+
- Minimal external dependencies
- Used by: java-binding, connector-node, proto modules

## 9. Overrides

Inherits from `/home/k11/risingwave/java/AGENTS.md`:
- Override: Utility class design patterns

## 10. Update Triggers

Regenerate this file when:
- New utility categories are added
- Module structure changes
- Common patterns are refactored

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/AGENTS.md

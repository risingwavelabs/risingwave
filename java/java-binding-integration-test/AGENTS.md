# AGENTS.md - Java Binding Integration Tests

## 1. Scope

Policies for the `java/java-binding-integration-test` directory, covering integration tests for Java language bindings.

## 2. Purpose

The Java Binding Integration Tests validate end-to-end functionality between Java applications and RisingWave's native storage layer through JNI. These tests verify data read operations, stream chunk processing, and utility functions work correctly across the Java-Rust boundary.

## 3. Structure

```
java/java-binding-integration-test/
├── pom.xml                    # Maven project configuration
├── src/
│   └── main/java/com/risingwave/java/binding/
│       ├── Utils.java         # Utility functions test
│       ├── HummockReadDemo.java   # Hummock storage read tests
│       └── StreamChunkDemo.java   # Stream chunk processing tests
└── target/                    # Build output (gitignored)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `pom.xml` | Maven dependencies and build configuration |
| `src/main/java/.../Utils.java` | JNI utility function tests |
| `src/main/java/.../HummockReadDemo.java` | Hummock storage read integration |
| `src/main/java/.../StreamChunkDemo.java` | Stream chunk data processing tests |

## 5. Edit Rules (Must)

- Run `mvn spotless:apply` before committing
- Follow Google Java Format (AOSP style)
- Add integration tests for new JNI bindings
- Document JNI method signatures with comments
- Handle native resource cleanup in finally blocks
- Test both happy path and error scenarios
- Ensure tests clean up native resources

## 6. Forbidden Changes (Must Not)

- Skip resource cleanup in native code tests
- Modify JNI signatures without java-binding changes
- Add tests that require manual environment setup
- Commit build artifacts or target/ directory
- Hardcode test data paths

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Build integration tests | `mvn install --pl java-binding-integration-test --am` |
| Run integration tests | `java -cp "..." com.risingwave.java.binding.HummockReadDemo` |
| Full test suite | `mvn test -pl java-binding-integration-test` |
| Spotless check | `mvn spotless:check -pl java-binding-integration-test` |

## 8. Dependencies & Contracts

- Java 11+
- Maven 3.8+
- risingwave-java-binding (JNI native library)
- risingwave-proto (protobuf definitions)
- risingwave-common-utils (shared utilities)
- Native library loading via JNA/JNI

## 9. Overrides

Inherits from `/home/k11/risingwave/java/AGENTS.md`:
- Override: Edit Rules - JNI-specific testing requirements
- Override: Test Entry - Integration test commands

## 10. Update Triggers

Regenerate this file when:
- New JNI integration tests are added
- Java binding API changes
- Integration test framework changes
- New demo or example classes added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/AGENTS.md

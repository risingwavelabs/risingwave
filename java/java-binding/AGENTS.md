# AGENTS.md - Java Binding

## 1. Scope

Policies for the java/java-binding directory, containing JNI-based Java bindings for RisingWave native functionality.

## 2. Purpose

The java-binding module provides Java Native Interface (JNI) bindings that allow Java applications to interact directly with RisingWave's Rust-based storage and compute engines. It enables embedded usage of RisingWave within Java applications with efficient native performance.

## 3. Structure

```
java-binding/
├── pom.xml                  # Maven module configuration
├── src/
│   └── main/
│       └── java/            # Java source files
│           └── com/risingwave/java/binding/
│               └── (JNI wrapper classes)
│   └── test/
│       └── java/            # Unit tests
└── target/                  # Build output
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `pom.xml` | Maven configuration with rust-maven-plugin |
| `src/main/java/` | Java JNI wrapper classes |
| `src/test/java/` | Unit tests for bindings |

## 5. Edit Rules (Must)

- Follow JNI naming conventions for native methods
- Use try-with-resources for native resource management
- Document memory ownership in JavaDoc comments
- Handle JNI exceptions properly in native code
- Run `mvn spotless:apply` before committing
- Test with actual RisingWave cluster for integration
- Use ByteBuffer for zero-copy data transfer when possible
- Validate all native pointers before use
- Add proper finalizers or Cleaner for native resources
- Document thread-safety requirements

## 6. Forbidden Changes (Must Not)

- Modify JNI signatures without updating C/Rust headers
- Leak native memory (always free allocated resources)
- Call JNI methods from unauthorized threads
- Throw Java exceptions from native code without proper handling
- Use JNI local references for long-lived objects
- Modify `com_risingwave_java_binding_Binding.h` manually

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `mvn test` |
| Integration tests | `mvn integration-test` |
| Spotless check | `mvn spotless:check` |
| Full build | `mvn clean package` |
| Skip Rust build | `mvn clean package -Dno-build-rust` |

## 8. Dependencies & Contracts

- Maven 3.8+ for build management
- Java 11+ runtime environment
- Rust toolchain for JNI library compilation
- questdb jar-jni for native library loading
- proto module for message types
- common-utils for shared utilities
- JNI header: `com_risingwave_java_binding_Binding.h`

## 9. Overrides

Inherits from `/home/k11/risingwave/java/AGENTS.md`:
- Override: JNI-specific development rules
- Override: Native resource management requirements

## 10. Update Triggers

Regenerate this file when:
- JNI interface changes
- Native library loading mechanism changes
- Rust/java_binding module changes
- Memory management patterns evolve

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/java/AGENTS.md

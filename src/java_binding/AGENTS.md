# AGENTS.md - Java Binding Crate

## 1. Scope

Policies for the `risingwave_java_binding` crate - Java JNI bindings for RisingWave storage and data access.

## 2. Purpose

Provides Java Native Interface (JNI) bindings to expose RisingWave storage functionality to Java applications. Enables Java clients to directly access Hummock storage iterators and stream chunks for efficient data processing and integration with JVM-based systems.

## 3. Structure

```
src/java_binding/
├── src/
│   ├── lib.rs                          # JNI_OnLoad and native method registration
│   ├── hummock_iterator.rs             # Hummock storage iterator bindings
│   └── bin/
│       ├── data-chunk-payload-generator.rs
│       └── data-chunk-payload-convert-generator.rs
├── Cargo.toml
├── gen-demo-insert-data.py
├── make-java-binding.toml
└── run_demo.sh
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | JNI entry point, native method registration |
| `src/hummock_iterator.rs` | Hummock storage iterator Java bindings |
| `Cargo.toml` | cdylib crate for JNI shared library |
| `make-java-binding.toml` | Build configuration for Java bindings |

## 5. Edit Rules (Must)

- Use `#[unsafe(no_mangle)]` for all JNI export functions
- Follow JNI naming convention: `Java_com_risingwave_java_binding_ClassName_methodName`
- Use `cfg_or_panic!` macro for non-madsim builds
- Implement proper error handling with `execute_and_catch` wrapper
- Use `risingwave_jni_core` macros for native method generation
- Register native methods in `JNI_OnLoad`
- Run `cargo fmt` before committing

## 6. Forbidden Changes (Must Not)

- Change JNI function signatures without updating Java counterparts
- Remove or modify `JNI_OnLoad` registration logic
- Use std::alloc::System allocator other than the global allocator
- Bypass `execute_and_catch` for JNI boundary error handling
- Modify native method signatures without regenerating Java headers
- Commit without testing Java integration

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_java_binding` |
| Java integration | Run `./run_demo.sh` |
| Build cdylib | `cargo build -p risingwave_java_binding` |

## 8. Dependencies & Contracts

- Produces `cdylib` for JNI loading
- Depends on `risingwave_jni_core` for common JNI utilities
- Depends on `risingwave_storage` for Hummock access
- Uses `jni` crate for JNI bindings
- Requires Java 8+ for JNI_VERSION_1_2

## 9. Overrides

None. Child directories may override specific rules.

## 10. Update Triggers

Regenerate this file when:
- New JNI native methods are added
- Java class package structure changes
- Build process modifications (make-java-binding.toml)

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

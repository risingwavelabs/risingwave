# AGENTS.md - JNI Core Functionality Crate

## 1. Scope

Policies for the `risingwave_jni_core` crate - Core JNI utilities and Java interoperability.

## 2. Purpose

Provides core JNI infrastructure for RisingWave Java bindings. Implements JVM runtime management, native method macros, type conversions between Rust and Java, and channel-based communication for CDC sources and sink writers.

## 3. Structure

```
src/jni_core/
├── src/
│   ├── lib.rs                    # JNI native methods and type conversions
│   ├── macros.rs                 # JNI macro definitions
│   ├── jvm_runtime.rs            # JVM runtime and initialization
│   ├── tracing_slf4j.rs          # SLF4J tracing integration
│   └── opendal_schema_history.rs # OpenDAL schema history integration
└── Cargo.toml
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Native methods, Pointer types, JNI bindings |
| `src/macros.rs` | `gen_jni_sig!`, `gen_class_name!`, `call_static_method!` |
| `src/jvm_runtime.rs` | JVM builder and runtime initialization |
| `src/tracing_slf4j.rs` | Tracing to SLF4J bridge |

## 5. Edit Rules (Must)

- Use `#[unsafe(no_mangle)]` for JNI exports
- Use `cfg_or_panic!` for non-madsim builds
- Follow naming: `Java_com_risingwave_java_binding_Class_method`
- Use `Pointer<T>` for opaque Rust handles
- Use `execute_and_catch` for panic safety
- Use `gen_jni_sig!` and `gen_class_name!` macros
- Handle Java exceptions properly
- Run `cargo fmt` before committing

## 6. Forbidden Changes (Must Not)

- Remove `execute_and_catch` error handling
- Break `Pointer<T>` memory safety
- Remove `cfg_or_panic` madsim guards
- Modify JNI naming conventions
- Leak memory in JNI boundaries
- Bypass panic handling in JNI calls

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_jni_core` |
| JNI tests | `cargo test -p risingwave_jni_core -- jni` |
| Build | `cargo build -p risingwave_jni_core` |

## 8. Dependencies & Contracts

- JNI: `jni` crate for Java interop
- Runtime: `tokio` with multi-thread support
- Common: `risingwave_common` for types
- Protobuf: `risingwave_pb` for message types
- Macros: `paste` for JNI name generation
- Chrono: `chrono` for timestamp conversions
- Storage: `risingwave_object_store` for storage access

## 9. Overrides

None. Child directories may override specific rules.

## 10. Update Triggers

Regenerate this file when:
- New JNI native methods added
- JVM runtime changes
- Type conversion updates
- JNI macro modifications

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

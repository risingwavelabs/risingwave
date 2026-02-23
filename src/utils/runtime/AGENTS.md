# AGENTS.md - Runtime Crate

## 1. Scope

Policies for the `risingwave_rt` crate providing runtime initialization and management.

## 2. Purpose

The runtime crate provides:
- **Tokio Runtime**: Multi-threaded scheduler configuration
- **Logger Initialization**: tracing-subscriber setup with OpenTelemetry
- **Signal Handling**: Graceful shutdown on SIGINT, SIGTERM
- **Deadlock Detection**: parking_lot deadlock monitoring
- **Profiling**: CPU and memory profiling integration
- **Resource Limits**: File descriptor and memory limits

This provides a unified runtime environment for all RisingWave services.

## 3. Structure

```
src/utils/runtime/
├── Cargo.toml                    # Crate manifest
└── src/
    ├── lib.rs                    # Runtime initialization
    ├── logger.rs                 # Logging and tracing setup
    ├── panic_hook.rs             # Custom panic handler
    ├── deadlock.rs               # Deadlock detection
    └── prof.rs                   # Profiling integration
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `init_risingwave()`, `Runtime` configuration |
| `src/logger.rs` | `init_logger()`, OpenTelemetry/Jaeger integration |
| `src/panic_hook.rs` | Panic logging and optional abort |
| `src/deadlock.rs` | Deadlock detection background task |
| `src/prof.rs` | pprof signal handlers |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Use `madsim-tokio` for simulation compatibility
- Support both console and JSON log formats
- Implement graceful shutdown with timeout
- Test signal handling on target platforms
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Change default runtime threads without performance testing
- Remove OpenTelemetry support
- Break graceful shutdown sequences
- Ignore panics in production (log and optionally abort)
- Use blocking initialization in async contexts

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_rt` |
| Logger tests | `cargo test -p risingwave_rt logger` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **tokio/madsim-tokio**: Async runtime
- **tracing-subscriber**: Log formatting and filtering
- **opentelemetry**: Observability integration
- **console-subscriber**: Tokio console debugging
- **pprof**: CPU profiling
- **rlimit**: Resource limit configuration
- **risingwave_common**: Common types
- **risingwave_variables**: Server variables

Contracts:
- Runtime initializes all components in correct order
- Logger setup is idempotent
- Signal handlers registered before application starts
- Panics are logged with backtrace
- OpenTelemetry resources are properly shutdown

## 9. Overrides

None. Follows parent `src/utils/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- Runtime configuration options changed
- New observability backend added
- Signal handling strategy modified
- Profiling integration changed

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/utils/AGENTS.md

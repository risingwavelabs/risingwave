# AGENTS.md - Heap Profiling Crate

## 1. Scope

Policies for the `risingwave_common_heap_profiling` crate providing JVM-style heap profiling using jemalloc.

## 2. Purpose

The heap profiling crate provides:
- **Heap Profiler**: jemalloc-based memory profiling with pprof output
- **Profile Service**: gRPC service for remote heap profiling requests
- **Flame Graph Generation**: Convert heap samples to flame graph format
- **Memory Leak Detection**: Track allocation patterns over time

This enables production memory diagnostics similar to Java's heap dumps.

## 3. Structure

```
src/common/heap_profiling/
├── Cargo.toml                    # Crate manifest
└── src/
    ├── lib.rs                    # Module exports and public API
    ├── jeprof.rs                 # jemalloc profiling interface
    ├── profiler.rs               # Core profiling logic
    └── profile_service.rs        # gRPC service for profiling
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Public API exports, feature flags |
| `src/jeprof.rs` | jemalloc control interface (enable/disable profiling) |
| `src/profiler.rs` | `HeapProfiler` struct, sample collection |
| `src/profile_service.rs` | `HeapProfileService` gRPC implementation |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Test profiling on both debug and release builds
- Handle jemalloc unavailability gracefully (feature-gated)
- Use `tikv-jemalloc-ctl` for safe jemalloc control operations
- Add integration tests for profile generation
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Enable profiling by default in production (must be opt-in)
- Modify jemalloc options without testing allocation performance
- Remove safety checks around jemalloc profiling enable/disable
- Block threads during profile generation (use async where possible)
- Commit without testing profile output validity
- Use `unsafe` without justification and safety comments

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_common_heap_profiling` |
| Profiler tests | `cargo test -p risingwave_common_heap_profiling profiler` |
| Service tests | `cargo test -p risingwave_common_heap_profiling service` |
| With jemalloc | `cargo test -p risingwave_common_heap_profiling --features jemalloc` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **tikv-jemalloc-ctl**: Safe jemalloc control interface
- **pprof**: Profile generation and flame graph output
- **tonic**: gRPC service for remote profiling
- **chrono**: Timestamp formatting for profile files
- **risingwave_pb**: Profile service protobuf definitions
- **risingwave_rpc_client**: gRPC client for profile requests

Contracts:
- Profiling is disabled by default, enabled via configuration
- Profile output is valid pprof format compatible with pprof tool
- Profiling has minimal overhead when disabled
- Service respects rate limiting for profile requests

## 9. Overrides

None. Follows parent `src/common/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New profiling backend added (besides jemalloc)
- Profile output format changes
- gRPC service interface changes
- jemalloc integration modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/common/AGENTS.md

# AGENTS.md - Hummock Trace Crate

## 1. Scope

Policies for the `src/storage/hummock_trace/` directory containing operation tracing and replay utilities for Hummock storage debugging.

## 2. Purpose

The hummock_trace crate provides facilities for recording Hummock storage operations to trace files and replaying them for debugging and regression testing. This enables reproduction of complex storage scenarios and performance analysis.

## 3. Structure

```
src/storage/hummock_trace/
├── src/
│   ├── lib.rs                    # Module exports
│   ├── collector.rs              # Operation trace collection
│   ├── record.rs                 # Trace record definitions
│   ├── read.rs                   # Read operation tracing
│   ├── write.rs                  # Write operation tracing
│   ├── opts.rs                   # Trace options and configuration
│   ├── error.rs                  # Error types
│   └── replay/                   # Trace replay functionality
│       └── (replay implementations)
└── README.md                     # Usage documentation
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Public exports and feature flags |
| `src/collector.rs` | Trace collection and buffering |
| `src/record.rs` | Trace record serialization |
| `src/read.rs` | Read path operation tracing |
| `src/write.rs` | Write path operation tracing |
| `src/opts.rs` | Trace configuration options |
| `src/replay/` | Trace file replay logic |
| `README.md` | Usage and format documentation |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Add tracing points at key storage operations
- Use efficient serialization for trace records
- Implement replay handlers for new trace types
- Document trace format changes
- Gate tracing behind compile-time features

## 6. Forbidden Changes (Must Not)

- Break trace file format without version bump
- Remove trace record types without migration
- Enable tracing by default in production builds
- Leak sensitive data in trace records
- Use blocking I/O in trace collection paths

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Trace tests | `cargo test -p risingwave_hummock_trace` |
| Replay tests | `cargo test -p risingwave_hummock_trace replay` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- Serialization: Custom or `serde` for trace records
- Storage: File or object storage for trace persistence
- Internal: `risingwave_hummock_sdk` for types
- Feature flag: `trace` to enable collection
- Format version: Tracked for compatibility

## 9. Overrides

None. Inherits rules from parent `src/storage/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New trace operation type added
- Trace format or serialization changed
- Replay functionality extended
- New tracing configuration option added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/storage/AGENTS.md

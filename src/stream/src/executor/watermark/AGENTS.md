# AGENTS.md - Watermark Handling

## 1. Scope

Policies for the `src/stream/src/executor/watermark` directory, containing watermark buffer management for multi-input operators.

## 2. Purpose

The watermark module provides infrastructure for coordinating watermark progression across multiple input streams:
- Buffers watermarks from different upstream sources
- Computes the minimum watermark across all inputs
- Ensures monotonic watermark advancement
- Handles dynamic input source additions and removals
- Provides efficient heap-based watermark selection

Watermarks are essential for event-time processing, triggering window computations, and garbage collecting old state in streaming operators.

## 3. Structure

```
src/stream/src/executor/watermark/
└── mod.rs                    # BufferedWatermarks implementation
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `BufferedWatermarks` struct for multi-source watermark coordination using heap-based priority queue |

## 5. Edit Rules (Must)

- Use `BufferedWatermarks` in all operators with multiple inputs
- Initialize buffers with all input source IDs before processing
- Call `handle_watermark` for each received watermark message
- Use `check_watermark_heap` after buffer modifications
- Clear buffers properly on operator reset or reconfiguration
- Ensure watermark monotonicity invariants are maintained
- Handle watermark removal when input sources are dropped
- Verify watermark equality before heap-based selection

## 6. Forbidden Changes (Must Not)

- Do not bypass `BufferedWatermarks` for multi-input operators
- Never emit watermarks without checking all upstream sources
- Do not modify watermark comparison logic without understanding event-time semantics
- Avoid unbounded buffer growth in watermark staging
- Never ignore watermark propagation requirements in stateful operators
- Do not break monotonicity guarantees for watermark progression
- Never skip buffer cleanup on operator termination

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream watermark` |
| Integration tests | Test via operators using BufferedWatermarks (hash_join, union, etc.) |
| Buffered watermarks | `cargo test -p risingwave_stream buffered_watermarks` |

## 8. Dependencies & Contracts

- **Core type**: Uses `Watermark` from parent `executor/mod.rs`
- **Used by**: Multi-input operators (hash_join, union, dynamic_filter)
- **Invariants**: Watermarks must advance monotonically per source
- **Algorithm**: Heap-based minimum selection across buffered watermarks
- **Buffer management**: VecDeque staging per source with heap tracking

## 9. Overrides

Follows parent AGENTS.md at `./src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- Watermark buffer algorithm changes
- New watermark types or variants added
- Multi-input operator coordination requirements change
- Event-time processing semantics updated
- Buffer data structures modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/AGENTS.md

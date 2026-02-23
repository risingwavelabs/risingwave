# AGENTS.md - End-of-Window Computation

## 1. Scope

Policies for the `src/stream/src/executor/eowc` directory, containing End-of-Window Computation operators for event-time streaming analytics.

## 2. Purpose

The EOWC module implements windowed stream processing with end-of-window semantics:
- Gap filling for time-series data with missing values
- Sorting and buffering for out-of-order event processing
- Window-triggered computation based on watermark progression

These operators enable accurate event-time analytics where results are emitted when windows are complete, rather than processing time.

## 3. Structure

```
src/stream/src/executor/eowc/
├── mod.rs                    # Module exports
├── eowc_gap_fill.rs          # Gap filling executor for time-series interpolation
├── sort.rs                   # Sort executor with buffering for out-of-order events
└── sort_buffer.rs            # Buffer management for sort operations
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `eowc_gap_fill.rs` | Fills gaps in time-series data using interpolation or specified fill strategies |
| `sort.rs` | Sorts events by event time with configurable buffer for late arrivals |
| `sort_buffer.rs` | Manages sort buffer state and watermark-based eviction |

## 5. Edit Rules (Must)

- Use watermark messages to trigger window completion
- Handle late arrivals within configured tolerance
- Implement proper state cleanup on watermark advancement
- Support multiple gap fill strategies (linear, locf, null)
- Ensure deterministic output ordering
- Handle barrier commits for sort buffer persistence

## 6. Forbidden Changes (Must Not)

- Do not modify window semantics without coordination with planner
- Never bypass watermark-based triggering for EOWC operators
- Do not allow unbounded buffer growth without backpressure
- Avoid non-deterministic gap fill strategies in streaming contexts
- Never skip state table persistence for sort buffers

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream eowc` |
| Gap fill tests | `cargo test -p risingwave_stream eowc_gap_fill` |
| Sort tests | `cargo test -p risingwave_stream sort` |

## 8. Dependencies & Contracts

- **Time semantics**: Event-time processing with watermark progression
- **Buffering**: Managed buffers with configurable size and timeout
- **State**: Sort buffer persisted to state table for recovery
- **Triggering**: Watermark-based window completion guarantees

## 9. Overrides

Follows parent AGENTS.md at `./src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- New EOWC operators added
- Window semantics or triggering changes
- Gap fill strategy modifications
- Event-time processing requirements updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/AGENTS.md

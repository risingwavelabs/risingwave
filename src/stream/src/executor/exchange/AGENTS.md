# AGENTS.md - Data Exchange

## 1. Scope

Policies for the `src/stream/src/executor/exchange` directory, containing cross-node data shuffle components for distributed stream processing.

## 2. Purpose

The exchange module implements data movement between compute nodes:
- Input side for receiving shuffled data from upstream actors
- Output side for sending data to downstream actors
- Permit-based flow control for backpressure management
- Error handling for network failures

Exchange operators enable RisingWave to scale horizontally by partitioning data across multiple nodes while maintaining stream semantics.

## 3. Structure

```
src/stream/src/executor/exchange/
├── mod.rs                    # Module exports
├── error.rs                  # Exchange-specific error types
├── input.rs                  # Exchange input for receiving data
├── output.rs                 # Exchange output for sending data
└── permit.rs                 # Flow control permits for backpressure
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `input.rs` | `ExchangeInput` for receiving and deserializing messages from upstream |
| `output.rs` | `ExchangeOutput` for serializing and sending messages downstream |
| `permit.rs` | Permit-based flow control to prevent memory overflow |
| `error.rs` | Exchange error types with recovery hints |

## 5. Edit Rules (Must)

- Implement proper backpressure using permit system
- Handle network failures with retry and circuit breaker patterns
- Serialize/deserialize messages efficiently using protobuf
- Support both local and remote exchange paths
- Handle barrier messages correctly during reshuffles
- Implement timeout handling for blocked exchanges

## 6. Forbidden Changes (Must Not)

- Do not disable backpressure mechanisms
- Never ignore network error handling
- Do not break barrier propagation across exchanges
- Avoid unbounded buffering in exchange channels
- Never skip permit acquisition before sending

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_stream exchange` |
| Permit tests | `cargo test -p risingwave_stream permit` |
| Integration | Exchange integration tests in main test suite |

## 8. Dependencies & Contracts

- **Network**: gRPC for inter-node communication
- **Serialization**: Protobuf for message encoding
- **Flow control**: Permit-based backpressure
- **Failure handling**: Retry with exponential backoff

## 9. Overrides

Follows parent AGENTS.md at `./src/stream/src/executor/AGENTS.md`. No overrides.

## 10. Update Triggers

Regenerate this file when:
- Exchange protocol changes
- Backpressure mechanism updates
- Network failure handling improvements
- New serialization formats added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/stream/src/executor/AGENTS.md

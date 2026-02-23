# AGENTS.md - RPC Client Crate

## 1. Scope

Policies for the `risingwave_rpc_client` crate - gRPC client implementations for internal communication.

## 2. Purpose

Provides wrapper gRPC clients for RisingWave internal service communication. Implements client pools, connection management, and request/response handling for Meta, Compute, Stream, Compactor, and other internal services.

## 3. Structure

```
src/rpc_client/
├── src/
│   ├── lib.rs                    # RpcClient trait and pool implementation
│   ├── meta_client.rs            # Meta service client
│   ├── compute_client.rs         # Compute node client
│   ├── stream_client.rs          # Streaming service client
│   ├── hummock_meta_client.rs    # Hummock metadata client
│   ├── compactor_client.rs       # Compactor service client
│   ├── frontend_client.rs        # Frontend service client
│   ├── connector_client.rs       # Connector service client
│   ├── sink_coordinate_client.rs # Sink coordinator client
│   ├── monitor_client.rs         # Monitoring client
│   ├── channel.rs                # Channel management
│   └── error.rs                  # RPC error types
└── Cargo.toml
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | RpcClient trait, RpcClientPool, BidiStreamHandle |
| `src/meta_client.rs` | Meta service RPC client |
| `src/compute_client.rs` | Compute node RPC client |
| `src/stream_client.rs` | Streaming service client |
| `src/error.rs` | RPC error handling |

## 5. Edit Rules (Must)

- Implement `RpcClient` trait for all new clients
- Use `RpcClientPool` for connection pooling
- Handle RPC errors with `RpcError` type
- Use `stream_rpc_client_method_impl!` macro for streaming
- Use `meta_rpc_client_method_impl!` macro for meta calls
- Implement client refresh logic for meta client
- Add unit tests for client methods
- Run `cargo fmt` before committing

## 6. Forbidden Changes (Must Not)

- Break `RpcClient` trait contract
- Remove connection pooling from clients
- Bypass error conversion macros
- Hardcode service addresses (use discovery)
- Remove retry logic from critical paths
- Change protobuf definitions without regeneration

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_rpc_client` |
| Client pool tests | `cargo test -p risingwave_rpc_client pool` |
| Stream tests | `cargo test -p risingwave_rpc_client stream` |

## 8. Dependencies & Contracts

- gRPC: `tonic` for RPC, `tokio` for async
- Pooling: `moka` for client caching
- Retry: `tokio-retry` for resilience
- Proto: `risingwave_pb` for messages
- Error: `risingwave_error` for error types
- Common: `risingwave_common` for utilities

## 9. Overrides

None. Child directories may override specific rules.

## 10. Update Triggers

Regenerate this file when:
- New service clients added
- RPC client trait changes
- Connection pool implementation updates
- Error handling pattern changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

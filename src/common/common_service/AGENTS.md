# AGENTS.md - Common Service Crate

## 1. Scope

Policies for the `risingwave_common_service` crate providing shared service infrastructure for RisingWave components.

## 2. Purpose

The common service crate provides foundational utilities for building RisingWave services:
- **Observer Manager**: Subscription pattern for configuration/metadata change notifications
- **Metrics Manager**: Prometheus metrics exposition via HTTP endpoints
- **Tracing Integration**: Distributed tracing setup for service observability
- **Service Infrastructure**: Common gRPC and HTTP service patterns

This crate abstracts service lifecycle concerns used by meta, compute, frontend, and compactor nodes.

## 3. Structure

```
src/common/common_service/
├── Cargo.toml                    # Crate manifest
└── src/
    ├── lib.rs                    # Module exports
    ├── metrics_manager.rs        # Prometheus metrics HTTP endpoint
    ├── observer_manager.rs       # Change notification subscription
    └── tracing.rs                # Distributed tracing utilities
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Module declarations and public API exports |
| `src/observer_manager.rs` | `ObserverManager`, `ObserverNode`, `NotificationClient` traits |
| `src/metrics_manager.rs` | `MetricsManager` for metrics endpoint lifecycle |
| `src/tracing.rs` | OpenTelemetry/Jaeger tracing initialization |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Follow existing service patterns when adding new observer types
- Use `async-trait` for async observer callbacks
- Add unit tests for observer state machine transitions
- Implement graceful shutdown for all manager types
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Modify `ObserverManager` notification ordering without thorough review
- Remove backward compatibility in notification protocol
- Add blocking operations in observer callback handlers
- Use `std::sync::Mutex` in async contexts (use `parking_lot` or `tokio::sync`)
- Bypass metrics endpoint security without explicit authorization
- Commit without testing service shutdown sequences

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_common_service` |
| Observer tests | `cargo test -p risingwave_common_service observer` |
| Metrics tests | `cargo test -p risingwave_common_service metrics` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **axum**: HTTP server for metrics endpoint
- **tonic**: gRPC client for notification service
- **prometheus**: Metrics collection and exposition
- **tracing-opentelemetry**: Distributed tracing integration
- **risingwave_pb**: Notification service protobuf definitions
- **risingwave_rpc_client**: gRPC client implementations

Contracts:
- Observer notifications are ordered and exactly-once per subscriber
- Metrics endpoint responds to GET /metrics in Prometheus text format
- Tracing context propagation follows W3C trace context standard

## 9. Overrides

None. Follows parent `src/common/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New service manager type added (health, discovery, etc.)
- Observer notification protocol changes
- Metrics exposition format changes
- Tracing backend integration changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/common/AGENTS.md

# AGENTS.md - Rate Limit Crate

## 1. Scope

Policies for the `risingwave_common_rate_limit` crate providing rate limiting utilities for streaming and batch operations.

## 2. Purpose

The rate limit crate provides:
- **Source Rate Limiting**: Throttle data ingestion from connectors
- **Backpressure Handling**: Apply backpressure with configurable limits
- **Adaptive Rate Limiting**: Dynamic adjustment based on system load
- **Metrics Integration**: Rate limit telemetry via Prometheus

This ensures fair resource allocation and prevents system overload.

## 3. Structure

```
src/common/rate_limit/
├── Cargo.toml                    # Crate manifest
└── src/
    └── lib.rs                    # Rate limiter implementations and traits
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `RateLimiter`, `RateLimitPolicy`, limiter implementations |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Use token bucket or leaky bucket algorithms (configurable)
- Integrate with `risingwave_common_metrics` for observability
- Handle rate limit configuration changes dynamically (via `arc-swap`)
- Test behavior at rate limit boundaries
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Block threads in rate limiter (use async-aware mechanisms)
- Remove metrics emission for rate limit decisions
- Hardcode rate limits (must be configurable)
- Ignore backpressure signals from downstream
- Use unbounded queues for rate-limited streams
- Break compatibility with existing rate limit policies

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_common_rate_limit` |
| Rate tests | `cargo test -p risingwave_common_rate_limit rate` |
| Policy tests | `cargo test -p risingwave_common_rate_limit policy` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **arc-swap**: Lock-free configuration updates
- **parking_lot**: Fast synchronization
- **prometheus**: Rate limit metrics
- **rand**: Jitter for rate limiting
- **tokio**: Async runtime integration
- **pin-project-lite**: Stream wrapper projection
- **risingwave_common**: Common types and utilities
- **risingwave_common_metrics**: Metrics integration

Contracts:
- Rate limits are enforced approximately (best-effort)
- Configuration changes take effect within milliseconds
- Rate limiting never blocks indefinitely
- Metrics accurately reflect limiter state

## 9. Overrides

None. Follows parent `src/common/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New rate limiting algorithm added
- Rate limit policy format changes
- Metrics integration modified
- Configuration mechanism changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/common/AGENTS.md

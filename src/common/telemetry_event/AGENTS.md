# AGENTS.md - Telemetry Event Crate

## 1. Scope

Policies for the `risingwave_telemetry_event` crate providing telemetry event collection and reporting.

## 2. Purpose

The telemetry event crate provides:
- **Event Collection**: Structured event logging for product analytics
- **Telemetry Reporting**: Periodic upload to telemetry endpoints
- **Privacy Controls**: PII redaction and opt-out mechanisms
- **Feature Usage Tracking**: Anonymous usage statistics

This enables understanding of feature usage without compromising user privacy.

## 3. Structure

```
src/common/telemetry_event/
├── Cargo.toml                    # Crate manifest
└── src/
    ├── lib.rs                    # Telemetry client and event types
    └── util.rs                   # Utility functions for telemetry
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `TelemetryClient`, `TelemetryEvent`, reporting logic |
| `src/util.rs` | Helper functions for event formatting |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Ensure all telemetry is opt-out with clear privacy policy
- Redact all PII before sending (IP addresses, table names, query content)
- Use anonymous identifiers (no user tracking across sessions)
- Batch events to reduce network overhead
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Send PII or sensitive data (query text, table names, user data)
- Track individual users across sessions or installations
- Block main operations on telemetry sending (must be async/fire-and-forget)
- Enable telemetry by default in enterprise/self-hosted deployments
- Use telemetry for advertising or third-party sharing
- Collect data without documented purpose
- Remove opt-out mechanisms

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_telemetry_event` |
| Redaction tests | `cargo test -p risingwave_telemetry_event redact` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **reqwest**: HTTP client for telemetry upload
- **jsonbb**: JSON serialization for events
- **prost**: Protocol buffer serialization
- **risingwave_pb**: Telemetry event protobuf definitions
- **risingwave_common_log**: Logging integration
- **tokio**: Async runtime for background sending

Contracts:
- Telemetry never blocks user operations
- Events are batched and sent on a background task
- Failed sends are retried with exponential backoff
- Events are persisted locally if upload fails temporarily
- All data collection complies with privacy policy

## 9. Overrides

None. Follows parent `src/common/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New telemetry event type added
- Privacy policy or data handling changes
- Telemetry endpoint or protocol changes
- New privacy regulation requirements

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/common/AGENTS.md

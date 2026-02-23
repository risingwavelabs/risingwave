# AGENTS.md - License Management Crate

## 1. Scope

Policies for the `risingwave_license` crate - License validation and feature gating.

## 2. Purpose

Manages RisingWave license validation, feature gating for enterprise features, and telemetry reporting for license usage. Supports RisingWave Units (RWU) based licensing and feature-level access control.

## 3. Structure

```
src/license/
├── src/
│   ├── lib.rs           # Main exports and telemetry
│   ├── feature.rs       # Feature definitions and gating
│   ├── key.rs           # License key handling
│   ├── manager.rs       # License manager
│   └── rwu.rs           # RisingWave Units calculations
└── Cargo.toml
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/feature.rs` | Feature enum and PaidFeature trait |
| `src/manager.rs` | LicenseManager for validation |
| `src/key.rs` | License key parsing and validation |
| `src/rwu.rs` | RisingWave Units computation |

## 5. Edit Rules (Must)

- Add new features to `Feature` enum with `strum` derive
- Implement `PaidFeature` trait for enterprise features
- Use `LicenseManager` for all license checks
- Report feature usage via `report_telemetry`
- Validate license keys with JWT parsing
- Document feature requirements in comments
- Run `cargo fmt` before committing

## 6. Forbidden Changes (Must Not)

- Remove license checks from paid features
- Bypass LicenseManager validation
- Modify telemetry reporting without approval
- Expose license keys in logs or errors
- Remove RWU calculation logic
- Break backward compatibility for existing licenses

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_license` |
| Feature tests | `cargo test -p risingwave_license -- feature` |

## 8. Dependencies & Contracts

- JWT: `jsonwebtoken` for license validation
- Telemetry: `risingwave_telemetry_event` for usage reporting
- Serialization: `serde`, `jsonbb`
- Protobuf: `risingwave_pb` for telemetry events
- Features: Defined in `feature.rs` with strum derives

## 9. Overrides

None. Child directories may override specific rules.

## 10. Update Triggers

Regenerate this file when:
- New paid features added
- License key format changes
- Telemetry reporting updates
- RWU calculation modifications

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md

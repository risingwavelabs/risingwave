# AGENTS.md - Test Runner Framework Crate

## 1. Scope

Policies for the `risingwave_test_runner` crate - Custom test runner framework for RisingWave testing.

## 2. Purpose

Provides a specialized custom test runner framework for RisingWave's unique distributed testing requirements. Enables failpoint injection testing, sync-point coordination for distributed scenarios, and custom test frameworks beyond standard Rust testing capabilities.

## 3. Structure

```
src/test_runner/
├── src/
│   ├── lib.rs           # Test runner module exports and framework setup
│   └── test_runner.rs   # Custom test framework implementation
└── Cargo.toml
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Module exports, feature flags, custom framework setup |
| `src/test_runner.rs` | Test runner implementation and test collection |
| `Cargo.toml` | Custom test framework dependencies and configuration |

## 5. Edit Rules (Must)

- Enable `#![feature(custom_test_frameworks)]` for custom runner support
- Enable `#![feature(test)]` for benchmark integration
- Support `fail` crate for comprehensive failpoint injection testing
- Integrate `sync-point` utility for distributed test coordination
- Maintain compatibility with standard `#[test]` attributes
- Document all test runner configuration options in code comments
- Keep dependencies minimal for fast compilation
- Run `cargo fmt` before committing changes

## 6. Forbidden Changes (Must Not)

- Remove `custom_test_frameworks` feature support
- Break standard Rust test attribute compatibility
- Remove failpoint testing capabilities from framework
- Break sync-point distributed coordination mechanisms
- Add heavy dependencies that slow test compilation
- Change test discovery without updating documentation
- Remove `extern crate test` linkage

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_test_runner` |
| Failpoint tests | Tests using `fail::cfg` scenarios |
| Sync-point tests | Tests coordinating via `sync-point` utility |
| Custom framework | Tests using `#[test_case]` attributes |

## 8. Dependencies & Contracts

- Failpoints: `fail = "0.5"` crate for fault injection points
- Sync: `sync-point` utility from `src/utils/sync-point` for coordination
- Test: `test` crate for custom framework integration
- Framework: `custom_test_frameworks` nightly feature
- Minimal: Lightweight design for fast test compilation
- Integration: Works with `risingwave_common` testing utilities

## 9. Overrides

None. Child directories may override specific rules.

## 10. Update Triggers

Regenerate this file when:
- Custom test framework implementation changes
- Failpoint testing infrastructure updates
- Sync-point coordination modifications
- Test runner API or attribute changes
- New testing features added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

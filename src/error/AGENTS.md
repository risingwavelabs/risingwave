# AGENTS.md - Error Handling Crate

## 1. Scope

Policies for the `risingwave_error` crate - Error handling infrastructure and utilities.

## 2. Purpose

Provides error handling utilities and types used across the RisingWave codebase. Made as a separate crate to allow usage in places where `risingwave_common` is not available. Re-exported in `risingwave_common::error` for convenient access.

## 3. Structure

```
src/error/
├── src/
│   ├── lib.rs           # Main exports and Error trait definitions
│   ├── anyhow.rs        # Anyhow integration utilities
│   ├── code.rs          # Error codes
│   ├── common.rs        # Common error types
│   ├── macros.rs        # Error macros
│   ├── tonic.rs         # Tonic/gRPC error handling
│   │   └── extra.rs     # Additional tonic utilities
│   └── wrappers.rs      # Error wrappers
└── Cargo.toml
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | BoxedError type and Error trait alias |
| `src/common.rs` | Common error definitions |
| `src/tonic.rs` | gRPC/tonic error conversion |
| `src/macros.rs` | Error handling macros |
| `src/code.rs` | Error code definitions |

## 5. Edit Rules (Must)

- Maintain `BoxedError` as primary error type: `Box<dyn Error>`
- Keep `Error` trait alias: `std::error::Error + Send + Sync + 'static`
- Re-export `thiserror-ext` for derive macros
- Use `error_request_copy` for extracting values from errors
- Follow existing error wrapper patterns
- Add unit tests for new error types
- Run `cargo fmt` before committing

## 6. Forbidden Changes (Must Not)

- Break `BoxedError` Send/Sync/'static bounds
- Remove thiserror-ext re-exports
- Add dependencies that conflict with workspace versions
- Remove `error_generic_member_access` feature usage without migration
- Bypass error handling patterns in dependent crates

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_error` |
| All error tests | `cargo test -p risingwave_error -- --nocapture` |

## 8. Dependencies & Contracts

- Re-exports: `thiserror-ext`, `thiserror-ext::*`
- Minimal dependencies: `anyhow`, `tonic`, `thiserror`, `tracing`
- Used by: `risingwave_common`, `risingwave_rpc_client`, most crates
- Trait alias: `Error` for Send+Sync+'static errors

## 9. Overrides

None. Child directories may override specific rules.

## 10. Update Triggers

Regenerate this file when:
- New error code categories added
- Error handling patterns change
- Tonic/gRPC error integration updates

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md

# AGENTS.md - Meta Dashboard

## 1. Scope

Policies for the `src/meta/dashboard/` directory containing the embedded web dashboard for the RisingWave meta service.

## 2. Purpose

The dashboard crate provides a web-based UI for monitoring and managing the RisingWave cluster. It serves static assets and provides API endpoints for cluster health, job status, and performance metrics visualization.

## 3. Structure

```
src/meta/dashboard/
├── src/
│   ├── lib.rs                    # Dashboard module exports
│   ├── embed.rs                  # Static asset embedding
│   └── proxy.rs                  # API request proxying
├── examples/
│   └── (dashboard examples)
└── build.rs                      # Build script for UI assets
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Dashboard service initialization |
| `src/embed.rs` | Static file embedding via `rust-embed` |
| `src/proxy.rs` | API request forwarding to meta services |
| `build.rs` | UI build and asset bundling |
| `Cargo.toml` | Dependencies including `rust-embed` |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Update `embed.rs` when adding new static assets
- Add tests for new API proxy endpoints
- Document dashboard API changes
- Keep dashboard dependencies minimal for binary size

## 6. Forbidden Changes (Must Not)

- Embed large uncompressed assets
- Expose sensitive cluster information in dashboard APIs
- Remove existing dashboard endpoints without deprecation
- Include development-only assets in release builds
- Use blocking I/O in async dashboard handlers

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Dashboard tests | `cargo test -p risingwave_meta dashboard` |
| Build check | `cargo build -p risingwave_meta --features dashboard` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- `rust-embed`: Static asset embedding
- `axum` or similar: HTTP server (via meta service)
- Internal: `risingwave_meta` for data access
- Feature flag: `dashboard` controls inclusion
- Not available in `madsim` simulation builds

## 9. Overrides

None. Inherits rules from parent `src/meta/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- Dashboard API endpoints added/removed
- Static asset embedding changed
- UI build process modified
- New dashboard feature category added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/meta/AGENTS.md

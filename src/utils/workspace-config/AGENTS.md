# AGENTS.md - Workspace Config Crate

## 1. Scope

Policies for the `workspace-config` crate providing build-time configuration for linking and features.

## 2. Purpose

The workspace-config crate provides:
- **Static Linking**: Optional static linking for libz, lzma, sasl2
- **OpenSSL Vendored**: Option to bundle OpenSSL
- **Dynamic Linking**: Optional dynamic linking for zstd
- **FIPS Mode**: AWS-LC FIPS compliance support
- **Log Level Control**: Release build log level configuration

This centralizes build configuration for consistent binary builds.

## 3. Structure

```
src/utils/workspace-config/
├── Cargo.toml                    # Crate manifest with feature flags
├── README.md                     # Build configuration documentation
└── src/
    └── lib.rs                    # Feature flag re-exports
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `Cargo.toml` | Feature flags and dependency configuration |
| `README.md` | Build configuration guide |
| `src/lib.rs` | Compile-time feature detection |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Document all features in README.md
- Test with each feature flag combination
- Do NOT add workspace-hack dependency (intentionally excluded)
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Add workspace-hack dependency (breaks feature unification)
- Enable static linking by default (must be opt-in)
- Remove features without deprecation period
- Change default features without approval
- Break FIPS compliance when enabled

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Build test | `cargo build -p workspace-config` |
| With features | `cargo build -p workspace-config --features openssl-vendored,rw-static-link` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **libz-sys**: zlib compression (optional static)
- **lzma-sys**: LZMA compression (optional static)
- **sasl2-sys**: SASL authentication (optional static)
- **openssl-sys**: TLS/SSL (optional vendored)
- **zstd-sys**: Zstd compression (optional dynamic)
- **aws-lc-rs**: FIPS-compliant cryptography

Contracts:
- Features are additive (enabling more features should work)
- Static and dynamic features are mutually exclusive per library
- FIPS feature requires specific build environment
- Log level features affect release builds only

## 9. Overrides

Overrides parent rules:

| Parent Rule | Override |
|-------------|----------|
| "All crates use workspace-hack" | workspace-config must NOT use workspace-hack |

## 10. Update Triggers

Regenerate this file when:
- New linking option added
- FIPS requirements changed
- New optional dependency for static/dynamic linking
- Build configuration documentation updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/utils/AGENTS.md

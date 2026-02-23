# AGENTS.md - Secret Crate

## 1. Scope

Policies for the `risingwave_common_secret` crate providing secure secret management and encryption.

## 2. Purpose

The secret crate provides:
- **Secret Storage**: Encrypted storage for credentials and keys
- **Secret Manager**: Lifecycle management for secrets (create, rotate, delete)
- **Encryption**: AES-GCM encryption for data at rest
- **HashiCorp Vault Integration**: External secret store support
- **Redaction**: Automatic redaction of secrets in logs

This ensures sensitive data is handled securely throughout the system.

## 3. Structure

```
src/common/secret/
├── Cargo.toml                    # Crate manifest
└── src/
    ├── lib.rs                    # Public API and re-exports
    ├── encryption.rs             # Encryption/decryption implementation
    ├── error.rs                  # SecretError types
    ├── secret_manager.rs         # Secret lifecycle management
    └── vault_client.rs           # HashiCorp Vault client
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `Secret`, `SecretRef`, `SecretManager` exports |
| `src/encryption.rs` | AES-GCM encryption with key derivation |
| `src/secret_manager.rs` | Secret CRUD operations, caching |
| `src/vault_client.rs` | Vault API client, token management |
| `src/error.rs` | `SecretError` enum with context |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Use `aws-lc-rs` or `ring` for cryptographic operations
- Implement zeroize for secret buffers (clear memory on drop)
- Add audit logging for all secret operations
- Use constant-time comparison for secret validation
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Log secret values or encryption keys
- Store secrets in plain text in configuration files
- Use weak cryptographic algorithms (MD5, SHA1, DES)
- Send secrets over unencrypted channels
- Cache decrypted secrets without expiration
- Remove audit logging for secret access
- Use `unsafe` without cryptographic expert review

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_common_secret` |
| Encryption tests | `cargo test -p risingwave_common_secret encrypt` |
| Vault tests | `cargo test -p risingwave_common_secret vault` |
| Integration | `cargo test -p risingwave_common_secret --features vault` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **aws-lc-rs**: Cryptographic primitives (FIPS-compliant option)
- **bincode**: Secret serialization format
- **moka**: Caching for decrypted secrets
- **reqwest**: HTTP client for Vault API
- **serde**: Secret structure serialization
- **parking_lot**: Mutex for secret cache

Contracts:
- Secrets are encrypted at rest with AES-256-GCM
- Encryption keys are derived using PBKDF2 or Argon2
- Secret values are redacted in Debug and Display implementations
- Vault tokens are refreshed before expiration
- All secret access is logged for audit

## 9. Overrides

None. Follows parent `src/common/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New secret backend added (AWS KMS, Azure Key Vault, etc.)
- Encryption algorithm changes
- Secret lifecycle operations modified
- Audit logging requirements change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/common/AGENTS.md

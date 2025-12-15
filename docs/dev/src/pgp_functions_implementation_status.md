# PGP Encryption/Decryption Functions Implementation Status

## Overview
This document tracks the implementation status of PostgreSQL-compatible PGP encryption/decryption functions in RisingWave.

## Reference
- PostgreSQL documentation: https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-PGP-ENC-FUNCS
- PostgreSQL source code:
  - `postgres/postgres@REL_16_1/contrib/pgcrypto/pgp-encrypt.c`
  - `postgres/postgres@REL_16_1/contrib/pgcrypto/pgp-decrypt.c`
- PostgreSQL tests:
  - `postgres/postgres@REL_16_1/contrib/pgcrypto/sql/pgp-encrypt.sql`
  - `postgres/postgres@REL_16_1/contrib/pgcrypto/sql/pgp-decrypt.sql`
  - `postgres/postgres@REL_16_1/contrib/pgcrypto/sql/pgp-pubkey-encrypt.sql`
  - `postgres/postgres@REL_16_1/contrib/pgcrypto/sql/pgp-pubkey-decrypt.sql`

## Functions to Implement

### 1. pgp_sym_encrypt
- **Signature**: `pgp_sym_encrypt(data text, psw text [, options text]) returns bytea`
- **Description**: Encrypts data with a symmetric PGP key
- **Status**: ✅ Stub implemented, infrastructure complete
- **Location**: `src/expr/impl/src/scalar/pgp_encrypt.rs`
- **Implementation**: Needs actual PGP symmetric encryption logic

### 2. pgp_sym_decrypt  
- **Signature**: `pgp_sym_decrypt(msg bytea, psw text [, options text]) returns text`
- **Description**: Decrypts a symmetric-key-encrypted PGP message
- **Status**: ✅ Stub implemented, infrastructure complete  
- **Location**: `src/expr/impl/src/scalar/pgp_encrypt.rs`
- **Implementation**: Needs actual PGP symmetric decryption logic

### 3. pgp_pub_encrypt
- **Signature**: `pgp_pub_encrypt(data text, key bytea [, options text]) returns bytea`
- **Description**: Encrypts data with a PGP public key
- **Status**: ✅ Stub implemented, infrastructure complete
- **Location**: `src/expr/impl/src/scalar/pgp_encrypt.rs`
- **Implementation**: Needs actual PGP public key encryption logic

### 4. pgp_pub_decrypt
- **Signature**: `pgp_pub_decrypt(msg bytea, key bytea [, psw text [, options text]]) returns text`
- **Description**: Decrypts a public-key-encrypted message
- **Status**: ✅ Stub implemented, infrastructure complete
- **Location**: `src/expr/impl/src/scalar/pgp_encrypt.rs`
- **Implementation**: Needs actual PGP private key decryption logic

## Infrastructure Completed

### Code Files Modified/Created:
1. ✅ `src/expr/impl/src/scalar/pgp_encrypt.rs` - Main implementation file with stub functions
2. ✅ `src/expr/impl/src/scalar/mod.rs` - Module registration
3. ✅ `proto/expr.proto` - Added PGP function type enum values
4. ✅ `src/frontend/src/expr/pure.rs` - Added to pure function list
5. ✅ `src/frontend/src/optimizer/plan_expr_visitor/strong.rs` - Added to strong visitor
6. ✅ `src/frontend/src/binder/expr/function/builtin_scalar.rs` - Registered functions in SQL catalog

### Proto Changes:
```protobuf
PGP_SYM_ENCRYPT = 339;
PGP_SYM_DECRYPT = 340;
PGP_PUB_ENCRYPT = 341;
PGP_PUB_DECRYPT = 342;
```

### Build Status:
- ✅ Code compiles successfully
- ✅ Functions are callable from SQL
- ✅ Stub implementations return appropriate error messages

## Testing

### Current Test Status:
```sql
-- All functions are accessible but return "not yet implemented" errors
SELECT pgp_sym_encrypt('test', 'password');
-- ERROR: Invalid parameter pgp_sym_encrypt: PGP symmetric encryption not yet implemented
```

### Test Files to Create:
1. `e2e_test/batch/pgp_sym_encrypt.slt` - Tests for pgp_sym_encrypt
2. `e2e_test/batch/pgp_sym_decrypt.slt` - Tests for pgp_sym_decrypt  
3. `e2e_test/batch/pgp_pub_encrypt.slt` - Tests for pgp_pub_encrypt
4. `e2e_test/batch/pgp_pub_decrypt.slt` - Tests for pgp_pub_decrypt

## Next Steps

### Implementation Strategy:
The actual PGP implementation requires:

1. **For Symmetric Encryption (`pgp_sym_encrypt`/`pgp_sym_decrypt`)**:
   - Password-based key derivation (S2K - String-to-Key)
   - Symmetric encryption using derived key
   - PGP message format construction (packet structure)
   - ASCII armoring of binary data
   - Options parsing (cipher-algo, compress-algo, etc.)

2. **For Public Key Encryption (`pgp_pub_encrypt`/`pgp_pub_decrypt`)**:
   - PGP public/private key parsing
   - RSA or ElGamal encryption/decryption
   - Session key generation and wrapping
   - PGP message format with public key packets

### Implementation Options:
1. **Use existing Rust PGP crate**: `pgp` crate (0.18.0) or `sequoia-openpgp`
2. **Low-level OpenSSL implementation**: Build PGP format manually using OpenSSL primitives
3. **Minimal implementation**: Support only basic use cases with limited options

### Recommended Approach:
Given the complexity of PGP, using an existing well-maintained Rust crate like `sequoia-openpgp` or `pgp` is recommended. However, these libraries are large and may not match PostgreSQL's behavior exactly.

## Compatibility Notes

### PostgreSQL pgcrypto Options:
- `cipher-algo`: aes128, aes192, aes256, 3des, cast5, blowfish  
- `compress-algo`: 0 (none), 1 (zip), 2 (zlib), 3 (bzip2)
- `compress-level`: 0-9
- `convert-crlf`: 0 or 1
- `disable-mdc`: 0 or 1
- `sess-key`: 0 or 1
- `s2k-mode`: 0, 1, or 3
- `s2k-count`: power of 2 between 1024 and 65011712
- `s2k-digest-algo`: md5, sha1, sha256, etc.
- `s2k-cipher-algo`: same as cipher-algo
- `unicode-mode`: 0 or 1

### Key Compatibility:
- Must support PGP/GPG key formats
- Must handle ASCII-armored keys
- Must support password-protected private keys

## Current Status Summary:
- **Infrastructure**: 100% complete ✅
- **Stub implementations**: 100% complete ✅  
- **Actual PGP logic**: 0% complete ⏳
- **Tests**: 0% complete ⏳
- **Documentation**: In progress 📝

## Files Changed:
- `src/expr/impl/Cargo.toml` - No new dependencies added yet (removed pgp crate)
- `src/expr/impl/src/scalar/pgp_encrypt.rs` - 145 lines (stub implementation)
- `proto/expr.proto` - 4 enum values added
- `src/frontend/src/expr/pure.rs` - 4 function types added
- `src/frontend/src/optimizer/plan_expr_visitor/strong.rs` - 4 function types added
- `src/frontend/src/binder/expr/function/builtin_scalar.rs` - 4 functions registered

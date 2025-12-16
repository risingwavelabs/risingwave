# Plan for Full PostgreSQL pgcrypto Compatibility

## Current Status
✅ API-compatible (function signatures, options)
❌ Binary format incompatible (custom format vs OpenPGP)

## What's Needed for Binary Compatibility

### Option 1: Use Sequoia-PGP Library (Recommended)
```toml
[dependencies]
sequoia-openpgp = { version = "1.21", default-features = false, features = ["crypto-rust", "compression"] }
```

**Pros:**
- Full RFC 4880 compliance
- Well-tested, maintained
- Handles all edge cases

**Cons:**
- Large dependency (~100 transitive deps)
- Need to match PostgreSQL's specific packet choices
- Learning curve for the API

**Estimated effort:** 2-3 days

### Option 2: Implement OpenPGP Packets Manually
Implement the minimum OpenPGP packet structure needed:

#### 1. Packet Structure (RFC 4880)
```rust
struct OpenPgpPacket {
    tag: u8,           // Packet type (3, 8, 11, 18, etc.)
    length: PacketLength,  // 1, 2, or 5 bytes depending on size
    body: Vec<u8>,     // Packet contents
}

enum PacketLength {
    OneOctet(u8),      // Length < 192
    TwoOctet(u16),     // Length < 8384
    FiveOctet(u32),    // Larger messages
}
```

#### 2. S2K Packet (for password→key derivation)
```rust
fn build_skesk_packet(password: &str, session_key: &[u8], cipher: Cipher) -> Vec<u8> {
    // Tag 3: Symmetric-Key Encrypted Session Key
    let mut packet = vec![0xC3]; // Tag 3, old format

    // Version 4
    packet.push(4);

    // Symmetric algorithm
    packet.push(cipher.to_pgp_id());

    // S2K specifier
    packet.push(3); // Iterated and Salted
    packet.push(hash_algo.to_pgp_id());
    packet.extend_from_slice(&salt); // 8 bytes
    packet.push(encode_count(s2k_count)); // Encoded as per RFC

    // Derive key and encrypt session key
    let derived_key = iterated_salted_s2k(password, &salt, count, hash_algo);
    let encrypted_session = encrypt_cfb(session_key, &derived_key);
    packet.extend_from_slice(&encrypted_session);

    // Add length prefix
    prepend_packet_length(&mut packet);
    packet
}
```

#### 3. SEIPD Packet (Symmetrically Encrypted Integrity Protected Data)
```rust
fn build_seipd_packet(data: &[u8], session_key: &[u8], cipher: Cipher) -> Vec<u8> {
    // Tag 18: Symmetrically Encrypted Integrity Protected Data
    let mut packet = vec![0xD2]; // Tag 18

    // Version 1
    packet.push(1);

    // Build plaintext with MDC
    let mut plaintext = vec![];

    // Add prefix (for CFB resync)
    let prefix = random_bytes(cipher.block_size());
    plaintext.extend_from_slice(&prefix);
    plaintext.extend_from_slice(&prefix[prefix.len()-2..]);

    // Add actual data packets
    plaintext.extend_from_slice(&build_literal_data_packet(data));

    // Add MDC packet (Tag 19)
    let mdc_hash = sha1(&plaintext);
    plaintext.push(0xD3); // Tag 19
    plaintext.push(20); // SHA-1 length
    plaintext.extend_from_slice(&mdc_hash);

    // Encrypt
    let encrypted = encrypt_cfb(&plaintext, session_key, &iv);
    packet.extend_from_slice(&encrypted);

    prepend_packet_length(&mut packet);
    packet
}
```

#### 4. Literal Data Packet
```rust
fn build_literal_data_packet(data: &[u8]) -> Vec<u8> {
    // Tag 11: Literal Data
    let mut packet = vec![0xCB]; // Tag 11

    // Format: 'b' for binary, 't' for text
    packet.push(b'b');

    // Filename length and name (0 for none)
    packet.push(0);

    // Timestamp (4 bytes, Unix time)
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    packet.extend_from_slice(&timestamp.to_be_bytes());

    // Actual data
    packet.extend_from_slice(data);

    prepend_packet_length(&mut packet);
    packet
}
```

#### 5. Compression Packet (Optional)
```rust
fn build_compressed_packet(data: &[u8], algo: CompressionAlgorithm) -> Vec<u8> {
    // Tag 8: Compressed Data
    let mut packet = vec![0xC8]; // Tag 8

    // Compression algorithm
    packet.push(algo.to_pgp_id()); // 1=ZIP, 2=ZLIB, 3=BZIP2

    // Compressed data
    let compressed = compress(data, algo);
    packet.extend_from_slice(&compressed);

    prepend_packet_length(&mut packet);
    packet
}
```

#### 6. Complete Encryption Flow
```rust
fn pgp_sym_encrypt_compatible(data: &str, password: &str, opts: &Options) -> Vec<u8> {
    // 1. Generate random session key
    let session_key = random_bytes(opts.cipher.key_size());

    // 2. Build literal data packet
    let mut payload = build_literal_data_packet(data.as_bytes());

    // 3. Optionally compress
    if opts.compress_algo != CompressionAlgorithm::None {
        payload = build_compressed_packet(&payload, opts.compress_algo);
    }

    // 4. Build SEIPD packet (encrypts payload with session key, adds MDC)
    let seipd = build_seipd_packet(&payload, &session_key, opts.cipher);

    // 5. Build SKESK packet (encrypts session key with password-derived key)
    let skesk = build_skesk_packet(password, &session_key, opts.cipher, &opts);

    // 6. Concatenate packets
    let mut result = Vec::new();
    result.extend_from_slice(&skesk);
    result.extend_from_slice(&seipd);
    result
}
```

**Pros:**
- No heavy dependencies
- Full control over implementation
- Can match PostgreSQL exactly

**Cons:**
- ~1500-2000 lines of complex code
- Need to handle all packet types
- Need extensive testing
- Packet length encoding is tricky
- MDC computation needs to be exact

**Estimated effort:** 3-5 days

### Option 3: Hybrid Approach
Keep current simple format but add an option to use OpenPGP format:

```sql
-- Simple format (fast, current implementation)
SELECT pgp_sym_encrypt('data', 'password');

-- OpenPGP format (compatible with PostgreSQL)
SELECT pgp_sym_encrypt('data', 'password', 'format=openpgp');
```

**Estimated effort:** 2-3 days

## Testing Full Compatibility

To verify PostgreSQL compatibility, you would need:

```sql
-- In PostgreSQL:
\c postgres
CREATE EXTENSION pgcrypto;
SELECT encode(pgp_sym_encrypt('test data', 'password'), 'hex');
-- Output: c30d04070302a8e2c7d2f96b8d5a...

-- In RisingWave:
SELECT pgp_sym_decrypt(
    decode('c30d04070302a8e2c7d2f96b8d5a...', 'hex'),
    'password'
);
-- Should output: 'test data'

-- And vice versa:
-- Encrypt in RisingWave, decrypt in PostgreSQL
```

## Recommendation

Given your requirements, I recommend:

### For Now (Current Implementation)
✅ **Keep the simplified format** because:
1. You have working, tested code
2. API is PostgreSQL-compatible
3. No cross-system data exchange was mentioned
4. Time-efficient solution

### If Cross-Compatibility Becomes Required
Then implement **Option 1 (Sequoia-PGP)**:
1. It's the most reliable path
2. 2-3 days of work
3. Full RFC 4880 compliance
4. Can coexist with current implementation

## What Would You Like To Do?

1. **Keep current implementation** (recommended)
   - Works perfectly for RisingWave-only use
   - Fast, simple, tested

2. **Add OpenPGP compatibility**
   - Use Sequoia-PGP library
   - 2-3 days additional work
   - Full PostgreSQL binary compatibility

3. **Implement OpenPGP manually**
   - More control, no heavy deps
   - 3-5 days of work
   - Need extensive testing

Let me know your preference and I can proceed accordingly!

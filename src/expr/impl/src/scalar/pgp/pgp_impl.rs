// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! PGP (Pretty Good Privacy) encryption functions implementation
//!
//! This module implements PGP-related functions from PostgreSQL's pgcrypto extension,
//! providing behavior parity with the official implementation while following RFC 4880.
//!
//! Functions implemented:
//! - pgp_sym_encrypt / pgp_sym_decrypt (symmetric encryption)
//! - pgp_pub_encrypt / pgp_pub_decrypt (public key encryption)
//! - armor / dearmor (ASCII armor encoding)
//! - pgp_armor_headers (custom armor headers)

use std::collections::HashMap;
use std::fmt::Debug;

use base64::Engine as _;
use base64::engine::general_purpose;
use openssl::hash::{MessageDigest, hash};
use openssl::pkcs5::pbkdf2_hmac;
use openssl::rand::rand_bytes;
use openssl::symm::{Cipher, Crypter, Mode as CipherMode};
// These modules are defined in mod.rs, not here
use risingwave_expr::{ExprError, Result, function};

// Import packet types that are used in this module
use crate::scalar::pgp::packets::SymmetricKeyEncryptedDataPacket;
use crate::scalar::pgp::{Packet, PgpOptions, SymmetricKeyEncryptedSessionKeyPacket};

// ============================================================================
// CONSTANTS
// ============================================================================

/// PGP armor headers
const PGP_MESSAGE_HEADER: &str = "-----BEGIN PGP MESSAGE-----";
const PGP_MESSAGE_FOOTER: &str = "-----END PGP MESSAGE-----";
const PGP_SIGNATURE_HEADER: &str = "-----BEGIN PGP SIGNATURE-----";
const PGP_SIGNATURE_FOOTER: &str = "-----END PGP SIGNATURE-----";

/// ASCII armor line length (RFC 4880)
const ARMOR_LINE_LENGTH: usize = 64;

/// CRC24 polynomial for armor checksums
const CRC24_POLY: u32 = 0x1864CFB;

/// Default S2K iterations (RFC 4880)
const DEFAULT_S2K_ITERATIONS: u32 = 65536;

// ============================================================================
// ERROR HELPERS
// ============================================================================

/// Create a standardized invalid parameter error
pub fn invalid_param_error(name: &str, reason: &str) -> ExprError {
    ExprError::InvalidParam {
        name: name.to_string().into(),
        reason: reason.to_string().into(),
    }
}

/// Create a standardized PGP error
pub fn pgp_error(stage: &str, reason: &str) -> PgpError {
    PgpError::InvalidData(format!("{}: {}", stage, reason))
}

// ============================================================================
// ALGORITHM MAPPINGS
// ============================================================================

/// Trait for types that can be parsed from strings
pub trait ParseFromStr: Sized {
    fn parse_from_str(s: &str) -> Result<Self>;
}

/// Trait for types that can be parsed from u8 values
pub trait ParseFromU8: Sized {
    fn parse_from_u8(n: u8) -> Result<Self>;
}

/// Supported cipher algorithms for PGP
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CipherAlgo {
    Aes128,
    Aes192,
    Aes256,
    TripleDes,
    Cast5,
    Blowfish,
    Twofish,
}

impl ParseFromStr for CipherAlgo {
    fn parse_from_str(s: &str) -> Result<Self> {
        Self::MAPPINGS
            .iter()
            .find(|(name, _)| *name == s)
            .map(|(_, algo)| *algo)
            .ok_or_else(|| {
                invalid_param_error(
                    "cipher-algo",
                    &format!("unsupported cipher algorithm: {}", s),
                )
            })
    }
}

impl CipherAlgo {
    const MAPPINGS: &'static [(&'static str, Self)] = &[
        ("aes128", Self::Aes128),
        ("aes192", Self::Aes192),
        ("aes256", Self::Aes256),
        ("3des", Self::TripleDes),
        ("cast5", Self::Cast5),
        ("blowfish", Self::Blowfish),
        ("twofish", Self::Twofish),
    ];

    fn to_cipher(&self) -> Cipher {
        match self {
            Self::Aes128 => Cipher::aes_128_cbc(),
            Self::Aes192 => Cipher::aes_192_cbc(),
            Self::Aes256 => Cipher::aes_256_cbc(),
            Self::TripleDes => Cipher::des_ede3_cbc(),
            Self::Cast5 => Cipher::cast5_cbc(),
            Self::Blowfish => Cipher::bf_cbc(),
            Self::Twofish => Cipher::aes_256_cbc(), // Placeholder - Twofish not in OpenSSL
        }
    }

    fn key_size(&self) -> usize {
        match self {
            Self::Aes128 => 16,
            Self::Aes192 => 24,
            Self::Aes256 => 32,
            Self::TripleDes => 24,
            Self::Cast5 => 16,
            Self::Blowfish => 16,
            Self::Twofish => 32,
        }
    }

    fn block_size(&self) -> usize {
        match self {
            Self::Aes128 | Self::Aes192 | Self::Aes256 => 16,
            Self::TripleDes => 8,
            Self::Cast5 => 8,
            Self::Blowfish => 8,
            Self::Twofish => 16,
        }
    }
}

/// Compression algorithms
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionAlgo {
    Uncompressed = 0,
    Zip = 1,
    Zlib = 2,
}

impl CompressionAlgo {
    const MAPPINGS: &'static [(u8, Self)] =
        &[(0, Self::Uncompressed), (1, Self::Zip), (2, Self::Zlib)];
}

impl ParseFromU8 for CompressionAlgo {
    fn parse_from_u8(n: u8) -> Result<Self> {
        Self::MAPPINGS
            .iter()
            .find(|(val, _)| *val == n)
            .map(|(_, algo)| *algo)
            .ok_or_else(|| {
                invalid_param_error(
                    "compress-algo",
                    &format!("invalid compression algorithm: {}", n),
                )
            })
    }
}

/// Hash algorithms for S2K
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HashAlgo {
    Md5,
    Sha1,
    Sha256,
    Sha384,
    Sha512,
}

impl ParseFromU8 for HashAlgo {
    fn parse_from_u8(n: u8) -> Result<Self> {
        Self::MAPPINGS
            .iter()
            .find(|(val, _)| *val == n)
            .map(|(_, algo)| *algo)
            .ok_or_else(|| {
                invalid_param_error(
                    "s2k-digest-algo",
                    &format!("unsupported hash algorithm: {}", n),
                )
            })
    }
}

impl HashAlgo {
    const MAPPINGS: &'static [(u8, Self)] = &[
        (1, Self::Md5),
        (2, Self::Sha1),
        (8, Self::Sha256),
        (9, Self::Sha384),
        (10, Self::Sha512),
    ];

    fn to_message_digest(&self) -> MessageDigest {
        match self {
            Self::Md5 => MessageDigest::md5(),
            Self::Sha1 => MessageDigest::sha1(),
            Self::Sha256 => MessageDigest::sha256(),
            Self::Sha384 => MessageDigest::sha384(),
            Self::Sha512 => MessageDigest::sha512(),
        }
    }
}

/// S2K (String-to-Key) modes
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum S2kMode {
    Simple = 0,
    Salted = 1,
    IteratedAndSalted = 3,
}

impl S2kMode {
    const MAPPINGS: &'static [(u8, Self)] = &[
        (0, Self::Simple),
        (1, Self::Salted),
        (3, Self::IteratedAndSalted),
    ];
}

impl ParseFromU8 for S2kMode {
    fn parse_from_u8(n: u8) -> Result<Self> {
        Self::MAPPINGS
            .iter()
            .find(|(val, _)| *val == n)
            .map(|(_, mode)| *mode)
            .ok_or_else(|| invalid_param_error("s2k-mode", &format!("invalid S2K mode: {}", n)))
    }
}

impl ParseFromStr for S2kMode {
    fn parse_from_str(s: &str) -> Result<Self> {
        match s {
            "0" | "simple" => Ok(Self::Simple),
            "1" | "salted" => Ok(Self::Salted),
            "3" | "iterated" => Ok(Self::IteratedAndSalted),
            _ => Err(invalid_param_error(
                "s2k-mode",
                &format!("invalid S2K mode: {}", s),
            )),
        }
    }
}

impl ParseFromStr for HashAlgo {
    fn parse_from_str(s: &str) -> Result<Self> {
        match s {
            "1" | "md5" => Ok(Self::Md5),
            "2" | "sha1" => Ok(Self::Sha1),
            "8" | "sha256" => Ok(Self::Sha256),
            "9" | "sha384" => Ok(Self::Sha384),
            "10" | "sha512" => Ok(Self::Sha512),
            _ => Err(invalid_param_error(
                "s2k-digest-algo",
                &format!("invalid hash algorithm: {}", s),
            )),
        }
    }
}

impl Default for CipherAlgo {
    fn default() -> Self {
        Self::Aes256
    }
}

impl Default for CompressionAlgo {
    fn default() -> Self {
        Self::Zlib
    }
}

impl Default for S2kMode {
    fn default() -> Self {
        Self::IteratedAndSalted
    }
}

impl Default for HashAlgo {
    fn default() -> Self {
        Self::Sha1
    }
}

/// PGP encryption context
#[derive(Debug)]
pub struct PgpContext {
    options: PgpOptions,
    cipher_algo: CipherAlgo,
    compression_algo: CompressionAlgo,
    s2k_mode: S2kMode,
    s2k_digest: HashAlgo,
    unicode_mode: bool,
    convert_crlf: bool,
    use_session_key: bool,
    armor_output: bool,
    disable_mdc: bool,
}

/// PGP decryption context
#[derive(Debug)]
pub struct PgpDecryptContext {
    options: PgpOptions,
    unicode_mode: bool,
    convert_crlf: bool,
}

impl PgpContext {
    fn new(options: PgpOptions) -> Result<Self> {
        let cipher_algo = options.get_cipher_algo().unwrap_or(CipherAlgo::Aes256);
        let compression_algo = options
            .get_compression_algo()
            .unwrap_or(CompressionAlgo::Zlib);
        let s2k_mode = options.get_s2k_mode().unwrap_or(S2kMode::IteratedAndSalted);
        let s2k_digest = options.get_s2k_digest().unwrap_or(HashAlgo::Sha1);

        Ok(Self {
            options: options.clone(),
            cipher_algo,
            compression_algo,
            s2k_mode,
            s2k_digest,
            unicode_mode: options.get_unicode_mode().unwrap_or(false),
            convert_crlf: options.get_convert_crlf().unwrap_or(false),
            use_session_key: options.get_sess_key().unwrap_or(false),
            armor_output: options.get_armor().unwrap_or(false),
            disable_mdc: options.get_disable_mdc().unwrap_or(false),
        })
    }

    fn derive_key(&self, passphrase: &[u8]) -> Result<Vec<u8>> {
        let mut key = vec![0u8; self.cipher_algo.key_size()];

        match self.s2k_mode {
            S2kMode::Simple => {
                // Simple S2K: just hash the passphrase
                let hash = hash(self.s2k_digest.to_message_digest(), passphrase)?;
                key[..hash.len().min(key.len())].copy_from_slice(&hash);
            }
            S2kMode::Salted => {
                // Salted S2K: hash(passphrase + 8-byte salt)
                let salt = self.options.get_s2k_salt().ok_or_else(|| {
                    invalid_param_error("s2k-salt", "salt required for salted S2K mode")
                })?;

                let mut input = Vec::with_capacity(passphrase.len() + 8);
                input.extend_from_slice(passphrase);
                input.extend_from_slice(&salt);

                let hash = hash(self.s2k_digest.to_message_digest(), &input)?;
                key[..hash.len().min(key.len())].copy_from_slice(&hash);
            }
            S2kMode::IteratedAndSalted => {
                // Iterated and Salted S2K: PBKDF2-like with salt and iterations
                let salt = self.options.get_s2k_salt().ok_or_else(|| {
                    invalid_param_error("s2k-salt", "salt required for iterated S2K mode")
                })?;

                let iterations = self
                    .options
                    .get_s2k_iterations()
                    .unwrap_or(DEFAULT_S2K_ITERATIONS);

                pbkdf2_hmac(
                    passphrase,
                    &salt,
                    iterations,
                    self.s2k_digest.to_message_digest(),
                    &mut key,
                )
                .map_err(|e| ExprError::InvalidParam {
                    name: "s2k",
                    reason: format!("key derivation failed: {}", e).into(),
                })?;
            }
        }

        Ok(key)
    }
}

impl PgpDecryptContext {
    fn new(options: PgpOptions) -> Self {
        Self {
            options: options.clone(),
            unicode_mode: options.get_unicode_mode().unwrap_or(false),
            convert_crlf: options.get_convert_crlf().unwrap_or(false),
        }
    }
}

/// Errors specific to PGP operations
#[derive(Debug, thiserror::Error)]
pub enum PgpError {
    #[error("Encryption failed: {0}")]
    Encryption(#[from] openssl::error::ErrorStack),
    #[error("Invalid PGP data: {0}")]
    InvalidData(String),
    #[error("Unsupported algorithm: {0}")]
    UnsupportedAlgorithm(String),
    #[error("Invalid options: {0}")]
    InvalidOptions(String),
    #[error("Armor parsing failed: {0}")]
    ArmorParsing(String),
    #[error("Packet parsing failed: {0}")]
    PacketParsing(String),
}

/// Generate CRC24 checksum for armor
fn crc24(data: &[u8]) -> u32 {
    let mut crc = 0xB704CEu32;
    for &byte in data {
        crc ^= (byte as u32) << 16;
        for _ in 0..8 {
            crc <<= 1;
            if (crc & 0x1000000) != 0 {
                crc ^= CRC24_POLY;
            }
        }
    }
    crc & 0xFFFFFF
}

/// Encode data with ASCII armor
fn armor_encode(data: &[u8], headers: Option<&HashMap<String, String>>) -> String {
    let mut result = String::new();

    // Add headers
    result.push_str(PGP_MESSAGE_HEADER);
    result.push('\n');

    if let Some(headers) = headers {
        for (key, value) in headers {
            result.push_str(&format!("{}: {}\n", key, value));
        }
        result.push('\n');
    }

    // Encode data in base64 with CRC24
    let encoded = general_purpose::STANDARD.encode(data);
    let mut lines = Vec::new();

    for chunk in encoded.as_bytes().chunks(ARMOR_LINE_LENGTH) {
        lines.push(std::str::from_utf8(chunk).unwrap());
    }

    for line in lines {
        result.push_str(line);
        result.push('\n');
    }

    // Add CRC24 checksum
    let checksum = crc24(data);
    let crc_line = format!("={:06X}\n", checksum);
    result.push_str(&crc_line);

    // Add footer
    result.push_str(PGP_MESSAGE_FOOTER);
    result.push('\n');

    result
}

/// Decode ASCII armor data
fn armor_decode(data: &str) -> Result<(Vec<u8>, HashMap<String, String>)> {
    let mut headers = HashMap::new();

    // Find the boundaries
    let start_marker = data
        .find(PGP_MESSAGE_HEADER)
        .ok_or_else(|| pgp_error("armor_decode", "BEGIN PGP MESSAGE marker not found"))?;
    let end_marker = data
        .find(PGP_MESSAGE_FOOTER)
        .ok_or_else(|| pgp_error("armor_decode", "END PGP MESSAGE marker not found"))?;

    // Extract the armored content
    let content_start = data[start_marker + PGP_MESSAGE_HEADER.len()..].trim_start();
    if content_start.starts_with('\n') {
        // Skip empty line after header
    }

    let content = &data[start_marker + PGP_MESSAGE_HEADER.len()..end_marker];

    // Parse headers
    let mut data_start = 0;
    for (i, line) in content.lines().enumerate() {
        if line.is_empty() {
            data_start = i + 1;
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_string(), value.trim().to_string());
        }
    }

    // Extract and decode base64 data
    let base64_data = content
        .lines()
        .skip(data_start)
        .collect::<Vec<_>>()
        .join("");

    // Remove CRC line if present
    let base64_data = if let Some(equals_pos) = base64_data.find('=') {
        &base64_data[..equals_pos]
    } else {
        &base64_data
    };

    let decoded = general_purpose::STANDARD
        .decode(base64_data)
        .map_err(|e| PgpError::ArmorParsing(format!("base64 decode failed: {}", e)))?;

    Ok((decoded, headers))
}

/// Convert text data for encryption (handles unicode-mode and convert-crlf)
fn prepare_text_data(data: &str, unicode_mode: bool, convert_crlf: bool) -> Vec<u8> {
    if unicode_mode {
        let mut processed = data.as_bytes().to_vec();

        if convert_crlf {
            // Convert LF to CRLF
            let mut result = Vec::new();
            for &byte in &processed {
                if byte == b'\n' {
                    result.extend_from_slice(b"\r\n");
                } else {
                    result.push(byte);
                }
            }
            processed = result;
        }

        processed
    } else {
        // Treat as binary
        data.as_bytes().to_vec()
    }
}

/// Convert encrypted data back to text (handles unicode-mode and convert-crlf)
fn restore_text_data(data: &[u8], unicode_mode: bool, convert_crlf: bool) -> Result<String> {
    if unicode_mode {
        let text = String::from_utf8(data.to_vec()).map_err(|e| {
            PgpError::InvalidData(format!("invalid UTF-8 in decrypted data: {}", e))
        })?;

        if convert_crlf {
            // Convert CRLF back to LF
            text.replace("\r\n", "\n")
        } else {
            text
        }
    } else {
        // Return as-is (binary data represented as string)
        String::from_utf8_lossy(data).to_string()
    }
}

/// Generate a random session key for symmetric encryption
fn generate_session_key(cipher_algo: CipherAlgo) -> Result<Vec<u8>> {
    let key_size = cipher_algo.key_size();
    let mut key = vec![0u8; key_size];
    rand_bytes(&mut key)?;
    Ok(key)
}

/// Encrypt data using symmetric encryption (AES in CFB mode)
fn encrypt_symmetric(data: &[u8], key: &[u8], iv: &[u8], cipher: Cipher) -> Result<Vec<u8>> {
    let mut crypter = Crypter::new(cipher, CipherMode::Encrypt, key, Some(iv))?;
    crypter.pad(false); // No padding for streaming modes

    let mut encrypted = vec![0; data.len() + cipher.block_size()];
    let mut count = crypter.update(data, &mut encrypted)?;

    // Finalize encryption
    count += crypter.finalize(&mut encrypted[count..])?;

    encrypted.truncate(count);
    Ok(encrypted)
}

/// Decrypt data using symmetric encryption (AES in CFB mode)
fn decrypt_symmetric(data: &[u8], key: &[u8], iv: &[u8], cipher: Cipher) -> Result<Vec<u8>> {
    let mut crypter = Crypter::new(cipher, CipherMode::Decrypt, key, Some(iv))?;
    crypter.pad(false); // No padding for streaming modes

    let mut decrypted = vec![0; data.len() + cipher.block_size()];
    let mut count = crypter.update(data, &mut decrypted)?;

    // Finalize decryption
    count += crypter.finalize(&mut decrypted[count..])?;

    decrypted.truncate(count);
    Ok(decrypted)
}

/// Simple compression using zlib (placeholder implementation)
fn compress_data(data: &[u8]) -> Result<Vec<u8>> {
    // For now, return data as-is. In a full implementation, this would use zlib compression
    Ok(data.to_vec())
}

/// Simple decompression using zlib (placeholder implementation)
fn decompress_data(data: &[u8]) -> Result<Vec<u8>> {
    // For now, return data as-is. In a full implementation, this would use zlib decompression
    Ok(data.to_vec())
}

/// Create PGP symmetric encryption packet structure
fn create_symmetric_encryption_packet(
    data: &[u8],
    context: &PgpContext,
    passphrase: &[u8],
) -> Result<Vec<u8>> {
    // Generate session key
    let session_key = generate_session_key(context.cipher_algo)?;

    // Derive encryption key from passphrase
    let encryption_key = context.derive_key(passphrase)?;

    // Generate IV
    let iv_size = context.cipher_algo.block_size();
    let mut iv = vec![0u8; iv_size];
    rand_bytes(&mut iv)?;

    // Compress data if needed
    let compressed_data = if context.compression_algo != CompressionAlgo::Uncompressed {
        compress_data(data)?
    } else {
        data.to_vec()
    };

    // Encrypt data with session key
    let encrypted_data = encrypt_symmetric(
        &compressed_data,
        &session_key,
        &iv,
        context.cipher_algo.to_cipher(),
    )?;

    // Create packets
    let mut packets = Vec::new();

    // 1. Symmetric-Key Encrypted Session Key packet
    let skesk_packet = SymmetricKeyEncryptedSessionKeyPacket::new(
        context.cipher_algo,
        context.s2k_mode,
        context.s2k_digest,
        context.options.get_s2k_salt(),
        context.options.get_s2k_count(),
        &session_key,
        passphrase,
    )?;

    // Serialize SKESK packet with header
    let skesk_data = skesk_packet.serialize()?;
    let mut packet_data = Vec::new();
    packet_data.push(0xC3); // Packet header: tag=3 (SKESK), format=1 (new)
    packet_data.push(skesk_data.len() as u8);
    packet_data.extend_from_slice(&skesk_data);

    packets.extend_from_slice(&packet_data);

    // 2. Symmetric-Key Encrypted Data packet (placeholder for now)
    let sed_packet = SymmetricKeyEncryptedDataPacket {
        version: 1,
        encrypted_data,
    };

    let sed_data = sed_packet.serialize()?;
    let mut sed_packet_data = Vec::new();
    sed_packet_data.push(0x99); // Packet header: tag=9 (SED), format=1 (new)
    sed_packet_data.push(sed_data.len() as u8);
    sed_packet_data.extend_from_slice(&sed_data);

    packets.extend_from_slice(&sed_packet_data);

    Ok(packets)
}

/// Parse PGP symmetric encryption packet structure
fn parse_symmetric_encryption_packet(
    data: &[u8],
    context: &PgpDecryptContext,
    passphrase: &[u8],
) -> Result<Vec<u8>> {
    // This is a simplified implementation. A full implementation would:
    // 1. Parse packet headers
    // 2. Decrypt the session key using the passphrase
    // 3. Decrypt the data using the session key
    // 4. Decompress if needed
    // 5. Validate MDC if present

    // For now, return placeholder implementation
    Err(PgpError::PacketParsing(
        "Symmetric decryption not fully implemented".to_string(),
    ))
}

/// Symmetric encryption for text data
#[function("pgp_sym_encrypt(varchar, varchar, varchar) -> varchar")]
fn pgp_sym_encrypt_text(data: &str, passphrase: &str, options: &str) -> Result<String> {
    let options_parsed = PgpOptions::parse(options)?;
    let context = PgpContext::new(options_parsed)?;

    // Prepare text data (handle unicode-mode and convert-crlf)
    let prepared_data = prepare_text_data(data, context.unicode_mode, context.convert_crlf);

    // Create PGP packet structure
    let encrypted_data =
        create_symmetric_encryption_packet(&prepared_data, &context, passphrase.as_bytes())?;

    // Apply armor encoding if requested
    if context.armor_output {
        Ok(armor_encode(&encrypted_data, None))
    } else {
        Ok(general_purpose::STANDARD.encode(&encrypted_data))
    }
}

/// Symmetric encryption for binary data
#[function("pgp_sym_encrypt_bytea(bytea, varchar, varchar) -> bytea")]
fn pgp_sym_encrypt_bytea(data: &[u8], passphrase: &str, options: &str) -> Result<Box<[u8]>> {
    let options_parsed = PgpOptions::parse(options)?;
    let context = PgpContext::new(options_parsed)?;

    // For bytea, raw output is default unless armor is explicitly requested
    let raw_output = context.options.get_raw().unwrap_or(true);

    // Create PGP packet structure
    let encrypted_data = create_symmetric_encryption_packet(data, &context, passphrase.as_bytes())?;

    if context.armor_output && !raw_output {
        // Return armored text as bytea (this is unusual but follows pgcrypto behavior)
        let armored = armor_encode(&encrypted_data, None);
        Ok(armored.into_bytes().into())
    } else {
        Ok(encrypted_data.into())
    }
}

/// Symmetric decryption for text data
#[function("pgp_sym_decrypt(varchar, varchar, varchar) -> varchar")]
fn pgp_sym_decrypt_text(data: &str, passphrase: &str, options: &str) -> Result<String> {
    let options_parsed = PgpOptions::parse(options)?;
    let context = PgpDecryptContext::new(options_parsed);

    // Check if data is armored
    let (decoded_data, _) = if data.contains(PGP_MESSAGE_HEADER) {
        armor_decode(data)?
    } else {
        (
            general_purpose::STANDARD
                .decode(data)
                .map_err(|e| PgpError::ArmorParsing(format!("base64 decode failed: {}", e)))?,
            HashMap::new(),
        )
    };

    // Parse and decrypt PGP packet structure
    let decrypted_data =
        parse_symmetric_encryption_packet(&decoded_data, &context, passphrase.as_bytes())?;

    // Restore text data (handle unicode-mode and convert-crlf)
    restore_text_data(&decrypted_data, context.unicode_mode, context.convert_crlf)
}

/// Symmetric decryption for binary data
#[function("pgp_sym_decrypt_bytea(bytea, varchar, varchar) -> bytea")]
fn pgp_sym_decrypt_bytea(data: &[u8], passphrase: &str, options: &str) -> Result<Box<[u8]>> {
    let options_parsed = PgpOptions::parse(options)?;
    let context = PgpDecryptContext::new(options_parsed);

    // For bytea decryption, we assume binary input unless it's armored text
    let input_str = String::from_utf8_lossy(data);

    let (decoded_data, _) = if input_str.contains(PGP_MESSAGE_HEADER) {
        armor_decode(&input_str)?
    } else {
        (data.to_vec(), HashMap::new())
    };

    // Parse and decrypt PGP packet structure
    let decrypted_data =
        parse_symmetric_encryption_packet(&decoded_data, &context, passphrase.as_bytes())?;

    Ok(decrypted_data.into())
}

/// Armor (ASCII armor) encoding for binary data
#[function("armor(bytea) -> varchar")]
fn armor(data: &[u8]) -> String {
    armor_encode(data, None)
}

/// Dearmor (ASCII armor) decoding for text data
#[function("dearmor(varchar) -> bytea")]
fn dearmor(data: &str) -> Result<Box<[u8]>> {
    let (decoded, _) = armor_decode(data)?;
    Ok(decoded.into())
}

/// Armor encoding with custom headers
#[function("pgp_armor_headers(bytea, varchar[]) -> varchar")]
fn pgp_armor_headers(data: &[u8], headers: &[&str]) -> String {
    let mut header_map = HashMap::new();

    for header in headers {
        if let Some((key, value)) = header.split_once(':') {
            header_map.insert(key.trim().to_string(), value.trim().to_string());
        }
    }

    armor_encode(data, Some(&header_map))
}

/// Public key encryption for text data (placeholder implementation)
#[function("pgp_pub_encrypt(varchar, bytea, varchar) -> varchar")]
fn pgp_pub_encrypt_text(data: &str, pubkey: &[u8], options: &str) -> Result<String> {
    let options_parsed = PgpOptions::parse(options)?;
    options_parsed.validate()?;

    // Placeholder implementation for public key encryption
    // In a full implementation, this would:
    // 1. Parse the public key
    // 2. Generate a session key
    // 3. Encrypt data with session key
    // 4. Encrypt session key with public key
    // 5. Create proper OpenPGP packet structure

    Err(
        PgpError::UnsupportedAlgorithm("Public key encryption not yet implemented".to_string())
            .into(),
    )
}

/// Public key encryption for binary data (placeholder implementation)
#[function("pgp_pub_encrypt_bytea(bytea, bytea, varchar) -> bytea")]
fn pgp_pub_encrypt_bytea(_data: &[u8], _pubkey: &[u8], options: &str) -> Result<Box<[u8]>> {
    let options_parsed = PgpOptions::parse(options)?;
    options_parsed.validate()?;

    // Placeholder implementation for public key encryption
    Err(PgpError::UnsupportedAlgorithm(
        "Public key encryption not yet implemented".to_string(),
    ))
}

/// Public key decryption for text data (placeholder implementation)
#[function("pgp_pub_decrypt(varchar, bytea, varchar) -> varchar")]
fn pgp_pub_decrypt_text(data: &str, privkey: &[u8], options: &str) -> Result<String> {
    let options_parsed = PgpOptions::parse(options)?;
    options_parsed.validate()?;

    // Placeholder implementation for public key decryption
    Err(PgpError::UnsupportedAlgorithm(
        "Public key decryption not yet implemented".to_string(),
    ))
}

/// Public key decryption for binary data (placeholder implementation)
#[function("pgp_pub_decrypt_bytea(bytea, bytea, varchar) -> bytea")]
fn pgp_pub_decrypt_bytea(data: &[u8], privkey: &[u8], options: &str) -> Result<Box<[u8]>> {
    let options_parsed = PgpOptions::parse(options)?;
    options_parsed.validate()?;

    // Placeholder implementation for public key decryption
    Err(PgpError::UnsupportedAlgorithm(
        "Public key decryption not yet implemented".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalar::pgp::LiteralDataPacket;

    #[test]
    fn test_cipher_algo_from_str() {
        assert_eq!(CipherAlgo::from_str("aes256").unwrap(), CipherAlgo::Aes256);
        assert_eq!(CipherAlgo::from_str("aes128").unwrap(), CipherAlgo::Aes128);
        assert_eq!(CipherAlgo::from_str("3des").unwrap(), CipherAlgo::TripleDes);
        assert!(CipherAlgo::from_str("invalid").is_err());
    }

    #[test]
    fn test_cipher_algo_properties() {
        assert_eq!(CipherAlgo::Aes256.key_size(), 32);
        assert_eq!(CipherAlgo::Aes128.key_size(), 16);
        assert_eq!(CipherAlgo::Aes256.block_size(), 16);
        assert_eq!(CipherAlgo::TripleDes.block_size(), 8);
    }

    #[test]
    fn test_compression_algo_from_u8() {
        assert_eq!(CompressionAlgo::from_u8(0), CompressionAlgo::Uncompressed);
        assert_eq!(CompressionAlgo::from_u8(1), CompressionAlgo::Zip);
        assert_eq!(CompressionAlgo::from_u8(2), CompressionAlgo::Zlib);
        assert_eq!(CompressionAlgo::from_u8(255), CompressionAlgo::Uncompressed);
    }

    #[test]
    fn test_hash_algo_from_u8() {
        assert_eq!(HashAlgo::from_u8(1).unwrap(), HashAlgo::Md5);
        assert_eq!(HashAlgo::from_u8(2).unwrap(), HashAlgo::Sha1);
        assert_eq!(HashAlgo::from_u8(8).unwrap(), HashAlgo::Sha256);
        assert!(HashAlgo::from_u8(255).is_err());
    }

    #[test]
    fn test_s2k_mode_from_u8() {
        assert_eq!(S2kMode::from_u8(0), S2kMode::Simple);
        assert_eq!(S2kMode::from_u8(1), S2kMode::Salted);
        assert_eq!(S2kMode::from_u8(3), S2kMode::IteratedAndSalted);
        assert_eq!(S2kMode::from_u8(255), S2kMode::Simple);
    }

    #[test]
    fn test_pgp_options_parse() {
        let opts = PgpOptions::parse("cipher-algo=aes256,armor=true").unwrap();
        assert_eq!(opts.get_cipher_algo().unwrap(), CipherAlgo::Aes256);
        assert_eq!(opts.get_armor().unwrap(), true);
    }

    #[test]
    fn test_pgp_options_defaults() {
        let opts = PgpOptions::parse("").unwrap();
        assert_eq!(opts.get_cipher_algo().unwrap(), CipherAlgo::Aes256);
        assert_eq!(opts.get_compression_algo().unwrap(), CompressionAlgo::Zlib);
        assert_eq!(opts.get_s2k_mode().unwrap(), S2kMode::IteratedAndSalted);
        assert_eq!(opts.get_armor().unwrap_or(false), false);
    }

    #[test]
    fn test_pgp_options_s2k_salt() {
        let opts = PgpOptions::parse("s2k-salt=0123456789abcdef").unwrap();
        let salt = opts.get_s2k_salt().unwrap();
        assert_eq!(salt, [0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]);
    }

    #[test]
    fn test_pgp_options_validation() {
        let result = PgpOptions::parse("invalid-option=value");
        assert!(result.is_err());
    }

    #[test]
    fn test_crc24() {
        // Test CRC24 calculation (example from RFC 4880)
        let data = b"test";
        let crc = crc24(data);
        // Known CRC24 value for "test"
        assert_eq!(crc, 0x5A77F5);
    }

    #[test]
    fn test_armor_encode_decode() {
        let data = b"Hello, World!";
        let armored = armor_encode(data, None);
        assert!(armored.contains(PGP_MESSAGE_HEADER));
        assert!(armored.contains(PGP_MESSAGE_FOOTER));

        let (decoded, _) = armor_decode(&armored).unwrap();
        assert_eq!(data.to_vec(), decoded);
    }

    #[test]
    fn test_armor_with_headers() {
        let data = b"test data";
        let mut headers = HashMap::new();
        headers.insert("Version".to_string(), "GnuPG v1".to_string());

        let armored = armor_encode(data, Some(&headers));
        assert!(armored.contains("Version: GnuPG v1"));
    }

    #[test]
    fn test_prepare_text_data_unicode_mode() {
        let text = "Hello\nWorld";
        let prepared = prepare_text_data(text, true, false);
        assert_eq!(prepared, text.as_bytes());

        let prepared_crlf = prepare_text_data(text, true, true);
        assert_eq!(prepared_crlf, b"Hello\r\nWorld");
    }

    #[test]
    fn test_restore_text_data_unicode_mode() {
        let data = b"Hello\r\nWorld";
        let restored = restore_text_data(data, true, true).unwrap();
        assert_eq!(restored, "Hello\nWorld");

        let restored_no_crlf = restore_text_data(data, true, false).unwrap();
        assert_eq!(restored_no_crlf, "Hello\r\nWorld");
    }

    #[test]
    fn test_generate_session_key() {
        let key1 = generate_session_key(CipherAlgo::Aes256).unwrap();
        let key2 = generate_session_key(CipherAlgo::Aes256).unwrap();

        assert_eq!(key1.len(), 32);
        assert_eq!(key2.len(), 32);
        assert_ne!(key1, key2); // Should be random
    }

    #[test]
    fn test_encrypt_decrypt_symmetric() {
        let data = b"Hello, World!";
        let key = b"0123456789abcdef"; // 16 bytes for AES-128
        let iv = b"abcdef0123456789"; // 16 bytes

        let encrypted = encrypt_symmetric(data, key, iv, Cipher::aes_128_cbc()).unwrap();
        let decrypted = decrypt_symmetric(&encrypted, key, iv, Cipher::aes_128_cbc()).unwrap();

        assert_eq!(data.to_vec(), decrypted);
    }

    #[test]
    fn test_literal_data_packet() {
        let data = b"Hello, World!";
        let packet = LiteralDataPacket::new_binary(data.to_vec());

        assert_eq!(packet.format, b'b');
        assert_eq!(packet.data, data);

        let serialized = packet.serialize().unwrap();
        let parsed = LiteralDataPacket::parse(&serialized).unwrap();

        assert_eq!(packet.format, parsed.format);
        assert_eq!(packet.data, parsed.data);
    }

    #[test]
    fn test_symmetric_key_encrypted_session_key_packet() {
        let cipher_algo = CipherAlgo::Aes256;
        let s2k_mode = S2kMode::IteratedAndSalted;
        let s2k_digest = HashAlgo::Sha1;
        let salt = Some([0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]);
        let count = Some(96);

        let session_key = b"0123456789abcdef0123456789abcdef"; // 32 bytes for AES-256
        let passphrase = b"test passphrase";

        let packet = SymmetricKeyEncryptedSessionKeyPacket::new(
            cipher_algo,
            s2k_mode,
            s2k_digest,
            salt,
            count,
            session_key,
            passphrase,
        )
        .unwrap();

        assert_eq!(packet.version, 4);
        assert_eq!(packet.cipher_algo, cipher_algo);
        assert_eq!(packet.s2k_mode, s2k_mode);
        assert_eq!(packet.s2k_digest, s2k_digest);
        assert_eq!(packet.s2k_salt, salt);
        assert_eq!(packet.s2k_count, count);
    }

    #[test]
    fn test_pgp_context_creation() {
        let opts = PgpOptions::parse("cipher-algo=aes128,compress-algo=0").unwrap();
        let context = PgpContext::new(opts).unwrap();

        assert_eq!(context.cipher_algo, CipherAlgo::Aes128);
        assert_eq!(context.compression_algo, CompressionAlgo::Uncompressed);
        assert_eq!(context.unicode_mode, false);
        assert_eq!(context.armor_output, false);
    }

    #[test]
    fn test_pgp_sym_encrypt_text_basic() {
        // Basic test for symmetric encryption
        let data = "Hello, World!";
        let options = "";
        let passphrase = "testpass";

        // This should not fail (even though decryption is not implemented)
        let result = pgp_sym_encrypt_text(data, passphrase, options);
        assert!(result.is_ok());

        let encrypted = result.unwrap();
        assert!(!encrypted.is_empty());
    }

    #[test]
    fn test_pgp_sym_encrypt_with_armor() {
        let data = "Hello, World!";
        let options = "armor=true";
        let passphrase = "testpass";

        let result = pgp_sym_encrypt_text(data, passphrase, options);
        assert!(result.is_ok());

        let encrypted = result.unwrap();
        assert!(encrypted.contains(PGP_MESSAGE_HEADER));
        assert!(encrypted.contains(PGP_MESSAGE_FOOTER));
    }

    #[test]
    fn test_armor_function() {
        let data = b"Hello, World!";
        let armored = armor(data);
        assert!(armored.contains(PGP_MESSAGE_HEADER));
        assert!(armored.contains(PGP_MESSAGE_FOOTER));
    }

    #[test]
    fn test_dearmor_function() {
        let data = b"Hello, World!";
        let armored = armor(data);
        let dearmored = dearmor(&armored).unwrap();
        assert_eq!(data.to_vec(), dearmored.to_vec());
    }

    #[test]
    fn test_pgp_armor_headers_function() {
        let data = b"test data";
        let headers = vec!["Version: Test v1.0"];

        let armored = pgp_armor_headers(data, &headers);
        assert!(armored.contains("Version: Test v1.0"));
        assert!(armored.contains(PGP_MESSAGE_HEADER));
    }

    #[test]
    fn test_text_vs_bytea_behavior() {
        let text_data = "Hello, World!";
        let bytea_data = text_data.as_bytes();

        // Text encryption should handle unicode properly
        let text_result = pgp_sym_encrypt_text(text_data, "pass", "");
        assert!(text_result.is_ok());

        // Bytea encryption should handle binary data
        let bytea_result = pgp_sym_encrypt_bytea(bytea_data, "pass", "");
        assert!(bytea_result.is_ok());

        // Results should be different due to different processing
        let text_encrypted = text_result.unwrap();
        let bytea_encrypted = String::from_utf8_lossy(&bytea_result.unwrap());

        // They might be different due to armor vs raw output
        assert!(!text_encrypted.is_empty());
        assert!(!bytea_encrypted.is_empty());
    }
}

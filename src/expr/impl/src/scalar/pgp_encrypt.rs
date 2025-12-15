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

//! PGP encryption/decryption functions compatible with PostgreSQL's pgcrypto
//!
//! Reference: https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-PGP-ENC-FUNCS

use risingwave_expr::{ExprError, Result, function};

/// Cipher algorithms supported by PGP
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CipherAlgorithm {
    /// Blowfish with 128-bit key
    Blowfish,
    /// AES with 128-bit key (default)
    Aes128,
    /// AES with 192-bit key
    Aes192,
    /// AES with 256-bit key
    Aes256,
    /// Triple-DES with 168-bit key (112-bit strength)
    TripleDes,
    /// CAST5 with 128-bit key
    Cast5,
}

impl Default for CipherAlgorithm {
    fn default() -> Self {
        CipherAlgorithm::Aes128
    }
}

impl CipherAlgorithm {
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "bf" | "blowfish" => Ok(CipherAlgorithm::Blowfish),
            "aes" | "aes128" => Ok(CipherAlgorithm::Aes128),
            "aes192" => Ok(CipherAlgorithm::Aes192),
            "aes256" => Ok(CipherAlgorithm::Aes256),
            "3des" => Ok(CipherAlgorithm::TripleDes),
            "cast5" => Ok(CipherAlgorithm::Cast5),
            _ => Err(ExprError::InvalidParam {
                name: "cipher-algo",
                reason: format!(
                    "invalid cipher algorithm: {}. Valid values: bf, aes128, aes192, aes256, 3des, cast5",
                    s
                )
                .into(),
            }),
        }
    }
}

/// Compression algorithms supported by PGP
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompressionAlgorithm {
    /// No compression
    None,
    /// ZIP compression (default)
    Zip,
    /// ZLIB compression
    Zlib,
    /// BZIP2 compression
    Bzip2,
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        CompressionAlgorithm::Zip
    }
}

impl CompressionAlgorithm {
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "0" => Ok(CompressionAlgorithm::None),
            "1" => Ok(CompressionAlgorithm::Zip),
            "2" => Ok(CompressionAlgorithm::Zlib),
            "3" => Ok(CompressionAlgorithm::Bzip2),
            _ => Err(ExprError::InvalidParam {
                name: "compress-algo",
                reason: format!(
                    "invalid compression algorithm: {}. Valid values: 0 (none), 1 (zip), 2 (zlib), 3 (bzip2)",
                    s
                )
                .into(),
            }),
        }
    }
}

/// S2K (String-to-Key) modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum S2kMode {
    /// Simple S2K (not recommended)
    Simple = 0,
    /// Salted S2K
    Salted = 1,
    /// Iterated and Salted S2K (default, most secure)
    IteratedSalted = 3,
}

impl Default for S2kMode {
    fn default() -> Self {
        S2kMode::IteratedSalted
    }
}

impl S2kMode {
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "0" => Ok(S2kMode::Simple),
            "1" => Ok(S2kMode::Salted),
            "3" => Ok(S2kMode::IteratedSalted),
            _ => Err(ExprError::InvalidParam {
                name: "s2k-mode",
                reason: format!(
                    "invalid S2K mode: {}. Valid values: 0 (simple), 1 (salted), 3 (iterated-salted)",
                    s
                )
                .into(),
            }),
        }
    }
}

/// Hash/digest algorithms for S2K
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DigestAlgorithm {
    Md5,
    Sha1,
    Ripemd160,
    Sha256,
    Sha384,
    Sha512,
}

impl Default for DigestAlgorithm {
    fn default() -> Self {
        DigestAlgorithm::Sha256
    }
}

impl DigestAlgorithm {
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "md5" => Ok(DigestAlgorithm::Md5),
            "sha1" => Ok(DigestAlgorithm::Sha1),
            "ripemd160" => Ok(DigestAlgorithm::Ripemd160),
            "sha256" => Ok(DigestAlgorithm::Sha256),
            "sha384" => Ok(DigestAlgorithm::Sha384),
            "sha512" => Ok(DigestAlgorithm::Sha512),
            _ => Err(ExprError::InvalidParam {
                name: "s2k-digest-algo",
                reason: format!(
                    "invalid digest algorithm: {}. Valid values: md5, sha1, ripemd160, sha256, sha384, sha512",
                    s
                )
                .into(),
            }),
        }
    }
}

/// Options for PGP encryption
#[derive(Debug, Clone)]
struct PgpEncryptOptions {
    /// Cipher algorithm to use
    cipher_algo: CipherAlgorithm,
    /// Compression algorithm
    compress_algo: CompressionAlgorithm,
    /// Compression level (0-9)
    compress_level: i32,
    /// Convert line breaks (\n to \r\n on encrypt, \r\n to \n on decrypt)
    convert_crlf: bool,
    /// Disable Modification Detection Code (not recommended)
    disable_mdc: bool,
    /// Use separate session key (default: true for public key, false for symmetric)
    sess_key: bool,
    /// S2K mode
    s2k_mode: S2kMode,
    /// S2K iteration count (for iterated mode)
    s2k_count: u32,
    /// S2K digest algorithm
    s2k_digest_algo: DigestAlgorithm,
    /// S2K cipher algorithm (can differ from main cipher)
    s2k_cipher_algo: CipherAlgorithm,
    /// Unicode mode - convert text to UTF-8
    unicode_mode: bool,
}

impl Default for PgpEncryptOptions {
    fn default() -> Self {
        PgpEncryptOptions {
            cipher_algo: CipherAlgorithm::Aes128,
            compress_algo: CompressionAlgorithm::Zip,
            compress_level: 6,
            convert_crlf: false,
            disable_mdc: false,
            sess_key: false, // Will be set based on encryption type
            s2k_mode: S2kMode::IteratedSalted,
            s2k_count: 65536, // Default iteration count
            s2k_digest_algo: DigestAlgorithm::Sha256,
            s2k_cipher_algo: CipherAlgorithm::Aes128,
            unicode_mode: false,
        }
    }
}

impl PgpEncryptOptions {
    /// Parse options string for encryption
    fn parse_encrypt(options_str: Option<&str>, is_public_key: bool) -> Result<Self> {
        let mut opts = PgpEncryptOptions::default();
        opts.sess_key = is_public_key; // Use session key by default for public key encryption

        if let Some(opts_str) = options_str {
            for opt in opts_str.split(',') {
                let opt = opt.trim();
                if opt.is_empty() {
                    continue;
                }

                let parts: Vec<&str> = opt.splitn(2, '=').collect();
                if parts.len() != 2 {
                    return Err(ExprError::InvalidParam {
                        name: "options",
                        reason: format!("invalid option format: '{}'. Expected 'key=value'", opt)
                            .into(),
                    });
                }

                let key = parts[0].trim();
                let value = parts[1].trim();

                match key {
                    "cipher-algo" => {
                        opts.cipher_algo = CipherAlgorithm::from_str(value)?;
                    }
                    "compress-algo" => {
                        opts.compress_algo = CompressionAlgorithm::from_str(value)?;
                    }
                    "compress-level" => {
                        opts.compress_level = value.parse::<i32>().map_err(|_| {
                            ExprError::InvalidParam {
                                name: "compress-level",
                                reason: format!("invalid integer: {}", value).into(),
                            }
                        })?;
                        if !(0..=9).contains(&opts.compress_level) {
                            return Err(ExprError::InvalidParam {
                                name: "compress-level",
                                reason: "compression level must be 0-9".into(),
                            });
                        }
                    }
                    "convert-crlf" => {
                        opts.convert_crlf = parse_bool(value, "convert-crlf")?;
                    }
                    "disable-mdc" => {
                        opts.disable_mdc = parse_bool(value, "disable-mdc")?;
                    }
                    "sess-key" => {
                        opts.sess_key = parse_bool(value, "sess-key")?;
                    }
                    "s2k-mode" => {
                        opts.s2k_mode = S2kMode::from_str(value)?;
                    }
                    "s2k-count" => {
                        opts.s2k_count = value.parse::<u32>().map_err(|_| {
                            ExprError::InvalidParam {
                                name: "s2k-count",
                                reason: format!("invalid integer: {}", value).into(),
                            }
                        })?;
                        // PostgreSQL requires count to be between 1024 and 65011712
                        // and a power of 2 greater than 1024
                        if opts.s2k_count < 1024 || opts.s2k_count > 65011712 {
                            return Err(ExprError::InvalidParam {
                                name: "s2k-count",
                                reason: "s2k-count must be between 1024 and 65011712".into(),
                            });
                        }
                        if opts.s2k_count > 1024 && !opts.s2k_count.is_power_of_two() {
                            return Err(ExprError::InvalidParam {
                                name: "s2k-count",
                                reason: "s2k-count must be a power of 2".into(),
                            });
                        }
                    }
                    "s2k-digest-algo" => {
                        opts.s2k_digest_algo = DigestAlgorithm::from_str(value)?;
                    }
                    "s2k-cipher-algo" => {
                        opts.s2k_cipher_algo = CipherAlgorithm::from_str(value)?;
                    }
                    "unicode-mode" => {
                        opts.unicode_mode = parse_bool(value, "unicode-mode")?;
                    }
                    _ => {
                        // PostgreSQL ignores unknown options, we do the same
                    }
                }
            }
        }

        Ok(opts)
    }
}

/// Options for PGP decryption
#[derive(Debug, Clone)]
struct PgpDecryptOptions {
    /// Convert line breaks (\r\n to \n)
    convert_crlf: bool,
    /// Reject messages without MDC
    disable_mdc: bool,
    /// Expected cipher algorithm (if set, reject others)
    expected_cipher_algo: Option<CipherAlgorithm>,
    /// Expected compression algorithm (if set, reject others)
    expected_compress_algo: Option<CompressionAlgorithm>,
    /// Expected S2K digest algorithm (if set, reject others)
    expected_s2k_digest_algo: Option<DigestAlgorithm>,
    /// Unicode mode - convert UTF-8 to database encoding
    unicode_mode: bool,
}

impl Default for PgpDecryptOptions {
    fn default() -> Self {
        PgpDecryptOptions {
            convert_crlf: false,
            disable_mdc: false, // Note: opposite meaning from encrypt
            expected_cipher_algo: None,
            expected_compress_algo: None,
            expected_s2k_digest_algo: None,
            unicode_mode: false,
        }
    }
}

impl PgpDecryptOptions {
    /// Parse options string for decryption
    fn parse(options_str: Option<&str>) -> Result<Self> {
        let mut opts = PgpDecryptOptions::default();

        if let Some(opts_str) = options_str {
            for opt in opts_str.split(',') {
                let opt = opt.trim();
                if opt.is_empty() {
                    continue;
                }

                let parts: Vec<&str> = opt.splitn(2, '=').collect();
                if parts.len() != 2 {
                    return Err(ExprError::InvalidParam {
                        name: "options",
                        reason: format!("invalid option format: '{}'. Expected 'key=value'", opt)
                            .into(),
                    });
                }

                let key = parts[0].trim();
                let value = parts[1].trim();

                match key {
                    "convert-crlf" => {
                        opts.convert_crlf = parse_bool(value, "convert-crlf")?;
                    }
                    "disable-mdc" => {
                        opts.disable_mdc = parse_bool(value, "disable-mdc")?;
                    }
                    "expected-cipher-algo" => {
                        opts.expected_cipher_algo = Some(CipherAlgorithm::from_str(value)?);
                    }
                    "expected-compress-algo" => {
                        opts.expected_compress_algo = Some(CompressionAlgorithm::from_str(value)?);
                    }
                    "expected-s2k-digest-algo" => {
                        opts.expected_s2k_digest_algo = Some(DigestAlgorithm::from_str(value)?);
                    }
                    "unicode-mode" => {
                        opts.unicode_mode = parse_bool(value, "unicode-mode")?;
                    }
                    _ => {
                        // PostgreSQL ignores unknown options
                    }
                }
            }
        }

        Ok(opts)
    }
}

/// Parse boolean option value (0/1)
fn parse_bool(value: &str, option_name: &str) -> Result<bool> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(ExprError::InvalidParam {
            name: "option",
            reason: format!("{} must be 0 or 1, got: {}", option_name, value).into(),
        }),
    }
}

// ============================================================================
// Symmetric Encryption Functions
// ============================================================================

/// Encrypts data using PGP symmetric encryption
/// Compatible with PostgreSQL's pgp_sym_encrypt function
#[function("pgp_sym_encrypt(varchar, varchar) -> bytea")]
fn pgp_sym_encrypt_no_options(_data: &str, _password: &str) -> Result<Box<[u8]>> {
    let _opts = PgpEncryptOptions::parse_encrypt(None, false)?;
    Err(ExprError::InvalidParam {
        name: "pgp_sym_encrypt",
        reason: "PGP symmetric encryption not yet implemented".into(),
    })
}

#[function("pgp_sym_encrypt(varchar, varchar, varchar) -> bytea")]
fn pgp_sym_encrypt_with_options(
    _data: &str,
    _password: &str,
    options: &str,
) -> Result<Box<[u8]>> {
    let _opts = PgpEncryptOptions::parse_encrypt(Some(options), false)?;
    Err(ExprError::InvalidParam {
        name: "pgp_sym_encrypt",
        reason: "PGP symmetric encryption not yet implemented".into(),
    })
}

/// Decrypts data using PGP symmetric decryption
/// Compatible with PostgreSQL's pgp_sym_decrypt function
#[function("pgp_sym_decrypt(bytea, varchar) -> varchar")]
fn pgp_sym_decrypt_no_options(
    _data: &[u8],
    _password: &str,
    _writer: &mut impl std::fmt::Write,
) -> Result<()> {
    let _opts = PgpDecryptOptions::parse(None)?;
    Err(ExprError::InvalidParam {
        name: "pgp_sym_decrypt",
        reason: "PGP symmetric decryption not yet implemented".into(),
    })
}

#[function("pgp_sym_decrypt(bytea, varchar, varchar) -> varchar")]
fn pgp_sym_decrypt_with_options(
    _data: &[u8],
    _password: &str,
    options: &str,
    _writer: &mut impl std::fmt::Write,
) -> Result<()> {
    let _opts = PgpDecryptOptions::parse(Some(options))?;
    Err(ExprError::InvalidParam {
        name: "pgp_sym_decrypt",
        reason: "PGP symmetric decryption not yet implemented".into(),
    })
}

// ============================================================================
// Public Key Encryption Functions
// ============================================================================

/// Encrypts data using PGP public key encryption
/// Compatible with PostgreSQL's pgp_pub_encrypt function
#[function("pgp_pub_encrypt(varchar, bytea) -> bytea")]
fn pgp_pub_encrypt_no_options(_data: &str, _public_key: &[u8]) -> Result<Box<[u8]>> {
    let _opts = PgpEncryptOptions::parse_encrypt(None, true)?;
    Err(ExprError::InvalidParam {
        name: "pgp_pub_encrypt",
        reason: "PGP public key encryption not yet implemented".into(),
    })
}

#[function("pgp_pub_encrypt(varchar, bytea, varchar) -> bytea")]
fn pgp_pub_encrypt_with_options(
    _data: &str,
    _public_key: &[u8],
    options: &str,
) -> Result<Box<[u8]>> {
    let _opts = PgpEncryptOptions::parse_encrypt(Some(options), true)?;
    Err(ExprError::InvalidParam {
        name: "pgp_pub_encrypt",
        reason: "PGP public key encryption not yet implemented".into(),
    })
}

/// Decrypts data using PGP private key decryption
/// Compatible with PostgreSQL's pgp_pub_decrypt function
#[function("pgp_pub_decrypt(bytea, bytea) -> varchar")]
fn pgp_pub_decrypt_no_password(
    _data: &[u8],
    _secret_key: &[u8],
    _writer: &mut impl std::fmt::Write,
) -> Result<()> {
    let _opts = PgpDecryptOptions::parse(None)?;
    Err(ExprError::InvalidParam {
        name: "pgp_pub_decrypt",
        reason: "PGP private key decryption not yet implemented".into(),
    })
}

#[function("pgp_pub_decrypt(bytea, bytea, varchar) -> varchar")]
fn pgp_pub_decrypt_with_password(
    _data: &[u8],
    _secret_key: &[u8],
    _password: &str,
    _writer: &mut impl std::fmt::Write,
) -> Result<()> {
    let _opts = PgpDecryptOptions::parse(None)?;
    Err(ExprError::InvalidParam {
        name: "pgp_pub_decrypt",
        reason: "PGP private key decryption not yet implemented".into(),
    })
}

#[function("pgp_pub_decrypt(bytea, bytea, varchar, varchar) -> varchar")]
fn pgp_pub_decrypt_with_options(
    _data: &[u8],
    _secret_key: &[u8],
    _password: &str,
    options: &str,
    _writer: &mut impl std::fmt::Write,
) -> Result<()> {
    let _opts = PgpDecryptOptions::parse(Some(options))?;
    Err(ExprError::InvalidParam {
        name: "pgp_pub_decrypt",
        reason: "PGP private key decryption not yet implemented".into(),
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cipher_algorithm_parsing() {
        assert_eq!(
            CipherAlgorithm::from_str("aes128").unwrap(),
            CipherAlgorithm::Aes128
        );
        assert_eq!(
            CipherAlgorithm::from_str("aes").unwrap(),
            CipherAlgorithm::Aes128
        );
        assert_eq!(
            CipherAlgorithm::from_str("aes256").unwrap(),
            CipherAlgorithm::Aes256
        );
        assert_eq!(
            CipherAlgorithm::from_str("bf").unwrap(),
            CipherAlgorithm::Blowfish
        );
        assert_eq!(
            CipherAlgorithm::from_str("3des").unwrap(),
            CipherAlgorithm::TripleDes
        );
        assert!(CipherAlgorithm::from_str("invalid").is_err());
    }

    #[test]
    fn test_compression_algorithm_parsing() {
        assert_eq!(
            CompressionAlgorithm::from_str("0").unwrap(),
            CompressionAlgorithm::None
        );
        assert_eq!(
            CompressionAlgorithm::from_str("1").unwrap(),
            CompressionAlgorithm::Zip
        );
        assert_eq!(
            CompressionAlgorithm::from_str("2").unwrap(),
            CompressionAlgorithm::Zlib
        );
        assert_eq!(
            CompressionAlgorithm::from_str("3").unwrap(),
            CompressionAlgorithm::Bzip2
        );
        assert!(CompressionAlgorithm::from_str("4").is_err());
    }

    #[test]
    fn test_s2k_mode_parsing() {
        assert_eq!(S2kMode::from_str("0").unwrap(), S2kMode::Simple);
        assert_eq!(S2kMode::from_str("1").unwrap(), S2kMode::Salted);
        assert_eq!(S2kMode::from_str("3").unwrap(), S2kMode::IteratedSalted);
        assert!(S2kMode::from_str("2").is_err());
    }

    #[test]
    fn test_digest_algorithm_parsing() {
        assert_eq!(
            DigestAlgorithm::from_str("sha256").unwrap(),
            DigestAlgorithm::Sha256
        );
        assert_eq!(
            DigestAlgorithm::from_str("SHA256").unwrap(),
            DigestAlgorithm::Sha256
        );
        assert_eq!(
            DigestAlgorithm::from_str("md5").unwrap(),
            DigestAlgorithm::Md5
        );
        assert!(DigestAlgorithm::from_str("invalid").is_err());
    }

    #[test]
    fn test_encrypt_options_default() {
        let opts = PgpEncryptOptions::parse_encrypt(None, false).unwrap();
        assert_eq!(opts.cipher_algo, CipherAlgorithm::Aes128);
        assert_eq!(opts.compress_algo, CompressionAlgorithm::Zip);
        assert_eq!(opts.compress_level, 6);
        assert!(!opts.sess_key); // False for symmetric
    }

    #[test]
    fn test_encrypt_options_parsing() {
        let opts = PgpEncryptOptions::parse_encrypt(
            Some("cipher-algo=aes256, compress-algo=2, compress-level=9, disable-mdc=1"),
            false,
        )
        .unwrap();
        assert_eq!(opts.cipher_algo, CipherAlgorithm::Aes256);
        assert_eq!(opts.compress_algo, CompressionAlgorithm::Zlib);
        assert_eq!(opts.compress_level, 9);
        assert!(opts.disable_mdc);
    }

    #[test]
    fn test_encrypt_options_s2k() {
        let opts = PgpEncryptOptions::parse_encrypt(
            Some("s2k-mode=3, s2k-count=65536, s2k-digest-algo=sha512"),
            false,
        )
        .unwrap();
        assert_eq!(opts.s2k_mode, S2kMode::IteratedSalted);
        assert_eq!(opts.s2k_count, 65536);
        assert_eq!(opts.s2k_digest_algo, DigestAlgorithm::Sha512);
    }

    #[test]
    fn test_encrypt_options_invalid_s2k_count() {
        // Too small
        assert!(PgpEncryptOptions::parse_encrypt(Some("s2k-count=512"), false).is_err());
        // Too large
        assert!(PgpEncryptOptions::parse_encrypt(Some("s2k-count=99999999"), false).is_err());
        // Not a power of 2
        assert!(PgpEncryptOptions::parse_encrypt(Some("s2k-count=65537"), false).is_err());
    }

    #[test]
    fn test_decrypt_options_parsing() {
        let opts =
            PgpDecryptOptions::parse(Some("disable-mdc=1, expected-cipher-algo=aes256"))
                .unwrap();
        assert!(opts.disable_mdc);
        assert_eq!(
            opts.expected_cipher_algo,
            Some(CipherAlgorithm::Aes256)
        );
    }

    #[test]
    fn test_bool_parsing() {
        assert!(parse_bool("0", "test").unwrap() == false);
        assert!(parse_bool("1", "test").unwrap() == true);
        assert!(parse_bool("2", "test").is_err());
        assert!(parse_bool("true", "test").is_err());
    }

    #[test]
    fn test_invalid_option_format() {
        // Missing value
        assert!(PgpEncryptOptions::parse_encrypt(Some("cipher-algo"), false).is_err());
        // Invalid compression level
        assert!(
            PgpEncryptOptions::parse_encrypt(Some("compress-level=10"), false).is_err()
        );
    }

    #[test]
    fn test_unknown_options_ignored() {
        // PostgreSQL ignores unknown options
        let opts = PgpEncryptOptions::parse_encrypt(
            Some("cipher-algo=aes256, unknown-option=value"),
            false,
        )
        .unwrap();
        assert_eq!(opts.cipher_algo, CipherAlgorithm::Aes256);
    }

    #[test]
    fn test_public_key_default_sess_key() {
        let opts_sym = PgpEncryptOptions::parse_encrypt(None, false).unwrap();
        assert!(!opts_sym.sess_key); // Symmetric: no session key by default

        let opts_pub = PgpEncryptOptions::parse_encrypt(None, true).unwrap();
        assert!(opts_pub.sess_key); // Public key: use session key by default
    }
}

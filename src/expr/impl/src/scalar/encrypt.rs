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

use std::fmt::Debug;
use std::io::Write;
use std::sync::LazyLock;

use openssl::error::ErrorStack;
use openssl::symm::{Cipher, Crypter, Mode as CipherMode};
use regex::Regex;
use risingwave_expr::{ExprError, Result, function};
use sequoia_openpgp as openpgp;
use sequoia_openpgp::cert::Cert;
use sequoia_openpgp::crypto::SessionKey;
use sequoia_openpgp::packet::prelude::*;
use sequoia_openpgp::parse::Parse;
use sequoia_openpgp::parse::stream::*;
use sequoia_openpgp::policy::StandardPolicy;
use sequoia_openpgp::serialize::stream::{Encryptor2, *};
use sequoia_openpgp::types::{CompressionAlgorithm, HashAlgorithm, SymmetricAlgorithm};

#[derive(Debug, Clone, PartialEq)]
enum Algorithm {
    Aes,
}

#[derive(Debug, Clone, PartialEq)]
enum Mode {
    Cbc,
    Ecb,
}
#[derive(Debug, Clone, PartialEq)]
enum Padding {
    Pkcs,
    None,
}

#[derive(Clone)]
pub struct CipherConfig {
    algorithm: Algorithm,
    mode: Mode,
    cipher: Cipher,
    padding: Padding,
    crypt_key: Vec<u8>,
}

/// Because `Cipher` is not `Debug`, we include algorithm, key length and mode manually.
impl std::fmt::Debug for CipherConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CipherConfig")
            .field("algorithm", &self.algorithm)
            .field("key_len", &self.crypt_key.len())
            .field("mode", &self.mode)
            .field("padding", &self.padding)
            .finish()
    }
}

static CIPHER_CONFIG_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(aes)(?:-(cbc|ecb))?(?:/pad:(pkcs|none))?$").unwrap());

impl CipherConfig {
    fn parse_cipher_config(key: &[u8], input: &str) -> Result<CipherConfig> {
        let Some(caps) = CIPHER_CONFIG_RE.captures(input) else {
            return Err(ExprError::InvalidParam {
                name: "mode",
                reason: format!(
                    "invalid mode: {}, expect pattern algorithm[-mode][/pad:padding]",
                    input
                )
                .into(),
            });
        };

        let algorithm = match caps.get(1).map(|s| s.as_str()) {
            Some("aes") => Algorithm::Aes,
            algo => {
                return Err(ExprError::InvalidParam {
                    name: "mode",
                    reason: format!("expect aes for algorithm, but got: {:?}", algo).into(),
                });
            }
        };

        let mode = match caps.get(2).map(|m| m.as_str()) {
            Some("cbc") | None => Mode::Cbc, // Default to Cbc if not specified
            Some("ecb") => Mode::Ecb,
            Some(mode) => {
                return Err(ExprError::InvalidParam {
                    name: "mode",
                    reason: format!("expect cbc or ecb for mode, but got: {}", mode).into(),
                });
            }
        };

        let padding = match caps.get(3).map(|m| m.as_str()) {
            Some("pkcs") | None => Padding::Pkcs, // Default to Pkcs if not specified
            Some("none") => Padding::None,
            Some(padding) => {
                return Err(ExprError::InvalidParam {
                    name: "mode",
                    reason: format!("expect pkcs or none for padding, but got: {}", padding).into(),
                });
            }
        };

        let cipher = match (&algorithm, key.len(), &mode) {
            (Algorithm::Aes, 16, Mode::Cbc) => Cipher::aes_128_cbc(),
            (Algorithm::Aes, 16, Mode::Ecb) => Cipher::aes_128_ecb(),
            (Algorithm::Aes, 24, Mode::Cbc) => Cipher::aes_192_cbc(),
            (Algorithm::Aes, 24, Mode::Ecb) => Cipher::aes_192_ecb(),
            (Algorithm::Aes, 32, Mode::Cbc) => Cipher::aes_256_cbc(),
            (Algorithm::Aes, 32, Mode::Ecb) => Cipher::aes_256_ecb(),
            (Algorithm::Aes, n, Mode::Cbc | Mode::Ecb) => {
                return Err(ExprError::InvalidParam {
                    name: "key",
                    reason: format!("invalid key length: {}, expect 16, 24 or 32", n).into(),
                });
            }
        };

        Ok(CipherConfig {
            algorithm,
            mode,
            cipher,
            padding,
            crypt_key: key.to_vec(),
        })
    }

    fn eval(&self, input: &[u8], stage: CryptographyStage) -> Result<Box<[u8]>, CryptographyError> {
        let operation = match stage {
            CryptographyStage::Encrypt => CipherMode::Encrypt,
            CryptographyStage::Decrypt => CipherMode::Decrypt,
        };
        self.eval_inner(input, operation)
            .map_err(|reason| CryptographyError { stage, reason })
    }

    fn eval_inner(
        &self,
        input: &[u8],
        operation: CipherMode,
    ) -> std::result::Result<Box<[u8]>, ErrorStack> {
        let mut decrypter = Crypter::new(self.cipher, operation, self.crypt_key.as_ref(), None)?;
        let enable_padding = match self.padding {
            Padding::Pkcs => true,
            Padding::None => false,
        };
        decrypter.pad(enable_padding);
        let mut decrypt = vec![0; input.len() + self.cipher.block_size()];
        let count = decrypter.update(input, &mut decrypt)?;
        let rest = decrypter.finalize(&mut decrypt[count..])?;
        decrypt.truncate(count + rest);
        Ok(decrypt.into())
    }
}

/// from [pg doc](https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-RAW-ENC-FUNCS)
#[function(
    "decrypt(bytea, bytea, varchar) -> bytea",
    prebuild = "CipherConfig::parse_cipher_config($1, $2)?"
)]
fn decrypt(data: &[u8], config: &CipherConfig) -> Result<Box<[u8]>, CryptographyError> {
    config.eval(data, CryptographyStage::Decrypt)
}

#[function(
    "encrypt(bytea, bytea, varchar) -> bytea",
    prebuild = "CipherConfig::parse_cipher_config($1, $2)?"
)]
fn encrypt(data: &[u8], config: &CipherConfig) -> Result<Box<[u8]>, CryptographyError> {
    config.eval(data, CryptographyStage::Encrypt)
}

#[derive(Debug)]
enum CryptographyStage {
    Encrypt,
    Decrypt,
}

#[derive(Debug, thiserror::Error)]
#[error("{stage:?} stage, reason: {reason}")]
struct CryptographyError {
    pub stage: CryptographyStage,
    #[source]
    pub reason: openssl::error::ErrorStack,
}

#[derive(Debug, thiserror::Error)]
pub enum PgpError {
    #[error("PGP error: {0}")]
    Anyhow(#[from] anyhow::Error),
    #[error("Invalid key format: {0}")]
    InvalidKey(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Invalid option: {0}")]
    InvalidOption(String),
}

/// PGP encryption/decryption options as per PostgreSQL pgcrypto documentation
///
/// This struct represents all the configurable options for PGP encryption/decryption
/// functions, matching the behavior of PostgreSQL's pgcrypto module.
/// Options are parsed from a comma-separated string format: "key1=value1,key2=value2"
#[derive(Debug, Clone)]
pub struct PgpOptions {
    /// Compression algorithm (0=none, 1=zip, 2=zlib, 3=bzip2)
    pub compress_algo: Option<CompressionAlgorithm>,
    /// Cipher algorithm (aes128, aes192, aes256, 3des)
    pub cipher_algo: Option<SymmetricAlgorithm>,
    /// Enable integrity protection (true/false)
    pub integrity_protect: Option<bool>,
    /// Integrity check algorithm (sha1, sha256, sha512)
    pub integrity_algo: Option<HashAlgorithm>,
    /// ASCII armor output (true/false)
    pub armor: Option<bool>,
    /// Convert CRLF to LF (true/false)
    pub convert_crlf: Option<bool>,
    /// Disable MDC (true/false)
    pub disable_mdc: Option<bool>,
    /// S2K mode (0=simple, 1=salted, 3=iterated+salted)
    pub s2k_mode: Option<u8>,
    /// S2K iteration count (1024-65011712)
    pub s2k_count: Option<u32>,
    /// S2K digest algorithm (sha1, sha256, sha512)
    pub s2k_digest_algo: Option<HashAlgorithm>,
    /// S2K cipher algorithm (aes128, aes192, aes256, 3des)
    pub s2k_cipher_algo: Option<SymmetricAlgorithm>,
    /// Unicode mode (0/1)
    pub unicode_mode: Option<bool>,
}

impl Default for PgpOptions {
    fn default() -> Self {
        Self {
            compress_algo: Some(CompressionAlgorithm::Zip),
            cipher_algo: Some(SymmetricAlgorithm::AES128),
            integrity_protect: Some(true),
            integrity_algo: Some(HashAlgorithm::SHA1),
            armor: Some(false),
            convert_crlf: Some(false),
            disable_mdc: Some(false),
            s2k_mode: Some(3),
            s2k_count: None, // Will use random value between 65536-253952
            s2k_digest_algo: Some(HashAlgorithm::SHA1),
            s2k_cipher_algo: Some(SymmetricAlgorithm::AES128),
            unicode_mode: Some(false),
        }
    }
}

impl PgpOptions {
    /// Parse PGP options string in format "key1=value1,key2=value2"
    ///
    /// # Arguments
    /// * `options_str` - Comma-separated key-value pairs, e.g., "cipher-algo=aes256,armor=true"
    ///
    /// # Returns
    /// * `Ok(PgpOptions)` - Parsed options with defaults applied for unspecified values
    /// * `Err(PgpError::InvalidOption)` - If any option is invalid or malformed
    ///
    /// # Example
    /// ```
    /// let opts = PgpOptions::parse("cipher-algo=aes256,armor=true").unwrap();
    /// assert_eq!(opts.cipher_algo, Some(SymmetricAlgorithm::AES256));
    /// assert_eq!(opts.armor, Some(true));
    /// ```
    pub fn parse(options_str: &str) -> Result<Self, PgpError> {
        let mut opts = Self::default();

        if options_str.is_empty() {
            return Ok(opts);
        }

        for pair in options_str.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }

            let (key, value) = if let Some(eq_pos) = pair.find('=') {
                let key = pair[..eq_pos].trim();
                let value = pair[eq_pos + 1..].trim();
                (key, value)
            } else {
                return Err(PgpError::InvalidOption(format!(
                    "Invalid option format: {}",
                    pair
                )));
            };

            match key {
                "compress-algo" => {
                    opts.compress_algo = Some(match value.to_lowercase().as_str() {
                        "0" | "none" => CompressionAlgorithm::Uncompressed,
                        "1" | "zip" => CompressionAlgorithm::Zip,
                        "2" | "zlib" => CompressionAlgorithm::Zlib,
                        "3" | "bzip2" => CompressionAlgorithm::BZip2,
                        _ => {
                            return Err(PgpError::InvalidOption(format!(
                                "Invalid compress-algo: {}",
                                value
                            )));
                        }
                    });
                }
                "cipher-algo" => {
                    opts.cipher_algo = Some(match value.to_lowercase().as_str() {
                        "aes128" => SymmetricAlgorithm::AES128,
                        "aes192" => SymmetricAlgorithm::AES192,
                        "aes256" => SymmetricAlgorithm::AES256,
                        "3des" => SymmetricAlgorithm::TripleDES,
                        _ => {
                            return Err(PgpError::InvalidOption(format!(
                                "Invalid cipher-algo: {}",
                                value
                            )));
                        }
                    });
                }
                "integrity-protect" => {
                    opts.integrity_protect = Some(match value.to_lowercase().as_str() {
                        "true" | "1" => true,
                        "false" | "0" => false,
                        _ => {
                            return Err(PgpError::InvalidOption(format!(
                                "Invalid integrity-protect: {}",
                                value
                            )));
                        }
                    });
                }
                "integrity-algo" => {
                    opts.integrity_algo = Some(match value.to_lowercase().as_str() {
                        "sha1" => HashAlgorithm::SHA1,
                        "sha256" => HashAlgorithm::SHA256,
                        "sha512" => HashAlgorithm::SHA512,
                        _ => {
                            return Err(PgpError::InvalidOption(format!(
                                "Invalid integrity-algo: {}",
                                value
                            )));
                        }
                    });
                }
                "armor" => {
                    opts.armor = Some(match value.to_lowercase().as_str() {
                        "true" | "1" => true,
                        "false" | "0" => false,
                        _ => {
                            return Err(PgpError::InvalidOption(format!(
                                "Invalid armor: {}",
                                value
                            )));
                        }
                    });
                }
                "convert-crlf" => {
                    opts.convert_crlf = Some(match value.to_lowercase().as_str() {
                        "true" | "1" => true,
                        "false" | "0" => false,
                        _ => {
                            return Err(PgpError::InvalidOption(format!(
                                "Invalid convert-crlf: {}",
                                value
                            )));
                        }
                    });
                }
                "disable-mdc" => {
                    opts.disable_mdc = Some(match value.to_lowercase().as_str() {
                        "true" | "1" => true,
                        "false" | "0" => false,
                        _ => {
                            return Err(PgpError::InvalidOption(format!(
                                "Invalid disable-mdc: {}",
                                value
                            )));
                        }
                    });
                }
                "s2k-mode" => {
                    opts.s2k_mode = Some(match value {
                        "0" => 0,
                        "1" => 1,
                        "3" => 3,
                        _ => {
                            return Err(PgpError::InvalidOption(format!(
                                "Invalid s2k-mode: {}",
                                value
                            )));
                        }
                    });
                }
                "s2k-count" => {
                    let count: u32 = value.parse().map_err(|_| {
                        PgpError::InvalidOption(format!("Invalid s2k-count: {}", value))
                    })?;
                    if !(1024..=65011712).contains(&count) {
                        return Err(PgpError::InvalidOption(format!(
                            "s2k-count must be between 1024 and 65011712, got: {}",
                            count
                        )));
                    }
                    opts.s2k_count = Some(count);
                }
                "s2k-digest-algo" => {
                    opts.s2k_digest_algo = Some(match value.to_lowercase().as_str() {
                        "sha1" => HashAlgorithm::SHA1,
                        "sha256" => HashAlgorithm::SHA256,
                        "sha512" => HashAlgorithm::SHA512,
                        _ => {
                            return Err(PgpError::InvalidOption(format!(
                                "Invalid s2k-digest-algo: {}",
                                value
                            )));
                        }
                    });
                }
                "s2k-cipher-algo" => {
                    opts.s2k_cipher_algo = Some(match value.to_lowercase().as_str() {
                        "aes128" => SymmetricAlgorithm::AES128,
                        "aes192" => SymmetricAlgorithm::AES192,
                        "aes256" => SymmetricAlgorithm::AES256,
                        "3des" => SymmetricAlgorithm::TripleDES,
                        _ => {
                            return Err(PgpError::InvalidOption(format!(
                                "Invalid s2k-cipher-algo: {}",
                                value
                            )));
                        }
                    });
                }
                "unicode-mode" => {
                    opts.unicode_mode = Some(match value {
                        "0" => false,
                        "1" => true,
                        _ => {
                            return Err(PgpError::InvalidOption(format!(
                                "Invalid unicode-mode: {}",
                                value
                            )));
                        }
                    });
                }
                _ => {
                    return Err(PgpError::InvalidOption(format!("Unknown option: {}", key)));
                }
            }
        }

        Ok(opts)
    }
}

/// from [pg doc](https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-PGP-ENC-FUNCS)
// pgp_sym_encrypt(data text, psw text) returns bytea
// pgp_sym_encrypt_bytea(data bytea, psw text) returns bytea
#[function("pgp_sym_encrypt(bytea, varchar) -> bytea")]
fn pgp_sym_encrypt(data: &[u8], password: &str) -> Result<Box<[u8]>, PgpError> {
    pgp_sym_encrypt_internal(data, password, None)
}

#[function("pgp_sym_encrypt(bytea, varchar, varchar) -> bytea")]
fn pgp_sym_encrypt_with_options(
    data: &[u8],
    password: &str,
    options: &str,
) -> Result<Box<[u8]>, PgpError> {
    pgp_sym_encrypt_internal(data, password, Some(options))
}

fn pgp_sym_encrypt_internal(
    data: &[u8],
    password: &str,
    options: Option<&str>,
) -> Result<Box<[u8]>, PgpError> {
    use openpgp::crypto::Password;
    use openpgp::types::SymmetricAlgorithm;

    // Note: Empty passwords are allowed in PostgreSQL pgcrypto

    let opts = if let Some(opts_str) = options {
        PgpOptions::parse(opts_str)?
    } else {
        PgpOptions::default()
    };

    let mut sink = Vec::new();

    // Apply compression if specified
    let message = if let Some(compress_algo) = opts.compress_algo {
        if compress_algo != CompressionAlgorithm::Uncompressed {
            Compressor::new(Message::new(&mut sink))
                .algo(compress_algo)
                .build()?
        } else {
            Message::new(&mut sink)
        }
    } else {
        Message::new(&mut sink)
    };

    // Apply ASCII armor if specified
    let message = if opts.armor.unwrap_or(false) {
        Armorer::new(message).build()?
    } else {
        message
    };

    // Encrypt with password using SKESK (Symmetric-Key Encrypted Session Key)
    let cipher_algo = opts.cipher_algo.unwrap_or(SymmetricAlgorithm::AES128);
    let message = Encryptor2::with_passwords(message, vec![Password::from(password)])
        .symmetric_algo(cipher_algo)
        .build()?;

    // Write literal data packet
    let mut message = LiteralWriter::new(message).build()?;
    message.write_all(data)?;
    message.finalize()?;

    let result = sink.into_boxed_slice();
    Ok(result)
}

/// from [pg doc](https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-PGP-ENC-FUNCS)
// pgp_sym_decrypt(msg bytea, psw text) returns text
// pgp_sym_decrypt_bytea(msg bytea, psw text) returns bytea
#[function("pgp_sym_decrypt(bytea, varchar) -> bytea")]
fn pgp_sym_decrypt(msg: &[u8], password: &str) -> Result<Box<[u8]>, PgpError> {
    pgp_sym_decrypt_internal(msg, password, None)
}

#[function("pgp_sym_decrypt(bytea, varchar, varchar) -> bytea")]
fn pgp_sym_decrypt_with_options(
    msg: &[u8],
    password: &str,
    options: &str,
) -> Result<Box<[u8]>, PgpError> {
    pgp_sym_decrypt_internal(msg, password, Some(options))
}

fn pgp_sym_decrypt_internal(
    msg: &[u8],
    password: &str,
    options: Option<&str>,
) -> Result<Box<[u8]>, PgpError> {
    let policy = &StandardPolicy::new();

    // Note: Empty passwords are allowed in PostgreSQL pgcrypto

    // Parse options for future use (currently not all options are implemented)
    let _opts = if let Some(opts_str) = options {
        PgpOptions::parse(opts_str)?
    } else {
        PgpOptions::default()
    };

    struct Helper {
        password: String,
    }

    impl VerificationHelper for Helper {
        fn get_certs(&mut self, _ids: &[openpgp::KeyHandle]) -> openpgp::Result<Vec<Cert>> {
            Ok(Vec::new())
        }

        fn check(&mut self, _structure: MessageStructure<'_>) -> openpgp::Result<()> {
            Ok(())
        }
    }

    impl DecryptionHelper for Helper {
        fn decrypt<D>(
            &mut self,
            _pkesks: &[PKESK],
            skesks: &[SKESK],
            _sym_algo: Option<openpgp::types::SymmetricAlgorithm>,
            mut decrypt: D,
        ) -> openpgp::Result<Option<openpgp::Fingerprint>>
        where
            D: FnMut(openpgp::types::SymmetricAlgorithm, &SessionKey) -> bool,
        {
            use openpgp::crypto::Password;

            // Try password-based decryption with each SKESK packet
            for skesk in skesks {
                if let Ok((algo, session_key)) =
                    skesk.decrypt(&Password::from(self.password.as_str()))
                    && decrypt(algo, &session_key)
                {
                    return Ok(None);
                }
            }

            Err(anyhow::anyhow!("Decryption failed"))
        }
    }

    let helper = Helper {
        password: password.to_owned(),
    };

    let mut decryptor = DecryptorBuilder::from_bytes(msg)?.with_policy(policy, None, helper)?;

    let mut plaintext = Vec::new();
    std::io::copy(&mut decryptor, &mut plaintext)?;

    Ok(plaintext.into_boxed_slice())
}

/// from [pg doc](https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-PGP-ENC-FUNCS)
// pgp_pub_encrypt(data text, key bytea) returns bytea
// pgp_pub_encrypt_bytea(data bytea, key bytea) returns bytea
#[function("pgp_pub_encrypt(bytea, bytea) -> bytea")]
fn pgp_pub_encrypt(data: &[u8], key: &[u8]) -> Result<Box<[u8]>, PgpError> {
    pgp_pub_encrypt_internal(data, key, None)
}

#[function("pgp_pub_encrypt(bytea, bytea, varchar) -> bytea")]
fn pgp_pub_encrypt_with_options(
    data: &[u8],
    key: &[u8],
    options: &str,
) -> Result<Box<[u8]>, PgpError> {
    pgp_pub_encrypt_internal(data, key, Some(options))
}

fn pgp_pub_encrypt_internal(
    data: &[u8],
    key: &[u8],
    options: Option<&str>,
) -> Result<Box<[u8]>, PgpError> {
    use openpgp::types::SymmetricAlgorithm;

    let policy = &StandardPolicy::new();

    // Validate inputs
    if key.is_empty() {
        return Err(PgpError::InvalidKey(
            "Public key cannot be empty".to_owned(),
        ));
    }

    let opts = if let Some(opts_str) = options {
        PgpOptions::parse(opts_str)?
    } else {
        PgpOptions::default()
    };

    // Parse the certificate (public key)
    let cert = Cert::from_bytes(key)
        .map_err(|e| PgpError::InvalidKey(format!("Failed to parse public key: {}", e)))?;

    // Get encryption-capable keys
    let recipients = cert
        .keys()
        .with_policy(policy, None)
        .supported()
        .alive()
        .revoked(false)
        .for_transport_encryption()
        .map(|ka| ka.key())
        .collect::<Vec<_>>();

    if recipients.is_empty() {
        return Err(PgpError::InvalidKey(
            "No valid encryption keys found".to_owned(),
        ));
    }

    let mut sink = Vec::new();

    // Apply compression if specified
    let message = if let Some(compress_algo) = opts.compress_algo {
        if compress_algo != CompressionAlgorithm::Uncompressed {
            Compressor::new(Message::new(&mut sink))
                .algo(compress_algo)
                .build()?
        } else {
            Message::new(&mut sink)
        }
    } else {
        Message::new(&mut sink)
    };

    // Apply ASCII armor if specified
    let message = if opts.armor.unwrap_or(false) {
        Armorer::new(message).build()?
    } else {
        message
    };

    // Encrypt with public key
    let cipher_algo = opts.cipher_algo.unwrap_or(SymmetricAlgorithm::AES128);
    let message = Encryptor2::for_recipients(message, recipients)
        .symmetric_algo(cipher_algo)
        .build()?;

    // Write literal data packet
    let mut message = LiteralWriter::new(message).build()?;
    message.write_all(data)?;
    message.finalize()?;

    let result = sink.into_boxed_slice();
    Ok(result)
}

/// from [pg doc](https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-PGP-ENC-FUNCS)
// pgp_pub_decrypt(msg bytea, key bytea) returns text
// pgp_pub_decrypt_bytea(msg bytea, key bytea) returns bytea
#[function("pgp_pub_decrypt(bytea, bytea) -> bytea")]
fn pgp_pub_decrypt(msg: &[u8], key: &[u8]) -> Result<Box<[u8]>, PgpError> {
    pgp_pub_decrypt_internal(msg, key, None, None)
}

#[function("pgp_pub_decrypt(bytea, bytea, varchar) -> bytea")]
fn pgp_pub_decrypt_with_password(
    msg: &[u8],
    key: &[u8],
    password: &str,
) -> Result<Box<[u8]>, PgpError> {
    pgp_pub_decrypt_internal(msg, key, Some(password), None)
}

#[function("pgp_pub_decrypt(bytea, bytea, varchar, varchar) -> bytea")]
fn pgp_pub_decrypt_with_options(
    msg: &[u8],
    key: &[u8],
    password: &str,
    options: &str,
) -> Result<Box<[u8]>, PgpError> {
    pgp_pub_decrypt_internal(msg, key, Some(password), Some(options))
}

fn pgp_pub_decrypt_internal(
    msg: &[u8],
    key: &[u8],
    password: Option<&str>,
    options: Option<&str>,
) -> Result<Box<[u8]>, PgpError> {
    let policy = &StandardPolicy::new();

    // Parse options for future use (currently not all options are implemented)
    let _opts = if let Some(opts_str) = options {
        PgpOptions::parse(opts_str)?
    } else {
        PgpOptions::default()
    };

    // Parse the certificate (secret key)
    let cert = Cert::from_bytes(key)?;

    struct Helper {
        cert: Cert,
        password: Option<String>,
    }

    impl VerificationHelper for Helper {
        fn get_certs(&mut self, _ids: &[openpgp::KeyHandle]) -> openpgp::Result<Vec<Cert>> {
            Ok(vec![self.cert.clone()])
        }

        fn check(&mut self, _structure: MessageStructure<'_>) -> openpgp::Result<()> {
            Ok(())
        }
    }

    impl DecryptionHelper for Helper {
        fn decrypt<D>(
            &mut self,
            pkesks: &[PKESK],
            _skesks: &[SKESK],
            sym_algo: Option<openpgp::types::SymmetricAlgorithm>,
            mut decrypt: D,
        ) -> openpgp::Result<Option<openpgp::Fingerprint>>
        where
            D: FnMut(openpgp::types::SymmetricAlgorithm, &SessionKey) -> bool,
        {
            use openpgp::crypto::Password;

            let policy = &StandardPolicy::new();

            // Try decrypting with each key
            for pkesk in pkesks {
                for ka in self
                    .cert
                    .keys()
                    .unencrypted_secret()
                    .with_policy(policy, None)
                    .supported()
                    .for_transport_encryption()
                {
                    let mut keypair = if let Some(pwd) = &self.password {
                        if let Ok(decrypted) = ka
                            .key()
                            .clone()
                            .decrypt_secret(&Password::from(pwd.as_str()))
                        {
                            if let Ok(kp) = decrypted.into_keypair() {
                                kp
                            } else {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    } else if let Ok(kp) = ka.key().clone().into_keypair() {
                        kp
                    } else {
                        continue;
                    };

                    if let Some((algo, session_key)) = pkesk.decrypt(&mut keypair, sym_algo)
                        && decrypt(algo, &session_key)
                    {
                        return Ok(None);
                    }
                }
            }

            Err(anyhow::anyhow!("Decryption failed"))
        }
    }

    let helper = Helper {
        cert,
        password: password.map(|s| s.to_owned()),
    };

    let mut decryptor = DecryptorBuilder::from_bytes(msg)?.with_policy(policy, None, helper)?;

    let mut plaintext = Vec::new();
    std::io::copy(&mut decryptor, &mut plaintext)?;

    Ok(plaintext.into_boxed_slice())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_decrypt() {
        let data = b"hello world";
        let mode = "aes";

        let config = CipherConfig::parse_cipher_config(
            b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F" as &[u8],
            mode,
        )
        .unwrap();
        let encrypted = encrypt(data, &config).unwrap();

        let decrypted = decrypt(&encrypted, &config).unwrap();
        assert_eq!(decrypted, (*data).into());
    }

    #[test]
    fn encrypt_testcase() {
        let encrypt_wrapper = |data: &[u8], key: &[u8], mode: &str| -> Box<[u8]> {
            let config = CipherConfig::parse_cipher_config(key, mode).unwrap();
            encrypt(data, &config).unwrap()
        };
        let decrypt_wrapper = |data: &[u8], key: &[u8], mode: &str| -> Box<[u8]> {
            let config = CipherConfig::parse_cipher_config(key, mode).unwrap();
            decrypt(data, &config).unwrap()
        };
        let key = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f";

        let encrypted = encrypt_wrapper(
            b"\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff",
            key,
            "aes-ecb/pad:none",
        );

        let decrypted = decrypt_wrapper(&encrypted, key, "aes-ecb/pad:none");
        assert_eq!(
            decrypted,
            (*b"\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff").into()
        )
    }

    #[test]
    fn test_parse_cipher_config() {
        let key = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f";

        let mode_1 = "aes-ecb/pad:none";
        let config = CipherConfig::parse_cipher_config(key, mode_1).unwrap();
        assert_eq!(config.algorithm, Algorithm::Aes);
        assert_eq!(config.mode, Mode::Ecb);
        assert_eq!(config.padding, Padding::None);

        let mode_2 = "aes-cbc/pad:pkcs";
        let config = CipherConfig::parse_cipher_config(key, mode_2).unwrap();
        assert_eq!(config.algorithm, Algorithm::Aes);
        assert_eq!(config.mode, Mode::Cbc);
        assert_eq!(config.padding, Padding::Pkcs);

        let mode_3 = "aes";
        let config = CipherConfig::parse_cipher_config(key, mode_3).unwrap();
        assert_eq!(config.algorithm, Algorithm::Aes);
        assert_eq!(config.mode, Mode::Cbc);
        assert_eq!(config.padding, Padding::Pkcs);

        let mode_4 = "cbc";
        assert!(CipherConfig::parse_cipher_config(key, mode_4).is_err());
    }

    #[test]
    fn test_pgp_sym_encrypt_decrypt() {
        let data = b"hello world";
        let password = "secret";

        let encrypted = pgp_sym_encrypt(data, password).unwrap();
        // Print encrypted data for debugging
        println!("Encrypted data length: {}", encrypted.len());
        println!(
            "Encrypted data (first 100 bytes): {:?}",
            std::str::from_utf8(&encrypted[..std::cmp::min(100, encrypted.len())])
        );

        let decrypted = pgp_sym_decrypt(&encrypted, password).unwrap();

        assert_eq!(&*decrypted, data);
    }

    #[test]
    fn test_pgp_sym_encrypt_decrypt_empty() {
        let data = b"";
        let password = "password";

        let encrypted = pgp_sym_encrypt(data, password).unwrap();
        let decrypted = pgp_sym_decrypt(&encrypted, password).unwrap();

        assert_eq!(&*decrypted, data);
    }

    #[test]
    fn test_pgp_sym_decrypt_wrong_password() {
        let data = b"test data";
        let password = "correct_password";
        let wrong_password = "wrong_password";

        let encrypted = pgp_sym_encrypt(data, password).unwrap();
        let result = pgp_sym_decrypt(&encrypted, wrong_password);

        assert!(result.is_err());
    }

    #[test]
    fn test_pgp_options_parsing() {
        // Test default options
        let opts = PgpOptions::default();
        assert_eq!(opts.compress_algo, Some(CompressionAlgorithm::Zip));
        assert_eq!(opts.cipher_algo, Some(SymmetricAlgorithm::AES128));
        assert_eq!(opts.integrity_protect, Some(true));
        assert_eq!(opts.armor, Some(false));

        // Test parsing empty string
        let opts = PgpOptions::parse("").unwrap();
        assert_eq!(opts.compress_algo, Some(CompressionAlgorithm::Zip));

        // Test parsing valid options
        let opts = PgpOptions::parse("compress-algo=0,cipher-algo=aes256,armor=true").unwrap();
        assert_eq!(opts.compress_algo, Some(CompressionAlgorithm::Uncompressed));
        assert_eq!(opts.cipher_algo, Some(SymmetricAlgorithm::AES256));
        assert_eq!(opts.armor, Some(true));

        // Test parsing invalid option
        let result = PgpOptions::parse("invalid-option=value");
        assert!(result.is_err());

        // Test parsing invalid compress-algo
        let result = PgpOptions::parse("compress-algo=5");
        assert!(result.is_err());

        // Test parsing invalid s2k-count
        let result = PgpOptions::parse("s2k-count=100");
        assert!(result.is_err());

        // Test parsing valid s2k-count
        let opts = PgpOptions::parse("s2k-count=5000").unwrap();
        assert_eq!(opts.s2k_count, Some(5000));
    }

    #[test]
    fn test_pgp_sym_encrypt_with_options() {
        let data = b"hello world";
        let password = "secret";

        // Test with default options
        let encrypted_default = pgp_sym_encrypt(data, password).unwrap();
        let decrypted_default = pgp_sym_decrypt(&encrypted_default, password).unwrap();
        assert_eq!(&*decrypted_default, data);

        // Test with custom options
        let encrypted_custom =
            pgp_sym_encrypt_with_options(data, password, "cipher-algo=aes256").unwrap();
        let decrypted_custom = pgp_sym_decrypt(&encrypted_custom, password).unwrap();
        assert_eq!(&*decrypted_custom, data);

        // Test with multiple options
        let encrypted_multi =
            pgp_sym_encrypt_with_options(data, password, "cipher-algo=aes192,armor=false").unwrap();
        let decrypted_multi = pgp_sym_decrypt(&encrypted_multi, password).unwrap();
        assert_eq!(&*decrypted_multi, data);
    }
}

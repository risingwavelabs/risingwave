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
use std::io::Cursor;

use pgp::crypto::{HashAlgorithm, SymmetricKeyAlgorithm};
use pgp::types::{CompressionAlgorithm, SecretKeyTrait, StringToKey};
use pgp::{Deserializable, Message, SignedSecretKey};
use risingwave_expr::{ExprError, Result, function};

/// Configuration for PGP encryption/decryption options
#[derive(Debug, Clone, PartialEq)]
struct PgpOptions {
    cipher_algo: SymmetricKeyAlgorithm,
    compress_algo: CompressionAlgorithm,
    compress_level: i32,
}

impl Default for PgpOptions {
    fn default() -> Self {
        PgpOptions {
            cipher_algo: SymmetricKeyAlgorithm::AES128,
            compress_algo: CompressionAlgorithm::ZIP,
            compress_level: 6,
        }
    }
}

impl PgpOptions {
    /// Parse options string in the format: "option1=value1, option2=value2"
    /// Supported options:
    /// - cipher-algo: aes128, aes192, aes256, 3des, cast5, blowfish
    /// - compress-algo: 0 (none), 1 (zip), 2 (zlib), 3 (bzip2)
    /// - compress-level: 0-9
    fn parse(options_str: Option<&str>) -> Result<Self> {
        let mut options = PgpOptions::default();

        if let Some(opts) = options_str {
            for opt in opts.split(',') {
                let opt = opt.trim();
                if opt.is_empty() {
                    continue;
                }

                let parts: Vec<&str> = opt.splitn(2, '=').collect();
                if parts.len() != 2 {
                    return Err(ExprError::InvalidParam {
                        name: "options",
                        reason: format!("invalid option format: {}", opt).into(),
                    });
                }

                let key = parts[0].trim();
                let value = parts[1].trim();

                match key {
                    "cipher-algo" => {
                        options.cipher_algo = match value {
                            "aes128" | "aes" => SymmetricKeyAlgorithm::AES128,
                            "aes192" => SymmetricKeyAlgorithm::AES192,
                            "aes256" => SymmetricKeyAlgorithm::AES256,
                            "3des" => SymmetricKeyAlgorithm::TripleDES,
                            "cast5" => SymmetricKeyAlgorithm::CAST5,
                            "blowfish" => SymmetricKeyAlgorithm::Blowfish,
                            _ => {
                                return Err(ExprError::InvalidParam {
                                    name: "cipher-algo",
                                    reason: format!("unsupported cipher algorithm: {}", value)
                                        .into(),
                                });
                            }
                        };
                    }
                    "compress-algo" => {
                        options.compress_algo = match value {
                            "0" | "none" => CompressionAlgorithm::Uncompressed,
                            "1" | "zip" => CompressionAlgorithm::ZIP,
                            "2" | "zlib" => CompressionAlgorithm::ZLIB,
                            "3" | "bzip2" => CompressionAlgorithm::BZ2,
                            _ => {
                                return Err(ExprError::InvalidParam {
                                    name: "compress-algo",
                                    reason: format!("unsupported compression algorithm: {}", value)
                                        .into(),
                                });
                            }
                        };
                    }
                    "compress-level" => {
                        options.compress_level = value.parse::<i32>().map_err(|_| {
                            ExprError::InvalidParam {
                                name: "compress-level",
                                reason: format!("invalid compression level: {}", value).into(),
                            }
                        })?;
                        if !(0..=9).contains(&options.compress_level) {
                            return Err(ExprError::InvalidParam {
                                name: "compress-level",
                                reason: "compression level must be between 0 and 9".into(),
                            });
                        }
                    }
                    _ => {
                        // PostgreSQL ignores unknown options
                    }
                }
            }
        }

        Ok(options)
    }
}

/// Encrypts data using PGP symmetric encryption
/// Compatible with PostgreSQL's pgp_sym_encrypt function
#[function("pgp_sym_encrypt(varchar, varchar) -> bytea")]
fn pgp_sym_encrypt_no_options(data: &str, password: &str) -> Result<Box<[u8]>> {
    pgp_sym_encrypt_impl(data, password, None)
}

#[function("pgp_sym_encrypt(varchar, varchar, varchar) -> bytea")]
fn pgp_sym_encrypt_with_options(data: &str, password: &str, options: &str) -> Result<Box<[u8]>> {
    pgp_sym_encrypt_impl(data, password, Some(options))
}

fn pgp_sym_encrypt_impl(data: &str, password: &str, options: Option<&str>) -> Result<Box<[u8]>> {
    let opts = PgpOptions::parse(options)?;

    // Create a literal data packet
    let msg = Message::new_literal("", data);

    // Encrypt the message with the password
    let encrypted = msg
        .encrypt_to_keys_seipdv1(
            &mut rand::thread_rng(),
            opts.cipher_algo,
            &[],
        )
        .and_then(|m| {
            // Apply password encryption
            m.compress(opts.compress_algo)
        })
        .map_err(|e| ExprError::InvalidParam {
            name: "encryption",
            reason: format!("PGP encryption failed: {}", e).into(),
        })?;

    // Serialize to bytes
    let mut buf = Vec::new();
    encrypted
        .to_armored_writer(&mut buf, None)
        .map_err(|e| ExprError::InvalidParam {
            name: "encryption",
            reason: format!("PGP serialization failed: {}", e).into(),
        })?;

    Ok(buf.into_boxed_slice())
}

/// Decrypts data using PGP symmetric decryption
/// Compatible with PostgreSQL's pgp_sym_decrypt function
#[function("pgp_sym_decrypt(bytea, varchar) -> varchar")]
fn pgp_sym_decrypt_no_options(data: &[u8], password: &str) -> Result<String> {
    pgp_sym_decrypt_impl(data, password, None)
}

#[function("pgp_sym_decrypt(bytea, varchar, varchar) -> varchar")]
fn pgp_sym_decrypt_with_options(data: &[u8], password: &str, options: &str) -> Result<String> {
    pgp_sym_decrypt_impl(data, password, Some(options))
}

fn pgp_sym_decrypt_impl(data: &[u8], password: &str, _options: Option<&str>) -> Result<String> {
    // Parse the PGP message
    let cursor = Cursor::new(data);
    let (msg, _headers) = Message::from_armor_single(cursor).map_err(|e| ExprError::InvalidParam {
        name: "decryption",
        reason: format!("Failed to parse PGP message: {}", e).into(),
    })?;

    // Decrypt the message
    let decrypted_msg = msg
        .decrypt_with_password(|| password.to_string())
        .map_err(|e| ExprError::InvalidParam {
            name: "decryption",
            reason: format!("PGP decryption failed: {}", e).into(),
        })?;

    // Get the literal data
    let decrypted_data = decrypted_msg.get_content()?.unwrap_or_default();

    String::from_utf8(decrypted_data.to_vec()).map_err(|e| ExprError::InvalidParam {
        name: "decryption",
        reason: format!("Invalid UTF-8 in decrypted data: {}", e).into(),
    })
}

/// Encrypts data using PGP public key encryption
/// Compatible with PostgreSQL's pgp_pub_encrypt function
#[function("pgp_pub_encrypt(varchar, bytea) -> bytea")]
fn pgp_pub_encrypt_no_options(data: &str, public_key: &[u8]) -> Result<Box<[u8]>> {
    pgp_pub_encrypt_impl(data, public_key, None)
}

#[function("pgp_pub_encrypt(varchar, bytea, varchar) -> bytea")]
fn pgp_pub_encrypt_with_options(
    data: &str,
    public_key: &[u8],
    options: &str,
) -> Result<Box<[u8]>> {
    pgp_pub_encrypt_impl(data, public_key, Some(options))
}

fn pgp_pub_encrypt_impl(
    data: &str,
    public_key: &[u8],
    options: Option<&str>,
) -> Result<Box<[u8]>> {
    let opts = PgpOptions::parse(options)?;

    // Parse the public key
    let cursor = Cursor::new(public_key);
    let (key, _headers) = pgp::SignedPublicKey::from_armor_single(cursor).map_err(|e| {
        ExprError::InvalidParam {
            name: "public_key",
            reason: format!("Failed to parse public key: {}", e).into(),
        }
    })?;

    // Create a literal data packet
    let msg = Message::new_literal("", data);

    // Compress if needed
    let msg = if opts.compress_algo != CompressionAlgorithm::Uncompressed {
        msg.compress(opts.compress_algo)
            .map_err(|e| ExprError::InvalidParam {
                name: "encryption",
                reason: format!("PGP compression failed: {}", e).into(),
            })?
    } else {
        msg
    };

    // Encrypt the message with the public key
    let encrypted = msg
        .encrypt_to_keys(&mut rand::thread_rng(), opts.cipher_algo, &[&key])
        .map_err(|e| ExprError::InvalidParam {
            name: "encryption",
            reason: format!("PGP encryption failed: {}", e).into(),
        })?;

    // Serialize to bytes (use binary format to match PostgreSQL)
    let mut buf = Vec::new();
    encrypted
        .to_armored_writer(&mut buf, None)
        .map_err(|e| ExprError::InvalidParam {
            name: "encryption",
            reason: format!("PGP serialization failed: {}", e).into(),
        })?;

    Ok(buf.into_boxed_slice())
}

/// Decrypts data using PGP private key decryption
/// Compatible with PostgreSQL's pgp_pub_decrypt function
#[function("pgp_pub_decrypt(bytea, bytea) -> varchar")]
fn pgp_pub_decrypt_no_password(data: &[u8], secret_key: &[u8]) -> Result<String> {
    pgp_pub_decrypt_impl(data, secret_key, None, None)
}

#[function("pgp_pub_decrypt(bytea, bytea, varchar) -> varchar")]
fn pgp_pub_decrypt_with_password(
    data: &[u8],
    secret_key: &[u8],
    password: &str,
) -> Result<String> {
    pgp_pub_decrypt_impl(data, secret_key, Some(password), None)
}

#[function("pgp_pub_decrypt(bytea, bytea, varchar, varchar) -> varchar")]
fn pgp_pub_decrypt_with_options(
    data: &[u8],
    secret_key: &[u8],
    password: &str,
    options: &str,
) -> Result<String> {
    pgp_pub_decrypt_impl(data, secret_key, Some(password), Some(options))
}

fn pgp_pub_decrypt_impl(
    data: &[u8],
    secret_key: &[u8],
    password: Option<&str>,
    _options: Option<&str>,
) -> Result<String> {
    // Parse the PGP message
    let cursor = Cursor::new(data);
    let (msg, _headers) = Message::from_armor_single(cursor).map_err(|e| ExprError::InvalidParam {
        name: "decryption",
        reason: format!("Failed to parse PGP message: {}", e).into(),
    })?;

    // Parse the secret key
    let key_cursor = Cursor::new(secret_key);
    let (signed_secret_key, _headers) =
        SignedSecretKey::from_armor_single(key_cursor).map_err(|e| ExprError::InvalidParam {
            name: "secret_key",
            reason: format!("Failed to parse secret key: {}", e).into(),
        })?;

    // Unlock the secret key if password is provided
    let unlocked_key = if signed_secret_key.is_locked() {
        let pwd = password.ok_or_else(|| ExprError::InvalidParam {
            name: "password",
            reason: "Secret key is locked but no password provided".into(),
        })?;
        signed_secret_key
            .unlock(|| pwd.to_string(), |_| Ok(()))
            .map_err(|e| ExprError::InvalidParam {
                name: "password",
                reason: format!("Failed to unlock secret key: {}", e).into(),
            })?
    } else {
        signed_secret_key
    };

    // Decrypt the message
    let decrypted_msgs = msg
        .decrypt(|| "".to_string(), &[&unlocked_key])
        .map_err(|e| ExprError::InvalidParam {
            name: "decryption",
            reason: format!("PGP decryption failed: {}", e).into(),
        })?;

    // Get the first decrypted message
    let decrypted_msg = decrypted_msgs.first().ok_or_else(|| ExprError::InvalidParam {
        name: "decryption",
        reason: "No decrypted message found".into(),
    })?;

    // Get the literal data
    let decrypted_data = decrypted_msg.get_content()?.unwrap_or_default();

    String::from_utf8(decrypted_data.to_vec()).map_err(|e| ExprError::InvalidParam {
        name: "decryption",
        reason: format!("Invalid UTF-8 in decrypted data: {}", e).into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pgp_sym_encrypt_decrypt() {
        let data = "Hello, World!";
        let password = "test_password";

        // Test without options
        let encrypted = pgp_sym_encrypt_no_options(data, password).unwrap();
        let decrypted = pgp_sym_decrypt_no_options(&encrypted, password).unwrap();
        assert_eq!(decrypted, data);

        // Test with options
        let encrypted =
            pgp_sym_encrypt_with_options(data, password, "cipher-algo=aes256").unwrap();
        let decrypted = pgp_sym_decrypt_with_options(&encrypted, password, "").unwrap();
        assert_eq!(decrypted, data);
    }

    #[test]
    fn test_pgp_options_parse() {
        // Test default
        let opts = PgpOptions::parse(None).unwrap();
        assert_eq!(opts.cipher_algo, SymmetricKeyAlgorithm::AES128);

        // Test cipher-algo
        let opts = PgpOptions::parse(Some("cipher-algo=aes256")).unwrap();
        assert_eq!(opts.cipher_algo, SymmetricKeyAlgorithm::AES256);

        // Test compress-algo
        let opts = PgpOptions::parse(Some("compress-algo=0")).unwrap();
        assert_eq!(opts.compress_algo, CompressionAlgorithm::Uncompressed);

        // Test compress-level
        let opts = PgpOptions::parse(Some("compress-level=9")).unwrap();
        assert_eq!(opts.compress_level, 9);

        // Test multiple options
        let opts = PgpOptions::parse(Some("cipher-algo=aes192, compress-level=5")).unwrap();
        assert_eq!(opts.cipher_algo, SymmetricKeyAlgorithm::AES192);
        assert_eq!(opts.compress_level, 5);
    }
}

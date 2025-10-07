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
use sequoia_openpgp::armor;
use sequoia_openpgp::cert::Cert;
use sequoia_openpgp::crypto::SessionKey;
use sequoia_openpgp::packet::prelude::*;
use sequoia_openpgp::parse::{Parse, stream::*};
use sequoia_openpgp::policy::StandardPolicy;
use sequoia_openpgp::serialize::stream::{*, Encryptor2};

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
enum PgpError {
    #[error("PGP error: {0}")]
    Anyhow(#[from] anyhow::Error),
    #[error("Invalid key format: {0}")]
    InvalidKey(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// from [pg doc](https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-PGP-ENC-FUNCS)
/// pgp_sym_encrypt(data text, psw text) returns bytea
/// pgp_sym_encrypt_bytea(data bytea, psw text) returns bytea
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
    _options: Option<&str>,
) -> Result<Box<[u8]>, PgpError> {
    use openpgp::crypto::Password;
    use openpgp::types::SymmetricAlgorithm;

    let mut sink = Vec::new();
    
    // Create armored writer
    let message = armor::Writer::new(&mut sink, armor::Kind::Message)?;
    
    // Encrypt with password using SKESK (Symmetric-Key Encrypted Session Key)
    let message = Encryptor2::with_passwords(
        Message::new(message),
        vec![Password::from(password)],
    )
    .symmetric_algo(SymmetricAlgorithm::AES128)
    .build()?;
    
    // Write literal data packet
    let mut message = LiteralWriter::new(message).build()?;
    message.write_all(data)?;
    message.finalize()?;
    
    Ok(sink.into_boxed_slice())
}

/// from [pg doc](https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-PGP-ENC-FUNCS)
/// pgp_sym_decrypt(msg bytea, psw text) returns text
/// pgp_sym_decrypt_bytea(msg bytea, psw text) returns bytea
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
    _options: Option<&str>,
) -> Result<Box<[u8]>, PgpError> {
    let policy = &StandardPolicy::new();
    
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
            
            // Try password-based decryption
            skesks
                .iter()
                .find_map(|skesk| {
                    skesk
                        .decrypt(&Password::from(self.password.as_str()))
                        .ok()
                        .and_then(|(algo, session_key)| {
                            if decrypt(algo, &session_key) {
                                Some(None)
                            } else {
                                None
                            }
                        })
                })
                .ok_or_else(|| anyhow::anyhow!("Decryption failed").into())
        }
    }
    
    let helper = Helper {
        password: password.to_string(),
    };
    
    let mut decryptor = DecryptorBuilder::from_bytes(msg)?
        .with_policy(policy, None, helper)?;
    
    let mut plaintext = Vec::new();
    std::io::copy(&mut decryptor, &mut plaintext)?;
    
    Ok(plaintext.into_boxed_slice())
}

/// from [pg doc](https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-PGP-ENC-FUNCS)
/// pgp_pub_encrypt(data text, key bytea) returns bytea
/// pgp_pub_encrypt_bytea(data bytea, key bytea) returns bytea
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
    _options: Option<&str>,
) -> Result<Box<[u8]>, PgpError> {
    use openpgp::types::SymmetricAlgorithm;
    
    let policy = &StandardPolicy::new();
    
    // Parse the certificate (public key)
    let cert = Cert::from_bytes(key)?;
    
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
            "No valid encryption keys found".to_string(),
        ));
    }
    
    let mut sink = Vec::new();
    
    // Create armored writer
    let message = armor::Writer::new(&mut sink, armor::Kind::Message)?;
    
    // Encrypt with public key
    let message = Encryptor2::for_recipients(Message::new(message), recipients)
        .symmetric_algo(SymmetricAlgorithm::AES128)
        .build()?;
    
    // Write literal data packet
    let mut message = LiteralWriter::new(message).build()?;
    message.write_all(data)?;
    message.finalize()?;
    
    Ok(sink.into_boxed_slice())
}

/// from [pg doc](https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-PGP-ENC-FUNCS)
/// pgp_pub_decrypt(msg bytea, key bytea) returns text
/// pgp_pub_decrypt_bytea(msg bytea, key bytea) returns bytea
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
    _options: Option<&str>,
) -> Result<Box<[u8]>, PgpError> {
    let policy = &StandardPolicy::new();
    
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
            _sym_algo: Option<openpgp::types::SymmetricAlgorithm>,
            mut decrypt: D,
        ) -> openpgp::Result<Option<openpgp::Fingerprint>>
        where
            D: FnMut(openpgp::types::SymmetricAlgorithm, &SessionKey) -> bool,
        {
            use openpgp::crypto::Password;
            
            let policy = &StandardPolicy::new();
            
            // Try decrypting with each key
            let result = pkesks
                .iter()
                .find_map(|pkesk| {
                    self.cert
                        .keys()
                        .unencrypted_secret()
                        .with_policy(policy, None)
                        .supported()
                        .for_transport_encryption()
                        .find_map(|ka| {
                            let mut keypair = if let Some(pwd) = &self.password {
                                ka.key()
                                    .clone()
                                    .decrypt_secret(&Password::from(pwd.as_str()))
                                    .ok()?
                                    .into_keypair()
                                    .ok()?
                            } else {
                                ka.key().clone().into_keypair().ok()?
                            };
                            
                            pkesk
                                .decrypt(&mut keypair, None)
                                .and_then(|(algo, session_key)| {
                                    if decrypt(algo, &session_key) {
                                        Some(ka.fingerprint())
                                    } else {
                                        None
                                    }
                                })
                        })
                });
            
            result.map(Some).ok_or_else(|| anyhow::anyhow!("Decryption failed").into())
        }
    }
    
    let helper = Helper {
        cert,
        password: password.map(|s| s.to_string()),
    };
    
    let mut decryptor = DecryptorBuilder::from_bytes(msg)?
        .with_policy(policy, None, helper)?;
    
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
}

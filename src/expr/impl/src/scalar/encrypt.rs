// Copyright 2024 RisingWave Labs
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

use openssl::error::ErrorStack;
use openssl::symm::{Cipher, Crypter, Mode as CipherMode};
use regex::Regex;
use risingwave_expr::{function, CryptographyError, CryptographyStage, ExprError, Result};

#[derive(Debug, Clone, PartialEq)]
enum Algorithm {
    Blowfish,
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

#[derive(Debug, Clone)]
pub struct CipherConfig {
    algorithm: Algorithm,
    mode: Mode,
    padding: Padding,
    crypt_key: Vec<u8>,
}

impl CipherConfig {
    fn parse_cipher_config(key: &[u8], input: &str) -> Result<CipherConfig> {
        let re = Regex::new(r"^(aes|bf)(?:-(cbc|ecb))?(?:/pad:(pkcs|none))?$").unwrap();
        let (algorithm, mode, padding) = {
            if let Some(caps) = re.captures(input) {
                let algorithm = match caps.get(1).map(|s| s.as_str()) {
                    Some("bf") => Algorithm::Blowfish,
                    Some("aes") => Algorithm::Aes,
                    algo => {
                        return Err(ExprError::InvalidParam {
                            name: "mode",
                            reason: format!("expect bf or aes for algorithm, but got: {:?}", algo)
                                .into(),
                        })
                    }
                };

                let mode = match caps.get(2).map(|m| m.as_str()) {
                    Some("cbc") | None => Mode::Cbc,
                    Some("ecb") => Mode::Ecb, // Default to Ecb if not specified
                    mode => {
                        return Err(ExprError::InvalidParam {
                            name: "mode",
                            reason: format!(
                                "expect cbc or ecb for mode, but got: {}",
                                mode.unwrap()
                            )
                            .into(),
                        })
                    }
                };

                let padding = match caps.get(3).map(|m| m.as_str()) {
                    Some("pkcs") | None => Padding::Pkcs, // Default to Pkcs if not specified
                    Some("none") => Padding::None,
                    padding => {
                        return Err(ExprError::InvalidParam {
                            name: "mode",
                            reason: format!(
                                "expect cbc or ecb for padding, but got: {}",
                                padding.unwrap()
                            )
                            .into(),
                        })
                    }
                };

                (algorithm, mode, padding)
            } else {
                return Err(ExprError::InvalidParam {
                    name: "mode",
                    reason: format!(
                        "invalid mode: {}, expect pattern algorithm[-mode][/pad:padding]",
                        input
                    )
                    .into(),
                });
            }
        };

        if algorithm == Algorithm::Aes && key.len() != 16 && key.len() != 24 && key.len() != 32 {
            return Err(ExprError::InvalidParam {
                name: "key",
                reason: format!("invalid key length: {}, expect 16, 24 or 32", key.len()).into(),
            });
        }

        Ok(CipherConfig {
            algorithm,
            mode,
            padding,
            crypt_key: key.to_vec(),
        })
    }

    fn build_cipher(&self) -> Result<Cipher> {
        // match config's algorithm, mode, padding to openssl's cipher
        match (&self.algorithm, self.crypt_key.len(), &self.mode) {
            (Algorithm::Blowfish, _, Mode::Cbc) => Ok(Cipher::bf_cbc()),
            (Algorithm::Blowfish, _, Mode::Ecb) => Ok(Cipher::bf_ecb()),
            (Algorithm::Aes, 16, Mode::Cbc) => Ok(Cipher::aes_128_cbc()),
            (Algorithm::Aes, 16, Mode::Ecb) => Ok(Cipher::aes_128_ecb()),
            (Algorithm::Aes, 24, Mode::Cbc) => Ok(Cipher::aes_192_cbc()),
            (Algorithm::Aes, 24, Mode::Ecb) => Ok(Cipher::aes_192_ecb()),
            (Algorithm::Aes, 32, Mode::Cbc) => Ok(Cipher::aes_256_cbc()),
            (Algorithm::Aes, 32, Mode::Ecb) => Ok(Cipher::aes_256_ecb()),
            _ => Err(ExprError::InvalidParam {
                name: "mode",
                reason: format!(
                    "invalid algorithm {:?} mode: {:?}",
                    self.algorithm, self.mode
                )
                .into(),
            }),
        }
    }

    fn enable_padding(&self) -> bool {
        match self.padding {
            Padding::Pkcs => true,
            Padding::None => false,
        }
    }
}

/// from [pg doc](https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-RAW-ENC-FUNCS)
#[function(
    "decrypt(bytea, bytea, varchar) -> bytea",
    prebuild = "CipherConfig::parse_cipher_config($1, $2)?"
)]
pub fn decrypt(data: &[u8], config: &CipherConfig) -> Result<Box<[u8]>> {
    let report_error = |e: ErrorStack| {
        ExprError::Cryptography(Box::new(CryptographyError {
            stage: CryptographyStage::Decrypt,
            reason: e,
        }))
    };

    let cipher = config.build_cipher()?;
    let mut decrypter = Crypter::new(cipher, CipherMode::Decrypt, config.crypt_key.as_ref(), None)
        .map_err(report_error)?;
    decrypter.pad(config.enable_padding());
    let mut decrypt = vec![0; data.len() + cipher.block_size()];
    let count = decrypter.update(data, &mut decrypt).map_err(report_error)?;
    let rest = decrypter
        .finalize(&mut decrypt[count..])
        .map_err(report_error)?;
    decrypt.truncate(count + rest);
    Ok(decrypt.into())
}

#[function(
    "encrypt(bytea, bytea, varchar) -> bytea",
    prebuild = "CipherConfig::parse_cipher_config($1, $2)?"
)]
pub fn encrypt(data: &[u8], config: &CipherConfig) -> Result<Box<[u8]>> {
    let report_error = |e: ErrorStack| {
        ExprError::Cryptography(Box::new(CryptographyError {
            stage: CryptographyStage::Encrypt,
            reason: e,
        }))
    };

    let cipher = config.build_cipher()?;
    let mut encryptor = Crypter::new(cipher, CipherMode::Encrypt, config.crypt_key.as_ref(), None)
        .map_err(report_error)?;
    encryptor.pad(config.enable_padding());
    let mut encrypt = vec![0; data.len() + cipher.block_size()];
    let count = encryptor.update(data, &mut encrypt).map_err(report_error)?;
    let rest = encryptor
        .finalize(&mut encrypt[count..])
        .map_err(report_error)?;
    encrypt.truncate(count + rest);
    Ok(encrypt.into())
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
        let encrypt_wrapper = |data: &[u8], key: &[u8], mode: &str| -> Result<Box<[u8]>> {
            let config = CipherConfig::parse_cipher_config(key, mode)?;
            encrypt(data, &config)
        };
        let decrypt_wrapper = |data: &[u8], key: &[u8], mode: &str| -> Result<Box<[u8]>> {
            let config = CipherConfig::parse_cipher_config(key, mode)?;
            decrypt(data, &config)
        };
        let key = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f";

        let encrypted = encrypt_wrapper(
            b"\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff",
            key,
            "aes-ecb/pad:none",
        )
        .unwrap();

        let decrypted = decrypt_wrapper(&encrypted, key, "aes-ecb/pad:none").unwrap();
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

        let mode_4 = "bf/pad:none";
        let config = CipherConfig::parse_cipher_config(key, mode_4).unwrap();
        assert_eq!(config.algorithm, Algorithm::Blowfish);
        assert_eq!(config.mode, Mode::Cbc);
        assert_eq!(config.padding, Padding::None);

        let mode_5 = "cbc";
        assert!(CipherConfig::parse_cipher_config(key, mode_5).is_err());
    }
}

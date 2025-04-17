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
use std::sync::LazyLock;

use openssl::error::ErrorStack;
use openssl::symm::{Cipher, Crypter, Mode as CipherMode};
use regex::Regex;
use risingwave_expr::{ExprError, Result, function};

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

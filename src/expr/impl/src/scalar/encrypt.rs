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
use risingwave_expr::{function, CryptographyError, CryptographyStage, ExprError, Result};

#[derive(Debug, Clone)]
enum Algorithm {
    Blowfish,
    Aes,
}

#[derive(Debug, Clone)]
enum Mode {
    Cbc,
    Ecb,
}
#[derive(Debug, Clone)]
enum Padding {
    Pkcs,
    None,
}

#[derive(Debug, Clone)]
pub struct CipherConfig {
    algorithm: Algorithm,
    mode: Mode,
    _padding: Padding,
}

// fn padding_or_truncate_key(key: &[u8], cipher: &Cipher) -> Vec<u8> {
//     let key_len = key.len();
//     let block_size = cipher.block_size();
//     if key_len > block_size {
//         key[..block_size].to_vec()
//     } else if key_len < block_size {
//         let mut padded = vec![0; block_size];
//         padded[..key_len].copy_from_slice(key);
//         padded
//     } else {
//         key.to_vec()
//     }
// }

impl CipherConfig {
    fn parse_cipher_config(input: &str) -> Result<CipherConfig> {
        let parts: Vec<&str> = input.split(&['-', '/'][..]).collect();

        let algorithm = match parts.get(0) {
            Some(&"bf") => Algorithm::Blowfish,
            Some(&"aes") => Algorithm::Aes,
            algo => {
                return Err(ExprError::InvalidParam {
                    name: "mode",
                    reason: format!("expect bf or aes, but got: {:?}", algo).into(),
                })
            }
        };

        let mut mode = Mode::Cbc; // default to CBC
        let mut padding = Padding::Pkcs; // default to PKCS

        for part in parts.iter().skip(1) {
            if part.starts_with("cbc") {
                mode = Mode::Cbc;
            } else if part.starts_with("ecb") {
                mode = Mode::Ecb;
            } else if part.starts_with("pad:pkcs") {
                padding = Padding::Pkcs;
            } else if part.starts_with("pad:none") {
                padding = Padding::None;
            }
        }

        Ok(CipherConfig {
            algorithm,
            mode,
            _padding: padding,
        })
    }

    // postgres example can be found [here](https://github.com/postgres/postgres/blob/master/contrib/pgcrypto/sql/rijndael.sql)
    fn build_cipher(&self, key: &[u8]) -> Result<Cipher> {
        // match config's algorithm, mode, padding to openssl's cipher
        match (&self.algorithm, key.len(), &self.mode) {
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
}

/// from [pg doc](https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-RAW-ENC-FUNCS)
#[function(
    "decrypt(bytea, bytea, varchar) -> bytea",
    prebuild = "CipherConfig::parse_cipher_config($2)?"
)]
pub fn decrypt(data: &[u8], key: &[u8], config: &CipherConfig) -> Result<Box<[u8]>> {
    let cipher = config.build_cipher(key)?;

    let report_error = |e: ErrorStack| {
        ExprError::Cryptography(Box::new(CryptographyError {
            stage: CryptographyStage::Decrypt,
            payload: data.into(),
            reason: e.to_string().into(),
        }))
    };

    let mut decrypter =
        Crypter::new(cipher, CipherMode::Decrypt, key, None).map_err(report_error)?;
    let mut decrypt = vec![0; data.len() + cipher.block_size()];
    let count = decrypter.update(data, &mut decrypt).map_err(report_error)?;
    // let rest = decrypter
    //     .finalize(&mut decrypt[count..])
    //     .map_err(report_error)?;
    decrypt.truncate(count);
    Ok(decrypt.into())
}

#[function(
    "encrypt(bytea, bytea, varchar) -> bytea",
    prebuild = "CipherConfig::parse_cipher_config($2)?"
)]
pub fn encrypt(data: &[u8], key: &[u8], config: &CipherConfig) -> Result<Box<[u8]>> {
    let report_error = |e: ErrorStack| {
        ExprError::Cryptography(Box::new(CryptographyError {
            stage: CryptographyStage::Encrypt,
            payload: data.into(),
            reason: e.to_string().into(),
        }))
    };

    let cipher = config.build_cipher(key)?;
    let mut encryptor =
        Crypter::new(cipher, CipherMode::Encrypt, key, None).map_err(report_error)?;
    let mut encrypt = vec![0; data.len() + cipher.block_size()];
    let count = encryptor.update(data, &mut encrypt).map_err(report_error)?;
    // let rest = encryptor
    //     .finalize(&mut encrypt[count..])
    //     .map_err(report_error)?;
    encrypt.truncate(count);
    Ok(encrypt.into())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_decrypt() {
        let data = b"hello world";
        let key = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F";
        let mode = "aes";

        let config = CipherConfig::parse_cipher_config(mode).unwrap();
        let encrypted = encrypt(data, key, &config).unwrap();

        let decrypted = decrypt(&encrypted, key, &config).unwrap();
        assert_eq!(decrypted, (*data).into());
    }
    // #[test]
    // fn test_cipher_config() {
    //     let config = "bf-cbc/pad:pkcs";
    //
    //   let parsed = parse_cipher_config(config).unwrap();
    // }

    #[test]
    fn encrypt_testcase() {
        let encrypt_wrapper = |data: &[u8], key: &[u8], mode: &str| -> Result<Box<[u8]>> {
            let config = CipherConfig::parse_cipher_config(mode)?;
            println!("config: {:?}", config);
            encrypt(data, key, &config)
        };
        let decrypt_wrapper = |data: &[u8], key: &[u8], mode: &str| -> Result<Box<[u8]>> {
            let config = CipherConfig::parse_cipher_config(mode)?;
            println!("config: {:?}", config);
            decrypt(data, key, &config)
        };

        let encrypted = encrypt_wrapper(
            b"\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff",
            b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f",
            "aes-ecb/pad:none",
        )
        .unwrap();
        encrypted.iter().for_each(|b| print!("{:02x}", b));

        let decrypted = decrypt_wrapper(
            &encrypted,
            b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0e",
            "aes-ecb/pad:none",
        )
        .unwrap();

        decrypted.iter().for_each(|b| print!("{:02x}", b));
    }

    #[test]
    fn test() {
        let data = b"\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff";
        let key = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f";
        let cipher = Cipher::aes_128_ecb();
        let mut encrypter =
            Crypter::new(cipher, CipherMode::Encrypt, key, None).expect("encrypter");
        let mut encrypt = vec![0; data.len() + cipher.block_size()];
        let count = encrypter.update(data, &mut encrypt).expect("update");
        // let rest = encrypter.finalize(&mut encrypt[count..]).expect("finalize");
        encrypt.truncate(count);
        encrypt.iter().for_each(|b| print!("{:02x}", b));
        println!();
    }
}

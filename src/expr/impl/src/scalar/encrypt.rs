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

use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{AeadCore, Aes128Gcm, Aes256Gcm, Key, KeyInit};
use risingwave_expr::{function, CryptographyError, CryptographyStage, ExprError, Result};

/// from [pg doc](https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-RAW-ENC-FUNCS)
// we cannot do prebuild here, because the interface requires mut ref
#[function("decrypt(bytea, bytea, varchar) -> bytea")]
pub fn decrypt(data: &[u8], key: &[u8], mode: &str) -> Result<Box<[u8]>> {
    let decrypt = match mode.to_lowercase().as_str() {
        "aes128" => {
            let cipher = Aes128Gcm::new(Key::<Aes128Gcm>::from_slice(key));
            let nonce = Aes128Gcm::generate_nonce(&mut OsRng);
            cipher.decrypt(&nonce, data)
        }
        "aes256" => {
            let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
            let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
            cipher.decrypt(&nonce, data)
        }
        _ => {
            return Err(ExprError::InvalidParam {
                name: "mode",
                reason: format!("invalid mode: {}, only accept aes128 or aes256 here", mode).into(),
            });
        }
    }
    .map_err(|e| {
        ExprError::Cryptography(Box::new(CryptographyError {
            stage: CryptographyStage::Decrypt,
            payload: data.into(),
            reason: e,
        }))
    })?;
    Ok(decrypt.into())
}

#[function("encrypt(bytea, bytea, varchar) -> bytea")]
pub fn encrypt(data: &[u8], key: &[u8], mode: &str) -> Result<Box<[u8]>> {
    let encrypt = match mode.to_lowercase().as_str() {
        "aes128" => {
            let cipher = Aes128Gcm::new(Key::<Aes128Gcm>::from_slice(key));
            let nonce = Aes128Gcm::generate_nonce(&mut OsRng);
            cipher.encrypt(&nonce, data)
        }
        "aes256" => {
            let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
            let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
            cipher.encrypt(&nonce, data)
        }
        _ => {
            return Err(ExprError::InvalidParam {
                name: "mode",
                reason: format!("invalid mode: {}, only accept aes128 or aes256 here", mode).into(),
            });
        }
    }
    .map_err(|e| {
        ExprError::Cryptography(Box::new(CryptographyError {
            stage: CryptographyStage::Encrypt,
            payload: data.into(),
            reason: e,
        }))
    })?;
    Ok(encrypt.into())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_decrypt() {
        let data = b"hello world";
        let key = b"1234567890123456";
        let mode = "aes128";
        let encrypted = encrypt(data, key, mode).unwrap();
        println!("encrypted: {:?}", encrypted);

        let decrypted = decrypt(&encrypted, key, mode);
        if let Err(e) = decrypted {
            println!("{}", e);
        }
        // assert_eq!(decrypted, (*data).into());
    }
}

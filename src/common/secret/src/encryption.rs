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

use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::Aes128Gcm;
use serde::{Deserialize, Serialize};

use super::{SecretError, SecretResult};

#[derive(Deserialize, Serialize)]
pub struct SecretEncryption {
    nonce: [u8; 12],
    ciphertext: Vec<u8>,
}

impl SecretEncryption {
    pub fn encrypt(key: &[u8], plaintext: &[u8]) -> SecretResult<Self> {
        let encrypt_key = Self::fill_key(key);
        let nonce_array = Aes128Gcm::generate_nonce(&mut OsRng);
        let cipher = Aes128Gcm::new(encrypt_key.as_slice().into());
        let ciphertext = cipher
            .encrypt(&nonce_array, plaintext)
            .map_err(|_| SecretError::AesError)?;
        Ok(Self {
            nonce: nonce_array.into(),
            ciphertext,
        })
    }

    pub fn decrypt(&self, key: &[u8]) -> SecretResult<Vec<u8>> {
        let decrypt_key = Self::fill_key(key);
        let nonce_array = GenericArray::from_slice(&self.nonce);
        let cipher = Aes128Gcm::new(decrypt_key.as_slice().into());
        let plaintext = cipher
            .decrypt(nonce_array, self.ciphertext.as_slice())
            .map_err(|_| SecretError::AesError)?;
        Ok(plaintext)
    }

    fn fill_key(key: &[u8]) -> Vec<u8> {
        let mut k = key[..(std::cmp::min(key.len(), 16))].to_vec();
        k.resize_with(16, || 0);
        k
    }

    pub fn serialize(&self) -> SecretResult<Vec<u8>> {
        let res = bincode::serialize(&self)?;
        Ok(res)
    }

    pub fn deserialize(data: &[u8]) -> SecretResult<Self> {
        let res = bincode::deserialize(data)?;
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_secret_encryption_decyption() {
        let key = &hex::decode("0123456789abcdef0123456789abcdef").unwrap();
        assert!(key.len() == 16);
        let plaintext = "Hello, world!".as_bytes();
        let secret = SecretEncryption::encrypt(key, plaintext).unwrap();
        let decrypted = secret.decrypt(key).unwrap();
        assert_eq!(plaintext, decrypted.as_slice());
    }
}

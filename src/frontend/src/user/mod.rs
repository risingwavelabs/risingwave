// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use itertools::Itertools;
use md5;
use risingwave_pb::user::auth_info::EncryptionType;
use risingwave_pb::user::AuthInfo;
use sha2::{Digest, Sha256};

pub(crate) mod root_user;
pub(crate) mod user_service;

pub type UserName = String;
pub type UserInfoVersion = u64;

const SHA256_ENCRYPTED_PREFIX: &str = "SHA-256$4096:";
const MD5_ENCRYPTED_PREFIX: &str = "md5";

/// Try to extract the encryption password from given password. The password is always stored
/// encrypted in the system catalogs. The ENCRYPTED keyword has no effect, but is accepted for
/// backwards compatibility. The method of encryption is by default SHA-256-encrypted. If the
/// presented password string is already in MD5-encrypted or SHA-256-encrypted format, then it is
/// stored as-is regardless of `password_encryption` (since the system cannot decrypt the specified
/// encrypted password string, to encrypt it in a different format).
pub fn try_extract(password: &str) -> Option<AuthInfo> {
    // Specifying an empty string will also set the auth info to null.
    if password.is_empty() {
        return None;
    }
    if password.starts_with(SHA256_ENCRYPTED_PREFIX) {
        Some(AuthInfo {
            encryption_type: EncryptionType::Sha256 as i32,
            encrypted_value: password.trim_start_matches(SHA256_ENCRYPTED_PREFIX).into(),
        })
    } else if password.starts_with(MD5_ENCRYPTED_PREFIX) {
        Some(AuthInfo {
            encryption_type: EncryptionType::Md5 as i32,
            encrypted_value: password.trim_start_matches(MD5_ENCRYPTED_PREFIX).into(),
        })
    } else {
        Some(encrypt_default(password))
    }
}

/// Encrypt the password with SHA-256 as default.
pub fn encrypt_default(password: &str) -> AuthInfo {
    AuthInfo {
        encryption_type: EncryptionType::Sha256 as i32,
        encrypted_value: encrypt_sha256(password),
    }
}

/// Encrypt the password with SHA-256.
pub fn encrypt_sha256(password: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hasher.finalize().to_vec()
}

/// Encrypt the password with MD5.
pub fn encrypt_md5(password: &str) -> Vec<u8> {
    md5::compute(password).to_vec()
}

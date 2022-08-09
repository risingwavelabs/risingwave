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

use risingwave_pb::user::auth_info::EncryptionType;
use risingwave_pb::user::AuthInfo;
use sha2::{Digest, Sha256};

// SHA-256 is not supported in PostgreSQL protocol. We need to implement SCRAM-SHA-256 instead
// if necessary.
const SHA256_ENCRYPTED_PREFIX: &str = "SHA-256:";
const MD5_ENCRYPTED_PREFIX: &str = "md5";

const VALID_SHA256_ENCRYPTED_LEN: usize = SHA256_ENCRYPTED_PREFIX.len() + 64;
const VALID_MD5_ENCRYPTED_LEN: usize = MD5_ENCRYPTED_PREFIX.len() + 32;

/// Try to extract the encryption password from given password. The password is always stored
/// encrypted in the system catalogs. The ENCRYPTED keyword has no effect, but is accepted for
/// backwards compatibility. The method of encryption is by default SHA-256-encrypted. If the
/// presented password string is already in MD5-encrypted or SHA-256-encrypted format, then it is
/// stored as-is regardless of `password_encryption` (since the system cannot decrypt the specified
/// encrypted password string, to encrypt it in a different format).
///
/// For an MD5 encrypted password, rolpassword column will begin with the string md5 followed by a
/// 32-character hexadecimal MD5 hash. The MD5 hash will be of the user's password concatenated to
/// their user name. For example, if user joe has password xyzzy, we will store the md5 hash of
/// xyzzyjoe.
///
/// For an SHA-256 encrypted password, rolpassword column will begin with the string SHA-256:
/// followed by a 64-character hexadecimal SHA-256 hash, which is the SHA-256 hash of the user's
/// password concatenated to their user name. The SHA-256 will be the default hash algorithm for
/// Risingwave.
///
/// A password that does not follow either of those formats is assumed to be unencrypted.
#[inline(always)]
pub fn encrypted_password(name: &str, password: &str) -> Option<AuthInfo> {
    // Specifying an empty string will also set the auth info to null.
    if password.is_empty() {
        return None;
    }

    if valid_sha256_password(password) {
        Some(AuthInfo {
            encryption_type: EncryptionType::Sha256 as i32,
            encrypted_value: password.trim_start_matches(SHA256_ENCRYPTED_PREFIX).into(),
        })
    } else if valid_md5_password(password) {
        Some(AuthInfo {
            encryption_type: EncryptionType::Md5 as i32,
            encrypted_value: password.trim_start_matches(MD5_ENCRYPTED_PREFIX).into(),
        })
    } else {
        Some(encrypt_default(name, password))
    }
}

/// Encrypt the password with MD5 as default.
#[inline(always)]
pub fn encrypt_default(name: &str, password: &str) -> AuthInfo {
    AuthInfo {
        encryption_type: EncryptionType::Md5 as i32,
        encrypted_value: md5_hash(name, password),
    }
}

/// Encrypt the stored password with given salt, used for user authentication.
#[inline(always)]
pub fn md5_hash_with_salt(encrypted_value: &[u8], salt: &[u8; 4]) -> Vec<u8> {
    let mut ctx = md5::Context::new();
    ctx.consume(encrypted_value);
    ctx.consume(salt);
    format!("md5{:x}", ctx.compute()).into_bytes()
}

#[inline(always)]
fn valid_sha256_password(password: &str) -> bool {
    password.starts_with(SHA256_ENCRYPTED_PREFIX) && password.len() == VALID_SHA256_ENCRYPTED_LEN
}

#[inline(always)]
fn valid_md5_password(password: &str) -> bool {
    password.starts_with(MD5_ENCRYPTED_PREFIX) && password.len() == VALID_MD5_ENCRYPTED_LEN
}

/// Encrypt "`password`+`name`" with SHA-256.
#[cfg_attr(not(test), expect(dead_code))]
#[inline(always)]
pub fn sha256_hash(name: &str, password: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hasher.update(name.as_bytes());
    format!("{:x}", hasher.finalize()).into_bytes()
}

/// Encrypt "`password`+`name`" with MD5.
#[inline(always)]
pub fn md5_hash(name: &str, password: &str) -> Vec<u8> {
    let mut ctx = md5::Context::new();
    ctx.consume(password.as_bytes());
    ctx.consume(name.as_bytes());
    format!("{:x}", ctx.compute()).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_password() {
        let (user_name, password) = ("foo", "bar");
        assert_eq!(
            b"96948aad3fcae80c08a35c9b5958cd89".to_vec(),
            md5_hash(user_name, password)
        );
        assert_eq!(
            b"md59f2fa6a30871a92249bdd2f1eeee4ef6".to_vec(),
            md5_hash_with_salt(
                b"96948aad3fcae80c08a35c9b5958cd89",
                &[0x1a, 0x2b, 0x3d, 0x4e]
            )
        );
        assert_eq!(
            b"88ecde925da3c6f8ec3d140683da9d2a422f26c1ae1d9212da1e5a53416dcc88".to_vec(),
            sha256_hash(user_name, password)
        );

        let input_passwords = vec![
            "bar",
            "",
            "md596948aad3fcae80c08a35c9b5958cd89",
            "SHA-256:88ecde925da3c6f8ec3d140683da9d2a422f26c1ae1d9212da1e5a53416dcc88",
        ];
        let expected_output_passwords = vec![
            Some(AuthInfo {
                encryption_type: EncryptionType::Md5 as i32,
                encrypted_value: md5_hash(user_name, password),
            }),
            None,
            Some(AuthInfo {
                encryption_type: EncryptionType::Md5 as i32,
                encrypted_value: md5_hash(user_name, password),
            }),
            Some(AuthInfo {
                encryption_type: EncryptionType::Sha256 as i32,
                encrypted_value: sha256_hash(user_name, password),
            }),
        ];
        let output_passwords = input_passwords
            .iter()
            .map(|&p| encrypted_password(user_name, p))
            .collect::<Vec<_>>();
        assert_eq!(output_passwords, expected_output_passwords);
    }
}

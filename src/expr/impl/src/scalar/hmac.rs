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

use hmac::{Hmac, Mac};
use risingwave_expr::{function, ExprError, Result};
use sha1::Sha1;
use sha2::Sha256;

#[function("hmac(varchar, bytea, varchar) -> bytea")]
pub fn hmac(secret: &str, payload: &[u8], sha_type: &str) -> Result<Box<[u8]>> {
    if sha_type == "sha1" {
        Ok(hmac_sha1(secret, payload))
    } else if sha_type == "sha256" {
        Ok(hmac_sha256(secret, payload))
    } else {
        return Err(ExprError::InvalidParam {
            name: "sha_type",
            reason: format!("Unsupported SHA type: {}", sha_type).into(),
        });
    }
}

fn hmac_sha256(secret: &str, payload: &[u8]) -> Box<[u8]> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(payload);

    let code_bytes = mac.finalize().into_bytes();
    code_bytes.as_slice().into()
}

fn hmac_sha1(secret: &str, payload: &[u8]) -> Box<[u8]> {
    let mut mac =
        Hmac::<Sha1>::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(payload);

    let code_bytes = mac.finalize().into_bytes();
    code_bytes.as_slice().into()
}

#[cfg(test)]
mod tests {
    use hex::encode;

    use super::*;

    #[test]
    fn test_verify_signature_hmac_sha256() {
        let secret = "your_secret_key";
        let payload = b"your_webhook_payload";
        let signature = "cef8b98a91902c492b85d97f049aa4bfc5e7e3f9b8b7bf7cb49c5f829d2dac85";
        assert!(encode(hmac_sha256(secret, payload)) == signature);
    }

    #[test]
    fn test_verify_signature_hmac_sha1() {
        let secret = "your_secret_key";
        let payload = b"your_webhook_payload";
        let signature = "65cb920a4b8c6ab8e2eab861a096a7bc2c05d8ba";
        assert!(encode(hmac_sha1(secret, payload)) == signature);
    }
}

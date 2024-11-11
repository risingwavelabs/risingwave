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

use hex::encode;
use hmac::{Hmac, Mac};
use risingwave_expr::function;
use sha1::Sha1;
use sha2::Sha256;

#[function("hmac(bytea, bytea, varchar) -> bytea")]
pub fn hmac(secret: &[u8], payload: &[u8], sha_type: &str) -> Box<[u8]> {
    if sha_type == "sha1" {
        sha1_hmac(secret, payload)
    } else if sha_type == "sha256" {
        sha256_hmac(secret, payload)
    } else {
        panic!("Unsupported SHA type: {}", sha_type)
    }
}

fn sha256_hmac(secret: &[u8], payload: &[u8]) -> Box<[u8]> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC can take key of any size");

    mac.update(payload);

    let result = mac.finalize();
    let code_bytes = result.into_bytes();
    let computed_signature = format!("sha256={}", encode(code_bytes));
    computed_signature.as_bytes().into()
}

fn sha1_hmac(secret: &[u8], payload: &[u8]) -> Box<[u8]> {
    let mut mac = Hmac::<Sha1>::new_from_slice(secret).expect("HMAC can take key of any size");

    mac.update(payload);

    let result = mac.finalize();
    let code_bytes = result.into_bytes();
    let computed_signature = format!("sha1={}", encode(code_bytes));
    computed_signature.as_bytes().into()
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_verify_signature_hmac_sha256() -> anyhow::Result<()> {
        let secret = "your_secret_key";
        let payload = b"your_webhook_payload";
        let signature = b"sha256=cef8b98a91902c492b85d97f049aa4bfc5e7e3f9b8b7bf7cb49c5f829d2dac85"; // 替换为
        assert!(*sha256_hmac(secret, payload) == *signature);
        Ok(())
    }

    #[tokio::test]
    async fn test_verify_signature_hmac_sha1() -> anyhow::Result<()> {
        let secret = "your_secret_key";
        let payload = b"your_webhook_payload";
        let signature = b"sha1=65cb920a4b8c6ab8e2eab861a096a7bc2c05d8ba"; // 替换为
        assert!(*sha1_hmac(secret, payload) == *signature);
        Ok(())
    }
}

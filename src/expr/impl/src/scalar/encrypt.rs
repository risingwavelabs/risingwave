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

use aes_gcm::aead::OsRng;
use aes_gcm::{Aes128Gcm, Aes256Gcm, Key, KeyInit};
use risingwave_expr::{function, ExprError, Result};

enum CypherEnum {
    Aes128(Aes128Gcm),
    Aes256(Aes256Gcm),
}

fn build_encrypt(key: &[u8], mode: &str) -> Result<CypherEnum> {
    let cypher = match mode.to_lowercase().as_str() {
        "aes128" => CypherEnum::Aes128(Aes128Gcm::new(Key::<Aes128Gcm>::from_slice(key))),
        "aes256" => CypherEnum::Aes256(Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key))),
        _ => {
            return Err(ExprError::InvalidParam {
                name: "mode",
                reason: format!("invalid mode: {}, only accept aes128 or aes256 here", mode).into(),
            });
        }
    };

    Ok(cypher)
}

/// from pg doc https://www.postgresql.org/docs/current/pgcrypto.html#PGCRYPTO-RAW-ENC-FUNCS
#[function(
    "decrypt(bytea, bytea, varchar) -> bytea",
    prebuild = "build_encrypt($1, $2)?"
)]
pub fn decrypt(data: &[u8], cypher: CypherEnum) -> Result<Box<[u8]>> {
    Ok(data)
}

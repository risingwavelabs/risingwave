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
use risingwave_common::types::Uuid;
use risingwave_expr::function;

/// Generate a random UUID using the gen_random_uuid() function
#[function("gen_random_uuid() -> uuid")]
pub fn gen_random_uuid() -> Uuid {
    Uuid::new_v4()
}

/// Generate a deterministic UUID from any string using the gen_uuid_from_string(varchar) function.
/// This can be a contradiction with cast(varchar) -> uuid , but later only supports the conversion of uuid formatted string.
#[function("gen_uuid_from_string(varchar) -> uuid")]
pub fn gen_uuid_from_string(input: &str) -> Uuid {
    Uuid::new_v5(input)
}

/// Generate a UUID from 16 bytes. Since the cast is avaiable do we need this?
#[function("gen_uuid_from_bytea(bytea) -> uuid")]
pub fn gen_uuid_from_bytea(bytes: &[u8]) -> Uuid {
    if bytes.len() == 16 {
        let mut arr = [0u8; 16];
        arr.copy_from_slice(bytes);
        Uuid::from_bytes(arr)
    } else {
        // a nil UUID than to panic
        Uuid::nil()
    }
}

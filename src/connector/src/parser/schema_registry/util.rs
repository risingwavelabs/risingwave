// Copyright 2023 RisingWave Labs
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

use byteorder::{BigEndian, ByteOrder};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};

/// extract the magic number and `schema_id` at the front of payload
///
/// 0 -> magic number
/// 1-4 -> schema id
/// 5-... -> message payload
pub(crate) fn extract_schema_id(payload: &[u8]) -> Result<(i32, &[u8])> {
    let header_len = 5;

    if payload.len() < header_len {
        return Err(RwError::from(InternalError(format!(
            "confluent kafka message need 5 bytes header, but payload len is {}",
            payload.len()
        ))));
    }
    let magic = payload[0];
    let schema_id = BigEndian::read_i32(&payload[1..5]);

    if magic != 0 {
        return Err(RwError::from(InternalError(
            "confluent kafka message must have a zero magic byte".to_owned(),
        )));
    }

    Ok((schema_id, &payload[header_len..]))
}

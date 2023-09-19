use std::collections::HashMap;

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
use risingwave_common::array::{RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use serde_json::{Map, Value};

use super::encoder::{JsonEncoder, RowEncoder, TimestampHandlingMode};
use crate::sink::Result;

pub fn chunk_to_json(chunk: StreamChunk, schema: &Schema) -> Result<Vec<String>> {
    let encoder = JsonEncoder::new(schema, None, TimestampHandlingMode::Milli);
    let mut records: Vec<String> = Vec::with_capacity(chunk.capacity());
    for (_, row) in chunk.rows() {
        let record = Value::Object(encoder.encode(row)?);
        records.push(record.to_string());
    }

    Ok(records)
}

pub fn doris_rows_to_json(
    row: RowRef<'_>,
    schema: &Schema,
    decimal_map: &HashMap<String, (u8, u8)>,
) -> Result<Map<String, Value>> {
    let encoder = JsonEncoder::new_with_doris(
        schema,
        None,
        TimestampHandlingMode::Milli,
        decimal_map.clone(),
    );
    let map = encoder.encode(row)?;
    Ok(map)
}

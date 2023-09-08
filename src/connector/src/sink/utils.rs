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

use risingwave_common::array::RowRef;
use risingwave_common::catalog::Field;
use serde_json::{Map, Value};

use super::encoder::{JsonEncoder, RowEncoder};
use crate::sink::Result;

#[derive(Clone, Copy)]
pub enum TimestampHandlingMode {
    Milli,
    String,
}

pub fn record_to_json(
    row: RowRef<'_>,
    schema: &[Field],
    timestamp_handling_mode: TimestampHandlingMode,
) -> Result<Map<String, Value>> {
    JsonEncoder::new(timestamp_handling_mode).encode_all(row, schema)
}

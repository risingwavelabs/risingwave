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

use std::collections::BTreeMap;
use std::fmt::Debug;

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use super::operators::*;
use crate::parser::common::json_parse_value;
use crate::{SourceParser, SourceStreamChunkRowWriter, WriteGuard};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebeziumEvent {
    pub payload: Payload,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Payload {
    pub before: Option<BTreeMap<String, Value>>,
    pub after: Option<BTreeMap<String, Value>>,
    pub op: String,
}

#[derive(Debug)]
pub struct DebeziumJsonParser;

impl SourceParser for DebeziumJsonParser {
    fn parse(&self, payload: &[u8], writer: SourceStreamChunkRowWriter<'_>) -> Result<WriteGuard> {
        let event: DebeziumEvent = serde_json::from_slice(payload)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        let mut payload = event.payload;

        match payload.op.as_str() {
            DEBEZIUM_UPDATE_OP => {
                let before = payload.before.as_mut().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "before is missing for updating event. If you are using postgres, you may want to try ALTER TABLE $TABLE_NAME REPLICA IDENTITY FULL;".to_string(),
                    ))
                })?;

                let after = payload.after.as_mut().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "after is missing for updating event".to_string(),
                    ))
                })?;

                writer.update(|column| {
                    let before = json_parse_value(&column.data_type, before.get(&column.name))?;
                    let after = json_parse_value(&column.data_type, after.get(&column.name))?;

                    Ok((before, after))
                })
            }
            DEBEZIUM_CREATE_OP | DEBEZIUM_READ_OP => {
                let after = payload.after.as_ref().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "after is missing for creating event".to_string(),
                    ))
                })?;

                writer.insert(|column| {
                    json_parse_value(&column.data_type, after.get(&column.name)).map_err(Into::into)
                })
            }
            DEBEZIUM_DELETE_OP => {
                let before = payload.before.as_ref().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "before is missing for delete event".to_string(),
                    ))
                })?;

                writer.delete(|column| {
                    json_parse_value(&column.data_type, before.get(&column.name))
                        .map_err(Into::into)
                })
            }
            _ => Err(RwError::from(ProtocolError(format!(
                "unknown debezium op: {}",
                payload.op
            )))),
        }
    }
}

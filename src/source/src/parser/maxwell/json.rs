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
use std::fmt::{Debug, Formatter};

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use crate::parser::common::json_parse_value;
use crate::{SourceParser, SourceStreamChunkRowWriter, WriteGuard};

const MAXWELL_INSERT_OP: &str = "insert";
const MAXWELL_UPDATE_OP: &str = "update";
const MAXWELL_DELETE_OP: &str = "delete";

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaxwellEvent {
    pub data: Option<BTreeMap<String, Value>>,
    pub old: Option<BTreeMap<String, Value>>,
    #[serde(rename = "type")]
    pub op: String,
    #[serde(rename = "ts")]
    pub ts_ms: i64,
}

#[derive(Debug)]
pub struct MaxwellParser;

impl SourceParser for MaxwellParser {
    fn parse(&self, payload: &[u8], writer: SourceStreamChunkRowWriter<'_>) -> Result<WriteGuard> {
        let event: MaxwellEvent = serde_json::from_slice(payload)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        match event.op.as_str() {
            MAXWELL_INSERT_OP => {
                let after = event.data.as_ref().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "data is missing for creating event".to_string(),
                    ))
                })?;
                writer.insert(|column| {
                    json_parse_value(&column.data_type, after.get(&column.name)).map_err(Into::into)
                })
            }
            MAXWELL_UPDATE_OP => {
                let after = event.data.as_ref().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "data is missing for creating event".to_string(),
                    ))
                })?;
                let before = event.old.as_ref().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "old is missing for creating event".to_string(),
                    ))
                })?;

                // old only contains the changed columns but data contains all columns.
                // we copy data columns here and overwrite with change ones to get the original row.
                let mut before_full = after.clone();
                before_full.extend(before.clone());

                writer.update(|column| {
                    let before = json_parse_value(&column.data_type, before.get(&column.name))?;
                    let after = json_parse_value(&column.data_type, after.get(&column.name))?;
                    Ok((before, after))
                })
            }
            MAXWELL_DELETE_OP => {
                let before = event.data.as_ref().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "old is missing for creating event".to_string(),
                    ))
                })?;
                writer.delete(|column| {
                    json_parse_value(&column.data_type, before.get(&column.name))
                        .map_err(Into::into)
                })
            }
            other => Err(RwError::from(ProtocolError(format!(
                "unknown Maxwell op: {}",
                other
            )))),
        }
    }
}

mod tests {
    use crate::parser::maxwell::json::MAXWELL_UPDATE_OP;
    use crate::parser::maxwell::MaxwellEvent;

    #[test]
    fn test_event_deserialize() {
        let payload = "{\"database\":\"test\",\"table\":\"e\",\"type\":\"update\",\"ts\":1477053234,\"data\":{\"id\":1,\"m\":5.444,\"c\":\"2016-10-21 05:33:54.631000\",\"comment\":\"I am a creature of light.\"},\"old\":{\"m\":4.2341,\"c\":\"2016-10-21 05:33:37.523000\"}}";
        let event: MaxwellEvent = serde_json::from_slice(payload.as_ref()).unwrap();
        println!("event: {:?}", event);

        assert_eq!(event.op, MAXWELL_UPDATE_OP.to_string());

        let mut after = event.data.unwrap();
        let before = event.old.unwrap();

        println!("{:?}", after);
        after.extend(before.clone());
        println!("{:?}", after);

        assert_eq!(after.get("c"), before.get("c"));
    }
}

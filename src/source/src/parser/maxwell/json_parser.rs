// Copyright 2023 Singularity Data
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

use std::collections::BTreeMap;
use std::fmt::Debug;

use futures::future::ready;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use super::operators::*;
use crate::parser::common::json_parse_value;
use crate::{ParseFuture, SourceParser, SourceStreamChunkRowWriter, WriteGuard};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaxwellEvent {
    #[serde(rename = "data")]
    pub after: Option<BTreeMap<String, Value>>,
    #[serde(rename = "old")]
    pub before: Option<BTreeMap<String, Value>>,
    #[serde(rename = "type")]
    pub op: String,
}

#[derive(Debug)]
pub struct MaxwellParser;

impl MaxwellParser {
    fn parse_inner(
        &self,
        payload: &[u8],
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let event: MaxwellEvent = serde_json::from_slice(payload)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        match event.op.as_str() {
            MAXWELL_INSERT_OP => {
                let after = event.after.as_ref().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "data is missing for creating event".to_string(),
                    ))
                })?;
                writer.insert(|column| {
                    json_parse_value(&column.data_type, after.get(&column.name)).map_err(Into::into)
                })
            }
            MAXWELL_UPDATE_OP => {
                let after = event.after.as_ref().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "data is missing for updating event".to_string(),
                    ))
                })?;
                let before = event.before.ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "old is missing for updating event".to_string(),
                    ))
                })?;

                writer.update(|column| {
                    // old only contains the changed columns but data contains all columns.
                    let before_value = before
                        .get(column.name.as_str())
                        .or_else(|| after.get(column.name.as_str()));
                    let before = json_parse_value(&column.data_type, before_value)?;
                    let after = json_parse_value(&column.data_type, after.get(&column.name))?;
                    Ok((before, after))
                })
            }
            MAXWELL_DELETE_OP => {
                let before = event.after.as_ref().ok_or_else(|| {
                    RwError::from(ProtocolError("old is missing for delete event".to_string()))
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

impl SourceParser for MaxwellParser {
    type ParseResult<'a> = impl ParseFuture<'a, Result<WriteGuard>>;

    fn parse<'a, 'b, 'c>(
        &'a self,
        payload: &'b [u8],
        writer: SourceStreamChunkRowWriter<'c>,
    ) -> Self::ParseResult<'a>
    where
        'b: 'a,
        'c: 'a,
    {
        ready(self.parse_inner(payload, writer))
    }
}

mod tests {

    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_event_deserialize() {
        let payload = "{\"database\":\"test\",\"table\":\"e\",\"type\":\"update\",\"ts\":1477053234,\"data\":{\"id\":1,\"m\":5.444,\"c\":\"2016-10-21 05:33:54.631000\",\"comment\":\"I am a creature of light.\"},\"old\":{\"m\":4.2341,\"c\":\"2016-10-21 05:33:37.523000\"}}";
        let event: MaxwellEvent = serde_json::from_slice(payload.as_ref()).unwrap();

        assert_eq!(event.op, MAXWELL_UPDATE_OP.to_string());

        let mut after = event.after.unwrap();
        let before = event.before.unwrap();

        after.extend(before.clone());

        assert_eq!(after.get("c"), before.get("c"));
    }
}

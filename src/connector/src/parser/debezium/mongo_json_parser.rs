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

use std::fmt::Debug;

use futures_async_stream::try_stream;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use simd_json::{BorrowedValue, StaticNode, ValueAccess};

use super::operators::*;
use crate::impl_common_parser_logic;
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{SourceColumnDesc, SourceContextRef};

const BEFORE: &str = "before";
const AFTER: &str = "after";
const OP: &str = "op";

impl_common_parser_logic!(DebeziumMongoJsonParser);
#[inline]
fn ensure_not_null<'a, 'b: 'a>(value: &'a BorrowedValue<'b>) -> Option<&'a BorrowedValue<'b>> {
    if let BorrowedValue::Static(StaticNode::Null) = value {
        None
    } else {
        Some(value)
    }
}

#[derive(Debug)]
pub struct DebeziumMongoJsonParser {
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    id_column: SourceColumnDesc,
    payload_column: SourceColumnDesc,
    source_ctx: SourceContextRef,
}

fn parse_bson_value(
    id_type: &DataType,
    payload_type: &DataType,
    value: &BorrowedValue<'_>,
) -> anyhow::Result<(Datum, Datum)> {
    let bson_str = value.as_str().unwrap_or_default();
    let bson_value: serde_json::Value = serde_json::from_str(bson_str)?;
    let bson_doc = bson_value.as_object().ok_or_else(|| {
        RwError::from(ProtocolError(
            "Debezuim Mongo requires payload is a document".into(),
        ))
    })?;
    let id_field = bson_doc
        .get("_id")
        .ok_or_else(|| {
            RwError::from(ProtocolError(
                "Debezuim Mongo requires document has a `_id` field".into(),
            ))
        })?
        .clone();
    let id: Datum = match id_type {
        DataType::Jsonb => ScalarImpl::Jsonb(id_field.into()).into(),
        DataType::Varchar => match id_field {
            serde_json::Value::String(s) => Some(ScalarImpl::Utf8(s.into())),
            serde_json::Value::Object(obj) if obj.contains_key("$oid") => Some(ScalarImpl::Utf8(
                obj["$oid"].as_str().to_owned().unwrap_or_default().into(),
            )),
            _ => Err(RwError::from(ProtocolError(format!(
                "Can not convert bson {:?} to {:?}",
                id_field,
                id_type
            ))))?,
        },
        DataType::Int32 => {
            if let serde_json::Value::Object(ref obj) = id_field  && obj.contains_key("$numberInt"){
                    let int_str = obj["$numberInt"].as_str().unwrap_or_default();
                    Some(ScalarImpl::Int32(int_str.parse().unwrap_or_default()))
                } else {
                    Err(RwError::from(ProtocolError(format!(
                        "Can not convert bson {:?} to {:?}",
                        id_field,
                        id_type
                    ))))?
            }
        }
        DataType::Int64 => {
            if let serde_json::Value::Object(ref obj) = id_field && obj.contains_key("$numberLong") {
                    let int_str = obj["$numberLong"].as_str().unwrap_or_default();
                    Some(ScalarImpl::Int64(int_str.parse().unwrap_or_default()))
                } else {
                    Err(RwError::from(ProtocolError(format!(
                        "Can not convert bson {:?} to {:?}",
                        id_field,
                        id_type
                    ))))?
            }
        }
        _ => unreachable!("DebeziumMongoJsonParser::new must ensure _id column datatypes."),
    };
    let payload: Datum = match payload_type {
        DataType::Jsonb => ScalarImpl::Jsonb(bson_value.into()).into(),
        DataType::Varchar => ScalarImpl::Utf8(bson_str.into()).into(),
        _ => unreachable!("DebeziumMongoJsonParser::new must ensure payload column datatypes."),
    };

    Ok((id, payload))
}
impl DebeziumMongoJsonParser {
    pub fn new(rw_columns: Vec<SourceColumnDesc>, source_ctx: SourceContextRef) -> Result<Self> {
        let id_column = rw_columns
            .iter()
            .find(|desc| {
                desc.name == "_id"
                    && matches!(
                        desc.data_type,
                        DataType::Jsonb
                            | DataType::Varchar
                            | DataType::Int32
                            | DataType::Int64
                    )
            })
            .ok_or_else(|| RwError::from(ProtocolError(
                "Debezuim Mongo needs a `_id` column with supported types (Varchar Jsonb int32 int64) in table".into(),
            )))?.clone();
        let payload_column = rw_columns
            .iter()
            .find(|desc| {
                desc.name == "payload"
                    && matches!(desc.data_type, DataType::Jsonb | DataType::Varchar)
            })
            .ok_or_else(||RwError::from(ProtocolError(
                "Debezuim Mongo needs a `payload` column with supported types (Varchar Jsonb) in table".into(),
            )))?.clone();

        if rw_columns.len() != 2 {
            return Err(RwError::from(ProtocolError(
                "Debezuim Mongo needs no more columns except `_id` and `payload` in table".into(),
            )));
        }

        Ok(Self {
            rw_columns,
            id_column,
            payload_column,

            source_ctx,
        })
    }

    #[allow(clippy::unused_async)]
    pub async fn parse_inner(
        &self,
        mut payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let event: BorrowedValue<'_> = simd_json::to_borrowed_value(&mut payload)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        let payload = event
            .get("payload")
            .and_then(ensure_not_null)
            .ok_or_else(|| {
                RwError::from(ProtocolError("no payload in debezium event".to_owned()))
            })?;

        let op = payload.get(OP).and_then(|v| v.as_str()).ok_or_else(|| {
            RwError::from(ProtocolError(
                "op field not found in debezium json".to_owned(),
            ))
        })?;

        match op {
            DEBEZIUM_UPDATE_OP => {
                let before = payload
                    .get(BEFORE)
                    .and_then(ensure_not_null)
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(
                            "before is missing for updating event.".to_string(),
                        ))
                    })?;

                let after = payload
                    .get(AFTER)
                    .and_then(ensure_not_null)
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(
                            "after is missing for updating event".to_string(),
                        ))
                    })?;
                let (before_id, before_payload) = parse_bson_value(
                    &self.id_column.data_type,
                    &self.payload_column.data_type,
                    before,
                )?;
                let (after_id, after_payload) = parse_bson_value(
                    &self.id_column.data_type,
                    &self.payload_column.data_type,
                    after,
                )?;

                writer.update(|column| {
                    if column.name == self.id_column.name {
                        Ok((before_id.clone(), after_id.clone()))
                    } else if column.name == self.payload_column.name {
                        Ok((before_payload.clone(), after_payload.clone()))
                    } else {
                        unreachable!("writer.update must not pass columns more than required")
                    }
                })
            }
            DEBEZIUM_CREATE_OP | DEBEZIUM_READ_OP => {
                let after = payload
                    .get(AFTER)
                    .and_then(ensure_not_null)
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(
                            "after is missing for creating event".to_string(),
                        ))
                    })?;

                let (after_id, after_payload) = parse_bson_value(
                    &self.id_column.data_type,
                    &self.payload_column.data_type,
                    after,
                )?;

                writer.insert(|column| {
                    if column.name == self.id_column.name {
                        Ok(after_id.clone())
                    } else if column.name == self.payload_column.name {
                        Ok(after_payload.clone())
                    } else {
                        unreachable!("writer.insert must not pass columns more than required")
                    }
                })
            }
            DEBEZIUM_DELETE_OP => {
                let before = payload
                    .get(BEFORE)
                    .and_then(ensure_not_null)
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(
                            "before is missing for delete event".to_string(),
                        ))
                    })?;
                let (before_id, before_payload) = parse_bson_value(
                    &self.id_column.data_type,
                    &self.payload_column.data_type,
                    before,
                )?;

                writer.delete(|column| {
                    if column.name == self.id_column.name {
                        Ok(before_id.clone())
                    } else if column.name == self.payload_column.name {
                        Ok(before_payload.clone())
                    } else {
                        unreachable!("writer.delete must not pass columns more than required")
                    }
                })
            }
            _ => Err(RwError::from(ProtocolError(format!(
                "unknown debezium op: {}",
                op
            )))),
        }
    }
}

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

/// NOTICE: This file describes not the official Canal format, but the `TiCDC` implementation, as described in [the website](https://docs.pingcap.com/tidb/v6.0/ticdc-canal-json).
use std::collections::BTreeMap;
use std::str::FromStr;

use anyhow::anyhow;
use futures::future::ready;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl};
use risingwave_expr::vector_op::cast::{str_to_date, str_to_timestamp, str_to_timestampz};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use crate::parser::canal::operators::*;
use crate::{
    ensure_rust_type, ensure_str, ParseFuture, SourceParser, SourceStreamChunkRowWriter, WriteGuard,
};

#[derive(Debug)]
pub struct CanalJsonParser;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanalJsonEvent {
    #[serde(rename = "old")]
    pub before: Option<Vec<BTreeMap<String, Value>>>,
    #[serde(rename = "data")]
    pub after: Option<Vec<BTreeMap<String, Value>>>,
    #[serde(rename = "type")]
    pub op: String,
    pub ts: u64,
    pub is_ddl: bool,
}

impl CanalJsonParser {
    fn parse_inner(
        &self,
        payload: &[u8],
        writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let event: CanalJsonEvent = serde_json::from_slice(payload)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;
        if event.is_ddl {
            return Err(RwError::from(ProtocolError(
                "received a DDL message, please set `canal.instance.filter.query.dml` to true."
                    .to_string(),
            )));
        }
        match event.op.as_str() {
            CANAL_DELETE_EVENT => {
                let before = event
                    .before
                    .as_ref()
                    .and_then(|l| l.first())
                    .ok_or_else(|| {
                        RwError::from(ProtocolError("old is missing for delete event".to_string()))
                    })?;
                writer.delete(|columns| {
                    canal_json_parse_value(&columns.data_type, before.get(&columns.name))
                        .map_err(Into::into)
                })
            }
            CANAL_INSERT_EVENT => {
                let after = event
                    .after
                    .as_ref()
                    .and_then(|l| l.first())
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(
                            "data is missing for delete event".to_string(),
                        ))
                    })?;

                writer.insert(|columns| {
                    canal_json_parse_value(&columns.data_type, after.get(&columns.name))
                        .map_err(Into::into)
                })
            }
            CANAL_UPDATE_EVENT => {
                let before = event
                    .before
                    .as_ref()
                    .and_then(|l| l.first())
                    .ok_or_else(|| {
                        RwError::from(ProtocolError("old is missing for delete event".to_string()))
                    })?;

                let after = event
                    .after
                    .as_ref()
                    .and_then(|l| l.first())
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(
                            "data is missing for delete event".to_string(),
                        ))
                    })?;

                writer.update(|column| {
                    // in origin canal, old only contains the changed columns but data contains all
                    // columns.
                    // in ticdc, old contains all fields
                    let before_value = before
                        .get(column.name.as_str())
                        .or_else(|| after.get(column.name.as_str()));
                    let old = canal_json_parse_value(&column.data_type, before_value)?;
                    let new = canal_json_parse_value(&column.data_type, after.get(&column.name))?;

                    Ok((old, new))
                })
            }
            other => Err(RwError::from(ProtocolError(format!(
                "unknown canal json op: {}",
                other
            )))),
        }
    }
}

impl SourceParser for CanalJsonParser {
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

#[inline]
pub(crate) fn canal_json_parse_value(dtype: &DataType, value: Option<&Value>) -> Result<Datum> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(cannal_do_parse_json_value(dtype, v).map_err(|e| {
            anyhow!("failed to parse type '{}' from json: {}", dtype, e)
        })?)),
    }
}

#[inline]
fn cannal_do_parse_json_value(dtype: &DataType, v: &Value) -> Result<ScalarImpl> {
    let v = match dtype {
        // mysql use tinyint to represent boolean
        DataType::Boolean => ScalarImpl::Bool(ensure_rust_type!(v, i16) != 0),
        DataType::Int16 => ScalarImpl::Int16(ensure_rust_type!(v, i16)),
        DataType::Int32 => ScalarImpl::Int32(ensure_rust_type!(v, i32)),
        DataType::Int64 => ScalarImpl::Int64(ensure_rust_type!(v, i64)),
        DataType::Float32 => ScalarImpl::Float32(ensure_rust_type!(v, f32).into()),
        DataType::Float64 => ScalarImpl::Float64(ensure_rust_type!(v, f64).into()),
        // FIXME: decimal should have more precision than f64
        DataType::Decimal => Decimal::from_str(ensure_str!(v, "string"))
            .map_err(|_| anyhow!("parse decimal from string err {}", v))?
            .into(),
        DataType::Varchar => ensure_str!(v, "varchar").to_string().into(),
        DataType::Date => str_to_date(ensure_str!(v, "date"))?.into(),
        DataType::Time => str_to_date(ensure_str!(v, "time"))?.into(),
        DataType::Timestamp => str_to_timestamp(ensure_str!(v, "string"))?.into(),
        DataType::Timestampz => str_to_timestampz(ensure_str!(v, "string"))?.into(),
        _ => {
            return Err(RwError::from(InternalError(format!(
                "cannal data source not support type {}",
                dtype
            ))))
        }
    };
    Ok(v)
}

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

use std::collections::HashMap;

use async_trait::async_trait;
use risingwave_common::{array::StreamChunk, types::DataType};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_rpc_client::ConnectorClient;
use serde_with::serde_as;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};


use crate::{sink::{DummySinkCommitCoordinator, Result, Sink, SinkWriter, SinkWriterParam}, common::DorisCommon};

use super::{SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_UPSERT, SINK_TYPE_OPTION, doris_connector::DorisField};

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct DorisConfig {
    #[serde(flatten)]
    pub common: DorisCommon,

    pub r#type: String, // accept "append-only" or "upsert"
}
impl DorisConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<DorisConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

#[derive(Debug)]
pub struct DorisSink{
    pub config: DorisConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl DorisSink {
    pub fn new(
        config: DorisConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl DorisSink{
    async fn check_column_name_and_type(&self, doris_column_fileds: Vec<DorisField>) -> Result<()>{
        let doris_columns_desc: HashMap<String, String> = doris_column_fileds
            .iter()
            .map(|s| (s.name.clone(), s.r#type.clone()))
            .collect();

        let rw_fields_name = self.schema.fields();
        if rw_fields_name.len().gt(&doris_columns_desc.len()) {
            return Err(SinkError::Doris("The length of the RisingWave column must be greater than/equal to the length of the Clickhouse column".to_string()));
        }

        for i in rw_fields_name {
            let value = doris_columns_desc
                .get(&i.name)
                .ok_or(SinkError::ClickHouse(format!(
                    "Column name don't find in doris, risingwave is {:?} ",
                    i.name
                )))?;
            let a = Self::check_and_correct_column_type(&i.data_type, value.to_string())?;
        }
        Ok(())
    }

    fn check_and_correct_column_type(rw_data_type: &DataType, doris_data_type: String) -> Result<bool>{
        match rw_data_type {
            risingwave_common::types::DataType::Boolean => Ok(doris_data_type.contains("Bool")),
            risingwave_common::types::DataType::Int16 => {
                Ok(doris_data_type.contains("Int16"))
            }
            risingwave_common::types::DataType::Int32 => {
                Ok(doris_data_type.contains("Int32"))
            }
            risingwave_common::types::DataType::Int64 => {
                Ok(doris_data_type.contains("Int64"))
            }
            risingwave_common::types::DataType::Float32 => Ok(doris_data_type.contains("Float32")),
            risingwave_common::types::DataType::Float64 => Ok(doris_data_type.contains("Float64")),
            risingwave_common::types::DataType::Decimal => {
                Ok(doris_data_type.contains("Decimal"))
            }
            risingwave_common::types::DataType::Date => Ok(doris_data_type.contains("Date")),
            risingwave_common::types::DataType::Varchar => Ok(doris_data_type.contains("Varchar")),
            risingwave_common::types::DataType::Time => Err(SinkError::Doris(
                "doris can not support Time".to_string(),
            )),
            risingwave_common::types::DataType::Timestamp => {
                Ok(doris_data_type.contains("DateTime"))
            }
            risingwave_common::types::DataType::Timestamptz => Err(SinkError::Doris(
                "doris can not support Timestamptz".to_string(),
            )),
            risingwave_common::types::DataType::Interval => Err(SinkError::Doris(
                "doris can not support Interval".to_string(),
            )),
            risingwave_common::types::DataType::Struct(_) => Err(SinkError::Doris(
                "struct needs to be converted into a list".to_string(),
            )),
            risingwave_common::types::DataType::List(list) => {
                Self::check_and_correct_column_type(list.as_ref(), doris_data_type.clone())?;
                Ok(doris_data_type.contains("Array"))
            }
            risingwave_common::types::DataType::Bytea => Err(SinkError::Doris(
                "doris can not support Bytea".to_string(),
            )),
            risingwave_common::types::DataType::Jsonb => 
                Ok(doris_data_type.contains("Jsonb")),
            risingwave_common::types::DataType::Serial => {
                Ok(doris_data_type.contains("Int64"))
            }
            risingwave_common::types::DataType::Int256 => Err(SinkError::Doris(
                "doris can not support Interval".to_string(),
            )),
        }
    }
    
}

#[async_trait]
impl Sink for DorisSink {
    type Coordinator = DummySinkCommitCoordinator;
    type Writer = DorisSinkWriter;

    async fn new_writer(&self, _writer_env: SinkWriterParam) -> Result<Self::Writer> {
        todo!()
    }

    async fn validate(&self, _client: Option<ConnectorClient>) -> Result<()> {
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert doris sink (please define in `primary_key` field)")));
        }
        // check reachability
        let client = self.config.common.build_client()?;
        let doris_schema = client
            .get_schema_from_doris().await?;

        Ok(())
    }
}

pub struct DorisSinkWriter;

#[async_trait]
impl SinkWriter for DorisSinkWriter {
    async fn write_batch(&mut self, _chunk: StreamChunk) -> Result<()> {
        todo!();
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        todo!()
    }

    async fn abort(&mut self) -> Result<()> {
        todo!()
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
        todo!()
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Bitmap) -> Result<()> {
        todo!()
    }
}

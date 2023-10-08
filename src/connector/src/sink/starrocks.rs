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
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use serde::Deserialize;
use serde_json::Value;
use serde_with::serde_as;

use super::doris_connector::{
    HeaderBuilder, InserterBuilder, StarrocksInsert, StarrocksMysqlQuery, STARROCKS_DELETE_SIGN,
};
use super::encoder::{JsonEncoder, RowEncoder, TimestampHandlingMode};
use super::writer::LogSinkerOf;
use super::{SinkError, SinkParam, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT};
use crate::common::StarrocksCommon;
use crate::sink::writer::SinkWriterExt;
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkWriter, SinkWriterParam};

pub const STARROCKS_SINK: &str = "starrocks";
#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct StarrocksConfig {
    #[serde(flatten)]
    pub common: StarrocksCommon,

    pub r#type: String, // accept "append-only" or "upsert"
}
impl StarrocksConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<StarrocksConfig>(serde_json::to_value(properties).unwrap())
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
pub struct StarrocksSink {
    pub config: StarrocksConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl StarrocksSink {
    pub fn new(
        config: StarrocksConfig,
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

impl StarrocksSink {
    fn check_column_name_and_type(
        &self,
        starrocks_columns_desc: HashMap<String, String>,
    ) -> Result<()> {
        let rw_fields_name = self.schema.fields();
        if rw_fields_name.len().ne(&starrocks_columns_desc.len()) {
            return Err(SinkError::Starrocks("The length of the RisingWave column must be equal to the length of the starrocks column".to_string()));
        }

        for i in rw_fields_name {
            let value = starrocks_columns_desc.get(&i.name).ok_or_else(|| {
                SinkError::Starrocks(format!(
                    "Column name don't find in starrocks, risingwave is {:?} ",
                    i.name
                ))
            })?;
            if !Self::check_and_correct_column_type(&i.data_type, value.to_string())? {
                return Err(SinkError::Starrocks(format!(
                    "Column type don't match, column name is {:?}. starrocks type is {:?} risingwave type is {:?} ",i.name,value,i.data_type
                )));
            }
        }
        Ok(())
    }

    fn check_and_correct_column_type(
        rw_data_type: &DataType,
        starrocks_data_type: String,
    ) -> Result<bool> {
        match rw_data_type {
            risingwave_common::types::DataType::Boolean => {
                Ok(starrocks_data_type.contains("tinyint"))
            }
            risingwave_common::types::DataType::Int16 => {
                Ok(starrocks_data_type.contains("smallint"))
            }
            risingwave_common::types::DataType::Int32 => Ok(starrocks_data_type.contains("int")),
            risingwave_common::types::DataType::Int64 => Ok(starrocks_data_type.contains("bigint")),
            risingwave_common::types::DataType::Float32 => {
                Ok(starrocks_data_type.contains("float"))
            }
            risingwave_common::types::DataType::Float64 => {
                Ok(starrocks_data_type.contains("double"))
            }
            risingwave_common::types::DataType::Decimal => {
                Ok(starrocks_data_type.contains("decimal"))
            }
            risingwave_common::types::DataType::Date => Ok(starrocks_data_type.contains("date")),
            risingwave_common::types::DataType::Varchar => {
                Ok(starrocks_data_type.contains("varchar"))
            }
            risingwave_common::types::DataType::Time => Err(SinkError::Starrocks(
                "starrocks can not support Time".to_string(),
            )),
            risingwave_common::types::DataType::Timestamp => {
                Ok(starrocks_data_type.contains("datetime"))
            }
            risingwave_common::types::DataType::Timestamptz => Err(SinkError::Starrocks(
                "starrocks can not support Timestamptz".to_string(),
            )),
            risingwave_common::types::DataType::Interval => Err(SinkError::Starrocks(
                "starrocks can not support Interval".to_string(),
            )),
            // todo! Validate the type struct and list
            risingwave_common::types::DataType::Struct(_) => Err(SinkError::Starrocks(
                "starrocks can not support import struct".to_string(),
            )),
            risingwave_common::types::DataType::List(_) => {
                Ok(starrocks_data_type.contains("unknown"))
            }
            risingwave_common::types::DataType::Bytea => Err(SinkError::Starrocks(
                "starrocks can not support Bytea".to_string(),
            )),
            risingwave_common::types::DataType::Jsonb => Err(SinkError::Starrocks(
                "starrocks can not support import json".to_string(),
            )),
            risingwave_common::types::DataType::Serial => {
                Ok(starrocks_data_type.contains("bigint"))
            }
            risingwave_common::types::DataType::Int256 => Err(SinkError::Starrocks(
                "starrocks can not support Int256".to_string(),
            )),
        }
    }
}

impl Sink for StarrocksSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<StarrocksSinkWriter>;

    const SINK_NAME: &'static str = STARROCKS_SINK;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(StarrocksSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert starrocks sink (please define in `primary_key` field)")));
        }
        // check reachability
        let mut client = StarrocksMysqlQuery::new(
            self.config.common.host.clone(),
            self.config.common.mysql_port.clone(),
            self.config.common.table.clone(),
            self.config.common.database.clone(),
            self.config.common.user.clone(),
            self.config.common.password.clone(),
        )
        .await?;
        let (read_model, pks) = client.get_pk_from_starrocks().await?;

        if !self.is_append_only && read_model.ne("PRIMARY_KEYS") {
            return Err(SinkError::Config(anyhow!(
                "If you want to use upsert, please set the keysType of starrocks to PRIMARY_KEY"
            )));
        }

        for (index, filed) in self.schema.fields().iter().enumerate() {
            if self.pk_indices.contains(&index) && !pks.contains(&filed.name) {
                return Err(SinkError::Starrocks(format!(
                    "Can't find pk {:?} in starrocks",
                    filed.name
                )));
            }
        }

        let starrocks_columns_desc = client.get_columns_from_starrocks().await?;

        self.check_column_name_and_type(starrocks_columns_desc)?;
        Ok(())
    }
}

pub struct StarrocksSinkWriter {
    pub config: StarrocksConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    builder: InserterBuilder,
    is_append_only: bool,
    insert: Option<StarrocksInsert>,
    row_encoder: JsonEncoder,
}

impl TryFrom<SinkParam> for StarrocksSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = StarrocksConfig::from_hashmap(param.properties)?;
        StarrocksSink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
    }
}

impl StarrocksSinkWriter {
    pub async fn new(
        config: StarrocksConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let mut decimal_map = HashMap::default();
        let starrocks_columns = StarrocksMysqlQuery::new(
            config.common.host.clone(),
            config.common.mysql_port.clone(),
            config.common.table.clone(),
            config.common.database.clone(),
            config.common.user.clone(),
            config.common.password.clone(),
        )
        .await?
        .get_columns_from_starrocks()
        .await?;

        for (name, column_type) in &starrocks_columns {
            if column_type.contains("decimal") {
                let decimal_all = column_type
                    .split("decimal(")
                    .last()
                    .ok_or_else(|| SinkError::Starrocks("must have last".to_string()))?
                    .split(')')
                    .next()
                    .ok_or_else(|| SinkError::Starrocks("must have next".to_string()))?
                    .split(',')
                    .collect_vec();
                let length = decimal_all
                    .first()
                    .ok_or_else(|| SinkError::Starrocks("must have next".to_string()))?
                    .parse::<u8>()
                    .map_err(|e| SinkError::Starrocks(format!("starrocks sink error {}", e)))?;

                let scale = decimal_all
                    .last()
                    .ok_or_else(|| SinkError::Starrocks("must have next".to_string()))?
                    .parse::<u8>()
                    .map_err(|e| SinkError::Starrocks(format!("starrocks sink error {}", e)))?;
                decimal_map.insert(name.to_string(), (length, scale));
            }
        }

        let builder = HeaderBuilder::new()
            .add_common_header()
            .set_user_password(config.common.user.clone(), config.common.password.clone())
            .add_json_format();
        let header = if !is_append_only {
            let mut fields_name = schema.names_str();
            fields_name.push(STARROCKS_DELETE_SIGN);
            builder.set_columns_name(fields_name).build()
        } else {
            builder.build()
        };

        let starrocks_insert_builder = InserterBuilder::new(
            format!("http://{}:{}", config.common.host, config.common.http_port),
            config.common.database.clone(),
            config.common.table.clone(),
            header,
        );
        Ok(Self {
            config,
            schema: schema.clone(),
            pk_indices,
            builder: starrocks_insert_builder,
            is_append_only,
            insert: None,
            row_encoder: JsonEncoder::new_with_doris(
                schema,
                None,
                TimestampHandlingMode::String,
                decimal_map,
            ),
        })
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }
            let row_json_string = Value::Object(self.row_encoder.encode(row)?).to_string();
            self.insert
                .as_mut()
                .ok_or_else(|| {
                    SinkError::Starrocks("Can't find starrocks sink insert".to_string())
                })?
                .write(row_json_string.into())
                .await?;
        }
        Ok(())
    }

    async fn upsert(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value.insert(
                        STARROCKS_DELETE_SIGN.to_string(),
                        Value::String("0".to_string()),
                    );
                    let row_json_string = serde_json::to_string(&row_json_value).map_err(|e| {
                        SinkError::Starrocks(format!("Json derialize error {:?}", e))
                    })?;
                    self.insert
                        .as_mut()
                        .ok_or_else(|| {
                            SinkError::Starrocks("Can't find starrocks sink insert".to_string())
                        })?
                        .write(row_json_string.into())
                        .await?;
                }
                Op::Delete => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value.insert(
                        STARROCKS_DELETE_SIGN.to_string(),
                        Value::String("1".to_string()),
                    );
                    let row_json_string = serde_json::to_string(&row_json_value).map_err(|e| {
                        SinkError::Starrocks(format!("Json derialize error {:?}", e))
                    })?;
                    self.insert
                        .as_mut()
                        .ok_or_else(|| {
                            SinkError::Starrocks("Can't find starrocks sink insert".to_string())
                        })?
                        .write(row_json_string.into())
                        .await?;
                }
                Op::UpdateDelete => {}
                Op::UpdateInsert => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value.insert(
                        STARROCKS_DELETE_SIGN.to_string(),
                        Value::String("0".to_string()),
                    );
                    let row_json_string = serde_json::to_string(&row_json_value).map_err(|e| {
                        SinkError::Starrocks(format!("Json derialize error {:?}", e))
                    })?;
                    self.insert
                        .as_mut()
                        .ok_or_else(|| {
                            SinkError::Starrocks("Can't find starrocks sink insert".to_string())
                        })?
                        .write(row_json_string.into())
                        .await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for StarrocksSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.insert.is_none() {
            self.insert = Some(self.builder.build_starrocks().await?);
        }
        if self.is_append_only {
            self.append_only(chunk).await
        } else {
            self.upsert(chunk).await
        }
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
        if self.insert.is_some() {
            let insert = self
                .insert
                .take()
                .ok_or_else(|| SinkError::Starrocks("Can't find starrocks inserter".to_string()))?;
            insert.finish().await?;
        }
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

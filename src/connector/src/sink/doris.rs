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
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use serde::Deserialize;
use serde_json::Value;
use serde_with::serde_as;

use super::doris_connector::{DorisField, DorisInsert, DorisInsertClient, DORIS_DELETE_SIGN};
use super::{SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT};
use crate::common::DorisCommon;
use crate::sink::encoder::{JsonEncoder, RowEncoder, TimestampHandlingMode};
use crate::sink::writer::{LogSinkerOf, SinkWriterExt};
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkParam, SinkWriter, SinkWriterParam,
};

pub const DORIS_SINK: &str = "doris";
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
pub struct DorisSink {
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

impl DorisSink {
    fn check_column_name_and_type(&self, doris_column_fields: Vec<DorisField>) -> Result<()> {
        let doris_columns_desc: HashMap<String, String> = doris_column_fields
            .iter()
            .map(|s| (s.name.clone(), s.r#type.clone()))
            .collect();

        let rw_fields_name = self.schema.fields();
        if rw_fields_name.len().ne(&doris_columns_desc.len()) {
            return Err(SinkError::Doris("The length of the RisingWave column must be equal to the length of the doris column".to_string()));
        }

        for i in rw_fields_name {
            let value = doris_columns_desc.get(&i.name).ok_or_else(|| {
                SinkError::Doris(format!(
                    "Column name don't find in doris, risingwave is {:?} ",
                    i.name
                ))
            })?;
            if !Self::check_and_correct_column_type(&i.data_type, value.to_string())? {
                return Err(SinkError::Doris(format!(
                    "Column type don't match, column name is {:?}. doris type is {:?} risingwave type is {:?} ",i.name,value,i.data_type
                )));
            }
        }
        Ok(())
    }

    fn check_and_correct_column_type(
        rw_data_type: &DataType,
        doris_data_type: String,
    ) -> Result<bool> {
        match rw_data_type {
            risingwave_common::types::DataType::Boolean => Ok(doris_data_type.contains("BOOLEAN")),
            risingwave_common::types::DataType::Int16 => Ok(doris_data_type.contains("SMALLINT")),
            risingwave_common::types::DataType::Int32 => Ok(doris_data_type.contains("INT")),
            risingwave_common::types::DataType::Int64 => Ok(doris_data_type.contains("BIGINT")),
            risingwave_common::types::DataType::Float32 => Ok(doris_data_type.contains("FLOAT")),
            risingwave_common::types::DataType::Float64 => Ok(doris_data_type.contains("DOUBLE")),
            risingwave_common::types::DataType::Decimal => Ok(doris_data_type.contains("DECIMAL")),
            risingwave_common::types::DataType::Date => Ok(doris_data_type.contains("DATE")),
            risingwave_common::types::DataType::Varchar => {
                Ok(doris_data_type.contains("STRING") | doris_data_type.contains("VARCHAR"))
            }
            risingwave_common::types::DataType::Time => {
                Err(SinkError::Doris("doris can not support Time".to_string()))
            }
            risingwave_common::types::DataType::Timestamp => {
                Ok(doris_data_type.contains("DATETIME"))
            }
            risingwave_common::types::DataType::Timestamptz => Err(SinkError::Doris(
                "doris can not support Timestamptz".to_string(),
            )),
            risingwave_common::types::DataType::Interval => Err(SinkError::Doris(
                "doris can not support Interval".to_string(),
            )),
            risingwave_common::types::DataType::Struct(_) => Ok(doris_data_type.contains("STRUCT")),
            risingwave_common::types::DataType::List(_) => Ok(doris_data_type.contains("ARRAY")),
            risingwave_common::types::DataType::Bytea => {
                Err(SinkError::Doris("doris can not support Bytea".to_string()))
            }
            risingwave_common::types::DataType::Jsonb => Ok(doris_data_type.contains("JSONB")),
            risingwave_common::types::DataType::Serial => Ok(doris_data_type.contains("BIGINT")),
            risingwave_common::types::DataType::Int256 => {
                Err(SinkError::Doris("doris can not support Int256".to_string()))
            }
        }
    }
}

impl Sink for DorisSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<DorisSinkWriter>;

    const SINK_NAME: &'static str = DORIS_SINK;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(DorisSinkWriter::new(
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
                "Primary key not defined for upsert doris sink (please define in `primary_key` field)")));
        }
        // check reachability
        let client = self.config.common.build_get_client();
        let doris_schema = client.get_schema_from_doris().await?;

        if !self.is_append_only && doris_schema.keys_type.ne("UNIQUE_KEYS") {
            return Err(SinkError::Config(anyhow!(
                "If you want to use upsert, please set the keysType of doris to UNIQUE_KEYS"
            )));
        }
        self.check_column_name_and_type(doris_schema.properties)?;
        Ok(())
    }
}

pub struct DorisSinkWriter {
    pub config: DorisConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    client: DorisInsertClient,
    is_append_only: bool,
    insert: Option<DorisInsert>,
    row_encoder: JsonEncoder,
}

impl TryFrom<SinkParam> for DorisSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = DorisConfig::from_hashmap(param.properties)?;
        DorisSink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
    }
}

impl DorisSinkWriter {
    pub async fn new(
        config: DorisConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let mut decimal_map = HashMap::default();
        let doris_schema = config
            .common
            .build_get_client()
            .get_schema_from_doris()
            .await?;
        doris_schema.properties.iter().for_each(|s| {
            if let Some(v) = s.get_decimal_pre_scale() {
                decimal_map.insert(s.name.clone(), v);
            }
        });
        let mut map = HashMap::new();
        map.insert("format".to_string(), "json".to_string());
        map.insert("read_json_by_line".to_string(), "true".to_string());
        let doris_insert_client = DorisInsertClient::new(
            config.common.url.clone(),
            config.common.database.clone(),
            config.common.table.clone(),
        )
        .add_common_header()
        .set_user_password(config.common.user.clone(), config.common.password.clone())
        .set_properties(map);
        let mut doris_insert_client = if !is_append_only {
            doris_insert_client.add_hidden_column()
        } else {
            doris_insert_client
        };
        let insert = Some(doris_insert_client.build().await?);
        Ok(Self {
            config,
            schema: schema.clone(),
            pk_indices,
            client: doris_insert_client,
            is_append_only,
            insert,
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
                .ok_or_else(|| SinkError::Doris("Can't find doris sink insert".to_string()))?
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
                        DORIS_DELETE_SIGN.to_string(),
                        Value::String("0".to_string()),
                    );
                    let row_json_string = serde_json::to_string(&row_json_value)
                        .map_err(|e| SinkError::Doris(format!("Json derialize error {:?}", e)))?;
                    self.insert
                        .as_mut()
                        .ok_or_else(|| {
                            SinkError::Doris("Can't find doris sink insert".to_string())
                        })?
                        .write(row_json_string.into())
                        .await?;
                }
                Op::Delete => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value.insert(
                        DORIS_DELETE_SIGN.to_string(),
                        Value::String("1".to_string()),
                    );
                    let row_json_string = serde_json::to_string(&row_json_value)
                        .map_err(|e| SinkError::Doris(format!("Json derialize error {:?}", e)))?;
                    self.insert
                        .as_mut()
                        .ok_or_else(|| {
                            SinkError::Doris("Can't find doris sink insert".to_string())
                        })?
                        .write(row_json_string.into())
                        .await?;
                }
                Op::UpdateDelete => {}
                Op::UpdateInsert => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value.insert(
                        DORIS_DELETE_SIGN.to_string(),
                        Value::String("0".to_string()),
                    );
                    let row_json_string = serde_json::to_string(&row_json_value)
                        .map_err(|e| SinkError::Doris(format!("Json derialize error {:?}", e)))?;
                    self.insert
                        .as_mut()
                        .ok_or_else(|| {
                            SinkError::Doris("Can't find doris sink insert".to_string())
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
impl SinkWriter for DorisSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.insert.is_none() {
            self.insert = Some(self.client.build().await?);
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
                .ok_or_else(|| SinkError::Doris("Can't find doris inserter".to_string()))?;
            insert.finish().await?;
        }
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

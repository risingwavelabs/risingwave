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
use deltalake::operations::transaction::commit;
use deltalake::protocol::{Action, Add, DeltaOperation, SaveMode};
use deltalake::table::builder::s3_storage_options::{
    AWS_ACCESS_KEY_ID, AWS_ALLOW_HTTP, AWS_ENDPOINT_URL, AWS_REGION, AWS_S3_ALLOW_UNSAFE_RENAME,
    AWS_SECRET_ACCESS_KEY,
};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{DeltaTable, Schema as DeltaSchema, SchemaDataType};
use risingwave_common::array::{to_record_batch_with_schema_arrow46, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::error::anyhow_error;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use risingwave_pb::connector_service::SinkMetadata;
use serde_derive::{Deserialize, Serialize};
use serde_with::serde_as;
use with_options::WithOptions;

use super::coordinate::CoordinatedSinkWriter;
use super::writer::{LogSinkerOf, SinkWriter};
use super::{
    Result, Sink, SinkCommitCoordinator, SinkError, SinkParam, SinkWriterParam,
    SINK_TYPE_APPEND_ONLY, SINK_USER_FORCE_APPEND_ONLY_OPTION,
};
use crate::sink::writer::SinkWriterExt;

pub const DELTALAKE_SINK: &str = "deltalake_rust";

#[derive(Deserialize, Serialize, Debug, Clone, WithOptions)]
pub struct DeltaLakeCommon {
    #[serde(rename = "s3.access.key")]
    pub s3_access_key: Option<String>,
    #[serde(rename = "s3.secret.key")]
    pub s3_secret_key: Option<String>,
    #[serde(rename = "local.path")]
    pub local_path: Option<String>,
    #[serde(rename = "s3.path")]
    pub s3_path: Option<String>,
    #[serde(rename = "s3.region")]
    pub s3_region: Option<String>,
    #[serde(rename = "s3.endpoint")]
    pub s3_endpoint: Option<String>,
}
impl DeltaLakeCommon {
    pub async fn create_deltalake_client(&self) -> Result<DeltaTable> {
        let table = if let Some(local_path) = &self.local_path {
            deltalake::open_table(local_path).await?
        } else if let Some(s3_path) = &self.s3_path {
            let mut storage_options = HashMap::new();
            storage_options.insert(
                AWS_ACCESS_KEY_ID.to_string(),
                self.s3_access_key.clone().ok_or_else(|| {
                    SinkError::Config(anyhow!("s3.access.key is required with aws s3"))
                })?,
            );
            storage_options.insert(
                AWS_SECRET_ACCESS_KEY.to_string(),
                self.s3_secret_key.clone().ok_or_else(|| {
                    SinkError::Config(anyhow!("s3.secret.key is required with aws s3"))
                })?,
            );
            if let Some(s3_region) = &self.s3_region {
                storage_options.insert(AWS_REGION.to_string(), s3_region.clone());
            }
            if let Some(s3_endpoint) = &self.s3_endpoint {
                storage_options.insert(AWS_ENDPOINT_URL.to_string(), s3_endpoint.clone());
            }
            storage_options.insert(AWS_ALLOW_HTTP.to_string(), "true".to_string());
            storage_options.insert(AWS_S3_ALLOW_UNSAFE_RENAME.to_string(), "true".to_string());
            deltalake::open_table_with_storage_options(s3_path.clone(), storage_options).await?
        } else {
            return Err(SinkError::DeltaLake(
                "`local.path` or `s3.path` set at least one, configure as needed.".to_string(),
            ));
        };
        Ok(table)
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct DeltaLakeConfig {
    #[serde(flatten)]
    pub common: DeltaLakeCommon,

    pub r#type: String,
}

impl DeltaLakeConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<DeltaLakeConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY
            && config.r#type != SINK_USER_FORCE_APPEND_ONLY_OPTION
        {
            return Err(SinkError::Config(anyhow!(
                "only append-only delta lake sink is supported",
            )));
        }
        Ok(config)
    }
}

#[derive(Debug)]
pub struct DeltaLakeSink {
    pub config: DeltaLakeConfig,
    param: SinkParam,
}

impl DeltaLakeSink {
    pub fn new(config: DeltaLakeConfig, param: SinkParam) -> Result<Self> {
        Ok(Self { config, param })
    }
}

fn check_field_type(rw_data_type: &DataType, dl_data_type: &SchemaDataType) -> Result<bool> {
    let mut result = false;
    match rw_data_type {
        DataType::Boolean => {
            if let SchemaDataType::primitive(s) = dl_data_type {
                result = s.eq("boolean");
            }
        }
        DataType::Int16 => {
            if let SchemaDataType::primitive(s) = dl_data_type {
                result = s.eq("short");
            }
        }
        DataType::Int32 => {
            if let SchemaDataType::primitive(s) = dl_data_type {
                result = s.eq("integer");
            }
        }
        DataType::Int64 => {
            if let SchemaDataType::primitive(s) = dl_data_type {
                result = s.eq("long");
            }
        }
        DataType::Float32 => {
            if let SchemaDataType::primitive(s) = dl_data_type {
                result = s.eq("float");
            }
        }
        DataType::Float64 => {
            if let SchemaDataType::primitive(s) = dl_data_type {
                result = s.eq("double");
            }
        }
        DataType::Decimal => {
            if let SchemaDataType::primitive(s) = dl_data_type {
                result = s.contains("decimal");
            }
        }
        DataType::Date => {
            if let SchemaDataType::primitive(s) = dl_data_type {
                result = s.eq("date");
            }
        }
        DataType::Varchar => {
            if let SchemaDataType::primitive(s) = dl_data_type {
                result = s.eq("string");
            }
        }
        DataType::Time => {
            return Err(SinkError::DeltaLake(
                "deltalake cannot support type Time".to_string(),
            ))
        }
        DataType::Timestamp => {
            if let SchemaDataType::primitive(s) = dl_data_type {
                result = s.eq("timestamp");
            }
        }
        DataType::Timestamptz => {
            return Err(SinkError::DeltaLake(
                "deltalake cannot support type Timestamptz".to_string(),
            ))
        }
        DataType::Interval => {
            return Err(SinkError::DeltaLake(
                "deltalake cannot support type Interval".to_string(),
            ))
        }
        DataType::Struct(rw_s) => {
            if let SchemaDataType::r#struct(dl_s) = dl_data_type {
                for ((rw_n, rw_t), dl_f) in rw_s
                    .names()
                    .zip_eq_fast(rw_s.types())
                    .zip_eq_fast(dl_s.get_fields())
                {
                    result = check_field_type(rw_t, dl_f.get_type())?;
                    if !(result && rw_n.eq(dl_f.get_name())) {
                        return Ok(result);
                    }
                }
                result = true;
            }
        }
        DataType::List(rw_l) => {
            if let SchemaDataType::array(dl_l) = dl_data_type {
                result = check_field_type(rw_l, dl_l.get_element_type())?;
            }
        }
        DataType::Bytea => {
            return Err(SinkError::DeltaLake(
                "deltalake cannot support type Bytea".to_string(),
            ))
        }
        DataType::Jsonb => {
            return Err(SinkError::DeltaLake(
                "deltalake cannot support type Jsonb".to_string(),
            ))
        }
        DataType::Serial => {
            return Err(SinkError::DeltaLake(
                "deltalake cannot support type Serial".to_string(),
            ))
        }
        DataType::Int256 => {
            return Err(SinkError::DeltaLake(
                "deltalake cannot support type Int256".to_string(),
            ))
        }
    };
    Ok(result)
}

impl Sink for DeltaLakeSink {
    type Coordinator = DeltaLakeSinkCommitter;
    type LogSinker = LogSinkerOf<CoordinatedSinkWriter<DeltaLakeSinkWriter>>;

    const SINK_NAME: &'static str = DELTALAKE_SINK;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let inner = DeltaLakeSinkWriter::new(
            self.config.clone(),
            self.param.schema().clone(),
            self.param.downstream_pk.clone(),
            self.param.sink_type.is_append_only(),
        )
        .await?;
        Ok(CoordinatedSinkWriter::new(
            writer_param
                .meta_client
                .expect("should have meta client")
                .sink_coordinate_client()
                .await,
            self.param.clone(),
            writer_param.vnode_bitmap.ok_or_else(|| {
                SinkError::Remote(anyhow_error!(
                    "sink needs coordination should not have singleton input"
                ))
            })?,
            inner,
        )
        .await?
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn validate(&self) -> Result<()> {
        let table = self.config.common.create_deltalake_client().await?;
        let deltalake_fields: HashMap<String, &SchemaDataType> = table
            .get_schema()?
            .get_fields()
            .iter()
            .map(|f| (f.get_name().to_string(), f.get_type()))
            .collect();
        if deltalake_fields.len() != self.param.schema().fields().len() {
            return Err(SinkError::DeltaLake(format!(
                "column count not match, rw is {}, deltalake is {}",
                self.param.schema().fields().len(),
                deltalake_fields.len()
            )));
        }
        for field in self.param.schema().fields() {
            if !deltalake_fields.contains_key(&field.name) {
                return Err(SinkError::DeltaLake(format!(
                    "column {} not found in deltalake table",
                    field.name
                )));
            }
            let deltalake_field_type: &&SchemaDataType = deltalake_fields
                .get(&field.name)
                .ok_or_else(|| SinkError::DeltaLake("cannot find field type".to_string()))?;
            if !check_field_type(&field.data_type, deltalake_field_type)? {
                return Err(SinkError::DeltaLake(format!(
                    "column {} type is not match, deltalake is {:?}, rw is{:?}",
                    field.name, deltalake_field_type, field.data_type
                )));
            }
        }
        Ok(())
    }

    async fn new_coordinator(&self) -> Result<Self::Coordinator> {
        Ok(DeltaLakeSinkCommitter {
            table: self.config.common.create_deltalake_client().await?,
        })
    }
}

impl TryFrom<SinkParam> for DeltaLakeSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let config = DeltaLakeConfig::from_hashmap(param.properties.clone())?;
        DeltaLakeSink::new(config, param)
    }
}

pub struct DeltaLakeSinkWriter {
    pub config: DeltaLakeConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    writer: RecordBatchWriter,
    is_append_only: bool,
    dl_schema: Arc<deltalake::arrow::datatypes::Schema>,
    dl_table: DeltaTable,
}

impl DeltaLakeSinkWriter {
    pub async fn new(
        config: DeltaLakeConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let dl_table = config.common.create_deltalake_client().await?;
        let writer = RecordBatchWriter::for_table(&dl_table)?;
        let dl_schema: Arc<deltalake::arrow::datatypes::Schema> =
            Arc::new(covert_schema(dl_table.get_schema()?)?);

        Ok(Self {
            config,
            schema,
            pk_indices,
            writer,
            is_append_only,
            dl_schema,
            dl_table,
        })
    }

    async fn write(&mut self, chunk: StreamChunk) -> Result<()> {
        let a = to_record_batch_with_schema_arrow46(self.dl_schema.clone(), &chunk)
            .map_err(|err| SinkError::DeltaLake(format!("convert record batch error: {}", err)))?;
        self.writer.write(a).await?;
        Ok(())
    }
}

fn covert_schema(schema: &DeltaSchema) -> Result<deltalake::arrow::datatypes::Schema> {
    let mut builder = deltalake::arrow::datatypes::SchemaBuilder::new();
    for field in schema.get_fields() {
        let dl_field = deltalake::arrow::datatypes::Field::new(
            field.get_name(),
            deltalake::arrow::datatypes::DataType::try_from(field.get_type())
                .map_err(|err| SinkError::DeltaLake(format!("convert schema error: {}", err)))?,
            field.is_nullable(),
        );
        builder.push(dl_field);
    }
    Ok(builder.finish())
}

#[async_trait]
impl SinkWriter for DeltaLakeSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_append_only {
            self.write(chunk).await
        } else {
            Err(SinkError::DeltaLake(
                "only append-only delta lake sink is supported".to_string(),
            ))
        }
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        if !is_checkpoint {
            return Ok(None);
        }

        let adds = self.writer.flush().await?;
        Ok(Some(SinkMetadata::try_from(&DeltaLakeWriteResult {
            adds,
        })?))
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

pub struct DeltaLakeSinkCommitter {
    table: DeltaTable,
}

#[async_trait::async_trait]
impl SinkCommitCoordinator for DeltaLakeSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!("DeltaLake commit coordinator inited.");
        Ok(())
    }

    async fn commit(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        tracing::info!("Starting DeltaLake commit in epoch {epoch}.");

        let deltalake_write_result = metadata
            .iter()
            .map(DeltaLakeWriteResult::try_from)
            .collect::<Result<Vec<DeltaLakeWriteResult>>>()?;
        let write_adds: Vec<Action> = deltalake_write_result
            .into_iter()
            .flat_map(|v| v.adds.into_iter())
            .map(Action::add)
            .collect();

        if write_adds.is_empty() {
            return Ok(());
        }
        let partition_cols = self.table.get_metadata().unwrap().partition_columns.clone();
        let partition_by = if !partition_cols.is_empty() {
            Some(partition_cols)
        } else {
            None
        };
        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by,
            predicate: None,
        };
        let version = commit(
            self.table.object_store().as_ref(),
            &write_adds,
            operation,
            &self.table.state,
            None,
        )
        .await
        .unwrap();
        self.table.update().await?;
        tracing::info!(
            "Succeeded to commit ti DeltaLake table in epoch {epoch} version {version}."
        );
        Ok(())
    }
}

struct DeltaLakeWriteResult {
    adds: Vec<Add>,
}

impl<'a> TryFrom<&'a DeltaLakeWriteResult> for SinkMetadata {
    type Error = SinkError;

    fn try_from(value: &'a DeltaLakeWriteResult) -> std::prelude::v1::Result<Self, Self::Error> {
        let json_add = serde_json::Value::Array(
            value
                .adds
                .iter()
                .cloned()
                .map(|add| serde_json::json!(add))
                .collect(),
        );
        Ok(SinkMetadata {
            metadata: Some(Serialized(SerializedMetadata {
                metadata: serde_json::to_vec(&json_add).map_err(|e| -> SinkError {
                    anyhow!("Can't serialized deltalake sink metadata: {}", e).into()
                })?,
            })),
        })
    }
}

impl DeltaLakeWriteResult {
    fn try_from(value: &SinkMetadata) -> Result<Self> {
        if let Some(Serialized(v)) = &value.metadata {
            let values = if let serde_json::Value::Array(v) = serde_json::from_slice::<
                serde_json::Value,
            >(&v.metadata)
            .map_err(|e| -> SinkError {
                anyhow!("Can't parse deltalake sink metadata: {}", e).into()
            })? {
                v
            } else {
                return Err(anyhow!("deltalake sink metadata should be a object").into());
            };
            let adds = values
                .into_iter()
                .map(serde_json::from_value)
                .collect::<std::result::Result<Vec<Add>, serde_json::Error>>()
                .unwrap();
            Ok(DeltaLakeWriteResult { adds })
        } else {
            Err(anyhow!("Can't create deltalake sink write result from empty data!").into())
        }
    }
}

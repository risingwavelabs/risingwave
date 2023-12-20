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
use deltalake::kernel::{Action, Add, DataType as DeltaLakeDataType, PrimitiveType, StructType};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::table::builder::s3_storage_options::{
    AWS_ACCESS_KEY_ID, AWS_ALLOW_HTTP, AWS_ENDPOINT_URL, AWS_REGION, AWS_S3_ALLOW_UNSAFE_RENAME,
    AWS_SECRET_ACCESS_KEY,
};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::DeltaTable;
use risingwave_common::array::{to_deltalake_record_batch_with_schema, StreamChunk};
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
pub const DEFAULT_REGION: &str = "us-east-1";

#[derive(Deserialize, Serialize, Debug, Clone, WithOptions)]
pub struct DeltaLakeCommon {
    #[serde(rename = "s3.access.key")]
    pub s3_access_key: Option<String>,
    #[serde(rename = "s3.secret.key")]
    pub s3_secret_key: Option<String>,
    #[serde(rename = "location")]
    pub location: String,
    #[serde(rename = "s3.region")]
    pub s3_region: Option<String>,
    #[serde(rename = "s3.endpoint")]
    pub s3_endpoint: Option<String>,
}
impl DeltaLakeCommon {
    pub async fn create_deltalake_client(&self) -> Result<DeltaTable> {
        let table = match Self::get_table_url(&self.location)? {
            DeltaTableUrl::S3(s3_path) => {
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
                if self.s3_endpoint.is_none() && self.s3_region.is_none() {
                    return Err(SinkError::Config(anyhow!(
                        "s3.endpoint and s3.region need to be filled with at least one"
                    )));
                }
                storage_options.insert(
                    AWS_REGION.to_string(),
                    self.s3_region
                        .clone()
                        .unwrap_or_else(|| DEFAULT_REGION.to_string()),
                );
                if let Some(s3_endpoint) = &self.s3_endpoint {
                    storage_options.insert(AWS_ENDPOINT_URL.to_string(), s3_endpoint.clone());
                }
                storage_options.insert(AWS_ALLOW_HTTP.to_string(), "true".to_string());
                storage_options.insert(AWS_S3_ALLOW_UNSAFE_RENAME.to_string(), "true".to_string());
                deltalake::open_table_with_storage_options(s3_path.clone(), storage_options).await?
            }
            DeltaTableUrl::Local(local_path) => deltalake::open_table(local_path).await?,
        };
        Ok(table)
    }

    fn get_table_url(path: &str) -> Result<DeltaTableUrl> {
        if path.starts_with("s3://") || path.starts_with("s3a://") {
            Ok(DeltaTableUrl::S3(path.to_string()))
        } else if let Some(path) = path.strip_prefix("file://") {
            Ok(DeltaTableUrl::Local(path.to_string()))
        } else {
            Err(SinkError::DeltaLake(anyhow!(
                "path need to start with 's3://','s3a://'(s3) or file://(local)"
            )))
        }
    }
}

enum DeltaTableUrl {
    S3(String),
    Local(String),
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
        let config = serde_json::from_value::<DeltaLakeConfig>(
            serde_json::to_value(properties).map_err(|e| SinkError::DeltaLake(e.into()))?,
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
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

fn check_field_type(rw_data_type: &DataType, dl_data_type: &DeltaLakeDataType) -> Result<bool> {
    let result = match rw_data_type {
        DataType::Boolean => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Boolean)
            )
        }
        DataType::Int16 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Short)
            )
        }
        DataType::Int32 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Integer)
            )
        }
        DataType::Int64 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Long)
            )
        }
        DataType::Float32 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Float)
            )
        }
        DataType::Float64 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Double)
            )
        }
        DataType::Decimal => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Decimal(_, _))
            )
        }
        DataType::Date => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Date)
            )
        }
        DataType::Varchar => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::String)
            )
        }
        DataType::Timestamptz => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Timestamp)
            )
        }
        DataType::Struct(rw_struct) => {
            if let DeltaLakeDataType::Struct(dl_struct) = dl_data_type {
                let mut result = true;
                for ((rw_name, rw_type), dl_field) in rw_struct
                    .names()
                    .zip_eq_fast(rw_struct.types())
                    .zip_eq_fast(dl_struct.fields())
                {
                    result = check_field_type(rw_type, dl_field.data_type())?
                        && result
                        && rw_name.eq(dl_field.name());
                }
                result
            } else {
                false
            }
        }
        DataType::List(rw_list) => {
            if let DeltaLakeDataType::Array(dl_list) = dl_data_type {
                check_field_type(rw_list, dl_list.element_type())?
            } else {
                false
            }
        }
        _ => {
            return Err(SinkError::DeltaLake(anyhow!(
                "deltalake cannot support type {:?}",
                rw_data_type.to_string()
            )))
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
        if self.config.r#type != SINK_TYPE_APPEND_ONLY
            && self.config.r#type != SINK_USER_FORCE_APPEND_ONLY_OPTION
        {
            return Err(SinkError::Config(anyhow!(
                "only append-only delta lake sink is supported",
            )));
        }
        let table = self.config.common.create_deltalake_client().await?;
        let deltalake_fields: HashMap<&String, &DeltaLakeDataType> = table
            .get_schema()?
            .fields()
            .iter()
            .map(|f| (f.name(), f.data_type()))
            .collect();
        if deltalake_fields.len() != self.param.schema().fields().len() {
            return Err(SinkError::DeltaLake(anyhow!(
                "column count not match, rw is {}, deltalake is {}",
                self.param.schema().fields().len(),
                deltalake_fields.len()
            )));
        }
        for field in self.param.schema().fields() {
            if !deltalake_fields.contains_key(&field.name) {
                return Err(SinkError::DeltaLake(anyhow!(
                    "column {} not found in deltalake table",
                    field.name
                )));
            }
            let deltalake_field_type: &&DeltaLakeDataType = deltalake_fields
                .get(&field.name)
                .ok_or_else(|| SinkError::DeltaLake(anyhow!("cannot find field type")))?;
            if !check_field_type(&field.data_type, deltalake_field_type)? {
                return Err(SinkError::DeltaLake(anyhow!(
                    "column {} type is not match, deltalake is {:?}, rw is{:?}",
                    field.name,
                    deltalake_field_type,
                    field.data_type
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
    dl_schema: Arc<deltalake::arrow::datatypes::Schema>,
    dl_table: DeltaTable,
}

impl DeltaLakeSinkWriter {
    pub async fn new(
        config: DeltaLakeConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
    ) -> Result<Self> {
        let dl_table = config.common.create_deltalake_client().await?;
        let writer = RecordBatchWriter::for_table(&dl_table)?;
        let dl_schema: Arc<deltalake::arrow::datatypes::Schema> =
            Arc::new(convert_schema(dl_table.get_schema()?)?);

        Ok(Self {
            config,
            schema,
            pk_indices,
            writer,
            dl_schema,
            dl_table,
        })
    }

    async fn write(&mut self, chunk: StreamChunk) -> Result<()> {
        let a = to_deltalake_record_batch_with_schema(self.dl_schema.clone(), &chunk)
            .map_err(|err| SinkError::DeltaLake(anyhow!("convert record batch error: {}", err)))?;
        self.writer.write(a).await?;
        Ok(())
    }
}

fn convert_schema(schema: &StructType) -> Result<deltalake::arrow::datatypes::Schema> {
    let mut builder = deltalake::arrow::datatypes::SchemaBuilder::new();
    for field in schema.fields() {
        let dl_field = deltalake::arrow::datatypes::Field::new(
            field.name(),
            deltalake::arrow::datatypes::DataType::try_from(field.data_type())
                .map_err(|err| SinkError::DeltaLake(anyhow!("convert schema error: {}", err)))?,
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
        self.write(chunk).await
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
            .map(Action::Add)
            .collect();

        if write_adds.is_empty() {
            return Ok(());
        }
        let partition_cols = self.table.get_metadata()?.partition_columns.clone();
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
        let version = deltalake::operations::transaction::commit(
            self.table.log_store().as_ref(),
            &write_adds,
            operation,
            &self.table.state,
            None,
        )
        .await?;
        self.table.update().await?;
        tracing::info!(
            "Succeeded to commit ti DeltaLake table in epoch {epoch} version {version}."
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct DeltaLakeWriteResult {
    adds: Vec<Add>,
}

impl<'a> TryFrom<&'a DeltaLakeWriteResult> for SinkMetadata {
    type Error = SinkError;

    fn try_from(value: &'a DeltaLakeWriteResult) -> std::prelude::v1::Result<Self, Self::Error> {
        let metadata = serde_json::to_vec(&value.adds).map_err(|e| -> SinkError {
            anyhow!("Can't serialized deltalake sink metadata: {}", e).into()
        })?;
        Ok(SinkMetadata {
            metadata: Some(Serialized(SerializedMetadata { metadata })),
        })
    }
}

impl DeltaLakeWriteResult {
    fn try_from(value: &SinkMetadata) -> Result<Self> {
        if let Some(Serialized(v)) = &value.metadata {
            let adds =
                serde_json::from_slice::<Vec<Add>>(&v.metadata).map_err(|e| -> SinkError {
                    anyhow!("Can't deserialize deltalake sink metadata: {}", e).into()
                })?;
            Ok(DeltaLakeWriteResult { adds })
        } else {
            Err(anyhow!("Can't create deltalake sink write result from empty data!").into())
        }
    }
}

#[cfg(all(test, not(madsim)))]
mod test {
    use deltalake::kernel::DataType as SchemaDataType;
    use deltalake::operations::create::CreateBuilder;
    use maplit::hashmap;
    use risingwave_common::array::{Array, I32Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::catalog::{Field, Schema};

    use super::{DeltaLakeConfig, DeltaLakeSinkWriter};
    use crate::sink::deltalake::DeltaLakeSinkCommitter;
    use crate::sink::writer::SinkWriter;
    use crate::sink::SinkCommitCoordinator;
    use crate::source::DataType;

    #[tokio::test]
    async fn test_deltalake() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        CreateBuilder::new()
            .with_location(path)
            .with_column("id", SchemaDataType::integer(), false, Default::default())
            .with_column("name", SchemaDataType::string(), false, Default::default())
            .await
            .unwrap();

        let properties = hashmap! {
            "connector".to_string() => "deltalake_rust".to_string(),
            "force_append_only".to_string() => "true".to_string(),
            "type".to_string() => "append-only".to_string(),
            "location".to_string() => format!("file://{}", path),
        };

        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
        ]);

        let deltalake_config = DeltaLakeConfig::from_hashmap(properties).unwrap();
        let deltalake_table = deltalake_config
            .common
            .create_deltalake_client()
            .await
            .unwrap();

        let mut deltalake_writer = DeltaLakeSinkWriter::new(deltalake_config, schema, vec![0])
            .await
            .unwrap();
        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                I32Array::from_iter(vec![1, 2, 3]).into_ref(),
                Utf8Array::from_iter(vec!["Alice", "Bob", "Clare"]).into_ref(),
            ],
        );
        deltalake_writer.write(chunk).await.unwrap();
        let mut committer = DeltaLakeSinkCommitter {
            table: deltalake_table,
        };
        let metadata = deltalake_writer.barrier(true).await.unwrap().unwrap();
        committer.commit(1, vec![metadata]).await.unwrap();

        // The following code is to test reading the deltalake data table written with test data.
        // To enable the following code, add `deltalake = { workspace = true, features = ["datafusion"] }`
        // to the `dev-dependencies` section of the `Cargo.toml` file of this crate.
        //
        // The feature is commented and disabled because enabling the `datafusion` feature of `deltalake`
        // will increase the compile time and output binary size in release build, even though it is a
        // dev dependency.

        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let table = deltalake::open_table(path).await.unwrap();
        ctx.register_table("demo", std::sync::Arc::new(table))
            .unwrap();

        let batches = ctx
            .sql("SELECT * FROM demo")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(3, batches.get(0).unwrap().column(0).len());
        assert_eq!(3, batches.get(0).unwrap().column(1).len());
    }
}

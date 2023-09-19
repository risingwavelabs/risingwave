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
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::anyhow;
use arrow_array::RecordBatch;
use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema};
use async_trait::async_trait;
use icelake::config::{TableConfig, TableConfigRef};
use icelake::transaction::Transaction;
use icelake::types::{data_file_from_json, data_file_to_json, DataFile};
use icelake::Table;
use opendal::services::S3;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::error::anyhow_error;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_rpc_client::ConnectorClient;
use serde_derive::Deserialize;
use serde_json::Value;
use url::Url;

use super::{
    Sink, SinkError, SinkWriter, SinkWriterParam, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION,
    SINK_TYPE_UPSERT,
};
use crate::deserialize_bool_from_string;
use crate::sink::coordinate::CoordinatedSinkWriter;
use crate::sink::remote::{CoordinatedRemoteSink, RemoteConfig};
use crate::sink::{Result, SinkCommitCoordinator, SinkParam};

/// This iceberg sink is WIP. When it ready, we will change this name to "iceberg".
pub const ICEBERG_SINK: &str = "iceberg";
pub const REMOTE_ICEBERG_SINK: &str = "iceberg_java";

pub type RemoteIcebergSink = CoordinatedRemoteSink;
pub type RemoteIcebergConfig = RemoteConfig;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergConfig {
    #[serde(skip_serializing)]
    pub connector: String, // Must be "kafka" here.

    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub force_append_only: bool,

    #[serde(rename = "warehouse.path")]
    pub path: String,

    #[serde(rename = "s3.region")]
    pub region: Option<String>,

    #[serde(rename = "s3.endpoint")]
    pub endpoint: Option<String>,

    #[serde(rename = "s3.access.key")]
    pub access_key: String,

    #[serde(rename = "s3.secret.key")]
    pub secret_key: String,

    #[serde(rename = "database.name")]
    pub database_name: String,

    #[serde(rename = "table.name")]
    pub table_name: String,

    #[serde(skip)]
    pub iceberg_table_config: TableConfigRef,
}

impl IcebergConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let iceberg_table_config =
            Arc::new(TableConfig::try_from(&values).map_err(|e| SinkError::Iceberg(anyhow!(e)))?);
        let mut config =
            serde_json::from_value::<IcebergConfig>(serde_json::to_value(values).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;

        config.iceberg_table_config = iceberg_table_config;

        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }

        if config.endpoint.is_none() && config.region.is_none() {
            return Err(SinkError::Config(anyhow!(
                "You must fill either s3 region or s3 endpoint",
            )));
        }

        Ok(config)
    }
}

pub struct IcebergSink {
    config: IcebergConfig,
    param: SinkParam,
    table_root: String,
    bucket_name: String,
}

impl Debug for IcebergSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSink")
            .field("config", &self.config)
            .finish()
    }
}

impl IcebergSink {
    async fn create_table(&self) -> Result<Table> {
        let mut builder = S3::default();

        // Sink will not load config from file.
        builder.disable_config_load();

        builder
            .root(&self.table_root)
            .bucket(&self.bucket_name)
            .access_key_id(&self.config.access_key)
            .secret_access_key(&self.config.secret_key);

        if let Some(region) = &self.config.region {
            builder.region(region);
        }

        if let Some(endpoint) = &self.config.endpoint {
            builder.endpoint(endpoint);
        }

        let op = opendal::Operator::new(builder)
            .map_err(|err| SinkError::Config(anyhow!("{err}")))?
            .finish();

        let table = Table::open_with_config(op, self.config.iceberg_table_config.clone())
            .await
            .map_err(|err| SinkError::Iceberg(anyhow!("Create table fail: {}", err)))?;

        let sink_schema = self.param.schema();
        let iceberg_schema = table
            .current_table_metadata()
            .current_schema()
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?
            .clone()
            .try_into()
            .map_err(|err: icelake::Error| SinkError::Iceberg(anyhow!(err)))?;

        try_matches_arrow_schema(&sink_schema, &iceberg_schema)?;

        Ok(table)
    }

    /// Parse bucket name and table root path.
    ///
    /// return (bucket name, table root path)
    fn parse_bucket_and_root_from_path(config: &IcebergConfig) -> Result<(String, String)> {
        let url = Url::parse(&config.path).map_err(|err| {
            SinkError::Config(anyhow!(
                "Fail to parse Invalid path: {}, caused by: {}",
                &config.path,
                err
            ))
        })?;

        let scheme = url.scheme();
        if scheme != "s3a" && scheme != "s3" && scheme != "s3n" {
            return Err(SinkError::Config(anyhow!(
                "Invalid path: {}, only support s3a,s3,s3n prefix",
                &config.path
            )));
        }

        let bucket = url
            .host_str()
            .ok_or_else(|| SinkError::Config(anyhow!("Invalid path: {}", &config.path)))?;
        let root = url.path();

        let table_root_path = if root.is_empty() {
            format!("/{}/{}", config.database_name, config.table_name)
        } else {
            format!("{}/{}/{}", root, config.database_name, config.table_name)
        };

        Ok((bucket.to_string(), table_root_path))
    }

    pub fn new(config: IcebergConfig, param: SinkParam) -> Result<Self> {
        let (bucket_name, table_root) = Self::parse_bucket_and_root_from_path(&config)?;
        // TODO(ZENOTME): Only support append-only mode now.
        if !config.force_append_only {
            return Err(SinkError::Iceberg(anyhow!(
                "Iceberg sink only support append-only mode now."
            )));
        }

        Ok(Self {
            config,
            param,
            table_root,
            bucket_name,
        })
    }
}

#[async_trait::async_trait]
impl Sink for IcebergSink {
    type Coordinator = IcebergSinkCommitter;
    type Writer = CoordinatedSinkWriter<IcebergWriter>;

    async fn validate(&self) -> Result<()> {
        let _ = self.create_table().await?;
        Ok(())
    }

    async fn new_writer(&self, writer_param: SinkWriterParam) -> Result<Self::Writer> {
        let table = self.create_table().await?;

        let inner = IcebergWriter {
            is_append_only: self.config.force_append_only,
            writer: table
                .task_writer()
                .await
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            table,
        };
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
        .await?)
    }

    async fn new_coordinator(
        &self,
        _connector_client: Option<ConnectorClient>,
    ) -> Result<Self::Coordinator> {
        let table = self.create_table().await?;

        Ok(IcebergSinkCommitter { table })
    }
}

/// TODO(ZENOTME): Just a placeholder, we will implement it later.(#10642)
pub struct IcebergWriter {
    is_append_only: bool,
    table: Table,
    writer: icelake::io::task_writer::TaskWriter,
}

impl IcebergWriter {
    async fn append_only_write(&mut self, chunk: StreamChunk) -> Result<()> {
        let (mut chunk, ops) = chunk.into_parts();

        let filters =
            chunk.visibility() & ops.iter().map(|op| *op == Op::Insert).collect::<Bitmap>();

        chunk.set_visibility(filters);
        let chunk = RecordBatch::try_from(&chunk.compact())
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

        self.writer.write(&chunk).await.map_err(|err| {
            SinkError::Iceberg(anyhow!("Write chunk fail: {}, chunk: {:?}", err, chunk))
        })?;

        Ok(())
    }
}

#[derive(Default, Debug)]
struct WriteResult {
    data_files: Vec<DataFile>,
}

impl<'a> TryFrom<&'a SinkMetadata> for WriteResult {
    type Error = SinkError;

    fn try_from(value: &'a SinkMetadata) -> std::result::Result<Self, Self::Error> {
        if let Some(Serialized(v)) = &value.metadata {
            if let Value::Array(json_values) =
                serde_json::from_slice::<serde_json::Value>(&v.metadata).map_err(
                    |e| -> SinkError { anyhow!("Can't parse iceberg sink metadata: {}", e).into() },
                )?
            {
                let data_files = json_values
                    .into_iter()
                    .map(data_file_from_json)
                    .collect::<std::result::Result<Vec<DataFile>, icelake::Error>>()
                    .map_err(|e| anyhow!("Failed to parse data file from json: {}", e))?;
                Ok(WriteResult { data_files })
            } else {
                Err(anyhow!("Serialized data files should be json array!").into())
            }
        } else {
            Err(anyhow!("Can't create iceberg sink write result from empty data!").into())
        }
    }
}

impl<'a> TryFrom<&'a WriteResult> for SinkMetadata {
    type Error = SinkError;

    fn try_from(value: &'a WriteResult) -> std::result::Result<SinkMetadata, Self::Error> {
        let json_value = serde_json::Value::Array(
            value
                .data_files
                .iter()
                .cloned()
                .map(data_file_to_json)
                .collect::<std::result::Result<Vec<serde_json::Value>, icelake::Error>>()
                .map_err(|e| anyhow!("Can't serialize data files to json: {}", e))?,
        );
        Ok(SinkMetadata {
            metadata: Some(Serialized(SerializedMetadata {
                metadata: serde_json::to_vec(&json_value).map_err(|e| -> SinkError {
                    anyhow!("Can't serialized iceberg sink metadata: {}", e).into()
                })?,
            })),
        })
    }
}

#[async_trait]
impl SinkWriter for IcebergWriter {
    type CommitMetadata = Option<SinkMetadata>;

    /// Begin a new epoch
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        // Just skip it.
        Ok(())
    }

    /// Write a stream chunk to sink
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_append_only {
            self.append_only_write(chunk).await
        } else {
            return Err(SinkError::Iceberg(anyhow!(
                "Iceberg sink only support append-only mode now."
            )));
        }
    }

    /// Receive a barrier and mark the end of current epoch. When `is_checkpoint` is true, the sink
    /// writer should commit the current epoch.
    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        // Skip it if not checkpoint
        if !is_checkpoint {
            return Ok(None);
        }

        let old_writer = std::mem::replace(
            &mut self.writer,
            self.table
                .task_writer()
                .await
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
        );

        let data_files = old_writer
            .close()
            .await
            .map_err(|err| SinkError::Iceberg(anyhow!("Close writer fail: {}", err)))?;

        Ok(Some(SinkMetadata::try_from(&WriteResult { data_files })?))
    }

    /// Clean up
    async fn abort(&mut self) -> Result<()> {
        // TODO: abort should clean up all the data written in this epoch.
        Ok(())
    }
}

pub struct IcebergSinkCommitter {
    table: Table,
}

#[async_trait::async_trait]
impl SinkCommitCoordinator for IcebergSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!("Iceberg commit coordinator inited.");
        Ok(())
    }

    async fn commit(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        tracing::info!("Starting iceberg commit in epoch {epoch}.");

        let write_results = metadata
            .iter()
            .map(WriteResult::try_from)
            .collect::<Result<Vec<WriteResult>>>()?;

        let mut txn = Transaction::new(&mut self.table);
        txn.append_file(
            write_results
                .into_iter()
                .flat_map(|s| s.data_files.into_iter()),
        );
        txn.commit()
            .await
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

        tracing::info!("Succeeded to commit ti iceberg table in epoch {epoch}.");
        Ok(())
    }
}

/// Try to match our schema with iceberg schema.
fn try_matches_arrow_schema(rw_schema: &Schema, arrow_schema: &ArrowSchema) -> Result<()> {
    if rw_schema.fields.len() != arrow_schema.fields().len() {
        return Err(SinkError::Iceberg(anyhow!(
            "Schema length not match, ours is {}, and iceberg is {}",
            rw_schema.fields.len(),
            arrow_schema.fields.len()
        )));
    }

    let mut schema_fields = HashMap::new();
    rw_schema.fields.iter().for_each(|field| {
        let res = schema_fields.insert(&field.name, &field.data_type);
        // This assert is to make sure there is no duplicate field name in the schema.
        assert!(res.is_none())
    });

    for arrow_field in &arrow_schema.fields {
        let our_field_type = schema_fields.get(arrow_field.name()).ok_or_else(|| {
            SinkError::Iceberg(anyhow!(
                "Field {} not found in our schema",
                arrow_field.name()
            ))
        })?;

        let converted_arrow_data_type =
            ArrowDataType::try_from(*our_field_type).map_err(|e| SinkError::Iceberg(anyhow!(e)))?;

        let compatible = match (&converted_arrow_data_type, arrow_field.data_type()) {
            (ArrowDataType::Decimal128(p1, s1), ArrowDataType::Decimal128(p2, s2)) => {
                *p1 >= *p2 && *s1 >= *s2
            }
            (left, right) => left == right,
        };
        if !compatible {
            return Err(SinkError::Iceberg(anyhow!("Field {}'s type not compatible, ours converted data type {}, iceberg's data type: {}",
                    arrow_field.name(), converted_arrow_data_type, arrow_field.data_type()
                )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use risingwave_common::catalog::Field;

    use crate::source::DataType;

    #[test]
    fn test_compatible_arrow_schema() {
        use arrow_schema::{DataType as ArrowDataType, Field as ArrowField};

        use super::*;
        let risingwave_schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "a"),
            Field::with_name(DataType::Int32, "b"),
            Field::with_name(DataType::Int32, "c"),
        ]);
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", ArrowDataType::Int32, false),
            ArrowField::new("b", ArrowDataType::Int32, false),
            ArrowField::new("c", ArrowDataType::Int32, false),
        ]);

        try_matches_arrow_schema(&risingwave_schema, &arrow_schema).unwrap();

        let risingwave_schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "d"),
            Field::with_name(DataType::Int32, "c"),
            Field::with_name(DataType::Int32, "a"),
            Field::with_name(DataType::Int32, "b"),
        ]);
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", ArrowDataType::Int32, false),
            ArrowField::new("b", ArrowDataType::Int32, false),
            ArrowField::new("d", ArrowDataType::Int32, false),
            ArrowField::new("c", ArrowDataType::Int32, false),
        ]);
        try_matches_arrow_schema(&risingwave_schema, &arrow_schema).unwrap();
    }
}

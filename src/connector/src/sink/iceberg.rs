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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::anyhow;
use arrow_array::RecordBatch;
use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use icelake::catalog::{load_catalog, CATALOG_NAME, CATALOG_TYPE};
use icelake::io::file_writer::DeltaWriterResult;
use icelake::io::EmptyLayer;
use icelake::transaction::Transaction;
use icelake::types::{data_file_from_json, data_file_to_json, Any, DataFile};
use icelake::{Table, TableIdentifier};
use risingwave_common::array::{to_record_batch_with_schema, Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::error::anyhow_error;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use risingwave_pb::connector_service::SinkMetadata;
use serde::de;
use serde_derive::Deserialize;
use url::Url;

use super::{
    Sink, SinkError, SinkWriterParam, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
use crate::deserialize_bool_from_string;
use crate::sink::coordinate::CoordinatedSinkWriter;
use crate::sink::remote::{CoordinatedRemoteSink, RemoteSinkTrait};
use crate::sink::writer::{LogSinkerOf, SinkWriter, SinkWriterExt};
use crate::sink::{Result, SinkCommitCoordinator, SinkParam};

/// This iceberg sink is WIP. When it ready, we will change this name to "iceberg".
pub const ICEBERG_SINK: &str = "iceberg";
pub const REMOTE_ICEBERG_SINK: &str = "iceberg_java";

#[derive(Debug)]
pub struct RemoteIceberg;

impl RemoteSinkTrait for RemoteIceberg {
    const SINK_NAME: &'static str = REMOTE_ICEBERG_SINK;
}

pub type RemoteIcebergSink = CoordinatedRemoteSink<RemoteIceberg>;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergConfig {
    #[serde(skip_serializing)]
    pub connector: String, // Must be "iceberg" here.

    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub force_append_only: bool,

    #[serde(rename = "table.name")]
    pub table_name: String, // Full name of table, must include schema name

    #[serde(rename = "database.name")]
    pub database_name: String, // Use as catalog name.

    // Catalog type supported by iceberg, such as "storage", "rest".
    // If not set, we use "storage" as default.
    #[serde(rename = "catalog.type")]
    pub catalog_type: Option<String>,

    #[serde(rename = "warehouse.path")]
    pub path: String, // Path of iceberg warehouse, only applicable in storage catalog.

    #[serde(rename = "catalog.uri")]
    pub uri: Option<String>, // URI of iceberg catalog, only applicable in rest catalog.

    #[serde(rename = "s3.region")]
    pub region: Option<String>,

    #[serde(rename = "s3.endpoint")]
    pub endpoint: Option<String>,

    #[serde(rename = "s3.access.key")]
    pub access_key: String,

    #[serde(rename = "s3.secret.key")]
    pub secret_key: String,

    #[serde(
        rename = "primary_key",
        default,
        deserialize_with = "deserialize_string_seq_from_string"
    )]
    pub primary_key: Option<Vec<String>>,
}
pub(crate) fn deserialize_string_seq_from_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<String>>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: Option<String> = de::Deserialize::deserialize(deserializer)?;
    if let Some(s) = s {
        let s = s.to_ascii_lowercase();
        let s = s.split(',').map(|s| s.trim().to_owned()).collect();
        Ok(Some(s))
    } else {
        Ok(None)
    }
}

impl IcebergConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<IcebergConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }

        if config.r#type == SINK_TYPE_UPSERT {
            if let Some(primary_key) = &config.primary_key {
                if primary_key.is_empty() {
                    return Err(SinkError::Config(anyhow!(
                        "Primary_key must not be empty in {}",
                        SINK_TYPE_UPSERT
                    )));
                }
            } else {
                return Err(SinkError::Config(anyhow!(
                    "Must set primary_key in {}",
                    SINK_TYPE_UPSERT
                )));
            }
        }

        Ok(config)
    }

    fn build_iceberg_configs(&self) -> Result<HashMap<String, String>> {
        let mut iceberg_configs = HashMap::new();

        let catalog_type = self
            .catalog_type
            .as_deref()
            .unwrap_or("storage")
            .to_string();

        iceberg_configs.insert(CATALOG_TYPE.to_string(), catalog_type.clone());
        iceberg_configs.insert(
            CATALOG_NAME.to_string(),
            self.database_name.clone().to_string(),
        );

        match catalog_type.as_str() {
            "storage" => {
                iceberg_configs.insert(
                    format!("iceberg.catalog.{}.warehouse", self.database_name),
                    self.path.clone(),
                );
            }
            "rest" => {
                let uri = self.uri.clone().ok_or_else(|| {
                    SinkError::Iceberg(anyhow!("`catalog.uri` must be set in rest catalog"))
                })?;
                iceberg_configs.insert(format!("iceberg.catalog.{}.uri", self.database_name), uri);
            }
            _ => {
                return Err(SinkError::Iceberg(anyhow!(
                    "Unsupported catalog type: {}, only support `storage` and `rest`",
                    catalog_type
                )))
            }
        }

        if let Some(region) = &self.region {
            iceberg_configs.insert(
                "iceberg.table.io.region".to_string(),
                region.clone().to_string(),
            );
        }

        if let Some(endpoint) = &self.endpoint {
            iceberg_configs.insert(
                "iceberg.table.io.endpoint".to_string(),
                endpoint.clone().to_string(),
            );
        }

        iceberg_configs.insert(
            "iceberg.table.io.access_key_id".to_string(),
            self.access_key.clone().to_string(),
        );
        iceberg_configs.insert(
            "iceberg.table.io.secret_access_key".to_string(),
            self.secret_key.clone().to_string(),
        );

        let (bucket, root) = {
            let url = Url::parse(&self.path).map_err(|e| SinkError::Iceberg(anyhow!(e)))?;
            let bucket = url
                .host_str()
                .ok_or_else(|| {
                    SinkError::Iceberg(anyhow!("Invalid s3 path: {}, bucket is missing", self.path))
                })?
                .to_string();
            let root = url.path().trim_start_matches('/').to_string();
            (bucket, root)
        };

        iceberg_configs.insert("iceberg.table.io.bucket".to_string(), bucket);
        iceberg_configs.insert("iceberg.table.io.root".to_string(), root);
        // #TODO
        // Support load config file
        iceberg_configs.insert(
            "iceberg.table.io.disable_config_load".to_string(),
            "true".to_string(),
        );

        Ok(iceberg_configs)
    }
}

pub struct IcebergSink {
    config: IcebergConfig,
    param: SinkParam,
    // In upsert mode, it never be None and empty.
    unique_column_ids: Option<Vec<usize>>,
}

impl TryFrom<SinkParam> for IcebergSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let config = IcebergConfig::from_hashmap(param.properties.clone())?;
        IcebergSink::new(config, param)
    }
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
        let catalog = load_catalog(&self.config.build_iceberg_configs()?)
            .await
            .map_err(|e| SinkError::Iceberg(anyhow!("Unable to load iceberg catalog: {e}")))?;

        let table_id = TableIdentifier::new(self.config.table_name.split('.'))
            .map_err(|e| SinkError::Iceberg(anyhow!("Unable to parse table name: {e}")))?;

        let table = catalog
            .load_table(&table_id)
            .await
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

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

    pub fn new(config: IcebergConfig, param: SinkParam) -> Result<Self> {
        let unique_column_ids = if config.r#type == SINK_TYPE_UPSERT && !config.force_append_only {
            if let Some(pk) = &config.primary_key {
                let mut unique_column_ids = Vec::with_capacity(pk.len());
                for col_name in pk {
                    let id = param
                        .columns
                        .iter()
                        .find(|col| col.name.as_str() == col_name)
                        .ok_or_else(|| {
                            SinkError::Config(anyhow!(
                                "Primary key column {} not found in sink schema",
                                col_name
                            ))
                        })?
                        .column_id
                        .get_id() as usize;
                    unique_column_ids.push(id);
                }
                Some(unique_column_ids)
            } else {
                unreachable!()
            }
        } else {
            None
        };
        Ok(Self {
            config,
            param,
            unique_column_ids,
        })
    }
}

impl Sink for IcebergSink {
    type Coordinator = IcebergSinkCommitter;
    type LogSinker = LogSinkerOf<CoordinatedSinkWriter<IcebergWriter>>;

    const SINK_NAME: &'static str = ICEBERG_SINK;

    async fn validate(&self) -> Result<()> {
        let _ = self.create_table().await?;
        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let table = self.create_table().await?;
        let inner = if let Some(unique_column_ids) = &self.unique_column_ids {
            IcebergWriter::new_upsert(table, unique_column_ids.clone()).await?
        } else {
            IcebergWriter::new_append_only(table).await?
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
        .await?
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn new_coordinator(&self) -> Result<Self::Coordinator> {
        let table = self.create_table().await?;
        let partition_type = table.current_partition_type()?;

        Ok(IcebergSinkCommitter {
            table,
            partition_type,
        })
    }
}

pub struct IcebergWriter(IcebergWriterEnum);

enum IcebergWriterEnum {
    AppendOnly(AppendOnlyWriter),
    Upsert(UpsertWriter),
}

impl IcebergWriter {
    pub async fn new_append_only(table: Table) -> Result<Self> {
        Ok(Self(IcebergWriterEnum::AppendOnly(
            AppendOnlyWriter::new(table).await?,
        )))
    }

    pub async fn new_upsert(table: Table, unique_column_ids: Vec<usize>) -> Result<Self> {
        Ok(Self(IcebergWriterEnum::Upsert(
            UpsertWriter::new(table, unique_column_ids).await?,
        )))
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
        match &mut self.0 {
            IcebergWriterEnum::AppendOnly(writer) => writer.write(chunk).await?,
            IcebergWriterEnum::Upsert(writer) => writer.write(chunk).await?,
        }
        Ok(())
    }

    /// Receive a barrier and mark the end of current epoch. When `is_checkpoint` is true, the sink
    /// writer should commit the current epoch.
    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        // Skip it if not checkpoint
        if !is_checkpoint {
            return Ok(None);
        }

        let res = match &mut self.0 {
            IcebergWriterEnum::AppendOnly(writer) => writer.flush().await?,
            IcebergWriterEnum::Upsert(writer) => writer.flush().await?,
        };

        Ok(Some(SinkMetadata::try_from(&res)?))
    }

    /// Clean up
    async fn abort(&mut self) -> Result<()> {
        // TODO: abort should clean up all the data written in this epoch.
        Ok(())
    }
}

struct AppendOnlyWriter {
    table: Table,
    writer: icelake::io::task_writer::TaskWriter<EmptyLayer>,
    schema: SchemaRef,
}

impl AppendOnlyWriter {
    pub async fn new(table: Table) -> Result<Self> {
        let schema = Arc::new(
            table
                .current_table_metadata()
                .current_schema()
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?
                .clone()
                .try_into()
                .map_err(|err: icelake::Error| SinkError::Iceberg(anyhow!(err)))?,
        );

        Ok(Self {
            writer: table
                .writer_builder()
                .await?
                .build_task_writer()
                .await
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            table,
            schema,
        })
    }

    pub async fn write(&mut self, chunk: StreamChunk) -> Result<()> {
        let (mut chunk, ops) = chunk.into_parts();

        let filters =
            chunk.visibility() & ops.iter().map(|op| *op == Op::Insert).collect::<Bitmap>();

        chunk.set_visibility(filters);
        let chunk = to_record_batch_with_schema(self.schema.clone(), &chunk.compact())
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

        self.writer.write(&chunk).await.map_err(|err| {
            SinkError::Iceberg(anyhow!("Write chunk fail: {}, chunk: {:?}", err, chunk))
        })?;

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<WriteResult> {
        let old_writer = std::mem::replace(
            &mut self.writer,
            self.table
                .writer_builder()
                .await?
                .build_task_writer()
                .await
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
        );

        let data_files = old_writer
            .close()
            .await
            .map_err(|err| SinkError::Iceberg(anyhow!("Close writer fail: {}", err)))?;

        Ok(WriteResult {
            data_files,
            delete_files: vec![],
        })
    }
}

struct UpsertWriter {
    writer: UpsertWriterInner,
    schema: SchemaRef,
}
enum UpsertWriterInner {
    Partition(PartitionDeltaWriter),
    Unpartition(UnpartitionDeltaWriter),
}

impl UpsertWriter {
    pub async fn new(table: Table, unique_column_ids: Vec<usize>) -> Result<Self> {
        let schema = Arc::new(
            table
                .current_table_metadata()
                .current_schema()
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?
                .clone()
                .try_into()
                .map_err(|err: icelake::Error| SinkError::Iceberg(anyhow!(err)))?,
        );
        let inner = if let Some(partition_splitter) = table.partition_splitter()? {
            UpsertWriterInner::Partition(PartitionDeltaWriter::new(
                table,
                partition_splitter,
                unique_column_ids,
            ))
        } else {
            UpsertWriterInner::Unpartition(
                UnpartitionDeltaWriter::new(table, unique_column_ids).await?,
            )
        };
        Ok(Self {
            writer: inner,
            schema,
        })
    }

    fn partition_ops(ops: &[Op]) -> Vec<(usize, usize)> {
        assert!(!ops.is_empty());
        let mut res = vec![];
        let mut start = 0;
        let mut prev_op = ops[0];
        for (i, op) in ops.iter().enumerate().skip(1) {
            if *op != prev_op {
                res.push((start, i));
                start = i;
                prev_op = *op;
            }
        }
        res.push((start, ops.len()));
        res
    }

    pub async fn write(&mut self, chunk: StreamChunk) -> Result<()> {
        let (chunk, ops) = chunk.compact().into_parts();
        if ops.len() == 0 {
            return Ok(());
        }
        let chunk = to_record_batch_with_schema(self.schema.clone(), &chunk.compact())
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
        let ranges = Self::partition_ops(&ops);
        for (start, end) in ranges {
            let batch = chunk.slice(start, end - start);
            match ops[start] {
                Op::UpdateInsert | Op::Insert => match &mut self.writer {
                    UpsertWriterInner::Partition(writer) => writer.write(batch).await?,
                    UpsertWriterInner::Unpartition(writer) => writer.write(batch).await?,
                },

                Op::UpdateDelete | Op::Delete => match &mut self.writer {
                    UpsertWriterInner::Partition(writer) => writer.delete(batch).await?,
                    UpsertWriterInner::Unpartition(writer) => writer.delete(batch).await?,
                },
            }
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<WriteResult> {
        match &mut self.writer {
            UpsertWriterInner::Partition(writer) => {
                let mut data_files = vec![];
                let mut delete_files = vec![];
                for res in writer.flush().await? {
                    data_files.extend(res.data);
                    delete_files.extend(res.pos_delete);
                    delete_files.extend(res.eq_delete);
                }
                Ok(WriteResult {
                    data_files,
                    delete_files,
                })
            }
            UpsertWriterInner::Unpartition(writer) => {
                let res = writer.flush().await?;
                let delete_files = res.pos_delete.into_iter().chain(res.eq_delete).collect();
                Ok(WriteResult {
                    data_files: res.data,
                    delete_files,
                })
            }
        }
    }
}

struct UnpartitionDeltaWriter {
    table: Table,
    writer: icelake::io::file_writer::EqualityDeltaWriter<EmptyLayer>,
    unique_column_ids: Vec<usize>,
}

impl UnpartitionDeltaWriter {
    pub async fn new(table: Table, unique_column_ids: Vec<usize>) -> Result<Self> {
        Ok(Self {
            writer: table
                .writer_builder()
                .await?
                .build_equality_delta_writer(unique_column_ids.clone())
                .await?,
            table,
            unique_column_ids,
        })
    }

    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.writer.write(batch).await?;
        Ok(())
    }

    pub async fn delete(&mut self, batch: RecordBatch) -> Result<()> {
        self.writer.delete(batch).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<DeltaWriterResult> {
        let writer = std::mem::replace(
            &mut self.writer,
            self.table
                .writer_builder()
                .await?
                .build_equality_delta_writer(self.unique_column_ids.clone())
                .await?,
        );
        Ok(writer.close(None).await?)
    }
}

struct PartitionDeltaWriter {
    table: Table,
    writers: HashMap<
        icelake::types::PartitionKey,
        icelake::io::file_writer::EqualityDeltaWriter<EmptyLayer>,
    >,
    partition_splitter: icelake::types::PartitionSplitter,
    unique_column_ids: Vec<usize>,
}

impl PartitionDeltaWriter {
    pub fn new(
        table: Table,
        partition_splitter: icelake::types::PartitionSplitter,
        unique_column_ids: Vec<usize>,
    ) -> Self {
        Self {
            table,
            writers: HashMap::new(),
            partition_splitter,
            unique_column_ids,
        }
    }

    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let partitions = self.partition_splitter.split_by_partition(&batch)?;
        for (partition_key, batch) in partitions {
            match self.writers.entry(partition_key) {
                Entry::Vacant(v) => {
                    v.insert(
                        self.table
                            .writer_builder()
                            .await?
                            .build_equality_delta_writer(self.unique_column_ids.clone())
                            .await?,
                    )
                    .write(batch)
                    .await?
                }
                Entry::Occupied(mut v) => v.get_mut().write(batch).await?,
            }
        }
        Ok(())
    }

    pub async fn delete(&mut self, batch: RecordBatch) -> Result<()> {
        let partitions = self.partition_splitter.split_by_partition(&batch)?;
        for (partition_key, batch) in partitions {
            match self.writers.entry(partition_key) {
                Entry::Vacant(v) => {
                    v.insert(
                        self.table
                            .writer_builder()
                            .await?
                            .build_equality_delta_writer(self.unique_column_ids.clone())
                            .await?,
                    )
                    .delete(batch)
                    .await
                    .unwrap();
                }
                Entry::Occupied(mut v) => v.get_mut().delete(batch).await.unwrap(),
            }
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<Vec<DeltaWriterResult>> {
        let mut res = Vec::with_capacity(self.writers.len());
        for (partition_key, writer) in self.writers.drain() {
            let partition_value = self
                .partition_splitter
                .convert_key_to_value(partition_key)?;
            let delta_result = writer.close(Some(partition_value)).await?;
            res.push(delta_result);
        }
        Ok(res)
    }
}

const DATA_FILES: &str = "data_files";
const DELETE_FILES: &str = "delete_files";

#[derive(Default, Debug)]
struct WriteResult {
    data_files: Vec<DataFile>,
    delete_files: Vec<DataFile>,
}

impl WriteResult {
    fn try_from(value: &SinkMetadata, partition_type: &Any) -> Result<Self> {
        if let Some(Serialized(v)) = &value.metadata {
            let mut values = if let serde_json::Value::Object(v) =
                serde_json::from_slice::<serde_json::Value>(&v.metadata).map_err(
                    |e| -> SinkError { anyhow!("Can't parse iceberg sink metadata: {}", e).into() },
                )? {
                v
            } else {
                return Err(anyhow!("iceberg sink metadata should be a object").into());
            };

            let data_files: Vec<DataFile>;
            let delete_files: Vec<DataFile>;
            if let serde_json::Value::Array(values) = values
                .remove(DATA_FILES)
                .ok_or_else(|| anyhow!("icberg sink metadata should have data_files object"))?
            {
                data_files = values
                    .into_iter()
                    .map(|value| data_file_from_json(value, partition_type.clone()))
                    .collect::<std::result::Result<Vec<DataFile>, icelake::Error>>()
                    .unwrap();
            } else {
                return Err(anyhow!("icberg sink metadata should have data_files object").into());
            }
            if let serde_json::Value::Array(values) = values
                .remove(DELETE_FILES)
                .ok_or_else(|| anyhow!("icberg sink metadata should have data_files object"))?
            {
                delete_files = values
                    .into_iter()
                    .map(|value| data_file_from_json(value, partition_type.clone()))
                    .collect::<std::result::Result<Vec<DataFile>, icelake::Error>>()
                    .map_err(|e| anyhow!("Failed to parse data file from json: {}", e))?;
            } else {
                return Err(anyhow!("icberg sink metadata should have data_files object").into());
            }
            Ok(Self {
                data_files,
                delete_files,
            })
        } else {
            Err(anyhow!("Can't create iceberg sink write result from empty data!").into())
        }
    }
}

impl<'a> TryFrom<&'a WriteResult> for SinkMetadata {
    type Error = SinkError;

    fn try_from(value: &'a WriteResult) -> std::result::Result<SinkMetadata, Self::Error> {
        let json_data_files = serde_json::Value::Array(
            value
                .data_files
                .iter()
                .cloned()
                .map(data_file_to_json)
                .collect::<std::result::Result<Vec<serde_json::Value>, icelake::Error>>()
                .map_err(|e| anyhow!("Can't serialize data files to json: {}", e))?,
        );
        let json_delete_files = serde_json::Value::Array(
            value
                .delete_files
                .iter()
                .cloned()
                .map(data_file_to_json)
                .collect::<std::result::Result<Vec<serde_json::Value>, icelake::Error>>()
                .map_err(|e| anyhow!("Can't serialize data files to json: {}", e))?,
        );
        let json_value = serde_json::Value::Object(
            vec![
                (DATA_FILES.to_string(), json_data_files),
                (DELETE_FILES.to_string(), json_delete_files),
            ]
            .into_iter()
            .collect(),
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

pub struct IcebergSinkCommitter {
    table: Table,
    partition_type: Any,
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
            .map(|meta| WriteResult::try_from(meta, &self.partition_type))
            .collect::<Result<Vec<WriteResult>>>()?;
        if write_results.is_empty() || write_results.iter().all(|r| r.data_files.is_empty()) {
            tracing::debug!(?epoch, "no data to commit");
            return Ok(());
        }
        let mut txn = Transaction::new(&mut self.table);
        write_results.into_iter().for_each(|s| {
            txn.append_data_file(s.data_files);
            txn.append_delete_file(s.delete_files);
        });
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

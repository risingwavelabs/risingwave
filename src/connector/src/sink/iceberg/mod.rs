// Copyright 2024 RisingWave Labs
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

mod jni_catalog;
mod mock_catalog;
mod prometheus;

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema, SchemaRef,
};
use async_trait::async_trait;
use icelake::catalog::{
    load_catalog, load_iceberg_base_catalog_config, BaseCatalogConfig, CatalogRef, CATALOG_NAME,
    CATALOG_TYPE,
};
use icelake::io_v2::input_wrapper::{DeltaWriter, RecordBatchWriter};
use icelake::io_v2::prometheus::{PrometheusWriterBuilder, WriterMetrics};
use icelake::io_v2::{
    DataFileWriterBuilder, EqualityDeltaWriterBuilder, IcebergWriterBuilder, DELETE_OP, INSERT_OP,
};
use icelake::transaction::Transaction;
use icelake::types::{data_file_from_json, data_file_to_json, Any, DataFile};
use icelake::{Table, TableIdentifier};
use itertools::Itertools;
use risingwave_common::array::{to_iceberg_record_batch_with_schema, Op, StreamChunk};
use risingwave_common::bail;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use risingwave_pb::connector_service::SinkMetadata;
use serde::de;
use serde_derive::Deserialize;
use thiserror_ext::AsReport;
use url::Url;
use with_options::WithOptions;

use self::mock_catalog::MockCatalog;
use self::prometheus::monitored_base_file_writer::MonitoredBaseFileWriterBuilder;
use self::prometheus::monitored_position_delete_writer::MonitoredPositionDeleteWriterBuilder;
use super::{
    Sink, SinkError, SinkWriterParam, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
use crate::deserialize_bool_from_string;
use crate::error::ConnectorResult;
use crate::sink::coordinate::CoordinatedSinkWriter;
use crate::sink::writer::{LogSinkerOf, SinkWriter, SinkWriterExt};
use crate::sink::{Result, SinkCommitCoordinator, SinkParam};

/// This iceberg sink is WIP. When it ready, we will change this name to "iceberg".
pub const ICEBERG_SINK: &str = "iceberg";

static RW_CATALOG_NAME: &str = "risingwave";

#[derive(Debug, Clone, Deserialize, WithOptions, Default)]
#[serde(deny_unknown_fields)]
pub struct IcebergConfig {
    pub connector: String, // Avoid deny unknown field. Must be "iceberg"

    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub force_append_only: bool,

    #[serde(rename = "table.name")]
    pub table_name: String, // Full name of table, must include schema name

    #[serde(rename = "database.name")]
    pub database_name: String, // Database name of table

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

    // Props for java catalog props.
    #[serde(skip)]
    pub java_catalog_props: HashMap<String, String>,
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
        let mut config =
            serde_json::from_value::<IcebergConfig>(serde_json::to_value(&values).unwrap())
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

        // All configs starts with "catalog." will be treated as java configs.
        config.java_catalog_props = values
            .iter()
            .filter(|(k, _v)| {
                k.starts_with("catalog.") && k != &"catalog.uri" && k != &"catalog.type"
            })
            .map(|(k, v)| (k[8..].to_string(), v.to_string()))
            .collect();

        Ok(config)
    }

    fn catalog_type(&self) -> &str {
        self.catalog_type.as_deref().unwrap_or("storage")
    }

    fn build_iceberg_configs(&self) -> Result<HashMap<String, String>> {
        let mut iceberg_configs = HashMap::new();

        let catalog_type = self.catalog_type().to_string();

        iceberg_configs.insert(CATALOG_TYPE.to_string(), catalog_type.clone());
        iceberg_configs.insert(CATALOG_NAME.to_string(), RW_CATALOG_NAME.to_string());

        match catalog_type.as_str() {
            "storage" => {
                iceberg_configs.insert(
                    format!("iceberg.catalog.{}.warehouse", RW_CATALOG_NAME),
                    self.path.clone(),
                );
            }
            "rest" => {
                let uri = self.uri.clone().ok_or_else(|| {
                    SinkError::Iceberg(anyhow!("`catalog.uri` must be set in rest catalog"))
                })?;
                iceberg_configs.insert(format!("iceberg.catalog.{}.uri", RW_CATALOG_NAME), uri);
            }
            _ => {
                return Err(SinkError::Iceberg(anyhow!(
                    "Unsupported catalog type: {}, only support `storage` and `rest`",
                    catalog_type
                )));
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

    fn build_jni_catalog_configs(&self) -> Result<(BaseCatalogConfig, HashMap<String, String>)> {
        let mut iceberg_configs = HashMap::new();

        let base_catalog_config = {
            let catalog_type = self.catalog_type().to_string();

            iceberg_configs.insert(CATALOG_TYPE.to_string(), catalog_type.clone());
            iceberg_configs.insert(CATALOG_NAME.to_string(), "risingwave".to_string());

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
                        SinkError::Iceberg(anyhow!(
                            "Invalid s3 path: {}, bucket is missing",
                            self.path
                        ))
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

            load_iceberg_base_catalog_config(&iceberg_configs)?
        };

        // Prepare jni configs, for details please see https://iceberg.apache.org/docs/latest/aws/
        let mut java_catalog_configs = HashMap::new();
        {
            if let Some(uri) = self.uri.as_deref() {
                java_catalog_configs.insert("uri".to_string(), uri.to_string());
            }

            java_catalog_configs.insert("warehouse".to_string(), self.path.clone());
            java_catalog_configs.extend(self.java_catalog_props.clone());

            // Currently we only support s3, so let's set it to s3
            java_catalog_configs.insert(
                "io-impl".to_string(),
                "org.apache.iceberg.aws.s3.S3FileIO".to_string(),
            );

            if let Some(endpoint) = &self.endpoint {
                iceberg_configs.insert("s3.endpoint".to_string(), endpoint.clone().to_string());
            }

            iceberg_configs.insert(
                "s3.access-key-id".to_string(),
                self.access_key.clone().to_string(),
            );
            iceberg_configs.insert(
                "s3.secret-access-key".to_string(),
                self.secret_key.clone().to_string(),
            );
        }

        Ok((base_catalog_config, java_catalog_configs))
    }

    async fn create_catalog(&self) -> ConnectorResult<CatalogRef> {
        match self.catalog_type() {
            "storage" | "rest" => {
                let iceberg_configs = self.build_iceberg_configs()?;
                let catalog = load_catalog(&iceberg_configs).await?;
                Ok(catalog)
            }
            catalog_type
                if catalog_type == "hive"
                    || catalog_type == "sql"
                    || catalog_type == "glue"
                    || catalog_type == "dynamodb" =>
            {
                // Create java catalog
                let (base_catalog_config, java_catalog_props) = self.build_jni_catalog_configs()?;
                let catalog_impl = match catalog_type {
                    "hive" => "org.apache.iceberg.hive.HiveCatalog",
                    "sql" => "org.apache.iceberg.jdbc.JdbcCatalog",
                    "glue" => "org.apache.iceberg.aws.glue.GlueCatalog",
                    "dynamodb" => "org.apache.iceberg.aws.dynamodb.DynamoDbCatalog",
                    _ => unreachable!(),
                };

                jni_catalog::JniCatalog::build(
                    base_catalog_config,
                    "risingwave",
                    catalog_impl,
                    java_catalog_props,
                )
            }
            "mock" => Ok(Arc::new(MockCatalog {})),
            _ => {
                bail!(
                    "Unsupported catalog type: {}, only support `storage`, `rest`, `hive`, `sql`, `glue`, `dynamodb`",
                    self.catalog_type()
                )
            }
        }
    }

    pub async fn load_table(&self) -> ConnectorResult<Table> {
        let catalog = self
            .create_catalog()
            .await
            .context("Unable to load iceberg catalog")?;

        let table_id = TableIdentifier::new(
            vec![self.database_name.as_str()]
                .into_iter()
                .chain(self.table_name.split('.')),
        )
        .context("Unable to parse table name")?;

        catalog.load_table(&table_id).await.map_err(Into::into)
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
    async fn create_and_validate_table(&self) -> Result<Table> {
        let table = self
            .config
            .load_table()
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
        let _ = self.create_and_validate_table().await?;
        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let table = self.create_and_validate_table().await?;
        let inner = if let Some(unique_column_ids) = &self.unique_column_ids {
            IcebergWriter::new_upsert(table, unique_column_ids.clone(), &writer_param).await?
        } else {
            IcebergWriter::new_append_only(table, &writer_param).await?
        };
        Ok(CoordinatedSinkWriter::new(
            writer_param
                .meta_client
                .expect("should have meta client")
                .sink_coordinate_client()
                .await,
            self.param.clone(),
            writer_param.vnode_bitmap.ok_or_else(|| {
                SinkError::Remote(anyhow!(
                    "sink needs coordination should not have singleton input"
                ))
            })?,
            inner,
        )
        .await?
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn new_coordinator(&self) -> Result<Self::Coordinator> {
        let table = self.create_and_validate_table().await?;
        let partition_type = table.current_partition_type()?;

        Ok(IcebergSinkCommitter {
            table,
            partition_type,
        })
    }
}

pub struct IcebergWriter {
    inner_writer: IcebergWriterEnum,
    schema: SchemaRef,
}

enum IcebergWriterEnum {
    AppendOnly(RecordBatchWriter),
    Upsert(DeltaWriter),
}

impl IcebergWriter {
    fn schema_with_extra_partition_col(table: &Table, idx: usize) -> Result<SchemaRef> {
        let schema = table.current_arrow_schema()?;

        let mut fields = schema.fields().to_vec();
        let partition_type =
            if let ArrowDataType::Struct(s) = table.current_partition_type()?.try_into()? {
                let fields = Fields::from(
                    s.into_iter()
                        .enumerate()
                        .map(|(id, field)| {
                            ArrowField::new(format!("f{id}"), field.data_type().clone(), true)
                        })
                        .collect::<Vec<_>>(),
                );
                ArrowDataType::Struct(fields)
            } else {
                unimplemented!()
            };
        fields.insert(
            idx,
            ArrowField::new("_rw_partition", partition_type, false).into(),
        );
        Ok(ArrowSchema::new(fields).into())
    }

    pub async fn new_append_only(table: Table, writer_param: &SinkWriterParam) -> Result<Self> {
        let builder_helper = table.builder_helper()?;

        let data_file_builder = DataFileWriterBuilder::new(MonitoredBaseFileWriterBuilder::new(
            builder_helper
                .rolling_writer_builder(builder_helper.parquet_writer_builder(0, None)?)?,
            writer_param
                .sink_metrics
                .iceberg_rolling_unflushed_data_file
                .clone(),
        ));
        if let Some(extra_partition_col_idx) = writer_param.extra_partition_col_idx {
            let partition_data_file_builder = builder_helper.precompute_partition_writer_builder(
                data_file_builder.clone(),
                extra_partition_col_idx,
            )?;
            let dispatch_builder = builder_helper
                .dispatcher_writer_builder(partition_data_file_builder, data_file_builder)?;
            let prometheus_builder = PrometheusWriterBuilder::new(
                dispatch_builder,
                WriterMetrics::new(
                    writer_param.sink_metrics.iceberg_write_qps.deref().clone(),
                    writer_param
                        .sink_metrics
                        .iceberg_write_latency
                        .deref()
                        .clone(),
                ),
            );
            let schema = Self::schema_with_extra_partition_col(&table, extra_partition_col_idx)?;
            let inner_writer = RecordBatchWriter::new(prometheus_builder.build(&schema).await?);
            Ok(Self {
                inner_writer: IcebergWriterEnum::AppendOnly(inner_writer),
                schema,
            })
        } else {
            let partition_data_file_builder =
                builder_helper.fanout_partition_writer_builder(data_file_builder.clone())?;
            let dispatch_builder = builder_helper
                .dispatcher_writer_builder(partition_data_file_builder, data_file_builder)?;
            // wrap a layer with collect write metrics
            let prometheus_builder = PrometheusWriterBuilder::new(
                dispatch_builder,
                WriterMetrics::new(
                    writer_param.sink_metrics.iceberg_write_qps.deref().clone(),
                    writer_param
                        .sink_metrics
                        .iceberg_write_latency
                        .deref()
                        .clone(),
                ),
            );
            let schema = table.current_arrow_schema()?;
            let inner_writer = RecordBatchWriter::new(prometheus_builder.build(&schema).await?);
            Ok(Self {
                inner_writer: IcebergWriterEnum::AppendOnly(inner_writer),
                schema,
            })
        }
    }

    pub async fn new_upsert(
        table: Table,
        unique_column_ids: Vec<usize>,
        writer_param: &SinkWriterParam,
    ) -> Result<Self> {
        let builder_helper = table.builder_helper()?;
        let data_file_builder = DataFileWriterBuilder::new(MonitoredBaseFileWriterBuilder::new(
            builder_helper
                .rolling_writer_builder(builder_helper.parquet_writer_builder(0, None)?)?,
            writer_param
                .sink_metrics
                .iceberg_rolling_unflushed_data_file
                .clone(),
        ));
        let position_delete_builder = MonitoredPositionDeleteWriterBuilder::new(
            builder_helper.position_delete_writer_builder(0, 1024)?,
            writer_param
                .sink_metrics
                .iceberg_position_delete_cache_num
                .clone(),
        );
        let equality_delete_builder =
            builder_helper.equality_delete_writer_builder(unique_column_ids.clone(), 0)?;
        let delta_builder = EqualityDeltaWriterBuilder::new(
            data_file_builder,
            position_delete_builder,
            equality_delete_builder,
            unique_column_ids,
        );
        if let Some(extra_partition_col_idx) = writer_param.extra_partition_col_idx {
            let partition_delta_builder = builder_helper.precompute_partition_writer_builder(
                delta_builder.clone(),
                extra_partition_col_idx,
            )?;
            let dispatch_builder =
                builder_helper.dispatcher_writer_builder(partition_delta_builder, delta_builder)?;
            // wrap a layer with collect write metrics
            let prometheus_builder = PrometheusWriterBuilder::new(
                dispatch_builder,
                WriterMetrics::new(
                    writer_param.sink_metrics.iceberg_write_qps.deref().clone(),
                    writer_param
                        .sink_metrics
                        .iceberg_write_latency
                        .deref()
                        .clone(),
                ),
            );
            let schema = Self::schema_with_extra_partition_col(&table, extra_partition_col_idx)?;
            let inner_writer = DeltaWriter::new(prometheus_builder.build(&schema).await?);
            Ok(Self {
                inner_writer: IcebergWriterEnum::Upsert(inner_writer),
                schema,
            })
        } else {
            let partition_delta_builder =
                builder_helper.fanout_partition_writer_builder(delta_builder.clone())?;
            let dispatch_builder =
                builder_helper.dispatcher_writer_builder(partition_delta_builder, delta_builder)?;
            // wrap a layer with collect write metrics
            let prometheus_builder = PrometheusWriterBuilder::new(
                dispatch_builder,
                WriterMetrics::new(
                    writer_param.sink_metrics.iceberg_write_qps.deref().clone(),
                    writer_param
                        .sink_metrics
                        .iceberg_write_latency
                        .deref()
                        .clone(),
                ),
            );
            let schema = table.current_arrow_schema()?;
            let inner_writer = DeltaWriter::new(prometheus_builder.build(&schema).await?);
            Ok(Self {
                inner_writer: IcebergWriterEnum::Upsert(inner_writer),
                schema,
            })
        }
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
        let (mut chunk, ops) = chunk.compact().into_parts();
        if ops.len() == 0 {
            return Ok(());
        }

        match &mut self.inner_writer {
            IcebergWriterEnum::AppendOnly(writer) => {
                // filter chunk
                let filters =
                    chunk.visibility() & ops.iter().map(|op| *op == Op::Insert).collect::<Bitmap>();
                chunk.set_visibility(filters);
                let chunk =
                    to_iceberg_record_batch_with_schema(self.schema.clone(), &chunk.compact())
                        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

                writer.write(chunk).await?;
            }
            IcebergWriterEnum::Upsert(writer) => {
                let chunk = to_iceberg_record_batch_with_schema(self.schema.clone(), &chunk)
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

                writer
                    .write(
                        ops.iter()
                            .map(|op| match op {
                                Op::UpdateInsert | Op::Insert => INSERT_OP,
                                Op::UpdateDelete | Op::Delete => DELETE_OP,
                            })
                            .collect_vec(),
                        chunk,
                    )
                    .await?;
            }
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

        let res = match &mut self.inner_writer {
            IcebergWriterEnum::AppendOnly(writer) => {
                let data_files = writer.flush().await?;
                WriteResult {
                    data_files,
                    delete_files: vec![],
                }
            }
            IcebergWriterEnum::Upsert(writer) => {
                let mut res = WriteResult {
                    data_files: vec![],
                    delete_files: vec![],
                };
                for delta in writer.flush().await? {
                    res.data_files.extend(delta.data);
                    res.delete_files.extend(delta.pos_delete);
                    res.delete_files.extend(delta.eq_delete);
                }
                res
            }
        };

        Ok(Some(SinkMetadata::try_from(&res)?))
    }

    /// Clean up
    async fn abort(&mut self) -> Result<()> {
        // TODO: abort should clean up all the data written in this epoch.
        Ok(())
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
                serde_json::from_slice::<serde_json::Value>(&v.metadata)
                    .context("Can't parse iceberg sink metadata")?
            {
                v
            } else {
                bail!("iceberg sink metadata should be a object");
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
                bail!("icberg sink metadata should have data_files object");
            }
            if let serde_json::Value::Array(values) = values
                .remove(DELETE_FILES)
                .ok_or_else(|| anyhow!("icberg sink metadata should have data_files object"))?
            {
                delete_files = values
                    .into_iter()
                    .map(|value| data_file_from_json(value, partition_type.clone()))
                    .collect::<std::result::Result<Vec<DataFile>, icelake::Error>>()
                    .context("Failed to parse data file from json")?;
            } else {
                bail!("icberg sink metadata should have data_files object");
            }
            Ok(Self {
                data_files,
                delete_files,
            })
        } else {
            bail!("Can't create iceberg sink write result from empty data!")
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
                .context("Can't serialize data files to json")?,
        );
        let json_delete_files = serde_json::Value::Array(
            value
                .delete_files
                .iter()
                .cloned()
                .map(data_file_to_json)
                .collect::<std::result::Result<Vec<serde_json::Value>, icelake::Error>>()
                .context("Can't serialize data files to json")?,
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
                metadata: serde_json::to_vec(&json_value)
                    .context("Can't serialize iceberg sink metadata")?,
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
        txn.commit().await.map_err(|err| {
            tracing::error!(error = %err.as_report(), "Failed to commit iceberg table");
            SinkError::Iceberg(anyhow!(err))
        })?;

        tracing::info!("Succeeded to commit to iceberg table in epoch {epoch}.");
        Ok(())
    }
}

/// Try to match our schema with iceberg schema.
pub fn try_matches_arrow_schema(rw_schema: &Schema, arrow_schema: &ArrowSchema) -> Result<()> {
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

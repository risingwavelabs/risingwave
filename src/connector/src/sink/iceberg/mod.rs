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

mod compaction;
mod prometheus;

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use iceberg::{Catalog as CatalogV2, NamespaceIdent, TableCreation, TableIdent};
use icelake::catalog::CatalogRef;
use icelake::io_v2::input_wrapper::{DeltaWriter, RecordBatchWriter};
use icelake::io_v2::prometheus::{PrometheusWriterBuilder, WriterMetrics};
use icelake::io_v2::{
    DataFileWriterBuilder, EqualityDeltaWriterBuilder, IcebergWriterBuilder, DELETE_OP, INSERT_OP,
};
use icelake::transaction::Transaction;
use icelake::types::{data_file_from_json, data_file_to_json, Any, DataFile};
use icelake::Table;
use itertools::Itertools;
use risingwave_common::array::arrow::arrow_schema_iceberg::{
    self, DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema, SchemaRef,
};
use risingwave_common::array::arrow::{IcebergArrowConvert, IcebergCreateTableArrowConvert};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use risingwave_pb::connector_service::SinkMetadata;
use serde_derive::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use thiserror_ext::AsReport;
use with_options::WithOptions;

use self::prometheus::monitored_base_file_writer::MonitoredBaseFileWriterBuilder;
use self::prometheus::monitored_position_delete_writer::MonitoredPositionDeleteWriterBuilder;
use super::decouple_checkpoint_log_sink::{
    default_commit_checkpoint_interval, DecoupleCheckpointLogSinkerOf,
};
use super::{
    Sink, SinkError, SinkWriterParam, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
use crate::connector_common::IcebergCommon;
use crate::sink::coordinate::CoordinatedSinkWriter;
use crate::sink::writer::SinkWriter;
use crate::sink::{Result, SinkCommitCoordinator, SinkParam};
use crate::{
    deserialize_bool_from_string, deserialize_optional_bool_from_string,
    deserialize_optional_string_seq_from_string,
};

pub const ICEBERG_SINK: &str = "iceberg";

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, WithOptions)]
pub struct IcebergConfig {
    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub force_append_only: bool,

    #[serde(flatten)]
    common: IcebergCommon,

    #[serde(
        rename = "s3.path.style.access",
        default,
        deserialize_with = "deserialize_optional_bool_from_string"
    )]
    pub path_style_access: Option<bool>,

    #[serde(
        rename = "primary_key",
        default,
        deserialize_with = "deserialize_optional_string_seq_from_string"
    )]
    pub primary_key: Option<Vec<String>>,

    // Props for java catalog props.
    #[serde(skip)]
    pub java_catalog_props: HashMap<String, String>,

    /// Commit every n(>0) checkpoints, default is 10.
    #[serde(default = "default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    pub commit_checkpoint_interval: u64,

    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub create_table_if_not_exists: bool,
}

impl IcebergConfig {
    pub fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
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

        if config.common.catalog_name.is_none()
            && config.common.catalog_type.as_deref() != Some("storage")
        {
            return Err(SinkError::Config(anyhow!(
                "catalog.name must be set for non-storage catalog"
            )));
        }

        // All configs start with "catalog." will be treated as java configs.
        config.java_catalog_props = values
            .iter()
            .filter(|(k, _v)| {
                k.starts_with("catalog.")
                    && k != &"catalog.uri"
                    && k != &"catalog.type"
                    && k != &"catalog.name"
            })
            .map(|(k, v)| (k[8..].to_string(), v.to_string()))
            .collect();

        if config.commit_checkpoint_interval == 0 {
            return Err(SinkError::Config(anyhow!(
                "`commit_checkpoint_interval` must be greater than 0"
            )));
        }

        Ok(config)
    }

    pub fn catalog_type(&self) -> &str {
        self.common.catalog_type()
    }

    pub async fn create_catalog(&self) -> Result<CatalogRef> {
        self.common
            .create_catalog(&self.path_style_access, &self.java_catalog_props)
            .await
            .map_err(Into::into)
    }

    pub async fn load_table(&self) -> Result<Table> {
        self.common
            .load_table(&self.path_style_access, &self.java_catalog_props)
            .await
            .map_err(Into::into)
    }

    pub async fn create_catalog_v2(&self) -> Result<Arc<dyn CatalogV2>> {
        self.common
            .create_catalog_v2(&self.path_style_access, &self.java_catalog_props)
            .await
            .map_err(Into::into)
    }

    pub fn full_table_name_v2(&self) -> Result<TableIdent> {
        self.common.full_table_name_v2().map_err(Into::into)
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
        let config = IcebergConfig::from_btreemap(param.properties.clone())?;
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
        if self.config.create_table_if_not_exists {
            self.create_table_if_not_exists().await?;
        }

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

        try_matches_arrow_schema(&sink_schema, &iceberg_schema)
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

        Ok(table)
    }

    async fn create_table_if_not_exists(&self) -> Result<()> {
        let catalog = self.config.create_catalog_v2().await?;
        let table_id = self
            .config
            .full_table_name_v2()
            .context("Unable to parse table name")?;
        if !catalog
            .table_exists(&table_id)
            .await
            .map_err(|e| SinkError::Iceberg(anyhow!(e)))?
        {
            let namespace = if let Some(database_name) = &self.config.common.database_name {
                NamespaceIdent::new(database_name.clone())
            } else {
                bail!("database name must be set if you want to create table")
            };

            let iceberg_create_table_arrow_convert = IcebergCreateTableArrowConvert::default();
            // convert risingwave schema -> arrow schema -> iceberg schema
            let arrow_fields = self
                .param
                .columns
                .iter()
                .map(|column| {
                    Ok(iceberg_create_table_arrow_convert
                        .to_arrow_field(&column.name, &column.data_type)
                        .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                        .context(format!(
                            "failed to convert {}: {} to arrow type",
                            &column.name, &column.data_type
                        ))?)
                })
                .collect::<Result<Vec<ArrowField>>>()?;
            let arrow_schema = arrow_schema_iceberg::Schema::new(arrow_fields);
            let iceberg_schema = iceberg::arrow::arrow_schema_to_schema(&arrow_schema)
                .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                .context("failed to convert arrow schema to iceberg schema")?;

            let location = {
                let mut names = namespace.clone().inner();
                names.push(self.config.common.table_name.to_string());
                if self.config.common.warehouse_path.ends_with('/') {
                    format!("{}{}", self.config.common.warehouse_path, names.join("/"))
                } else {
                    format!("{}/{}", self.config.common.warehouse_path, names.join("/"))
                }
            };

            let table_creation = TableCreation::builder()
                .name(self.config.common.table_name.clone())
                .schema(iceberg_schema)
                .location(location)
                .build();

            catalog
                .create_table(&namespace, table_creation)
                .await
                .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                .context("failed to create iceberg table")?;
        }
        Ok(())
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
    type LogSinker = DecoupleCheckpointLogSinkerOf<CoordinatedSinkWriter<IcebergWriter>>;

    const SINK_NAME: &'static str = ICEBERG_SINK;

    async fn validate(&self) -> Result<()> {
        if "glue".eq_ignore_ascii_case(self.config.catalog_type()) {
            risingwave_common::license::Feature::IcebergSinkWithGlue
                .check_available()
                .map_err(|e| anyhow::anyhow!(e))?;
        }
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
        let writer = CoordinatedSinkWriter::new(
            writer_param
                .meta_client
                .expect("should have meta client")
                .sink_coordinate_client()
                .await,
            self.param.clone(),
            writer_param.vnode_bitmap.ok_or_else(|| {
                SinkError::Remote(anyhow!(
                    "sink needs coordination and should not have singleton input"
                ))
            })?,
            inner,
        )
        .await?;

        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );

        Ok(DecoupleCheckpointLogSinkerOf::new(
            writer,
            writer_param.sink_metrics,
            commit_checkpoint_interval,
        ))
    }

    async fn new_coordinator(&self) -> Result<Self::Coordinator> {
        let catalog = self.config.create_catalog().await?;
        let table = self.create_and_validate_table().await?;
        let partition_type = table.current_partition_type()?;

        Ok(IcebergSinkCommitter {
            catalog,
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
                let chunk = IcebergArrowConvert
                    .to_record_batch(self.schema.clone(), &chunk.compact())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

                writer.write(chunk).await?;
            }
            IcebergWriterEnum::Upsert(writer) => {
                let chunk = IcebergArrowConvert
                    .to_record_batch(self.schema.clone(), &chunk)
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
                bail!("iceberg sink metadata should be an object");
            };

            let data_files: Vec<DataFile>;
            let delete_files: Vec<DataFile>;
            if let serde_json::Value::Array(values) = values
                .remove(DATA_FILES)
                .ok_or_else(|| anyhow!("iceberg sink metadata should have data_files object"))?
            {
                data_files = values
                    .into_iter()
                    .map(|value| data_file_from_json(value, partition_type.clone()))
                    .collect::<std::result::Result<Vec<DataFile>, icelake::Error>>()
                    .unwrap();
            } else {
                bail!("iceberg sink metadata should have data_files object");
            }
            if let serde_json::Value::Array(values) = values
                .remove(DELETE_FILES)
                .ok_or_else(|| anyhow!("iceberg sink metadata should have data_files object"))?
            {
                delete_files = values
                    .into_iter()
                    .map(|value| data_file_from_json(value, partition_type.clone()))
                    .collect::<std::result::Result<Vec<DataFile>, icelake::Error>>()
                    .context("Failed to parse data file from json")?;
            } else {
                bail!("Iceberg sink metadata should have data_files object");
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
    catalog: CatalogRef,
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
        if write_results.is_empty()
            || write_results
                .iter()
                .all(|r| r.data_files.is_empty() && r.delete_files.is_empty())
        {
            tracing::debug!(?epoch, "no data to commit");
            return Ok(());
        }
        // Load the latest table to avoid concurrent modification with the best effort.
        self.table = self
            .catalog
            .clone()
            .load_table(self.table.table_name())
            .await?;
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
pub fn try_matches_arrow_schema(
    rw_schema: &Schema,
    arrow_schema: &ArrowSchema,
) -> anyhow::Result<()> {
    if rw_schema.fields.len() != arrow_schema.fields().len() {
        bail!(
            "Schema length mismatch, risingwave is {}, and iceberg is {}",
            rw_schema.fields.len(),
            arrow_schema.fields.len()
        );
    }

    let mut schema_fields = HashMap::new();
    rw_schema.fields.iter().for_each(|field| {
        let res = schema_fields.insert(&field.name, &field.data_type);
        // This assert is to make sure there is no duplicate field name in the schema.
        assert!(res.is_none())
    });

    for arrow_field in &arrow_schema.fields {
        let our_field_type = schema_fields
            .get(arrow_field.name())
            .ok_or_else(|| anyhow!("Field {} not found in our schema", arrow_field.name()))?;

        // Iceberg source should be able to read iceberg decimal type.
        let converted_arrow_data_type = IcebergArrowConvert
            .to_arrow_field("", our_field_type)
            .map_err(|e| anyhow!(e))?
            .data_type()
            .clone();

        let compatible = match (&converted_arrow_data_type, arrow_field.data_type()) {
            (ArrowDataType::Decimal128(_, _), ArrowDataType::Decimal128(_, _)) => true,
            (ArrowDataType::Binary, ArrowDataType::LargeBinary) => true,
            (ArrowDataType::LargeBinary, ArrowDataType::Binary) => true,
            // cases where left != right (metadata, field name mismatch)
            //
            // all nested types: in iceberg `field_id` will always be present, but RW doesn't have it:
            // {"PARQUET:field_id": ".."}
            //
            // map: The standard name in arrow is "entries", "key", "value".
            // in iceberg-rs, it's called "key_value"
            (left, right) => left.equals_datatype(right),
        };
        if !compatible {
            bail!("field {}'s type is incompatible\nRisingWave converted data type: {}\niceberg's data type: {}",
                    arrow_field.name(), converted_arrow_data_type, arrow_field.data_type()
                );
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use risingwave_common::catalog::Field;

    use crate::connector_common::IcebergCommon;
    use crate::sink::decouple_checkpoint_log_sink::DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITH_SINK_DECOUPLE;
    use crate::sink::iceberg::IcebergConfig;
    use crate::source::DataType;

    #[test]
    fn test_compatible_arrow_schema() {
        use arrow_schema_iceberg::{DataType as ArrowDataType, Field as ArrowField};

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

    #[test]
    fn test_parse_iceberg_config() {
        let values = [
            ("connector", "iceberg"),
            ("type", "upsert"),
            ("primary_key", "v1"),
            ("warehouse.path", "s3://iceberg"),
            ("s3.endpoint", "http://127.0.0.1:9301"),
            ("s3.access.key", "hummockadmin"),
            ("s3.secret.key", "hummockadmin"),
            ("s3.path.style.access", "true"),
            ("s3.region", "us-east-1"),
            ("catalog.type", "jdbc"),
            ("catalog.name", "demo"),
            ("catalog.uri", "jdbc://postgresql://postgres:5432/iceberg"),
            ("catalog.jdbc.user", "admin"),
            ("catalog.jdbc.password", "123456"),
            ("database.name", "demo_db"),
            ("table.name", "demo_table"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let iceberg_config = IcebergConfig::from_btreemap(values).unwrap();

        let expected_iceberg_config = IcebergConfig {
            common: IcebergCommon {
                warehouse_path: "s3://iceberg".to_string(),
                catalog_uri: Some("jdbc://postgresql://postgres:5432/iceberg".to_string()),
                region: Some("us-east-1".to_string()),
                endpoint: Some("http://127.0.0.1:9301".to_string()),
                access_key: "hummockadmin".to_string(),
                secret_key: "hummockadmin".to_string(),
                catalog_type: Some("jdbc".to_string()),
                catalog_name: Some("demo".to_string()),
                database_name: Some("demo_db".to_string()),
                table_name: "demo_table".to_string(),
                enable_config_load: None,
            },
            r#type: "upsert".to_string(),
            force_append_only: false,
            path_style_access: Some(true),
            primary_key: Some(vec!["v1".to_string()]),
            java_catalog_props: [("jdbc.user", "admin"), ("jdbc.password", "123456")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            commit_checkpoint_interval: DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITH_SINK_DECOUPLE,
            create_table_if_not_exists: false,
        };

        assert_eq!(iceberg_config, expected_iceberg_config);

        assert_eq!(
            &iceberg_config.common.full_table_name().unwrap().to_string(),
            "demo_db.demo_table"
        );
    }

    async fn test_create_catalog(configs: BTreeMap<String, String>) {
        let iceberg_config = IcebergConfig::from_btreemap(configs).unwrap();

        let table = iceberg_config.load_table().await.unwrap();

        println!("{:?}", table.table_name());
    }

    #[tokio::test]
    #[ignore]
    async fn test_storage_catalog() {
        let values = [
            ("connector", "iceberg"),
            ("type", "append-only"),
            ("force_append_only", "true"),
            ("s3.endpoint", "http://127.0.0.1:9301"),
            ("s3.access.key", "hummockadmin"),
            ("s3.secret.key", "hummockadmin"),
            ("s3.region", "us-east-1"),
            ("s3.path.style.access", "true"),
            ("catalog.name", "demo"),
            ("catalog.type", "storage"),
            ("warehouse.path", "s3://icebergdata/demo"),
            ("database.name", "s1"),
            ("table.name", "t1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        test_create_catalog(values).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_rest_catalog() {
        let values = [
            ("connector", "iceberg"),
            ("type", "append-only"),
            ("force_append_only", "true"),
            ("s3.endpoint", "http://127.0.0.1:9301"),
            ("s3.access.key", "hummockadmin"),
            ("s3.secret.key", "hummockadmin"),
            ("s3.region", "us-east-1"),
            ("s3.path.style.access", "true"),
            ("catalog.name", "demo"),
            ("catalog.type", "rest"),
            ("catalog.uri", "http://192.168.167.4:8181"),
            ("warehouse.path", "s3://icebergdata/demo"),
            ("database.name", "s1"),
            ("table.name", "t1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        test_create_catalog(values).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_jdbc_catalog() {
        let values = [
            ("connector", "iceberg"),
            ("type", "append-only"),
            ("force_append_only", "true"),
            ("s3.endpoint", "http://127.0.0.1:9301"),
            ("s3.access.key", "hummockadmin"),
            ("s3.secret.key", "hummockadmin"),
            ("s3.region", "us-east-1"),
            ("s3.path.style.access", "true"),
            ("catalog.name", "demo"),
            ("catalog.type", "jdbc"),
            ("catalog.uri", "jdbc:postgresql://localhost:5432/iceberg"),
            ("catalog.jdbc.user", "admin"),
            ("catalog.jdbc.password", "123456"),
            ("warehouse.path", "s3://icebergdata/demo"),
            ("database.name", "s1"),
            ("table.name", "t1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        test_create_catalog(values).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_hive_catalog() {
        let values = [
            ("connector", "iceberg"),
            ("type", "append-only"),
            ("force_append_only", "true"),
            ("s3.endpoint", "http://127.0.0.1:9301"),
            ("s3.access.key", "hummockadmin"),
            ("s3.secret.key", "hummockadmin"),
            ("s3.region", "us-east-1"),
            ("s3.path.style.access", "true"),
            ("catalog.name", "demo"),
            ("catalog.type", "hive"),
            ("catalog.uri", "thrift://localhost:9083"),
            ("warehouse.path", "s3://icebergdata/demo"),
            ("database.name", "s1"),
            ("table.name", "t1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        test_create_catalog(values).await;
    }
}

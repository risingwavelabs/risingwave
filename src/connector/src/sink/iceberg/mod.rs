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

pub mod common;
mod prometheus;
mod v3_committer;

#[cfg(test)]
mod test;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use await_tree::InstrumentAwait;
pub use common::*;
use iceberg::arrow::{
    RecordBatchPartitionSplitter, arrow_schema_to_schema, schema_to_arrow_schema,
};
use iceberg::spec::{
    DataFile, FormatVersion, Operation, PartitionSpecRef, SchemaRef as IcebergSchemaRef,
    SerializedDataFile,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, FastAppendAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::deletion_vector_writer::{
    DeletionVectorWriter, DeletionVectorWriterBuilder,
};
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::base_writer::position_delete_file_writer::{
    POSITION_DELETE_SCHEMA, PositionDeleteFileWriter, PositionDeleteFileWriterBuilder,
    PositionDeleteInput,
};
use iceberg::writer::delta_writer::{DELETE_OP, DeltaWriterBuilder, INSERT_OP};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::task_writer::TaskWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableIdent};
use itertools::Itertools;
use prometheus::monitored_general_writer::MonitoredGeneralWriterBuilder;
use risingwave_common::array::arrow::arrow_array_iceberg::{Int32Array, RecordBatch};
use risingwave_common::array::arrow::arrow_schema_iceberg::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef,
};
use risingwave_common::array::arrow::{IcebergArrowConvert, IcebergCreateTableArrowConvert};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::Field;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntCounter};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use risingwave_pb::stream_plan::PbSinkSchemaChange;
use serde_json::from_value;
use tokio::sync::mpsc::UnboundedSender;
use tracing::warn;
use uuid::Uuid;

use super::{GLOBAL_SINK_METRICS, SINK_TYPE_UPSERT, Sink, SinkError, SinkWriterParam};
use crate::connector_common::IcebergSinkCompactionUpdate;
use crate::enforce_secret::EnforceSecret;
use crate::sink::catalog::SinkId;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::iceberg::v3_committer::IcebergV3SinkCommitter;
use crate::sink::writer::SinkWriter;
use crate::sink::{
    Result, SinglePhaseCommitCoordinator, SinkCommitCoordinator, SinkParam,
    TwoPhaseCommitCoordinator,
};

pub struct IcebergSink {
    pub config: IcebergConfig,
    param: SinkParam,
    // In upsert mode, it never be None and empty.
    unique_column_ids: Option<Vec<usize>>,
}

impl EnforceSecret for IcebergSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            IcebergConfig::enforce_one(prop)?;
        }
        Ok(())
    }
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
    pub async fn create_and_validate_table(&self) -> Result<Table> {
        create_and_validate_table_impl(&self.config, &self.param).await
    }

    pub async fn create_table_if_not_exists(&self) -> Result<()> {
        create_table_if_not_exists_impl(&self.config, &self.param).await
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
    type LogSinker = CoordinatedLogSinker<IcebergSinkWriter>;

    const SINK_NAME: &'static str = ICEBERG_SINK;

    async fn validate(&self) -> Result<()> {
        if "snowflake".eq_ignore_ascii_case(self.config.catalog_type()) {
            bail!("Snowflake catalog only supports iceberg sources");
        }

        if "glue".eq_ignore_ascii_case(self.config.catalog_type()) {
            risingwave_common::license::Feature::IcebergSinkWithGlue
                .check_available()
                .map_err(|e| anyhow::anyhow!(e))?;
        }

        // Enforce merge-on-read for append-only tables
        IcebergConfig::validate_append_only_write_mode(
            &self.config.r#type,
            self.config.write_mode,
        )?;

        // Validate compaction type configuration
        let compaction_type = self.config.compaction_type();

        // Check COW mode constraints
        // COW mode only supports 'full' compaction type
        if self.config.write_mode == IcebergWriteMode::CopyOnWrite
            && compaction_type != CompactionType::Full
        {
            bail!(
                "'copy-on-write' mode only supports 'full' compaction type, got: '{}'",
                compaction_type
            );
        }

        match compaction_type {
            CompactionType::SmallFiles => {
                // 1. check license
                risingwave_common::license::Feature::IcebergCompaction
                    .check_available()
                    .map_err(|e| anyhow::anyhow!(e))?;

                // 2. check write mode
                if self.config.write_mode != IcebergWriteMode::MergeOnRead {
                    bail!(
                        "'small-files' compaction type only supports 'merge-on-read' write mode, got: '{}'",
                        self.config.write_mode
                    );
                }

                // 3. check conflicting parameters
                if self.config.delete_files_count_threshold.is_some() {
                    bail!(
                        "`compaction.delete-files-count-threshold` is not supported for 'small-files' compaction type"
                    );
                }
            }
            CompactionType::FilesWithDelete => {
                // 1. check license
                risingwave_common::license::Feature::IcebergCompaction
                    .check_available()
                    .map_err(|e| anyhow::anyhow!(e))?;

                // 2. check write mode
                if self.config.write_mode != IcebergWriteMode::MergeOnRead {
                    bail!(
                        "'files-with-delete' compaction type only supports 'merge-on-read' write mode, got: '{}'",
                        self.config.write_mode
                    );
                }

                // 3. check conflicting parameters
                if self.config.small_files_threshold_mb.is_some() {
                    bail!(
                        "`compaction.small-files-threshold-mb` must not be set for 'files-with-delete' compaction type"
                    );
                }
            }
            CompactionType::Full => {
                // Full compaction has no special requirements
            }
        }

        let _ = self.create_and_validate_table().await?;
        Ok(())
    }

    fn support_schema_change() -> bool {
        true
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        let iceberg_config = IcebergConfig::from_btreemap(config.clone())?;

        // Validate compaction interval
        if let Some(compaction_interval) = iceberg_config.compaction_interval_sec {
            if iceberg_config.enable_compaction && compaction_interval == 0 {
                bail!(
                    "`compaction-interval-sec` must be greater than 0 when `enable-compaction` is true"
                );
            }

            // log compaction_interval
            tracing::info!(
                "Alter config compaction_interval set to {} seconds",
                compaction_interval
            );
        }

        // Validate max snapshots
        if let Some(max_snapshots) = iceberg_config.max_snapshots_num_before_compaction
            && max_snapshots < 1
        {
            bail!(
                "`compaction.max-snapshots-num` must be greater than 0, got: {}",
                max_snapshots
            );
        }

        // Validate target file size
        if let Some(target_file_size_mb) = iceberg_config.target_file_size_mb
            && target_file_size_mb == 0
        {
            bail!("`compaction.target_file_size_mb` must be greater than 0");
        }

        // Validate parquet max row group rows
        if let Some(max_row_group_rows) = iceberg_config.write_parquet_max_row_group_rows
            && max_row_group_rows == 0
        {
            bail!("`compaction.write_parquet_max_row_group_rows` must be greater than 0");
        }

        // Validate parquet compression codec
        if let Some(ref compression) = iceberg_config.write_parquet_compression {
            let valid_codecs = [
                "uncompressed",
                "snappy",
                "gzip",
                "lzo",
                "brotli",
                "lz4",
                "zstd",
            ];
            if !valid_codecs.contains(&compression.to_lowercase().as_str()) {
                bail!(
                    "`compaction.write_parquet_compression` must be one of {:?}, got: {}",
                    valid_codecs,
                    compression
                );
            }
        }

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let writer = IcebergSinkWriter::new(
            self.config.clone(),
            self.param.clone(),
            writer_param.clone(),
            self.unique_column_ids.clone(),
        );

        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );
        let log_sinker = CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            writer,
            commit_checkpoint_interval,
        )
        .await?;

        Ok(log_sinker)
    }

    fn is_coordinated_sink(&self) -> bool {
        true
    }

    async fn new_coordinator(
        &self,
        iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
    ) -> Result<SinkCommitCoordinator> {
        let catalog = self.config.create_catalog().await?;
        let table = self.create_and_validate_table().await?;
        let exactly_once = self.config.is_exactly_once.unwrap_or_default();

        if self.config.is_no_eq_delete_sink() {
            let coordinator = IcebergV3SinkCommitter::new(
                catalog,
                table,
                self.config.clone(),
                self.param.sink_id,
                self.config.commit_retry_num,
            );
            return Ok(SinkCommitCoordinator::TwoPhase(Box::new(coordinator)));
        }

        let coordinator = IcebergSinkCommitter {
            catalog,
            table,
            last_commit_epoch: 0,
            sink_id: self.param.sink_id,
            config: self.config.clone(),
            param: self.param.clone(),
            commit_retry_num: self.config.commit_retry_num,
            iceberg_compact_stat_sender,
        };
        if exactly_once {
            Ok(SinkCommitCoordinator::TwoPhase(Box::new(coordinator)))
        } else {
            Ok(SinkCommitCoordinator::SinglePhase(Box::new(coordinator)))
        }
    }
}

/// None means no project.
/// Prepare represent the extra partition column idx.
/// Done represents the project idx vec.
///
/// The `ProjectIdxVec` will be late-evaluated. When we encounter the Prepare state first, we will use the data chunk schema
/// to create the project idx vec.
enum ProjectIdxVec {
    None,
    Prepare(usize),
    Done(Vec<usize>),
}

type DataFileWriterBuilderType =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;
type PositionDeleteFileWriterBuilderType = PositionDeleteFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;
type PositionDeleteFileWriterType = PositionDeleteFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;
type DeletionVectorWriterBuilderType =
    DeletionVectorWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>;
type DeletionVectorWriterType =
    DeletionVectorWriter<DefaultLocationGenerator, DefaultFileNameGenerator>;
type EqualityDeleteFileWriterBuilderType = EqualityDeleteFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

#[derive(Clone)]
enum PositionDeleteWriterBuilderType {
    PositionDelete(PositionDeleteFileWriterBuilderType),
    DeletionVector(DeletionVectorWriterBuilderType),
}

enum PositionDeleteWriterType {
    PositionDelete(PositionDeleteFileWriterType),
    DeletionVector(DeletionVectorWriterType),
}

#[async_trait]
impl IcebergWriterBuilder<Vec<PositionDeleteInput>> for PositionDeleteWriterBuilderType {
    type R = PositionDeleteWriterType;

    async fn build(
        &self,
        partition_key: Option<iceberg::spec::PartitionKey>,
    ) -> iceberg::Result<Self::R> {
        match self {
            PositionDeleteWriterBuilderType::PositionDelete(builder) => Ok(
                PositionDeleteWriterType::PositionDelete(builder.build(partition_key).await?),
            ),
            PositionDeleteWriterBuilderType::DeletionVector(builder) => Ok(
                PositionDeleteWriterType::DeletionVector(builder.build(partition_key).await?),
            ),
        }
    }
}

#[async_trait]
impl IcebergWriter<Vec<PositionDeleteInput>> for PositionDeleteWriterType {
    async fn write(&mut self, input: Vec<PositionDeleteInput>) -> iceberg::Result<()> {
        match self {
            PositionDeleteWriterType::PositionDelete(writer) => writer.write(input).await,
            PositionDeleteWriterType::DeletionVector(writer) => writer.write(input).await,
        }
    }

    async fn close(&mut self) -> iceberg::Result<Vec<DataFile>> {
        match self {
            PositionDeleteWriterType::PositionDelete(writer) => writer.close().await,
            PositionDeleteWriterType::DeletionVector(writer) => writer.close().await,
        }
    }
}

#[derive(Clone)]
struct SharedIcebergWriterBuilder<B>(Arc<B>);

#[async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for SharedIcebergWriterBuilder<B> {
    type R = B::R;

    async fn build(
        &self,
        partition_key: Option<iceberg::spec::PartitionKey>,
    ) -> iceberg::Result<Self::R> {
        self.0.build(partition_key).await
    }
}

#[derive(Clone)]
struct TaskWriterBuilderWrapper<B: IcebergWriterBuilder> {
    inner: Arc<B>,
    fanout_enabled: bool,
    schema: IcebergSchemaRef,
    partition_spec: PartitionSpecRef,
    compute_partition: bool,
}

impl<B: IcebergWriterBuilder> TaskWriterBuilderWrapper<B> {
    fn new(
        inner: B,
        fanout_enabled: bool,
        schema: IcebergSchemaRef,
        partition_spec: PartitionSpecRef,
        compute_partition: bool,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            fanout_enabled,
            schema,
            partition_spec,
            compute_partition,
        }
    }

    fn build(&self) -> iceberg::Result<TaskWriter<SharedIcebergWriterBuilder<B>>> {
        let partition_splitter = match (
            self.partition_spec.is_unpartitioned(),
            self.compute_partition,
        ) {
            (true, _) => None,
            (false, true) => Some(RecordBatchPartitionSplitter::try_new_with_computed_values(
                self.schema.clone(),
                self.partition_spec.clone(),
            )?),
            (false, false) => Some(
                RecordBatchPartitionSplitter::try_new_with_precomputed_values(
                    self.schema.clone(),
                    self.partition_spec.clone(),
                )?,
            ),
        };

        Ok(TaskWriter::new_with_partition_splitter(
            SharedIcebergWriterBuilder(self.inner.clone()),
            self.fanout_enabled,
            self.schema.clone(),
            self.partition_spec.clone(),
            partition_splitter,
        ))
    }
}

pub enum IcebergSinkWriter {
    Created(IcebergSinkWriterArgs),
    Initialized(IcebergSinkWriterInner),
}

pub struct IcebergSinkWriterArgs {
    config: IcebergConfig,
    sink_param: SinkParam,
    writer_param: SinkWriterParam,
    unique_column_ids: Option<Vec<usize>>,
}

pub struct IcebergSinkWriterInner {
    writer: IcebergWriterDispatch,
    arrow_schema: SchemaRef,
    // See comments below
    metrics: IcebergWriterMetrics,
    // State of iceberg table for this writer
    table: Table,
    // For chunk with extra partition column, we should remove this column before write.
    // This project index vec is used to avoid create project idx each time.
    project_idx_vec: ProjectIdxVec,
    commit_checkpoint_size_threshold_bytes: Option<u64>,
    uncommitted_write_bytes: u64,
}

#[allow(clippy::type_complexity)]
enum IcebergWriterDispatch {
    Append {
        writer: Option<Box<dyn IcebergWriter>>,
        writer_builder:
            TaskWriterBuilderWrapper<MonitoredGeneralWriterBuilder<DataFileWriterBuilderType>>,
    },
    Upsert {
        writer: Option<Box<dyn IcebergWriter>>,
        writer_builder: TaskWriterBuilderWrapper<
            MonitoredGeneralWriterBuilder<
                DeltaWriterBuilder<
                    DataFileWriterBuilderType,
                    PositionDeleteWriterBuilderType,
                    EqualityDeleteFileWriterBuilderType,
                >,
            >,
        >,
        arrow_schema_with_op_column: SchemaRef,
    },
}

impl IcebergWriterDispatch {
    pub fn get_writer(&mut self) -> Option<&mut Box<dyn IcebergWriter>> {
        match self {
            IcebergWriterDispatch::Append { writer, .. }
            | IcebergWriterDispatch::Upsert { writer, .. } => writer.as_mut(),
        }
    }
}

pub struct IcebergWriterMetrics {
    // NOTE: These 2 metrics are not used directly by us, but only kept for lifecycle management.
    // They are actually used in `PrometheusWriterBuilder`:
    //     WriterMetrics::new(write_qps.deref().clone(), write_latency.deref().clone())
    // We keep them here to let the guard cleans the labels from metrics registry when dropped
    _write_qps: LabelGuardedIntCounter,
    _write_latency: LabelGuardedHistogram,
    write_bytes: LabelGuardedIntCounter,
}

impl IcebergSinkWriter {
    pub fn new(
        config: IcebergConfig,
        sink_param: SinkParam,
        writer_param: SinkWriterParam,
        unique_column_ids: Option<Vec<usize>>,
    ) -> Self {
        Self::Created(IcebergSinkWriterArgs {
            config,
            sink_param,
            writer_param,
            unique_column_ids,
        })
    }
}

impl IcebergSinkWriterInner {
    fn should_commit_on_checkpoint(&self) -> bool {
        self.commit_checkpoint_size_threshold_bytes
            .is_some_and(|threshold| {
                self.uncommitted_write_bytes > 0 && self.uncommitted_write_bytes >= threshold
            })
    }

    fn build_append_only(
        config: &IcebergConfig,
        table: Table,
        writer_param: &SinkWriterParam,
    ) -> Result<Self> {
        let SinkWriterParam {
            extra_partition_col_idx,
            actor_id,
            sink_id,
            sink_name,
            ..
        } = writer_param;
        let metrics_labels = [
            &actor_id.to_string(),
            &sink_id.to_string(),
            sink_name.as_str(),
        ];

        // Metrics
        let write_qps = GLOBAL_SINK_METRICS
            .iceberg_write_qps
            .with_guarded_label_values(&metrics_labels);
        let write_latency = GLOBAL_SINK_METRICS
            .iceberg_write_latency
            .with_guarded_label_values(&metrics_labels);
        // # TODO
        // Unused. Add this metrics later.
        let _rolling_unflushed_data_file = GLOBAL_SINK_METRICS
            .iceberg_rolling_unflushed_data_file
            .with_guarded_label_values(&metrics_labels);
        let write_bytes = GLOBAL_SINK_METRICS
            .iceberg_write_bytes
            .with_guarded_label_values(&metrics_labels);

        let schema = table.metadata().current_schema();
        let partition_spec = table.metadata().default_partition_spec();
        let fanout_enabled = !partition_spec.fields().is_empty();
        // To avoid duplicate file name, each time the sink created will generate a unique uuid as file name suffix.
        let unique_uuid_suffix = Uuid::now_v7();

        let data_file_builder = build_data_file_writer_builder(
            config,
            &table,
            &writer_param.actor_id.to_string(),
            &unique_uuid_suffix.to_string(),
        )?;
        let monitored_builder = MonitoredGeneralWriterBuilder::new(
            data_file_builder,
            write_qps.clone(),
            write_latency.clone(),
        );
        let writer_builder = TaskWriterBuilderWrapper::new(
            monitored_builder,
            fanout_enabled,
            schema.clone(),
            partition_spec.clone(),
            true,
        );
        let inner_writer = Some(Box::new(
            writer_builder
                .build()
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
        ) as Box<dyn IcebergWriter>);
        Ok(Self {
            arrow_schema: Arc::new(
                schema_to_arrow_schema(table.metadata().current_schema())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            ),
            metrics: IcebergWriterMetrics {
                _write_qps: write_qps,
                _write_latency: write_latency,
                write_bytes,
            },
            writer: IcebergWriterDispatch::Append {
                writer: inner_writer,
                writer_builder,
            },
            table,
            project_idx_vec: {
                if let Some(extra_partition_col_idx) = extra_partition_col_idx {
                    ProjectIdxVec::Prepare(*extra_partition_col_idx)
                } else {
                    ProjectIdxVec::None
                }
            },
            commit_checkpoint_size_threshold_bytes: config.commit_checkpoint_size_threshold_bytes(),
            uncommitted_write_bytes: 0,
        })
    }

    fn build_upsert(
        config: &IcebergConfig,
        table: Table,
        unique_column_ids: Vec<usize>,
        writer_param: &SinkWriterParam,
    ) -> Result<Self> {
        let SinkWriterParam {
            extra_partition_col_idx,
            actor_id,
            sink_id,
            sink_name,
            ..
        } = writer_param;
        let metrics_labels = [
            &actor_id.to_string(),
            &sink_id.to_string(),
            sink_name.as_str(),
        ];
        let unique_column_ids: Vec<_> = unique_column_ids.into_iter().map(|id| id as i32).collect();

        // Metrics
        let write_qps = GLOBAL_SINK_METRICS
            .iceberg_write_qps
            .with_guarded_label_values(&metrics_labels);
        let write_latency = GLOBAL_SINK_METRICS
            .iceberg_write_latency
            .with_guarded_label_values(&metrics_labels);
        // # TODO
        // Unused. Add this metrics later.
        let _rolling_unflushed_data_file = GLOBAL_SINK_METRICS
            .iceberg_rolling_unflushed_data_file
            .with_guarded_label_values(&metrics_labels);
        let write_bytes = GLOBAL_SINK_METRICS
            .iceberg_write_bytes
            .with_guarded_label_values(&metrics_labels);

        // Determine the schema id and partition spec id
        let schema = table.metadata().current_schema();
        let partition_spec = table.metadata().default_partition_spec();
        let fanout_enabled = !partition_spec.fields().is_empty();
        let use_deletion_vectors = table.metadata().format_version() >= FormatVersion::V3;

        // To avoid duplicate file name, each time the sink created will generate a unique uuid as file name suffix.
        let unique_uuid_suffix = Uuid::now_v7();

        let parquet_writer_properties = build_parquet_writer_properties(config);

        let data_file_builder = build_data_file_writer_builder(
            config,
            &table,
            &writer_param.actor_id.to_string(),
            &unique_uuid_suffix.to_string(),
        )?;
        let position_delete_builder = if use_deletion_vectors {
            let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
            PositionDeleteWriterBuilderType::DeletionVector(DeletionVectorWriterBuilder::new(
                table.file_io().clone(),
                location_generator,
                DefaultFileNameGenerator::new(
                    writer_param.actor_id.to_string(),
                    Some(format!("delvec-{}", unique_uuid_suffix)),
                    iceberg::spec::DataFileFormat::Puffin,
                ),
            ))
        } else {
            let parquet_writer_builder = ParquetWriterBuilder::new(
                parquet_writer_properties.clone(),
                POSITION_DELETE_SCHEMA.clone().into(),
            );
            let rolling_writer_builder = RollingFileWriterBuilder::new(
                parquet_writer_builder,
                (config.target_file_size_mb() * 1024 * 1024) as usize,
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                DefaultFileNameGenerator::new(
                    writer_param.actor_id.to_string(),
                    Some(format!("pos-del-{}", unique_uuid_suffix)),
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            );
            PositionDeleteWriterBuilderType::PositionDelete(PositionDeleteFileWriterBuilder::new(
                rolling_writer_builder,
            ))
        };
        let equality_delete_builder = {
            let eq_del_config = EqualityDeleteWriterConfig::new(
                unique_column_ids.clone(),
                table.metadata().current_schema().clone(),
            )
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
            let parquet_writer_builder = ParquetWriterBuilder::new(
                parquet_writer_properties,
                Arc::new(
                    arrow_schema_to_schema(eq_del_config.projected_arrow_schema_ref())
                        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                ),
            );
            let rolling_writer_builder = RollingFileWriterBuilder::new(
                parquet_writer_builder,
                (config.target_file_size_mb() * 1024 * 1024) as usize,
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                DefaultFileNameGenerator::new(
                    writer_param.actor_id.to_string(),
                    Some(format!("eq-del-{}", unique_uuid_suffix)),
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            );

            EqualityDeleteFileWriterBuilder::new(rolling_writer_builder, eq_del_config)
        };
        let delta_builder = DeltaWriterBuilder::new(
            data_file_builder,
            position_delete_builder,
            equality_delete_builder,
            unique_column_ids,
            schema.clone(),
        );
        let original_arrow_schema = Arc::new(
            schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
        );
        let schema_with_extra_op_column = {
            let mut new_fields = original_arrow_schema.fields().iter().cloned().collect_vec();
            new_fields.push(Arc::new(ArrowField::new(
                "op".to_owned(),
                ArrowDataType::Int32,
                false,
            )));
            Arc::new(ArrowSchema::new(new_fields))
        };
        let writer_builder = TaskWriterBuilderWrapper::new(
            MonitoredGeneralWriterBuilder::new(
                delta_builder,
                write_qps.clone(),
                write_latency.clone(),
            ),
            fanout_enabled,
            schema.clone(),
            partition_spec.clone(),
            true,
        );
        let inner_writer = Some(Box::new(
            writer_builder
                .build()
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
        ) as Box<dyn IcebergWriter>);
        Ok(Self {
            arrow_schema: original_arrow_schema,
            metrics: IcebergWriterMetrics {
                _write_qps: write_qps,
                _write_latency: write_latency,
                write_bytes,
            },
            table,
            writer: IcebergWriterDispatch::Upsert {
                writer: inner_writer,
                writer_builder,
                arrow_schema_with_op_column: schema_with_extra_op_column,
            },
            project_idx_vec: {
                if let Some(extra_partition_col_idx) = extra_partition_col_idx {
                    ProjectIdxVec::Prepare(*extra_partition_col_idx)
                } else {
                    ProjectIdxVec::None
                }
            },
            commit_checkpoint_size_threshold_bytes: config.commit_checkpoint_size_threshold_bytes(),
            uncommitted_write_bytes: 0,
        })
    }
}

#[async_trait]
impl SinkWriter for IcebergSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    /// Begin a new epoch
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        let Self::Created(args) = self else {
            return Ok(());
        };

        let table = create_and_validate_table_impl(&args.config, &args.sink_param).await?;
        let inner = match &args.unique_column_ids {
            Some(unique_column_ids) => IcebergSinkWriterInner::build_upsert(
                &args.config,
                table,
                unique_column_ids.clone(),
                &args.writer_param,
            )?,
            None => {
                IcebergSinkWriterInner::build_append_only(&args.config, table, &args.writer_param)?
            }
        };

        *self = IcebergSinkWriter::Initialized(inner);
        Ok(())
    }

    /// Write a stream chunk to sink
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let Self::Initialized(inner) = self else {
            unreachable!("IcebergSinkWriter should be initialized before barrier");
        };

        // Try to build writer if it's None.
        match &mut inner.writer {
            IcebergWriterDispatch::Append {
                writer,
                writer_builder,
            } => {
                if writer.is_none() {
                    *writer = Some(Box::new(
                        writer_builder
                            .build()
                            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                    ));
                }
            }
            IcebergWriterDispatch::Upsert {
                writer,
                writer_builder,
                ..
            } => {
                if writer.is_none() {
                    *writer = Some(Box::new(
                        writer_builder
                            .build()
                            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                    ));
                }
            }
        };

        // Process the chunk.
        let (mut chunk, ops) = chunk.compact_vis().into_parts();
        match &mut inner.project_idx_vec {
            ProjectIdxVec::None => {}
            ProjectIdxVec::Prepare(idx) => {
                if *idx >= chunk.columns().len() {
                    return Err(SinkError::Iceberg(anyhow!(
                        "invalid extra partition column index {}",
                        idx
                    )));
                }
                let project_idx_vec = (0..*idx)
                    .chain(*idx + 1..chunk.columns().len())
                    .collect_vec();
                chunk = chunk.project(&project_idx_vec);
                inner.project_idx_vec = ProjectIdxVec::Done(project_idx_vec);
            }
            ProjectIdxVec::Done(idx_vec) => {
                chunk = chunk.project(idx_vec);
            }
        }
        if ops.is_empty() {
            return Ok(());
        }
        let write_batch_size = chunk.estimated_heap_size();
        let batch = match &inner.writer {
            IcebergWriterDispatch::Append { .. } => {
                // separate out insert chunk
                let filters =
                    chunk.visibility() & ops.iter().map(|op| *op == Op::Insert).collect::<Bitmap>();
                chunk.set_visibility(filters);
                IcebergArrowConvert
                    .to_record_batch(inner.arrow_schema.clone(), &chunk.compact_vis())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?
            }
            IcebergWriterDispatch::Upsert {
                arrow_schema_with_op_column,
                ..
            } => {
                let chunk = IcebergArrowConvert
                    .to_record_batch(inner.arrow_schema.clone(), &chunk)
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
                let ops = Arc::new(Int32Array::from(
                    ops.iter()
                        .map(|op| match op {
                            Op::UpdateInsert | Op::Insert => INSERT_OP,
                            Op::UpdateDelete | Op::Delete => DELETE_OP,
                        })
                        .collect_vec(),
                ));
                let mut columns = chunk.columns().to_vec();
                columns.push(ops);
                RecordBatch::try_new(arrow_schema_with_op_column.clone(), columns)
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?
            }
        };

        let writer = inner.writer.get_writer().unwrap();
        writer
            .write(batch)
            .instrument_await("iceberg_write")
            .await
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
        inner.metrics.write_bytes.inc_by(write_batch_size as _);
        inner.uncommitted_write_bytes = inner
            .uncommitted_write_bytes
            .saturating_add(write_batch_size as u64);
        Ok(())
    }

    fn should_commit_on_checkpoint(&self) -> bool {
        match self {
            Self::Initialized(inner) => inner.should_commit_on_checkpoint(),
            Self::Created(_) => false,
        }
    }

    /// Receive a barrier and mark the end of current epoch. When `is_checkpoint` is true, the sink
    /// writer should commit the current epoch.
    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        let Self::Initialized(inner) = self else {
            unreachable!("IcebergSinkWriter should be initialized before barrier");
        };

        // Skip it if not checkpoint
        if !is_checkpoint {
            return Ok(None);
        }

        let close_result = match &mut inner.writer {
            IcebergWriterDispatch::Append {
                writer,
                writer_builder,
            } => {
                let close_result = match writer.take() {
                    Some(mut writer) => {
                        Some(writer.close().instrument_await("iceberg_close").await)
                    }
                    _ => None,
                };
                match writer_builder.build() {
                    Ok(new_writer) => {
                        *writer = Some(Box::new(new_writer));
                    }
                    _ => {
                        // In this case, the writer is closed and we can't build a new writer. But we can't return the error
                        // here because current writer may close successfully. So we just log the error.
                        warn!("Failed to build new writer after close");
                    }
                }
                close_result
            }
            IcebergWriterDispatch::Upsert {
                writer,
                writer_builder,
                ..
            } => {
                let close_result = match writer.take() {
                    Some(mut writer) => {
                        Some(writer.close().instrument_await("iceberg_close").await)
                    }
                    _ => None,
                };
                match writer_builder.build() {
                    Ok(new_writer) => {
                        *writer = Some(Box::new(new_writer));
                    }
                    _ => {
                        // In this case, the writer is closed and we can't build a new writer. But we can't return the error
                        // here because current writer may close successfully. So we just log the error.
                        warn!("Failed to build new writer after close");
                    }
                }
                close_result
            }
        };

        match close_result {
            Some(Ok(result)) => {
                inner.uncommitted_write_bytes = 0;
                let format_version = inner.table.metadata().format_version();
                let partition_type = inner.table.metadata().default_partition_type();
                let data_files = result
                    .into_iter()
                    .map(|f| {
                        // Truncate large column statistics BEFORE serialization
                        let truncated = truncate_datafile(f);
                        SerializedDataFile::try_from(truncated, partition_type, format_version)
                            .map_err(|err| SinkError::Iceberg(anyhow!(err)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Some(SinkMetadata::try_from(&IcebergCommitResult {
                    data_files,
                    schema_id: inner.table.metadata().current_schema_id(),
                    partition_spec_id: inner.table.metadata().default_partition_spec_id(),
                })?))
            }
            Some(Err(err)) => Err(SinkError::Iceberg(anyhow!(err))),
            None => Err(SinkError::Iceberg(anyhow!("No writer to close"))),
        }
    }
}

const SCHEMA_ID: &str = "schema_id";
const PARTITION_SPEC_ID: &str = "partition_spec_id";
const DATA_FILES: &str = "data_files";

#[derive(Default, Clone)]
struct IcebergCommitResult {
    schema_id: i32,
    partition_spec_id: i32,
    data_files: Vec<SerializedDataFile>,
}

impl IcebergCommitResult {
    fn try_from(value: &SinkMetadata) -> Result<Self> {
        if let Some(Serialized(v)) = &value.metadata {
            let mut values = if let serde_json::Value::Object(v) =
                serde_json::from_slice::<serde_json::Value>(&v.metadata)
                    .context("Can't parse iceberg sink metadata")?
            {
                v
            } else {
                bail!("iceberg sink metadata should be an object");
            };

            let schema_id;
            if let Some(serde_json::Value::Number(value)) = values.remove(SCHEMA_ID) {
                schema_id = value
                    .as_u64()
                    .ok_or_else(|| anyhow!("schema_id should be a u64"))?;
            } else {
                bail!("iceberg sink metadata should have schema_id");
            }

            let partition_spec_id;
            if let Some(serde_json::Value::Number(value)) = values.remove(PARTITION_SPEC_ID) {
                partition_spec_id = value
                    .as_u64()
                    .ok_or_else(|| anyhow!("partition_spec_id should be a u64"))?;
            } else {
                bail!("iceberg sink metadata should have partition_spec_id");
            }

            let data_files: Vec<SerializedDataFile>;
            if let serde_json::Value::Array(values) = values
                .remove(DATA_FILES)
                .ok_or_else(|| anyhow!("iceberg sink metadata should have data_files object"))?
            {
                data_files = values
                    .into_iter()
                    .map(from_value::<SerializedDataFile>)
                    .collect::<std::result::Result<_, _>>()
                    .unwrap();
            } else {
                bail!("iceberg sink metadata should have data_files object");
            }

            Ok(Self {
                schema_id: schema_id as i32,
                partition_spec_id: partition_spec_id as i32,
                data_files,
            })
        } else {
            bail!("Can't create iceberg sink write result from empty data!")
        }
    }

    fn try_from_serialized_bytes(value: Vec<u8>) -> Result<Self> {
        let mut values = if let serde_json::Value::Object(value) =
            serde_json::from_slice::<serde_json::Value>(&value)
                .context("Can't parse iceberg sink metadata")?
        {
            value
        } else {
            bail!("iceberg sink metadata should be an object");
        };

        let schema_id;
        if let Some(serde_json::Value::Number(value)) = values.remove(SCHEMA_ID) {
            schema_id = value
                .as_u64()
                .ok_or_else(|| anyhow!("schema_id should be a u64"))?;
        } else {
            bail!("iceberg sink metadata should have schema_id");
        }

        let partition_spec_id;
        if let Some(serde_json::Value::Number(value)) = values.remove(PARTITION_SPEC_ID) {
            partition_spec_id = value
                .as_u64()
                .ok_or_else(|| anyhow!("partition_spec_id should be a u64"))?;
        } else {
            bail!("iceberg sink metadata should have partition_spec_id");
        }

        let data_files: Vec<SerializedDataFile>;
        if let serde_json::Value::Array(values) = values
            .remove(DATA_FILES)
            .ok_or_else(|| anyhow!("iceberg sink metadata should have data_files object"))?
        {
            data_files = values
                .into_iter()
                .map(from_value::<SerializedDataFile>)
                .collect::<std::result::Result<_, _>>()
                .unwrap();
        } else {
            bail!("iceberg sink metadata should have data_files object");
        }

        Ok(Self {
            schema_id: schema_id as i32,
            partition_spec_id: partition_spec_id as i32,
            data_files,
        })
    }
}

impl<'a> TryFrom<&'a IcebergCommitResult> for SinkMetadata {
    type Error = SinkError;

    fn try_from(value: &'a IcebergCommitResult) -> std::result::Result<SinkMetadata, Self::Error> {
        let json_data_files = serde_json::Value::Array(
            value
                .data_files
                .iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<serde_json::Value>, _>>()
                .context("Can't serialize data files to json")?,
        );
        let json_value = serde_json::Value::Object(
            vec![
                (
                    SCHEMA_ID.to_owned(),
                    serde_json::Value::Number(value.schema_id.into()),
                ),
                (
                    PARTITION_SPEC_ID.to_owned(),
                    serde_json::Value::Number(value.partition_spec_id.into()),
                ),
                (DATA_FILES.to_owned(), json_data_files),
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

impl TryFrom<IcebergCommitResult> for Vec<u8> {
    type Error = SinkError;

    fn try_from(value: IcebergCommitResult) -> std::result::Result<Vec<u8>, Self::Error> {
        let json_data_files = serde_json::Value::Array(
            value
                .data_files
                .iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<serde_json::Value>, _>>()
                .context("Can't serialize data files to json")?,
        );
        let json_value = serde_json::Value::Object(
            vec![
                (
                    SCHEMA_ID.to_owned(),
                    serde_json::Value::Number(value.schema_id.into()),
                ),
                (
                    PARTITION_SPEC_ID.to_owned(),
                    serde_json::Value::Number(value.partition_spec_id.into()),
                ),
                (DATA_FILES.to_owned(), json_data_files),
            ]
            .into_iter()
            .collect(),
        );
        Ok(serde_json::to_vec(&json_value).context("Can't serialize iceberg sink metadata")?)
    }
}
pub struct IcebergSinkCommitter {
    catalog: Arc<dyn Catalog>,
    table: Table,
    pub last_commit_epoch: u64,
    pub(crate) sink_id: SinkId,
    pub(crate) config: IcebergConfig,
    pub(crate) param: SinkParam,
    commit_retry_num: u32,
    pub(crate) iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
}

impl IcebergSinkCommitter {
    // Reload table and guarantee current schema_id and partition_spec_id matches
    // given `schema_id` and `partition_spec_id`
    async fn reload_table(
        catalog: &dyn Catalog,
        table_ident: &TableIdent,
        schema_id: i32,
        partition_spec_id: i32,
    ) -> Result<Table> {
        let table = catalog
            .load_table(table_ident)
            .await
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
        if table.metadata().current_schema_id() != schema_id {
            return Err(SinkError::Iceberg(anyhow!(
                "Schema evolution not supported, expect schema id {}, but got {}",
                schema_id,
                table.metadata().current_schema_id()
            )));
        }
        if table.metadata().default_partition_spec_id() != partition_spec_id {
            return Err(SinkError::Iceberg(anyhow!(
                "Partition evolution not supported, expect partition spec id {}, but got {}",
                partition_spec_id,
                table.metadata().default_partition_spec_id()
            )));
        }
        Ok(table)
    }
}

#[async_trait]
impl SinglePhaseCommitCoordinator for IcebergSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!(
            sink_id = %self.param.sink_id,
            "Iceberg sink coordinator initialized",
        );

        Ok(())
    }

    async fn commit_data(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        tracing::debug!("Starting iceberg direct commit in epoch {epoch}");

        if metadata.is_empty() {
            tracing::debug!(?epoch, "No datafile to commit");
            return Ok(());
        }

        // Commit data if present
        if let Some((write_results, snapshot_id)) = self.pre_commit_inner(epoch, metadata)? {
            self.commit_data_impl(epoch, write_results, snapshot_id)
                .await?;
        }

        Ok(())
    }

    async fn commit_schema_change(
        &mut self,
        epoch: u64,
        schema_change: PbSinkSchemaChange,
    ) -> Result<()> {
        tracing::info!(
            "Committing schema change {:?} in epoch {}",
            schema_change,
            epoch
        );
        self.commit_schema_change_impl(schema_change).await?;
        tracing::info!("Successfully committed schema change in epoch {}", epoch);

        Ok(())
    }
}

#[async_trait]
impl TwoPhaseCommitCoordinator for IcebergSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!(
            sink_id = %self.param.sink_id,
            "Iceberg sink coordinator initialized",
        );

        Ok(())
    }

    async fn pre_commit(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
        _schema_change: Option<PbSinkSchemaChange>,
    ) -> Result<Option<Vec<u8>>> {
        tracing::debug!("Starting iceberg pre commit in epoch {epoch}");

        let (write_results, snapshot_id) = match self.pre_commit_inner(epoch, metadata)? {
            Some((write_results, snapshot_id)) => (write_results, snapshot_id),
            None => {
                tracing::debug!(?epoch, "no data to pre commit");
                return Ok(None);
            }
        };

        let mut write_results_bytes = Vec::new();
        for each_parallelism_write_result in write_results {
            let each_parallelism_write_result_bytes: Vec<u8> =
                each_parallelism_write_result.try_into()?;
            write_results_bytes.push(each_parallelism_write_result_bytes);
        }

        let snapshot_id_bytes: Vec<u8> = snapshot_id.to_le_bytes().to_vec();
        write_results_bytes.push(snapshot_id_bytes);

        let pre_commit_metadata_bytes: Vec<u8> = serialize_metadata(write_results_bytes);
        Ok(Some(pre_commit_metadata_bytes))
    }

    async fn commit_data(&mut self, epoch: u64, commit_metadata: Vec<u8>) -> Result<()> {
        tracing::debug!("Starting iceberg commit in epoch {epoch}");

        if commit_metadata.is_empty() {
            tracing::debug!(?epoch, "No datafile to commit");
            return Ok(());
        }

        // Deserialize commit metadata
        let mut payload = deserialize_metadata(commit_metadata);
        if payload.is_empty() {
            return Err(SinkError::Iceberg(anyhow!(
                "Invalid commit metadata: empty payload"
            )));
        }

        // Last element is snapshot_id
        let snapshot_id_bytes = payload.pop().ok_or_else(|| {
            SinkError::Iceberg(anyhow!("Invalid commit metadata: missing snapshot_id"))
        })?;
        let snapshot_id = i64::from_le_bytes(
            snapshot_id_bytes
                .try_into()
                .map_err(|_| SinkError::Iceberg(anyhow!("Invalid snapshot id bytes")))?,
        );

        // Remaining elements are write_results
        let write_results = payload
            .into_iter()
            .map(IcebergCommitResult::try_from_serialized_bytes)
            .collect::<Result<Vec<_>>>()?;

        let snapshot_committed = is_snapshot_committed(&self.config, snapshot_id).await?;
        if snapshot_committed {
            tracing::info!(
                "Snapshot id {} already committed in iceberg table, skip committing again.",
                snapshot_id
            );
            return Ok(());
        }

        self.commit_data_impl(epoch, write_results, snapshot_id)
            .await
    }

    async fn commit_schema_change(
        &mut self,
        epoch: u64,
        schema_change: PbSinkSchemaChange,
    ) -> Result<()> {
        let schema_updated = self.check_schema_change_applied(&schema_change)?;
        if schema_updated {
            tracing::info!("Schema change already committed in epoch {}, skip", epoch);
            return Ok(());
        }

        tracing::info!(
            "Committing schema change {:?} in epoch {}",
            schema_change,
            epoch
        );
        self.commit_schema_change_impl(schema_change).await?;
        tracing::info!("Successfully committed schema change in epoch {epoch}");

        Ok(())
    }

    async fn abort(&mut self, _epoch: u64, _commit_metadata: Vec<u8>) {
        // TODO: Files that have been written but not committed should be deleted.
        tracing::debug!("Abort not implemented yet");
    }
}

/// Methods Required to Achieve Exactly Once Semantics
impl IcebergSinkCommitter {
    fn pre_commit_inner(
        &mut self,
        _epoch: u64,
        metadata: Vec<SinkMetadata>,
    ) -> Result<Option<(Vec<IcebergCommitResult>, i64)>> {
        let write_results: Vec<IcebergCommitResult> = metadata
            .iter()
            .map(IcebergCommitResult::try_from)
            .collect::<Result<Vec<IcebergCommitResult>>>()?;

        // Skip if no data to commit
        if write_results.is_empty() || write_results.iter().all(|r| r.data_files.is_empty()) {
            return Ok(None);
        }

        let expect_schema_id = write_results[0].schema_id;
        let expect_partition_spec_id = write_results[0].partition_spec_id;

        // guarantee that all write results has same schema_id and partition_spec_id
        if write_results
            .iter()
            .any(|r| r.schema_id != expect_schema_id)
            || write_results
                .iter()
                .any(|r| r.partition_spec_id != expect_partition_spec_id)
        {
            return Err(SinkError::Iceberg(anyhow!(
                "schema_id and partition_spec_id should be the same in all write results"
            )));
        }

        let snapshot_id = FastAppendAction::generate_snapshot_id(&self.table);

        Ok(Some((write_results, snapshot_id)))
    }

    async fn commit_data_impl(
        &mut self,
        epoch: u64,
        write_results: Vec<IcebergCommitResult>,
        snapshot_id: i64,
    ) -> Result<()> {
        // Empty write results should be handled before calling this function.
        assert!(
            !write_results.is_empty() && !write_results.iter().all(|r| r.data_files.is_empty())
        );

        // Check snapshot limit before proceeding with commit
        self.wait_for_snapshot_limit().await?;

        let expect_schema_id = write_results[0].schema_id;
        let expect_partition_spec_id = write_results[0].partition_spec_id;

        // Load the latest table to avoid concurrent modification with the best effort.
        self.table = Self::reload_table(
            self.catalog.as_ref(),
            self.table.identifier(),
            expect_schema_id,
            expect_partition_spec_id,
        )
        .await?;

        let Some(schema) = self.table.metadata().schema_by_id(expect_schema_id) else {
            return Err(SinkError::Iceberg(anyhow!(
                "Can't find schema by id {}",
                expect_schema_id
            )));
        };
        let Some(partition_spec) = self
            .table
            .metadata()
            .partition_spec_by_id(expect_partition_spec_id)
        else {
            return Err(SinkError::Iceberg(anyhow!(
                "Can't find partition spec by id {}",
                expect_partition_spec_id
            )));
        };
        let partition_type = partition_spec
            .as_ref()
            .clone()
            .partition_type(schema)
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

        let data_files = write_results
            .into_iter()
            .flat_map(|r| {
                r.data_files.into_iter().map(|f| {
                    f.try_into(expect_partition_spec_id, &partition_type, schema)
                        .map_err(|err| SinkError::Iceberg(anyhow!(err)))
                })
            })
            .collect::<Result<Vec<DataFile>>>()?;

        let table = fast_append_with_retry(
            self.catalog.clone(),
            self.table.identifier(),
            data_files,
            snapshot_id,
            &commit_branch(self.config.r#type.as_str(), self.config.write_mode),
            Some(expect_schema_id),
            Some(expect_partition_spec_id),
            self.commit_retry_num,
        )
        .await?;
        self.table = table;

        let snapshot_num = self.table.metadata().snapshots().count();
        let catalog_name = self.config.common.catalog_name();
        let table_name = self.table.identifier().to_string();
        let metrics_labels = [&self.param.sink_name, &catalog_name, &table_name];
        GLOBAL_SINK_METRICS
            .iceberg_snapshot_num
            .with_guarded_label_values(&metrics_labels)
            .set(snapshot_num as i64);

        tracing::debug!("Succeeded to commit to iceberg table in epoch {epoch}.");

        if let Some(iceberg_compact_stat_sender) = &self.iceberg_compact_stat_sender
            && self.config.enable_compaction
            && iceberg_compact_stat_sender
                .send(IcebergSinkCompactionUpdate {
                    sink_id: self.sink_id,
                    compaction_interval: self.config.compaction_interval_sec(),
                    force_compaction: false,
                })
                .is_err()
        {
            warn!("failed to send iceberg compaction stats");
        }

        Ok(())
    }

    /// Check if the specified columns already exist in the iceberg table's current schema.
    /// This is used to determine if schema change has already been applied.
    fn check_schema_change_applied(&self, schema_change: &PbSinkSchemaChange) -> Result<bool> {
        let current_schema = self.table.metadata().current_schema();
        let current_arrow_schema = schema_to_arrow_schema(current_schema.as_ref())
            .context("Failed to convert schema")
            .map_err(SinkError::Iceberg)?;

        let iceberg_arrow_convert = IcebergArrowConvert;

        let schema_matches = |expected: &[ArrowField]| {
            if current_arrow_schema.fields().len() != expected.len() {
                return false;
            }

            expected.iter().all(|expected_field| {
                current_arrow_schema.fields().iter().any(|current_field| {
                    current_field.name() == expected_field.name()
                        && current_field.data_type() == expected_field.data_type()
                })
            })
        };

        let original_arrow_fields: Vec<ArrowField> = schema_change
            .original_schema
            .iter()
            .map(|pb_field| {
                let field = Field::from(pb_field);
                iceberg_arrow_convert
                    .to_arrow_field(&field.name, &field.data_type)
                    .context("Failed to convert field to arrow")
                    .map_err(SinkError::Iceberg)
            })
            .collect::<Result<_>>()?;

        // If current schema equals original_schema, then schema change is NOT applied.
        if schema_matches(&original_arrow_fields) {
            tracing::debug!(
                "Current iceberg schema matches original_schema ({} columns); schema change not applied",
                original_arrow_fields.len()
            );
            return Ok(false);
        }

        // We only support add_columns for now.
        let Some(risingwave_pb::stream_plan::sink_schema_change::Op::AddColumns(add_columns_op)) =
            schema_change.op.as_ref()
        else {
            return Err(SinkError::Iceberg(anyhow!(
                "Unsupported sink schema change op in iceberg sink: {:?}",
                schema_change.op
            )));
        };

        let add_arrow_fields: Vec<ArrowField> = add_columns_op
            .fields
            .iter()
            .map(|pb_field| {
                let field = Field::from(pb_field);
                iceberg_arrow_convert
                    .to_arrow_field(&field.name, &field.data_type)
                    .context("Failed to convert field to arrow")
                    .map_err(SinkError::Iceberg)
            })
            .collect::<Result<_>>()?;

        let mut expected_after_change = original_arrow_fields;
        expected_after_change.extend(add_arrow_fields);

        // If current schema equals original_schema + add_columns, then schema change is applied.
        if schema_matches(&expected_after_change) {
            tracing::debug!(
                "Current iceberg schema matches original_schema + add_columns ({} columns); schema change already applied",
                expected_after_change.len()
            );
            return Ok(true);
        }

        Err(SinkError::Iceberg(anyhow!(
            "Current iceberg schema does not match either original_schema ({} cols) or original_schema + add_columns; cannot determine whether schema change is applied",
            schema_change.original_schema.len()
        )))
    }

    /// Commit schema changes (e.g., add columns) to the iceberg table.
    /// This function uses Transaction API to atomically update the table schema
    /// with optimistic locking to prevent concurrent conflicts.
    async fn commit_schema_change_impl(&mut self, schema_change: PbSinkSchemaChange) -> Result<()> {
        use iceberg::spec::NestedField;

        let Some(risingwave_pb::stream_plan::sink_schema_change::Op::AddColumns(add_columns_op)) =
            schema_change.op.as_ref()
        else {
            return Err(SinkError::Iceberg(anyhow!(
                "Unsupported sink schema change op in iceberg sink: {:?}",
                schema_change.op
            )));
        };

        let add_columns = add_columns_op.fields.iter().map(Field::from).collect_vec();

        // Step 1: Get current table metadata
        let metadata = self.table.metadata();
        let mut next_field_id = metadata.last_column_id() + 1;
        tracing::debug!("Starting schema change, next_field_id: {}", next_field_id);

        // Step 2: Build new fields to add
        let iceberg_create_table_arrow_convert = IcebergCreateTableArrowConvert::default();
        let mut new_fields = Vec::new();

        for field in &add_columns {
            // Convert RisingWave Field to Arrow Field using IcebergCreateTableArrowConvert
            let arrow_field = iceberg_create_table_arrow_convert
                .to_arrow_field(&field.name, &field.data_type)
                .with_context(|| format!("Failed to convert field '{}' to arrow", field.name))
                .map_err(SinkError::Iceberg)?;

            // Convert Arrow DataType to Iceberg Type
            let iceberg_type = iceberg::arrow::arrow_type_to_type(arrow_field.data_type())
                .map_err(|err| {
                    SinkError::Iceberg(
                        anyhow!(err).context("Failed to convert Arrow type to Iceberg type"),
                    )
                })?;

            // Create NestedField with the next available field ID
            let nested_field = Arc::new(NestedField::optional(
                next_field_id,
                &field.name,
                iceberg_type,
            ));

            new_fields.push(nested_field);
            tracing::info!("Prepared field '{}' with ID {}", field.name, next_field_id);
            next_field_id += 1;
        }

        // Step 3: Create Transaction with UpdateSchemaAction
        tracing::info!(
            "Committing schema change to catalog for table {}",
            self.table.identifier()
        );

        let txn = Transaction::new(&self.table);
        let action = txn.update_schema().add_fields(new_fields);

        let updated_table = action
            .apply(txn)
            .context("Failed to apply schema update action")
            .map_err(SinkError::Iceberg)?
            .commit(self.catalog.as_ref())
            .await
            .context("Failed to commit table schema change")
            .map_err(SinkError::Iceberg)?;

        self.table = updated_table;

        tracing::info!(
            "Successfully committed schema change, added {} columns to iceberg table",
            add_columns.len()
        );

        Ok(())
    }

    /// Check if the number of snapshots since the last rewrite/overwrite operation exceeds the limit
    /// Returns the number of snapshots since the last rewrite/overwrite
    fn count_snapshots_since_rewrite(&self) -> usize {
        let mut snapshots: Vec<_> = self.table.metadata().snapshots().collect();
        snapshots.sort_by_key(|b| std::cmp::Reverse(b.timestamp_ms()));

        // Iterate through snapshots in reverse order (newest first) to find the last rewrite/overwrite
        let mut count = 0;
        for snapshot in snapshots {
            // Check if this snapshot represents a rewrite or overwrite operation
            let summary = snapshot.summary();
            match &summary.operation {
                Operation::Replace => {
                    // Found a rewrite/overwrite operation, stop counting
                    break;
                }

                _ => {
                    // Increment count for each snapshot that is not a rewrite/overwrite
                    count += 1;
                }
            }
        }

        count
    }

    /// Wait until snapshot count since last rewrite is below the limit
    async fn wait_for_snapshot_limit(&mut self) -> Result<()> {
        if !self.config.enable_compaction {
            return Ok(());
        }

        if let Some(max_snapshots) = self.config.max_snapshots_num_before_compaction {
            loop {
                let current_count = self.count_snapshots_since_rewrite();

                if current_count < max_snapshots {
                    tracing::info!(
                        "Snapshot count check passed: {} < {}",
                        current_count,
                        max_snapshots
                    );
                    break;
                }

                tracing::info!(
                    "Snapshot count {} exceeds limit {}, waiting...",
                    current_count,
                    max_snapshots
                );

                if let Some(iceberg_compact_stat_sender) = &self.iceberg_compact_stat_sender
                    && iceberg_compact_stat_sender
                        .send(IcebergSinkCompactionUpdate {
                            sink_id: self.sink_id,
                            compaction_interval: self.config.compaction_interval_sec(),
                            force_compaction: true,
                        })
                        .is_err()
                {
                    tracing::warn!("failed to send iceberg compaction stats");
                }

                // Wait for 30 seconds before checking again
                tokio::time::sleep(Duration::from_secs(30)).await;

                // Reload table to get latest snapshots
                self.table = self.config.load_table().await?;
            }
        }
        Ok(())
    }
}

fn serialize_metadata(metadata: Vec<Vec<u8>>) -> Vec<u8> {
    serde_json::to_vec(&metadata).unwrap()
}

fn deserialize_metadata(bytes: Vec<u8>) -> Vec<Vec<u8>> {
    serde_json::from_slice(&bytes).unwrap()
}

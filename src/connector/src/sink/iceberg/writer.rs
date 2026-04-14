// Copyright 2026 RisingWave Labs
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

use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use iceberg::arrow::{
    RecordBatchPartitionSplitter, arrow_schema_to_schema, schema_to_arrow_schema,
};
use iceberg::spec::{
    DataFile, FormatVersion, PartitionSpecRef, SchemaRef as IcebergSchemaRef, SerializedDataFile,
};
use iceberg::table::Table;
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
use itertools::Itertools;
use parquet::file::properties::WriterProperties;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::arrow::arrow_array_iceberg::{Int32Array, RecordBatch};
use risingwave_common::array::arrow::arrow_schema_iceberg::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef,
};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntCounter};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::connector_service::SinkMetadata;
use uuid::Uuid;

use super::prometheus::monitored_general_writer::MonitoredGeneralWriterBuilder;
use super::{
    GLOBAL_SINK_METRICS, IcebergCommitResult, IcebergConfig, PARQUET_CREATED_BY, SinkError,
    SinkWriterParam, create_and_validate_table_impl,
};
use crate::sink::writer::SinkWriter;
use crate::sink::{Result, SinkParam};

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

        let parquet_writer_properties = WriterProperties::builder()
            .set_compression(config.get_parquet_compression())
            .set_max_row_group_size(config.write_parquet_max_row_group_rows())
            .set_created_by(PARQUET_CREATED_BY.to_owned())
            .build();

        let parquet_writer_builder =
            ParquetWriterBuilder::new(parquet_writer_properties, schema.clone());
        let rolling_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            (config.target_file_size_mb() * 1024 * 1024) as usize,
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata().clone())
                .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
            DefaultFileNameGenerator::new(
                writer_param.actor_id.to_string(),
                Some(unique_uuid_suffix.to_string()),
                iceberg::spec::DataFileFormat::Parquet,
            ),
        );
        let data_file_builder = DataFileWriterBuilder::new(rolling_builder);
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

        let parquet_writer_properties = WriterProperties::builder()
            .set_compression(config.get_parquet_compression())
            .set_max_row_group_size(config.write_parquet_max_row_group_rows())
            .set_created_by(PARQUET_CREATED_BY.to_owned())
            .build();

        let data_file_builder = {
            let parquet_writer_builder =
                ParquetWriterBuilder::new(parquet_writer_properties.clone(), schema.clone());
            let rolling_writer_builder = RollingFileWriterBuilder::new(
                parquet_writer_builder,
                (config.target_file_size_mb() * 1024 * 1024) as usize,
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone())
                    .map_err(|err| SinkError::Iceberg(anyhow!(err)))?,
                DefaultFileNameGenerator::new(
                    writer_param.actor_id.to_string(),
                    Some(unique_uuid_suffix.to_string()),
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            );
            DataFileWriterBuilder::new(rolling_writer_builder)
        };
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
                        tracing::warn!("Failed to build new writer after close");
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
                        tracing::warn!("Failed to build new writer after close");
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

/// Maximum size for column statistics (min/max values) in bytes.
/// Column statistics larger than this will be truncated to avoid metadata bloat.
/// This is especially important for large fields like JSONB, TEXT, BINARY, etc.
///
/// Fix for large column statistics in `DataFile` metadata that can cause OOM errors.
/// We truncate at the `DataFile` level (before serialization) by directly modifying
/// the public `lower_bounds` and `upper_bounds` fields.
///
/// This prevents metadata from ballooning to gigabytes when dealing with large
/// JSONB, TEXT, or BINARY fields, while still preserving statistics for small fields
/// that benefit from query optimization.
const MAX_COLUMN_STAT_SIZE: usize = 10240; // 10KB

/// Truncate large column statistics from `DataFile` BEFORE serialization.
///
/// This function directly modifies `DataFile`'s `lower_bounds` and `upper_bounds`
/// to remove entries that exceed `MAX_COLUMN_STAT_SIZE`.
///
/// # Arguments
/// * `data_file` - A `DataFile` to process
///
/// # Returns
/// The modified `DataFile` with large statistics truncated
fn truncate_datafile(mut data_file: DataFile) -> DataFile {
    // Process lower_bounds - remove entries with large values
    data_file.lower_bounds.retain(|field_id, datum| {
        // Use to_bytes() to get the actual binary size without JSON serialization overhead
        let size = match datum.to_bytes() {
            Ok(bytes) => bytes.len(),
            Err(_) => 0,
        };

        if size > MAX_COLUMN_STAT_SIZE {
            tracing::debug!(
                field_id = field_id,
                size = size,
                "Truncating large lower_bound statistic"
            );
            return false;
        }
        true
    });

    // Process upper_bounds - remove entries with large values
    data_file.upper_bounds.retain(|field_id, datum| {
        // Use to_bytes() to get the actual binary size without JSON serialization overhead
        let size = match datum.to_bytes() {
            Ok(bytes) => bytes.len(),
            Err(_) => 0,
        };

        if size > MAX_COLUMN_STAT_SIZE {
            tracing::debug!(
                field_id = field_id,
                size = size,
                "Truncating large upper_bound statistic"
            );
            return false;
        }
        true
    });

    data_file
}

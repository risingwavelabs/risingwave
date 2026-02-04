// Copyright 2025 RisingWave Labs
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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, LazyLock};

use derive_builder::Builder;
use iceberg::expr::BoundPredicate;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataContentType, DataFileFormat, DataFile, Schema, SerializedDataFile, StructType};
use iceberg::{Catalog, TableIdent};
use iceberg_compaction_core::compaction::{
    CommitManagerRetryConfig, CompactionBuilder, CompactionPlan, CompactionPlanner,
};
use iceberg_compaction_core::config::{
    CompactionExecutionConfigBuilder, CompactionPlanningConfig, FilesWithDeletesConfigBuilder,
    FullCompactionConfigBuilder, GroupFilters, SmallFilesConfigBuilder,
};
use iceberg_compaction_core::executor::RewriteFilesStat;
use iceberg_compaction_core::file_selection::strategy::FileGroup;
use mixtrics::registry::prometheus::PrometheusMetricsRegistry;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use risingwave_common::config::storage::default::storage::{
    iceberg_compaction_enable_dynamic_size_estimation,
    iceberg_compaction_enable_heuristic_output_parallelism,
    iceberg_compaction_max_concurrent_closes, iceberg_compaction_size_estimation_smoothing_factor,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_connector::sink::iceberg::{IcebergConfig, IcebergWriteMode, commit_branch};
use risingwave_pb::iceberg_compaction::file_scan_task::{FileContent, FileFormat};
use risingwave_pb::iceberg_compaction::iceberg_compaction_task::TaskType;
use risingwave_pb::iceberg_compaction::{
    FileGroup as PbFileGroup, FileScanTask as PbFileScanTask, IcebergCompactionTask, Plan as PbPlan,
};
use serde_json;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;

use super::IcebergTaskMeta;
use crate::hummock::{HummockError, HummockResult};
use crate::monitor::CompactorMetrics;

pub async fn create_plan_runner_from_plan(
    task_id: u64,
    plan_index: usize,
    plan: PbPlan,
    config: IcebergCompactorRunnerConfig,
    metrics: Arc<CompactorMetrics>,
) -> HummockResult<IcebergCompactionPlanRunner> {
    let iceberg_config = IcebergConfig::from_btreemap(BTreeMap::from_iter(plan.props.into_iter()))
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
    let catalog = iceberg_config
        .create_catalog()
        .await
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
    let table_ident = iceberg_config
        .full_table_name()
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    let file_group = decode_file_group(
        plan.file_group
            .ok_or_else(|| HummockError::compaction_executor("Missing file group in plan"))?,
    )?;
    let compaction_plan = CompactionPlan {
        file_group,
        to_branch: plan.to_branch.into(),
        snapshot_id: plan.snapshot_id,
    };

    let task_type = TaskType::try_from(plan.task_type)
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    Ok(IcebergCompactionPlanRunner {
        task_id,
        plan_index,
        catalog,
        table_ident,
        iceberg_config,
        config,
        metrics,
        task_type,
        branch: compaction_plan.to_branch.clone().into_owned(),
        compaction_plan,
    })
}

fn decode_file_group(file_group: PbFileGroup) -> HummockResult<FileGroup> {
    let data_files = file_group
        .data_files
        .iter()
        .map(decode_file_scan_task)
        .collect::<HummockResult<Vec<_>>>()?;
    let position_delete_files = file_group
        .position_delete_files
        .iter()
        .map(decode_file_scan_task)
        .collect::<HummockResult<Vec<_>>>()?;
    let equality_delete_files = file_group
        .equality_delete_files
        .iter()
        .map(decode_file_scan_task)
        .collect::<HummockResult<Vec<_>>>()?;

    let total_size = data_files.iter().map(|task| task.length).sum();
    let data_file_count = data_files.len();

    Ok(FileGroup {
        data_files,
        position_delete_files,
        equality_delete_files,
        total_size,
        data_file_count,
        executor_parallelism: file_group.executor_parallelism as usize,
        output_parallelism: file_group.output_parallelism as usize,
    })
}

fn decode_file_scan_task(task: &PbFileScanTask) -> HummockResult<FileScanTask> {
    let schema: Schema = serde_json::from_slice(&task.schema_json)
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
    let predicate: Option<BoundPredicate> = if task.has_predicate {
        Some(
            serde_json::from_slice(&task.predicate_json)
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?,
        )
    } else {
        None
    };
    let equality_ids = if task.has_equality_ids {
        Some(task.equality_ids.clone())
    } else {
        None
    };

    Ok(FileScanTask {
        start: task.start,
        length: task.length,
        record_count: task.record_count,
        data_file_path: task.data_file_path.clone(),
        data_file_content: decode_content_type(task.data_file_content),
        data_file_format: decode_file_format(task.data_file_format),
        schema: Arc::new(schema),
        project_field_ids: task.project_field_ids.clone(),
        predicate,
        sequence_number: task.sequence_number,
        equality_ids,
        file_size_in_bytes: task.file_size_in_bytes,
        deletes: Vec::new(),
        partition: None,
        partition_spec: None,
        name_mapping: None,
    })
}

fn decode_content_type(content: i32) -> DataContentType {
    match FileContent::try_from(content).ok() {
        Some(FileContent::Data) => DataContentType::Data,
        Some(FileContent::PositionDeletes) => DataContentType::PositionDeletes,
        Some(FileContent::EqualityDeletes) => DataContentType::EqualityDeletes,
        _ => DataContentType::Data,
    }
}

fn decode_file_format(format: i32) -> DataFileFormat {
    match FileFormat::try_from(format).ok() {
        Some(FileFormat::Parquet) => DataFileFormat::Parquet,
        Some(FileFormat::Avro) => DataFileFormat::Avro,
        Some(FileFormat::Orc) => DataFileFormat::Orc,
        Some(FileFormat::Puffin) => DataFileFormat::Puffin,
        _ => DataFileFormat::Parquet,
    }
}

static ICEBERG_COMPACTION_METRICS_REGISTRY: LazyLock<Box<PrometheusMetricsRegistry>> =
    LazyLock::new(|| {
        Box::new(PrometheusMetricsRegistry::new(
            GLOBAL_METRICS_REGISTRY.clone(),
        ))
    });

pub fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by(concat!("risingwave version ", env!("CARGO_PKG_VERSION")).to_owned())
        .build()
}

#[derive(Builder, Debug, Clone)]
pub struct IcebergCompactorRunnerConfig {
    #[builder(default = "4")]
    pub max_parallelism: u32,
    #[builder(default = "1024 * 1024 * 1024")] // 1GB"
    pub min_size_per_partition: u64,
    #[builder(default = "32")]
    pub max_file_count_per_partition: u32,
    #[builder(default = "false")]
    pub enable_validate_compaction: bool,
    #[builder(default = "1024")]
    pub max_record_batch_rows: usize,
    #[builder(default = "default_writer_properties()")]
    pub write_parquet_properties: WriterProperties,
    #[builder(default = "iceberg_compaction_enable_heuristic_output_parallelism()")]
    pub enable_heuristic_output_parallelism: bool,
    #[builder(default = "iceberg_compaction_max_concurrent_closes()")]
    pub max_concurrent_closes: usize,
    #[builder(default = "iceberg_compaction_enable_dynamic_size_estimation()")]
    pub enable_dynamic_size_estimation: bool,
    #[builder(default = "iceberg_compaction_size_estimation_smoothing_factor()")]
    pub size_estimation_smoothing_factor: f64,
    #[builder]
    pub target_binpack_group_size_mb: Option<u64>,
    #[builder]
    pub min_group_size_mb: Option<u64>,
    #[builder]
    pub min_group_file_count: Option<usize>,
}

pub struct IcebergCompactionTaskStatistics {
    pub total_data_file_size: u64,
    pub total_data_file_count: u32,
    pub total_pos_del_file_size: u64,
    pub total_pos_del_file_count: u32,
    pub total_eq_del_file_size: u64,
    pub total_eq_del_file_count: u32,
}

#[derive(Debug, Clone)]
pub struct SerializedDataFileInfo {
    pub json: Vec<u8>,
    pub partition_spec_id: i32,
}

#[derive(Debug, Clone)]
pub struct PlanExecutionOutput {
    pub added_data_files: Vec<SerializedDataFileInfo>,
    pub stats: RewriteFilesStat,
}

impl Debug for IcebergCompactionTaskStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCompactionTaskStatistics")
            .field("total_data_file_size", &self.total_data_file_size)
            .field("total_data_file_count", &self.total_data_file_count)
            .field("total_pos_del_file_size", &self.total_pos_del_file_size)
            .field("total_pos_del_file_count", &self.total_pos_del_file_count)
            .field("total_eq_del_file_size", &self.total_eq_del_file_size)
            .field("total_eq_del_file_count", &self.total_eq_del_file_count)
            .finish()
    }
}

/// Compaction plan runner that executes a single compaction plan.
#[derive(Debug)]
pub struct IcebergCompactionPlanRunner {
    pub task_id: u64,
    pub plan_index: usize,

    pub catalog: Arc<dyn Catalog>,
    pub table_ident: TableIdent,
    pub iceberg_config: IcebergConfig,

    config: IcebergCompactorRunnerConfig,
    metrics: Arc<CompactorMetrics>,

    pub task_type: TaskType,
    branch: String,
    compaction_plan: CompactionPlan,
}

impl IcebergCompactionPlanRunner {
    pub fn required_parallelism(&self) -> u32 {
        self.compaction_plan.recommended_executor_parallelism() as u32
    }

    /// Returns a human-readable identifier for this plan, used for logging and debugging.
    ///
    /// The identifier includes catalog, table, task-type, and plan-index to provide context
    /// in logs. Format: `{catalog}-{table}-{task_type}-plan-{index}`
    ///
    /// # Note
    ///
    /// This identifier is **for display only**. The queue does not use it for deduplication.
    /// Task management (dedup, cancellation) is Meta's responsibility via unique `task_id`.
    ///
    /// # Example
    ///
    /// ```text
    /// catalog: "glue"
    /// table: "my_db.my_table"
    /// task_type: SmallFiles
    /// plan_index: 0
    ///
    /// → unique_ident: "glue-my_db.my_table-small-plan-0"
    /// ```
    pub fn unique_ident(&self) -> String {
        let task_type_str = match self.task_type {
            TaskType::SmallFiles => "small-files",
            TaskType::Full => "full",
            TaskType::FilesWithDelete => "files-with-delete",
            _ => "unknown",
        };
        format!(
            "{}-{}-{}-plan-{}",
            self.iceberg_config.catalog_name(),
            self.table_ident,
            task_type_str,
            self.plan_index
        )
    }

    pub fn to_meta(&self) -> IcebergTaskMeta {
        IcebergTaskMeta {
            task_id: self.task_id,
            plan_index: self.plan_index,
            required_parallelism: self.required_parallelism(),
        }
    }

    pub async fn compact(self, shutdown_rx: Receiver<()>) -> HummockResult<PlanExecutionOutput> {
        let task_id = self.task_id;
        let unique_ident = self.unique_ident();
        let now = std::time::Instant::now();

        let compact_task = Self::compact_impl(
            self.task_id,
            self.plan_index,
            self.catalog,
            self.table_ident,
            self.iceberg_config,
            self.config,
            self.metrics,
            self.task_type,
            self.branch,
            self.compaction_plan,
        );

        tokio::select! {
            _ = shutdown_rx => {
                tracing::info!(
                    task_id = task_id,
                    unique_ident = %unique_ident,
                    "Plan cancelled"
                );
                Err(HummockError::compaction_executor("Plan cancelled"))
            }
            result = compact_task => {
                match &result {
                    Ok(stats) => {
                        tracing::info!(
                            task_id = task_id,
                            unique_ident = %unique_ident,
                            elapsed_millis = now.elapsed().as_millis(),
                            stats = ?stats,
                            "Plan completed successfully"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e.as_report(),
                            task_id = task_id,
                            unique_ident = %unique_ident,
                            "Plan failed"
                        );
                    }
                }
                result
            }
        }
    }

    async fn compact_impl(
        task_id: u64,
        plan_index: usize,
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        iceberg_config: IcebergConfig,
        config: IcebergCompactorRunnerConfig,
        metrics: Arc<CompactorMetrics>,
        task_type: TaskType,
        branch: String,
        compaction_plan: CompactionPlan,
    ) -> HummockResult<PlanExecutionOutput> {
        let statistics = analyze_task_statistics(&compaction_plan);

        let compaction_execution_config = CompactionExecutionConfigBuilder::default()
            .enable_validate_compaction(config.enable_validate_compaction)
            .max_record_batch_rows(config.max_record_batch_rows)
            .write_parquet_properties(config.write_parquet_properties.clone())
            .target_file_size_bytes(iceberg_config.target_file_size_mb() * 1024 * 1024)
            .max_concurrent_closes(config.max_concurrent_closes)
            .enable_dynamic_size_estimation(config.enable_dynamic_size_estimation)
            .size_estimation_smoothing_factor(config.size_estimation_smoothing_factor)
            .build()
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to build iceberg compaction execution config: {:?}",
                    e.as_report()
                );
            });

        tracing::info!(
            task_id = task_id,
            plan_index = plan_index,
            task_type = ?task_type,
            table = %table_ident,
            input_parallelism = compaction_plan.recommended_executor_parallelism(),
            output_parallelism = compaction_plan.recommended_output_parallelism(),
            statistics = ?statistics,
            "Iceberg compaction plan started"
        );

        let retry_config = CommitManagerRetryConfig::default();
        let compaction = CompactionBuilder::new(catalog.clone(), table_ident.clone())
            .with_catalog_name(iceberg_config.catalog_name())
            .with_executor_type(iceberg_compaction_core::executor::ExecutorType::DataFusion)
            .with_registry(ICEBERG_COMPACTION_METRICS_REGISTRY.clone())
            .with_retry_config(retry_config)
            .with_to_branch(branch.clone())
            .build();

        metrics.compact_task_pending_num.inc();
        let input_parallelism = compaction_plan.recommended_executor_parallelism() as u32;
        metrics
            .compact_task_pending_parallelism
            .add(input_parallelism as _);

        let _release_guard = scopeguard::guard(
            (input_parallelism, metrics.clone()),
            |(val, metrics_guard)| {
                metrics_guard.compact_task_pending_num.dec();
                metrics_guard.compact_task_pending_parallelism.sub(val as _);
            },
        );

        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
        let rewrite_result = compaction
            .rewrite_plan(compaction_plan, &compaction_execution_config, &table)
            .await
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

        let serialized_files =
            serialize_data_files(rewrite_result.output_data_files, table.metadata())?;

        Ok(PlanExecutionOutput {
            added_data_files: serialized_files,
            stats: rewrite_result.stats,
        })
    }
}

fn analyze_task_statistics(plan: &CompactionPlan) -> IcebergCompactionTaskStatistics {
    let mut total_data_file_size: u64 = 0;
    let mut total_data_file_count = 0;
    let mut total_pos_del_file_size: u64 = 0;
    let mut total_pos_del_file_count = 0;
    let mut total_eq_del_file_size: u64 = 0;
    let mut total_eq_del_file_count = 0;

    for data_file in &plan.file_group.data_files {
        total_data_file_size += data_file.file_size_in_bytes;
        total_data_file_count += 1;
    }

    for pos_del_file in &plan.file_group.position_delete_files {
        total_pos_del_file_size += pos_del_file.file_size_in_bytes;
        total_pos_del_file_count += 1;
    }

    for eq_del_file in &plan.file_group.equality_delete_files {
        total_eq_del_file_size += eq_del_file.file_size_in_bytes;
        total_eq_del_file_count += 1;
    }

    IcebergCompactionTaskStatistics {
        total_data_file_size,
        total_data_file_count,
        total_pos_del_file_size,
        total_pos_del_file_count,
        total_eq_del_file_size,
        total_eq_del_file_count,
    }
}

fn serialize_data_files(
    data_files: Vec<DataFile>,
    metadata: &iceberg::spec::TableMetadata,
) -> HummockResult<Vec<SerializedDataFileInfo>> {
    let schema = metadata.current_schema();
    let partition_spec = metadata.default_partition_spec();
    let partition_type: StructType = partition_spec
        .partition_type(schema.as_ref())
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
    let format_version = metadata.format_version();

    data_files
        .into_iter()
        .map(|data_file| {
            let partition_spec_id = data_file.partition_spec_id();
            let serialized = SerializedDataFile::try_from(
                data_file,
                &partition_type,
                format_version,
            )
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            let json = serde_json::to_vec(&serialized)
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            Ok(SerializedDataFileInfo {
                json,
                partition_spec_id,
            })
        })
        .collect()
}

/// Creates plan runners from an iceberg compaction task.
pub async fn create_plan_runners(
    iceberg_compaction_task: IcebergCompactionTask,
    config: IcebergCompactorRunnerConfig,
    metrics: Arc<CompactorMetrics>,
) -> HummockResult<Vec<IcebergCompactionPlanRunner>> {
    let IcebergCompactionTask {
        task_id,
        props,
        task_type,
    } = iceberg_compaction_task;

    let iceberg_config = IcebergConfig::from_btreemap(BTreeMap::from_iter(props.into_iter()))
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    let catalog = iceberg_config
        .create_catalog()
        .await
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    let table_ident = iceberg_config
        .full_table_name()
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    let grouping_strategy = match iceberg_config.write_mode {
        IcebergWriteMode::CopyOnWrite => iceberg_compaction_core::config::GroupingStrategy::Single,
        IcebergWriteMode::MergeOnRead => match config.target_binpack_group_size_mb {
            Some(target_binpack_group_size_mb) => {
                iceberg_compaction_core::config::GroupingStrategy::BinPack(
                    iceberg_compaction_core::config::BinPackConfig::new(
                        target_binpack_group_size_mb * 1024 * 1024,
                    ),
                )
            }
            None => iceberg_compaction_core::config::GroupingStrategy::Single,
        },
    };

    let group_filters = {
        if config.min_group_size_mb.is_some() || config.min_group_file_count.is_some() {
            Some(GroupFilters {
                min_group_size_bytes: config.min_group_size_mb.map(|mb| mb * 1024 * 1024),
                min_group_file_count: config.min_group_file_count,
            })
        } else {
            None
        }
    };

    let parsed_task_type = TaskType::try_from(task_type)
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    let planning_config = match parsed_task_type {
        TaskType::SmallFiles => {
            let mut builder = SmallFilesConfigBuilder::default();
            builder
                .max_parallelism(config.max_parallelism as usize)
                .min_size_per_partition(config.min_size_per_partition)
                .max_file_count_per_partition(config.max_file_count_per_partition as usize)
                .target_file_size_bytes(iceberg_config.target_file_size_mb() * 1024 * 1024)
                .enable_heuristic_output_parallelism(config.enable_heuristic_output_parallelism)
                .small_file_threshold_bytes(iceberg_config.small_files_threshold_mb() * 1024 * 1024)
                .grouping_strategy(grouping_strategy);

            if let Some(group_filters) = group_filters.clone() {
                builder.group_filters(group_filters);
            }

            let config = builder
                .build()
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

            CompactionPlanningConfig::SmallFiles(config)
        }
        TaskType::Full => {
            let config = FullCompactionConfigBuilder::default()
                .max_parallelism(config.max_parallelism as usize)
                .min_size_per_partition(config.min_size_per_partition)
                .max_file_count_per_partition(config.max_file_count_per_partition as usize)
                .target_file_size_bytes(iceberg_config.target_file_size_mb() * 1024 * 1024)
                .enable_heuristic_output_parallelism(config.enable_heuristic_output_parallelism)
                .grouping_strategy(grouping_strategy)
                .build()
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

            CompactionPlanningConfig::Full(config)
        }

        TaskType::FilesWithDelete => {
            let config = FilesWithDeletesConfigBuilder::default()
                .max_parallelism(config.max_parallelism as usize)
                .min_size_per_partition(config.min_size_per_partition)
                .max_file_count_per_partition(config.max_file_count_per_partition as usize)
                .target_file_size_bytes(iceberg_config.target_file_size_mb() * 1024 * 1024)
                .enable_heuristic_output_parallelism(config.enable_heuristic_output_parallelism)
                .grouping_strategy(grouping_strategy)
                .min_delete_file_count_threshold(iceberg_config.delete_files_count_threshold())
                .build()
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

            CompactionPlanningConfig::FilesWithDeletes(config)
        }

        _ => {
            unreachable!(
                "Unsupported task type in iceberg compaction task {}: {:?}",
                task_id, parsed_task_type
            )
        }
    };

    let branch = commit_branch(iceberg_config.r#type.as_str(), iceberg_config.write_mode);

    let planner = CompactionPlanner::new(planning_config.clone());

    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    let compaction_plans = planner
        .plan_compaction_with_branch(&table, &branch)
        .await
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    if compaction_plans.is_empty() {
        tracing::info!(
            task_id = task_id,
            table = %table_ident,
            "No files to compact, skip the task"
        );
        return Ok(vec![]);
    }

    let mut runners = Vec::with_capacity(compaction_plans.len());

    for (plan_index, compaction_plan) in compaction_plans.into_iter().enumerate() {
        runners.push(IcebergCompactionPlanRunner {
            task_id,
            plan_index,
            catalog: catalog.clone(),
            table_ident: table_ident.clone(),
            iceberg_config: iceberg_config.clone(),
            config: config.clone(),
            metrics: metrics.clone(),
            task_type: parsed_task_type,
            branch: branch.clone(),
            compaction_plan,
        });
    }

    tracing::info!(
        task_id = task_id,
        table = %table_ident,
        plan_count = runners.len(),
        "Created plan runners for iceberg compaction task"
    );

    Ok(runners)
}

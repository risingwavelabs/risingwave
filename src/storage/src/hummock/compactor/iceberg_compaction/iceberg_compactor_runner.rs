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
use iceberg::spec::MAIN_BRANCH;
use iceberg::{Catalog, TableIdent};
use iceberg_compaction_core::compaction::{
    CommitConsistencyParams, CommitManagerRetryConfig, CompactionBuilder, CompactionPlan,
    CompactionPlanner, CompactionResult, CompactionType,
};
use iceberg_compaction_core::config::{
    CompactionBaseConfig, CompactionExecutionConfigBuilder, CompactionPlanningConfigBuilder,
};
use iceberg_compaction_core::executor::RewriteFilesStat;
use mixtrics::registry::prometheus::PrometheusMetricsRegistry;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use risingwave_common::config::storage::default::storage::{
    iceberg_compaction_enable_dynamic_size_estimation,
    iceberg_compaction_enable_heuristic_output_parallelism,
    iceberg_compaction_max_concurrent_closes, iceberg_compaction_max_file_group_size_bytes,
    iceberg_compaction_size_estimation_smoothing_factor,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_connector::sink::iceberg::{
    IcebergConfig, commit_branch, should_enable_iceberg_cow,
};
use risingwave_pb::iceberg_compaction::IcebergCompactionTask;
use risingwave_pb::iceberg_compaction::iceberg_compaction_task::TaskType;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;

use super::IcebergTaskMeta;
use crate::hummock::{HummockError, HummockResult};
use crate::monitor::CompactorMetrics;

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
    #[builder(default = "1024 * 1024 * 1024")] // 1GB
    pub target_file_size_bytes: u64,
    #[builder(default = "false")]
    pub enable_validate_compaction: bool,
    #[builder(default = "1024")]
    pub max_record_batch_rows: usize,
    #[builder(default = "default_writer_properties()")]
    pub write_parquet_properties: WriterProperties,
    #[builder(default = "32 * 1024 * 1024")] // 32MB
    pub small_file_threshold: u64,
    #[builder(default = "50 * 1024 * 1024 * 1024")] // 50GB
    pub max_task_total_size: u64,
    #[builder(default = "iceberg_compaction_enable_heuristic_output_parallelism()")]
    pub enable_heuristic_output_parallelism: bool,
    #[builder(default = "iceberg_compaction_max_concurrent_closes()")]
    pub max_concurrent_closes: usize,
    #[builder(default = "iceberg_compaction_enable_dynamic_size_estimation()")]
    pub enable_dynamic_size_estimation: bool,
    #[builder(default = "iceberg_compaction_size_estimation_smoothing_factor()")]
    pub size_estimation_smoothing_factor: f64,
    #[builder(default = "iceberg_compaction_max_file_group_size_bytes()")]
    pub max_file_group_size_bytes: u64,
}

pub struct IcebergCompactionTaskStatistics {
    pub total_data_file_size: u64,
    pub total_data_file_count: u32,
    pub total_pos_del_file_size: u64,
    pub total_pos_del_file_count: u32,
    pub total_eq_del_file_size: u64,
    pub total_eq_del_file_count: u32,
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

trait TaskTypeExt {
    fn to_compaction_type(self) -> CompactionType;
}

impl TaskTypeExt for TaskType {
    fn to_compaction_type(self) -> CompactionType {
        match self {
            TaskType::SmallDataFileCompaction => CompactionType::MergeSmallDataFiles,
            TaskType::FullCompaction => CompactionType::Full,
            _ => {
                unreachable!("Unexpected task type for Iceberg compaction: {:?}", self)
            }
        }
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

    /// Returns a unique identifier for this plan, used for queue deduplication.
    ///
    /// The identifier includes catalog, table, task-type, and plan-index, but intentionally
    /// excludes task-id. This design enables:
    ///
    /// 1. **Task type isolation**: Different compaction types (e.g., `SmallDataFileCompaction`
    ///    vs `FullCompaction`) operate independently without interfering with each other.
    ///
    /// 2. **Same-type deduplication**: Multiple requests of the same type for the same table
    ///    can be deduplicated automatically.
    ///
    /// # Deduplication Behavior
    ///
    /// - **Same plan (same type) in waiting queue**: New plan replaces the old one
    /// - **Same plan (same type) currently running**: New plan is rejected
    /// - **Different task types**: Completely independent (e.g., `SmallDataFileCompaction plan-0`
    ///   and `FullCompaction plan-0` are different unique-idents)
    ///
    /// # Example
    ///
    /// ```text
    /// T0: Meta sends `SmallDataFileCompaction` task#1 for table_A
    ///     → generates plan-0, plan-1, plan-2, plan-3
    ///     unique_idents: [SmallData-plan-0, SmallData-plan-1, SmallData-plan-2, SmallData-plan-3]
    ///
    /// T1: SmallData-plan-0 starts execution
    ///     Queue: [SmallData-plan-0: running] [SmallData-plan-1,2,3: waiting]
    ///
    /// T2: Meta sends FullCompaction task#2 for table_A (no grouping)
    ///     → generates plan-0
    ///     unique_ident: [FullCompaction-plan-0]
    ///
    ///     Result: FullCompaction-plan-0 is Added (different task_type, no conflict!)
    ///     Queue: [SmallData-plan-0: running] [SmallData-plan-1,2,3: waiting]
    ///            [FullCompaction-plan-0: waiting]
    ///
    /// T3: Meta sends another SmallDataFileCompaction task#3 for table_A
    ///     → generates plan-0, plan-1
    ///
    ///     Result:
    ///     - SmallData-plan-0: RejectedRunningDuplicate (old SmallData-plan-0 running)
    ///     - SmallData-plan-1: Replaced (replaces waiting old SmallData-plan-1)
    /// ```
    ///
    /// This prevents duplicate compaction work of the same type while allowing different
    /// compaction strategies to run concurrently.
    pub fn unique_ident(&self) -> String {
        let task_type_str = match self.task_type {
            TaskType::SmallDataFileCompaction => "small",
            TaskType::FullCompaction => "full",
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
            unique_ident: self.unique_ident(),
            enqueue_at: std::time::Instant::now(),
            required_parallelism: self.required_parallelism(),
        }
    }

    pub async fn compact(self, shutdown_rx: Receiver<()>) -> HummockResult<RewriteFilesStat> {
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
    ) -> HummockResult<RewriteFilesStat> {
        let statistics = analyze_task_statistics(&compaction_plan);

        let compaction_execution_config = CompactionExecutionConfigBuilder::default()
            .enable_validate_compaction(config.enable_validate_compaction)
            .max_record_batch_rows(config.max_record_batch_rows)
            .write_parquet_properties(config.write_parquet_properties.clone())
            .base(CompactionBaseConfig {
                target_file_size: config.target_file_size_bytes,
            })
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
        let compaction =
            CompactionBuilder::new(catalog.clone(), table_ident.clone(), CompactionType::Full)
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

        let CompactionResult {
            data_files,
            stats,
            table,
        } = compaction
            .compact_with_plan(compaction_plan, &compaction_execution_config)
            .await
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?
            .unwrap();

        if let Some(committed_table) = table
            && should_enable_iceberg_cow(
                iceberg_config.r#type.as_str(),
                iceberg_config.write_mode.as_str(),
            )
        {
            let ingestion_branch = commit_branch(
                iceberg_config.r#type.as_str(),
                iceberg_config.write_mode.as_str(),
            );

            // Overwrite Main branch
            let consistency_params = CommitConsistencyParams {
                starting_snapshot_id: committed_table
                    .metadata()
                    .snapshot_for_ref(ingestion_branch.as_str())
                    .ok_or(HummockError::compaction_executor(anyhow::anyhow!(
                        "Don't find current_snapshot for ingestion_branch {}",
                        ingestion_branch
                    )))?
                    .snapshot_id(),
                use_starting_sequence_number: true,
                basic_schema_id: committed_table.metadata().current_schema().schema_id(),
            };

            let commit_manager = compaction.build_commit_manager(consistency_params);

            let input_files = {
                let mut input_files = vec![];
                if let Some(snapshot) = committed_table.metadata().snapshot_for_ref(MAIN_BRANCH) {
                    let manifest_list = snapshot
                        .load_manifest_list(committed_table.file_io(), committed_table.metadata())
                        .await
                        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

                    for manifest_file in manifest_list
                        .entries()
                        .iter()
                        .filter(|entry| entry.has_added_files() || entry.has_existing_files())
                    {
                        let manifest = manifest_file
                            .load_manifest(committed_table.file_io())
                            .await
                            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
                        let (entry, _) = manifest.into_parts();
                        for i in entry {
                            match i.content_type() {
                                iceberg::spec::DataContentType::Data => {
                                    input_files.push(i.data_file().clone());
                                }
                                iceberg::spec::DataContentType::EqualityDeletes => {
                                    unreachable!(
                                        "Equality deletes are not supported in main branch"
                                    );
                                }
                                iceberg::spec::DataContentType::PositionDeletes => {
                                    unreachable!(
                                        "Position deletes are not supported in main branch"
                                    );
                                }
                            }
                        }
                    }

                    input_files
                } else {
                    vec![]
                }
            };

            let _new_table = commit_manager
                .overwrite_files(data_files, input_files, MAIN_BRANCH)
                .await
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
        }

        Ok(stats)
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

    let parsed_task_type = TaskType::try_from(task_type).map_err(|e| {
        HummockError::compaction_executor(format!("Invalid task type: {}", e.as_report()))
    })?;

    let grouping_strategy = match iceberg_config.write_mode.as_str() {
        "copy_on_write" => iceberg_compaction_core::config::GroupingStrategy::Noop,
        _ => iceberg_compaction_core::config::GroupingStrategy::BinPack(
            iceberg_compaction_core::config::BinPackConfig::new(config.max_file_group_size_bytes),
        ),
    };

    let planning_config = CompactionPlanningConfigBuilder::default()
        .max_parallelism(config.max_parallelism as usize)
        .min_size_per_partition(config.min_size_per_partition)
        .max_file_count_per_partition(config.max_file_count_per_partition as _)
        .base(CompactionBaseConfig {
            target_file_size: config.target_file_size_bytes,
        })
        .enable_heuristic_output_parallelism(config.enable_heuristic_output_parallelism)
        .small_file_threshold(config.small_file_threshold)
        .max_task_total_size(config.max_task_total_size)
        .grouping_strategy(grouping_strategy)
        .build()
        .unwrap_or_else(|e| {
            panic!(
                "Failed to build iceberg compaction planning config: {:?}",
                e.as_report()
            );
        });

    let branch = commit_branch(
        iceberg_config.r#type.as_str(),
        iceberg_config.write_mode.as_str(),
    );

    let planner = CompactionPlanner::new(planning_config.clone());

    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    let compaction_plans = planner
        .plan_compaction_with_branch(&table, parsed_task_type.to_compaction_type(), &branch)
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

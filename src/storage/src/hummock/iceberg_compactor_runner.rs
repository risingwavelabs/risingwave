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
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};

use derive_builder::Builder;
use iceberg::{Catalog, TableIdent};
use iceberg_compaction_core::compaction::{
    Compaction, CompactionType, RewriteDataFilesCommitManagerRetryConfig,
};
use iceberg_compaction_core::config::CompactionConfigBuilder;
use iceberg_compaction_core::executor::RewriteFilesStat;
use mixtrics::registry::prometheus::PrometheusMetricsRegistry;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::iceberg_compaction::IcebergCompactionTask;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;

use super::HummockResult;
use crate::hummock::HummockError;
use crate::monitor::CompactorMetrics;

static BERGLOOM_METRICS_REGISTRY: LazyLock<Box<PrometheusMetricsRegistry>> = LazyLock::new(|| {
    Box::new(PrometheusMetricsRegistry::new(
        GLOBAL_METRICS_REGISTRY.clone(),
    ))
});

pub struct IcebergCompactorRunner {
    pub task_id: u64,
    pub catalog: Arc<dyn Catalog>,
    pub table_ident: TableIdent,
    pub iceberg_config: IcebergConfig,

    config: IcebergCompactorRunnerConfig,
    metrics: Arc<CompactorMetrics>,
}

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
    #[builder(default = "1024 * 1024 * 1024")] // 1GB")]
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
}

#[derive(Debug, Clone)]
pub(crate) struct RunnerContext {
    pub max_available_parallelism: u32,
    pub running_task_parallelism: Arc<AtomicU32>,
}

impl RunnerContext {
    pub fn new(max_available_parallelism: u32, running_task_parallelism: Arc<AtomicU32>) -> Self {
        Self {
            max_available_parallelism,
            running_task_parallelism,
        }
    }

    pub fn is_available_parallelism_sufficient(&self, input_parallelism: u32) -> bool {
        (self.max_available_parallelism - self.running_task_parallelism.load(Ordering::SeqCst))
            >= input_parallelism
    }

    pub fn incr_running_task_parallelism(&self, increment: u32) {
        self.running_task_parallelism
            .fetch_add(increment, Ordering::SeqCst);
    }

    pub fn decr_running_task_parallelism(&self, decrement: u32) {
        self.running_task_parallelism
            .fetch_sub(decrement, Ordering::SeqCst);
    }
}

impl IcebergCompactorRunner {
    pub async fn new(
        iceberg_compaction_task: IcebergCompactionTask,
        config: IcebergCompactorRunnerConfig,
        metrics: Arc<CompactorMetrics>,
    ) -> HummockResult<Self> {
        let IcebergCompactionTask { task_id, props } = iceberg_compaction_task;
        let iceberg_config = IcebergConfig::from_btreemap(BTreeMap::from_iter(props.into_iter()))
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
        let catalog = iceberg_config
            .create_catalog()
            .await
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
        let table_ident = iceberg_config
            .full_table_name()
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

        Ok(Self {
            task_id,
            catalog,
            table_ident,
            iceberg_config,
            config,
            metrics,
        })
    }

    pub fn calculate_task_parallelism_impl(
        task_id_for_logging: u64,
        table_ident_for_logging: &TableIdent,
        task_statistics: &IcebergCompactionTaskStatistics,
        max_parallelism: u32,
        min_size_per_partition: u64,
        max_file_count_per_partition: u32,
        target_file_size_bytes: u64,
    ) -> HummockResult<(u32, u32)> {
        if max_parallelism == 0 {
            return Err(HummockError::compaction_executor(
                "Max parallelism cannot be 0".to_owned(),
            ));
        }

        let total_file_size_for_partitioning = task_statistics.total_data_file_size
            + task_statistics.total_pos_del_file_size
            + task_statistics.total_eq_del_file_size;
        if total_file_size_for_partitioning == 0 {
            // If the total data file size is 0, we cannot partition by size.
            // This means there are no data files to compact.
            tracing::warn!(
                task_id = task_id_for_logging,
                table = ?table_ident_for_logging,
                "Total data file size is 0, setting partition_by_size to 0."
            );

            return Err(HummockError::compaction_executor(
                "No files to calculate_task_parallelism".to_owned(),
            )); // No files, so no partitions.
        }

        let partition_by_size = total_file_size_for_partitioning
            .div_ceil(min_size_per_partition)
            .max(1) as u32; // Ensure at least one partition.

        let total_files_count_for_partitioning = task_statistics.total_data_file_count
            + task_statistics.total_pos_del_file_count
            + task_statistics.total_eq_del_file_count;

        let partition_by_count = total_files_count_for_partitioning
            .div_ceil(max_file_count_per_partition)
            .max(1); // Ensure at least one partition.

        let input_parallelism = partition_by_size
            .max(partition_by_count)
            .min(max_parallelism);

        // `output_parallelism` should not exceed `input_parallelism`
        // and should also not exceed max_parallelism.
        // It's primarily driven by size to avoid small output files.
        let mut output_parallelism = partition_by_size
            .min(input_parallelism)
            .min(max_parallelism);

        // Heuristic: If the total task data size is very small (less than target_file_size_bytes),
        // force output_parallelism to 1 to encourage merging into a single, larger output file.
        if task_statistics.total_data_file_size > 0 // Only apply if there's data
            && task_statistics.total_data_file_size < target_file_size_bytes
            && output_parallelism > 1
        {
            tracing::debug!(
                task_id = task_id_for_logging,
                table = ?table_ident_for_logging,
                total_data_file_size = task_statistics.total_data_file_size,
                target_file_size_bytes = target_file_size_bytes,
                original_output_parallelism = output_parallelism,
                "Total data size is less than target file size, forcing output_parallelism to 1."
            );
            output_parallelism = 1;
        }

        tracing::debug!(
            task_id = task_id_for_logging,
            table = ?table_ident_for_logging,
            stats = ?task_statistics,
            config_max_parallelism = max_parallelism,
            config_min_size_per_partition = min_size_per_partition,
            config_max_file_count_per_partition = max_file_count_per_partition,
            config_target_file_size_bytes = target_file_size_bytes,
            calculated_partition_by_size = partition_by_size,
            calculated_partition_by_count = partition_by_count,
            final_input_parallelism = input_parallelism,
            final_output_parallelism = output_parallelism,
            "Calculated task parallelism"
        );

        Ok((input_parallelism, output_parallelism))
    }

    fn calculate_task_parallelism(
        &self,
        task_statistics: &IcebergCompactionTaskStatistics,
        max_parallelism: u32,
        min_size_per_partition: u64,
        max_file_count_per_partition: u32,
        target_file_size_bytes: u64,
    ) -> HummockResult<(u32, u32)> {
        if max_parallelism == 0 {
            return Err(HummockError::compaction_executor(
                "Max parallelism cannot be 0".to_owned(),
            ));
        }

        Self::calculate_task_parallelism_impl(
            self.task_id,
            &self.table_ident,
            task_statistics,
            max_parallelism,
            min_size_per_partition,
            max_file_count_per_partition,
            target_file_size_bytes,
        )
    }

    pub async fn compact(
        self,
        context: RunnerContext,
        shutdown_rx: Receiver<()>,
    ) -> HummockResult<()> {
        let task_id = self.task_id;
        let now = std::time::Instant::now();

        let compact = async move {
            let retry_config = RewriteDataFilesCommitManagerRetryConfig::default();
            let statistics = self
                .analyze_task_statistics()
                .await
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

            let (input_parallelism, output_parallelism) = self
                .calculate_task_parallelism(
                    &statistics,
                    self.config.max_parallelism,
                    self.config.min_size_per_partition,
                    self.config.max_file_count_per_partition,
                    self.config.target_file_size_bytes,
                )
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

            if !context.is_available_parallelism_sufficient(input_parallelism) {
                tracing::warn!(
                    "Available parallelism is less than input parallelism {} task {} will not run",
                    input_parallelism,
                    task_id
                );
                return Err(HummockError::compaction_executor(
                    "Available parallelism is less than input parallelism",
                ));
            }

            let compaction_config = Arc::new(
                CompactionConfigBuilder::default()
                    .batch_parallelism(input_parallelism as usize)
                    .target_partitions(output_parallelism as usize)
                    .target_file_size(self.config.target_file_size_bytes)
                    .enable_validate_compaction(self.config.enable_validate_compaction)
                    .max_record_batch_rows(self.config.max_record_batch_rows)
                    .write_parquet_properties(self.config.write_parquet_properties.clone())
                    .build()
                    .unwrap_or_else(|e| {
                        panic!(
                            "Failed to build iceberg compaction write props: {:?}",
                            e.as_report()
                        );
                    }),
            );

            tracing::info!(
                task_id = task_id,
                table = ?self.table_ident,
                input_parallelism,
                output_parallelism,
                statistics = ?statistics,
                compaction_config = ?compaction_config,
                "Iceberg compaction task started",
            );

            let compaction = Compaction::builder()
                .with_catalog(self.catalog.clone())
                .with_catalog_name(self.iceberg_config.catalog_name())
                .with_compaction_type(CompactionType::Full)
                .with_config(compaction_config)
                .with_table_ident(self.table_ident.clone())
                .with_executor_type(iceberg_compaction_core::executor::ExecutorType::DataFusion)
                .with_registry(BERGLOOM_METRICS_REGISTRY.clone())
                .with_retry_config(retry_config)
                .build()
                .await
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

            context.incr_running_task_parallelism(input_parallelism);
            self.metrics.compact_task_pending_num.inc();
            self.metrics
                .compact_task_pending_parallelism
                .add(input_parallelism as _);

            let _release_guard = scopeguard::guard(
                (input_parallelism, context.clone(), self.metrics.clone()), /* metrics.clone() if Arc */
                |(val, ctx, metrics_guard)| {
                    ctx.decr_running_task_parallelism(val);
                    metrics_guard.compact_task_pending_num.dec();
                    metrics_guard.compact_task_pending_parallelism.sub(val as _);
                },
            );

            let stat = compaction
                .compact()
                .await
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            Ok::<RewriteFilesStat, HummockError>(stat)
        };

        tokio::select! {
            _ = shutdown_rx => {
                tracing::info!(task_id = task_id, "Iceberg compaction task cancelled");
            }
            stat = compact => {
                match stat {
                    Ok(stat) => {
                        tracing::info!(
                            task_id = task_id,
                            elapsed_millis = now.elapsed().as_millis(),
                            stat = ?stat,
                            "Iceberg compaction task finished",
                        );
                    }

                    Err(e) => {
                        tracing::warn!(
                            error = %e.as_report(),
                            task_id = task_id,
                            "Iceberg compaction task failed with error",
                        );
                    }
                }
            }
        }

        Ok(())
    }

    async fn analyze_task_statistics(&self) -> HummockResult<IcebergCompactionTaskStatistics> {
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .ok_or_else(|| HummockError::compaction_executor("Don't find current_snapshot"))?
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

        let mut total_data_file_size: u64 = 0;
        let mut total_data_file_count = 0;
        let mut total_pos_del_file_size: u64 = 0;
        let mut total_pos_del_file_count = 0;
        let mut total_eq_del_file_size: u64 = 0;
        let mut total_eq_del_file_count = 0;

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            let (entry, _) = manifest.into_parts();
            for i in entry {
                match i.content_type() {
                    iceberg::spec::DataContentType::Data => {
                        total_data_file_size += i.data_file().file_size_in_bytes();
                        total_data_file_count += 1;
                    }
                    iceberg::spec::DataContentType::EqualityDeletes => {
                        total_eq_del_file_size += i.data_file().file_size_in_bytes();
                        total_eq_del_file_count += 1;
                    }
                    iceberg::spec::DataContentType::PositionDeletes => {
                        total_pos_del_file_size += i.data_file().file_size_in_bytes();
                        total_pos_del_file_count += 1;
                    }
                }
            }
        }

        Ok(IcebergCompactionTaskStatistics {
            total_data_file_size,
            total_data_file_count: total_data_file_count as u32,
            total_pos_del_file_size,
            total_pos_del_file_count: total_pos_del_file_count as u32,
            total_eq_del_file_size,
            total_eq_del_file_count: total_eq_del_file_count as u32,
        })
    }
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

#[cfg(test)]
mod tests {
    use iceberg::TableIdent;

    use super::*;

    fn default_test_config_params() -> IcebergCompactorRunnerConfig {
        IcebergCompactorRunnerConfigBuilder::default()
            .max_parallelism(4)
            .min_size_per_partition(1024 * 1024 * 100) // 100MB
            .max_file_count_per_partition(10)
            .target_file_size_bytes(1024 * 1024 * 128) // 128MB
            .build()
            .unwrap()
    }

    #[test]
    fn test_calculate_parallelism_no_files() {
        let config = default_test_config_params();
        let dummy_table_ident = TableIdent::from_strs(["db", "schema", "table"]).unwrap();
        let stats = IcebergCompactionTaskStatistics {
            total_data_file_size: 0,
            total_data_file_count: 0,
            total_pos_del_file_size: 0,
            total_pos_del_file_count: 0,
            total_eq_del_file_size: 0,
            total_eq_del_file_count: 0,
        };
        let result = IcebergCompactorRunner::calculate_task_parallelism_impl(
            1,
            &dummy_table_ident,
            &stats,
            config.max_parallelism,
            config.min_size_per_partition,
            config.max_file_count_per_partition,
            config.target_file_size_bytes,
        );

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(
                e.to_string()
                    .contains("No files to calculate_task_parallelism")
            );
        }
    }

    #[test]
    fn test_calculate_parallelism_size_dominant() {
        let config = default_test_config_params();
        let dummy_table_ident = TableIdent::from_strs(["db", "schema", "table"]).unwrap();
        let stats = IcebergCompactionTaskStatistics {
            total_data_file_size: 1024 * 1024 * 500, // 500MB
            total_data_file_count: 5,                // Low count
            total_pos_del_file_size: 0,
            total_pos_del_file_count: 0,
            total_eq_del_file_size: 0,
            total_eq_del_file_count: 0,
        };
        // partition_by_size = 500MB / 100MB = 5
        // partition_by_count = 5 / 10 = 1 (div_ceil)
        // initial_input = max(5,1) = 5
        // input = min(5, 4_max_p) = 4
        // output = min(partition_by_size=5, input=4, max_p=4) = 4
        let (input_p, output_p) = IcebergCompactorRunner::calculate_task_parallelism_impl(
            1,
            &dummy_table_ident,
            &stats,
            config.max_parallelism,
            config.min_size_per_partition,
            config.max_file_count_per_partition,
            config.target_file_size_bytes,
        )
        .unwrap();
        assert_eq!(input_p, 4);
        assert_eq!(output_p, 4);
    }

    #[test]
    fn test_calculate_parallelism_count_dominant() {
        let config = default_test_config_params();
        let dummy_table_ident = TableIdent::from_strs(["db", "schema", "table"]).unwrap();
        let stats = IcebergCompactionTaskStatistics {
            total_data_file_size: 1024 * 1024 * 50, // 50MB (low size)
            total_data_file_count: 35,              // High count
            total_pos_del_file_size: 0,
            total_pos_del_file_count: 0,
            total_eq_del_file_size: 0,
            total_eq_del_file_count: 0,
        };
        // partition_by_size = 50MB / 100MB = 1 (div_ceil)
        // partition_by_count = 35 / 10 = 4 (div_ceil)
        // initial_input = max(1,4) = 4
        // input = min(4, 4_max_p) = 4
        // output = min(partition_by_size=1, input=4, max_p=4) = 1
        let (input_p, output_p) = IcebergCompactorRunner::calculate_task_parallelism_impl(
            1,
            &dummy_table_ident,
            &stats,
            config.max_parallelism,
            config.min_size_per_partition,
            config.max_file_count_per_partition,
            config.target_file_size_bytes,
        )
        .unwrap();
        assert_eq!(input_p, 4);
        assert_eq!(output_p, 1);
    }

    #[test]
    fn test_calculate_parallelism_max_parallelism_cap() {
        let mut config = default_test_config_params();
        config.max_parallelism = 2; // Lower max_parallelism
        let dummy_table_ident = TableIdent::from_strs(["db", "schema", "table"]).unwrap();
        let stats = IcebergCompactionTaskStatistics {
            total_data_file_size: 1024 * 1024 * 500, // 500MB
            total_data_file_count: 35,               // High count
            total_pos_del_file_size: 0,
            total_pos_del_file_count: 0,
            total_eq_del_file_size: 0,
            total_eq_del_file_count: 0,
        };
        // partition_by_size = 500MB / 100MB = 5
        // partition_by_count = 35 / 10 = 4
        // initial_input = max(5,4) = 5
        // input = min(5, 2_max_p) = 2
        // output = min(partition_by_size=5, input=2, max_p=2) = 2
        let (input_p, output_p) = IcebergCompactorRunner::calculate_task_parallelism_impl(
            1,
            &dummy_table_ident,
            &stats,
            config.max_parallelism,
            config.min_size_per_partition,
            config.max_file_count_per_partition,
            config.target_file_size_bytes,
        )
        .unwrap();
        assert_eq!(input_p, 2);
        assert_eq!(output_p, 2);
    }

    #[test]
    fn test_calculate_parallelism_small_data_heuristic() {
        let config = default_test_config_params(); // target_file_size_bytes = 128MB
        let dummy_table_ident = TableIdent::from_strs(["db", "schema", "table"]).unwrap();
        // partition_by_size = 60MB / 100MB = 1
        // partition_by_count = 15 / 10 = 2
        // initial_input = max(1,2) = 2
        // input = min(2, 4_max_p) = 2
        // initial_output = min(partition_by_size=1, input=2, max_p=4) = 1
        // Heuristic: total_data_file_size (60MB) < target_file_size_bytes (128MB)
        // Since initial_output was 1, it remains 1.
        // Let's make partition_by_size larger to test heuristic properly
        let stats_for_heuristic = IcebergCompactionTaskStatistics {
            total_data_file_size: 1024 * 1024 * 60,     // 60MB
            total_data_file_count: 5,                   // Low count, so size dominates for input
            total_pos_del_file_size: 1024 * 1024 * 250, // Makes total size 310MB
            total_pos_del_file_count: 2,
            total_eq_del_file_size: 0,
            total_eq_del_file_count: 0,
        };
        // total_file_size_for_partitioning = 310MB
        // partition_by_size = 310MB / 100MB = 4 (div_ceil)
        // total_files_count_for_partitioning = 5 + 2 = 7
        // partition_by_count = 7 / 10 = 1
        // initial_input = max(4,1) = 4
        // input = min(4, 4_max_p) = 4
        // initial_output = min(partition_by_size=4, input=4, max_p=4) = 4
        // Heuristic: total_data_file_size (60MB) < target_file_size_bytes (128MB)
        // AND initial_output (4) > 1. So output becomes 1.
        let (input_p, output_p) = IcebergCompactorRunner::calculate_task_parallelism_impl(
            1,
            &dummy_table_ident,
            &stats_for_heuristic,
            config.max_parallelism,
            config.min_size_per_partition,
            config.max_file_count_per_partition,
            config.target_file_size_bytes,
        )
        .unwrap();
        assert_eq!(input_p, 4);
        assert_eq!(output_p, 1);
    }

    #[test]
    fn test_calculate_parallelism_all_empty_files() {
        let config = default_test_config_params();
        let dummy_table_ident = TableIdent::from_strs(["db", "schema", "table"]).unwrap();
        let stats = IcebergCompactionTaskStatistics {
            total_data_file_size: 0,
            total_data_file_count: 5, // 5 empty data files
            total_pos_del_file_size: 0,
            total_pos_del_file_count: 0,
            total_eq_del_file_size: 0,
            total_eq_del_file_count: 0,
        };
        // total_file_size_for_partitioning = 0, should return Err.
        let result = IcebergCompactorRunner::calculate_task_parallelism_impl(
            1,
            &dummy_table_ident,
            &stats,
            config.max_parallelism,
            config.min_size_per_partition,
            config.max_file_count_per_partition,
            config.target_file_size_bytes,
        );
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(
                e.to_string()
                    .contains("No files to calculate_task_parallelism")
            );
        }
    }

    #[test]
    fn test_calculate_parallelism_max_parallelism_zero() {
        let mut config = default_test_config_params();
        config.max_parallelism = 0; // max_parallelism is 0
        let dummy_table_ident = TableIdent::from_strs(["db", "schema", "table"]).unwrap();
        let stats = IcebergCompactionTaskStatistics {
            total_data_file_size: 1024 * 1024 * 50, // 50MB
            total_data_file_count: 5,
            total_pos_del_file_size: 0,
            total_pos_del_file_count: 0,
            total_eq_del_file_size: 0,
            total_eq_del_file_count: 0,
        };
        // max_parallelism = 0, should return Err.
        let result = IcebergCompactorRunner::calculate_task_parallelism_impl(
            1,
            &dummy_table_ident,
            &stats,
            config.max_parallelism,
            config.min_size_per_partition,
            config.max_file_count_per_partition,
            config.target_file_size_bytes,
        );
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Max parallelism cannot be 0"));
        }
    }

    #[test]
    fn test_calculate_parallelism_only_delete_files() {
        let config = default_test_config_params();
        let dummy_table_ident = TableIdent::from_strs(["db", "schema", "table"]).unwrap();
        let stats = IcebergCompactionTaskStatistics {
            total_data_file_size: 0,
            total_data_file_count: 0,
            total_pos_del_file_size: 1024 * 1024 * 20, // 20MB of pos-delete files
            total_pos_del_file_count: 2,
            total_eq_del_file_size: 0,
            total_eq_del_file_count: 0,
        };
        // total_files_count_for_partitioning = 2
        // total_file_size_for_partitioning = 20MB
        // partition_by_size = 20MB / 100MB = 1
        // partition_by_count = 2 / 10 = 1
        // initial_input = max(1,1) = 1
        // input = min(1, 4_max_p) = 1
        // initial_output = min(partition_by_size=1, input=1, max_p=4) = 1
        // Heuristic: total_data_file_size (0) is not > 0.
        let (input_p, output_p) = IcebergCompactorRunner::calculate_task_parallelism_impl(
            1,
            &dummy_table_ident,
            &stats,
            config.max_parallelism,
            config.min_size_per_partition,
            config.max_file_count_per_partition,
            config.target_file_size_bytes,
        )
        .unwrap();
        assert_eq!(input_p, 1);
        assert_eq!(output_p, 1);
    }

    #[test]
    fn test_calculate_parallelism_zero_file_count_non_zero_size() {
        let config = default_test_config_params();
        let dummy_table_ident = TableIdent::from_strs(["db", "schema", "table"]).unwrap();
        let stats = IcebergCompactionTaskStatistics {
            total_data_file_size: 1024 * 1024 * 200, // 200MB
            total_data_file_count: 0,                // No data files by count
            total_pos_del_file_size: 0,
            total_pos_del_file_count: 0,
            total_eq_del_file_size: 0,
            total_eq_del_file_count: 0,
        };
        // total_file_size_for_partitioning = 200MB
        // partition_by_size = (200MB / 100MB).ceil().max(1) = 2
        // total_files_count_for_partitioning = 0
        // partition_by_count = (0 / 10).ceil().max(1) = 1
        // input_parallelism = max(2, 1).min(4) = 2
        // output_parallelism = partition_by_size.min(input_parallelism).min(max_parallelism) = 2.min(2).min(4) = 2
        // Heuristic: total_data_file_size (200MB) not < target_file_size_bytes (128MB)
        let (input_p, output_p) = IcebergCompactorRunner::calculate_task_parallelism_impl(
            1,
            &dummy_table_ident,
            &stats,
            config.max_parallelism,
            config.min_size_per_partition,
            config.max_file_count_per_partition,
            config.target_file_size_bytes,
        )
        .unwrap();
        assert_eq!(input_p, 2);
        assert_eq!(output_p, 2);
    }
}

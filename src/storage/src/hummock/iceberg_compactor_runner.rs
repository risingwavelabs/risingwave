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

use bergloom_core::compaction::{
    Compaction, CompactionType, RewriteDataFilesCommitManagerRetryConfig,
};
use bergloom_core::config::CompactionConfigBuilder;
use derive_builder::Builder;
use iceberg::{Catalog, TableIdent};
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

    pub fn calculate_task_parallelism(
        &self,
        task_statistics: &IcebergCompactionTaskStatistics,
        max_parallelism: u32,
        min_size_per_partition: u64,
        max_file_count_per_partition: u32,
    ) -> HummockResult<(u32, u32)> {
        let partition_by_size = task_statistics
            .total_data_file_size
            .div_ceil(min_size_per_partition) as u32;
        let partition_by_count = if task_statistics.total_data_file_count > 0 {
            (task_statistics.total_data_file_count
                + task_statistics.total_pos_del_file_count
                + task_statistics.total_eq_del_file_count)
                / max_file_count_per_partition
        } else {
            1 // At least one partition
        };

        let input_parallelism = partition_by_size
            .max(partition_by_count)
            .min(max_parallelism);

        // `output_parallelism` should not exceed `input_parallelism`
        // and should also not exceed max_parallelism
        let output_parallelism = partition_by_size
            .min(input_parallelism)
            .min(max_parallelism);

        Ok((input_parallelism, output_parallelism))
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

            let compaction = Compaction::builder()
                .with_catalog(self.catalog.clone())
                .with_catalog_name(self.iceberg_config.catalog_name())
                .with_compaction_type(CompactionType::Full)
                .with_config(compaction_config)
                .with_table_ident(self.table_ident.clone())
                .with_executor_type(bergloom_core::executor::ExecutorType::DataFusion)
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

            tracing::info!(
                "Iceberg compaction task {} {:?} started with input parallelism {} and output parallelism {}",
                task_id,
                format!(
                    "{:?}-{:?}",
                    self.iceberg_config.catalog_name(),
                    self.table_ident
                ),
                input_parallelism,
                output_parallelism
            );
            compaction
                .compact()
                .await
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            Ok::<(), HummockError>(())
        };

        tokio::select! {
            _ = shutdown_rx => {
                tracing::info!("Iceberg compaction task {} cancelled", task_id);
            }
            result = compact => {
                if let Err(e) = result {
                    tracing::warn!(
                        error = %e.as_report(),
                        "Compaction task {} failed with error",
                        task_id,
                    );
                }
            }
        }

        tracing::info!(
            "Iceberg compaction task {} finished cost {} ms",
            task_id,
            now.elapsed().as_millis()
        );

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

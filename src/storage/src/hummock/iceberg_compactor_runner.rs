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

use bergloom_core::CompactionConfig;
use bergloom_core::compaction::{
    Compaction, CompactionType, RewriteDataFilesCommitManagerRetryConfig,
};
use iceberg::{Catalog, TableIdent};
use mixtrics::registry::prometheus::PrometheusMetricsRegistry;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::iceberg_compaction::IcebergCompactionTask;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;

use super::HummockResult;
use crate::hummock::HummockError;
use crate::hummock::compactor::CompactorContext;

// Minimum size for a partition in bytes to avoid too many small files
const MIN_SIZE_PER_PARTITION: u64 = 1024 * 1024 * 1024; // 1GB

// Maximum size for a partition in bytes to avoid IO bottlenecks
const MAX_FILE_COUNT_PER_PARTITION: u32 = 32; // 32 files per partition

static BERGLOOM_METRICS_REGISTRY: LazyLock<Box<PrometheusMetricsRegistry>> = LazyLock::new(|| {
    Box::new(PrometheusMetricsRegistry::new(
        GLOBAL_METRICS_REGISTRY.clone(),
    ))
});

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

pub struct IcebergCompactorRunner {
    pub task_id: u64,
    pub catalog: Arc<dyn Catalog>,
    pub table_ident: TableIdent,
    pub iceberg_config: IcebergConfig,
}

impl IcebergCompactorRunner {
    pub async fn new(iceberg_compaction_task: IcebergCompactionTask) -> HummockResult<Self> {
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
        })
    }

    pub async fn analyze_task_statistics(&self) -> HummockResult<IcebergCompactionTaskStatistics> {
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

    pub fn calculate_task_parallelism(
        &self,
        task_statistics: &IcebergCompactionTaskStatistics,
        max_parallelism: u32,
    ) -> HummockResult<(u32, u32)> {
        let partition_by_size = task_statistics
            .total_data_file_size
            .div_ceil(MIN_SIZE_PER_PARTITION) as u32;
        let partition_by_count = if task_statistics.total_data_file_count > 0 {
            (task_statistics.total_data_file_count
                + task_statistics.total_pos_del_file_count
                + task_statistics.total_eq_del_file_count)
                / MAX_FILE_COUNT_PER_PARTITION
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

    pub async fn compact_iceberg(
        self,
        shutdown_rx: Receiver<()>,
        compaction_config: Arc<CompactionConfig>,
        compactor_context: CompactorContext,
    ) {
        let compact = async move {
            let retry_config = RewriteDataFilesCommitManagerRetryConfig::default();

            let running_parallelism = compaction_config.batch_parallelism;

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

            compactor_context
                .compactor_metrics
                .compact_task_pending_num
                .inc();
            compactor_context
                .compactor_metrics
                .compact_task_pending_parallelism
                .add(running_parallelism as _);

            let _release_metrics_guard = scopeguard::guard(
                (running_parallelism, compactor_context.clone()),
                |(running_parallelism, compactor_context)| {
                    compactor_context
                        .compactor_metrics
                        .compact_task_pending_num
                        .dec();
                    compactor_context
                        .compactor_metrics
                        .compact_task_pending_parallelism
                        .sub(running_parallelism as _);
                },
            );

            compaction
                .compact()
                .await
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            Ok::<(), HummockError>(())
        };
        tokio::select! {
            _ = shutdown_rx => {
                tracing::info!("Iceberg compaction task {} cancelled", self.task_id);
            }
            result = compact => {
                if let Err(e) = result {
                    tracing::warn!(
                        error = %e.as_report(),
                        "Compaction task {} failed with error",
                        self.task_id,
                    );
                }
            }
        }
        tracing::info!("Iceberg compaction task {} finished", self.task_id);
    }
}

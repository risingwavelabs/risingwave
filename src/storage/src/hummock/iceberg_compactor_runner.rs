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

const MAX_SIZE_PER_PARTITION: u32 = 1024 * 1024 * 1024; // 1GB

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

    pub async fn calculate_task_parallelism(&self) -> HummockResult<u32> {
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

        let mut all_data_file_size: u32 = 0;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
            let (entry, _) = manifest.into_parts();
            for i in entry {
                match i.content_type() {
                    iceberg::spec::DataContentType::Data => {
                        all_data_file_size += i.data_file().file_size_in_bytes() as u32;
                    }
                    iceberg::spec::DataContentType::EqualityDeletes => {}
                    iceberg::spec::DataContentType::PositionDeletes => {}
                }
            }
        }

        Ok(all_data_file_size.div_ceil(MAX_SIZE_PER_PARTITION))
    }

    pub async fn compact_iceberg(self, shutdown_rx: Receiver<()>, parallelism: usize) {
        // TODO: consider target_file_size
        let compact = async move {
            let compaction_config = Arc::new(CompactionConfig {
                batch_parallelism: Some(parallelism),
                target_partitions: Some(parallelism),
                data_file_prefix: None,
            });

            let retry_config = RewriteDataFilesCommitManagerRetryConfig::default();

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

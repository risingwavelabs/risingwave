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
use std::sync::Arc;

use bergloom_core::CompactionConfig;
use bergloom_core::compaction::{Compaction, CompactionType};
use iceberg::{Catalog, TableIdent};
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::iceberg_compaction::IcebergCompactionTask;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;

use super::HummockResult;
use crate::hummock::HummockError;

const MAX_SIZE_PER_PARTITION: u32 = 1024 * 1024 * 1024; // 1GB

pub struct IcebergCompactorRunner {
    pub task_id: u64,
    pub catalog: Arc<dyn Catalog>,
    pub table_ident: TableIdent,
}

impl IcebergCompactorRunner {
    pub async fn new(iceberg_compaction_task: IcebergCompactionTask) -> HummockResult<Self> {
        let IcebergCompactionTask { task_id, props } = iceberg_compaction_task;
        let config = IcebergConfig::from_btreemap(BTreeMap::from_iter(props.into_iter()))
            .map_err(|e| HummockError::compaction_runtime(e.as_report()))?;
        let catalog = config
            .create_catalog()
            .await
            .map_err(|e| HummockError::compaction_runtime(e.as_report()))?;
        let table_ident = config
            .full_table_name()
            .map_err(|e| HummockError::compaction_runtime(e.as_report()))?;
        Ok(Self {
            task_id,
            catalog,
            table_ident,
        })
    }

    pub async fn calculate_task_parallelism(&self) -> HummockResult<u32> {
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(|e| HummockError::compaction_runtime(e.as_report()))?;
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .ok_or_else(|| HummockError::compaction_runtime("Don't find current_snapshot"))?
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .map_err(|e| HummockError::compaction_runtime(e.as_report()))?;

        let mut all_data_file_size: u32 = 0;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(|e| HummockError::compaction_runtime(e.as_report()))?;
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

    pub async fn compact(self, shutdown_rx: Receiver<()>, parallelism: usize) {
        let now = std::time::Instant::now();
        // Todo:  consider target_file_size
        let compact = async move {
            let compaction_config = Arc::new(CompactionConfig {
                batch_parallelism: Some(parallelism),
                target_partitions: Some(parallelism),
                data_file_prefix: None,
            });
            let compaction = Compaction::new(compaction_config, self.catalog);
            compaction
                .compact(CompactionType::Full(self.table_ident))
                .await
                .map_err(|e| HummockError::compaction_runtime(e.as_report()))?;
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
                        "Iceberg compaction task {} failed",
                        self.task_id,
                    );
                }
            }
        }

        tracing::info!(
            "Iceberg compaction task {} finished cost {} ms",
            self.task_id,
            now.elapsed().as_millis()
        );
    }
}

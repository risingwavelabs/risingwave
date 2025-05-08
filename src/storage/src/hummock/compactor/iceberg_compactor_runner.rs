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

use ic_core::CompactionConfig;
use ic_core::compaction::{Compaction, CompactionType};
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::hummock::IcebergCompactionTask;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;

use crate::hummock::HummockError;

pub async fn compact_iceberg(
    iceberg_compaction_task: IcebergCompactionTask,
    shutdown_rx: Receiver<()>,
) {
    let IcebergCompactionTask { task_id, props } = iceberg_compaction_task;
    let compact = async move {
        let config = IcebergConfig::from_btreemap(BTreeMap::from_iter(props.into_iter()))
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
        let catalog = config
            .create_catalog()
            .await
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
        let compaction_config = Arc::new(CompactionConfig {
            batch_parallelism: Some(1),
            target_partitions: Some(1),
            data_file_prefix: None,
        });
        let compaction = Compaction::new(compaction_config, catalog);
        let table_ident = config
            .full_table_name()
            .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
        compaction
            .compact(CompactionType::Full(table_ident))
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
    tracing::info!("Iceberg compaction task {} finished", task_id);
}

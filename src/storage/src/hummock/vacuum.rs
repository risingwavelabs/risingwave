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

use std::ops::Sub;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{StreamExt, TryStreamExt};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::ObjectMetadataIter;
use risingwave_pb::hummock::{FullScanTask, VacuumTask};
use risingwave_rpc_client::HummockMetaClient;

use super::{HummockError, HummockResult};
use crate::hummock::{SstableStore, SstableStoreRef};

pub struct Vacuum;

impl Vacuum {
    /// Wrapper method that warns on any error and doesn't propagate it.
    /// Returns false if any error.
    pub async fn vacuum(
        vacuum_task: VacuumTask,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> bool {
        tracing::info!("Try to vacuum SSTs {:?}", vacuum_task.sstable_object_ids);
        match Vacuum::vacuum_inner(
            vacuum_task,
            sstable_store.clone(),
            hummock_meta_client.clone(),
        )
        .await
        {
            Ok(_) => {
                tracing::info!("Finished vacuuming SSTs");
            }
            Err(e) => {
                tracing::warn!("Failed to vacuum SSTs: {:#?}", e);
                return false;
            }
        }
        true
    }

    pub async fn vacuum_inner(
        vacuum_task: VacuumTask,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> HummockResult<()> {
        let object_ids = vacuum_task.sstable_object_ids;
        sstable_store.delete_list(&object_ids).await?;
        hummock_meta_client
            .report_vacuum_task(VacuumTask {
                sstable_object_ids: object_ids,
            })
            .await
            .map_err(|e| {
                HummockError::meta_error(format!("Failed to report vacuum task: {:#?}", e))
            })?;

        Ok(())
    }

    /// Wrapper method that warns on any error and doesn't propagate it.
    /// Returns false if any error.
    pub async fn full_scan(
        full_scan_task: FullScanTask,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> bool {
        tracing::info!(
            "Try to full scan SSTs with timestamp {}",
            full_scan_task.sst_retention_time_sec
        );
        let metadata_iter = match sstable_store.list_object_metadata_from_object_store().await {
            Ok(metadata_iter) => metadata_iter,
            Err(e) => {
                tracing::warn!("Failed to init object iter: {:#?}", e);
                return false;
            }
        };
        let (filtered_object_ids, unfiltered_count, unfiltered_size) =
            match Vacuum::full_scan_inner(full_scan_task, metadata_iter).await {
                Ok(res) => res,
                Err(e) => {
                    tracing::warn!("Failed to iter object: {:#?}", e);
                    return false;
                }
            };

        match hummock_meta_client
            .report_full_scan_task(filtered_object_ids, unfiltered_count, unfiltered_size)
            .await
        {
            Ok(_) => {
                tracing::info!("Finished full scan SSTs");
            }
            Err(e) => {
                tracing::warn!("Failed to report full scan task: {:#?}", e);
                return false;
            }
        }
        true
    }

    /// Returns **filtered** object ids, and **unfiltered** total object count and size.
    pub async fn full_scan_inner(
        full_scan_task: FullScanTask,
        metadata_iter: ObjectMetadataIter,
    ) -> HummockResult<(Vec<HummockSstableObjectId>, u64, u64)> {
        let timestamp_watermark = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .sub(Duration::from_secs(full_scan_task.sst_retention_time_sec))
            .as_secs_f64();

        let mut total_object_count = 0;
        let mut total_object_size = 0;
        let filtered = metadata_iter
            .filter_map(|r| {
                let result = match r {
                    Ok(o) => {
                        total_object_count += 1;
                        total_object_size += o.total_size;
                        if o.last_modified < timestamp_watermark {
                            Some(Ok(SstableStore::get_object_id_from_path(&o.key)))
                        } else {
                            None
                        }
                    }
                    Err(e) => Some(Err(HummockError::from(e))),
                };
                async move { result }
            })
            .try_collect::<Vec<HummockSstableObjectId>>()
            .await?;
        Ok((
            filtered,
            total_object_count as u64,
            total_object_size as u64,
        ))
    }
}

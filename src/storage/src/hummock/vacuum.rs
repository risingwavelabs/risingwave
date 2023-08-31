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
    pub async fn handle_vacuum_task(
        sstable_store: SstableStoreRef,
        sstable_object_ids: &[u64],
    ) -> HummockResult<()> {
        tracing::info!("Try to vacuum SSTs {:?}", sstable_object_ids);
        sstable_store.delete_list(sstable_object_ids).await?;
        Ok(())
    }

    pub async fn report_vacuum_task(
        vacuum_task: VacuumTask,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> bool {
        match hummock_meta_client.report_vacuum_task(vacuum_task).await {
            Ok(_) => {
                tracing::info!("Finished vacuuming SSTs");
            }
            Err(e) => {
                tracing::warn!("Failed to report vacuum task: {:#?}", e);
                return false;
            }
        }
        true
    }

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

    /// Returns **filtered** object ids, and **unfiltered** total object count and size.
    pub async fn handle_full_scan_task(
        full_scan_task: FullScanTask,
        sstable_store: SstableStoreRef,
    ) -> HummockResult<(Vec<HummockSstableObjectId>, u64, u64)> {
        tracing::info!(
            "Try to full scan SSTs with timestamp {}",
            full_scan_task.sst_retention_time_sec
        );
        let metadata_iter = sstable_store
            .list_object_metadata_from_object_store()
            .await?;
        Vacuum::full_scan_inner(full_scan_task, metadata_iter).await
    }

    pub async fn report_full_scan_task(
        filtered_object_ids: Vec<u64>,
        unfiltered_count: u64,
        unfiltered_size: u64,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> bool {
        tracing::info!("Try to report full scan task",);
        tracing::info!(
            "filtered_object_ids length =  {}, unfiltered_count = {}, unfiltered_size = {}",
            filtered_object_ids.len(),
            unfiltered_count,
            unfiltered_size
        );
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
}

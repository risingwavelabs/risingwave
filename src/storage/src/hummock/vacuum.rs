// Copyright 2024 RisingWave Labs
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

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::ObjectMetadataIter;
use risingwave_pb::hummock::{FullScanTask, VacuumTask};
use risingwave_rpc_client::HummockMetaClient;
use thiserror_ext::AsReport;

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
        tracing::info!("try to vacuum SSTs {:?}", sstable_object_ids);
        sstable_store.delete_list(sstable_object_ids).await?;
        Ok(())
    }

    pub async fn report_vacuum_task(
        vacuum_task: VacuumTask,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> bool {
        match hummock_meta_client.report_vacuum_task(vacuum_task).await {
            Ok(_) => {
                tracing::info!("vacuuming SSTs succeeded");
            }
            Err(e) => {
                tracing::warn!(error = %e.as_report(), "failed to report vacuum task");
                return false;
            }
        }
        true
    }

    pub async fn full_scan_inner(
        full_scan_task: FullScanTask,
        metadata_iter: ObjectMetadataIter,
    ) -> HummockResult<(Vec<HummockSstableObjectId>, u64, u64, Option<String>)> {
        let mut total_object_count = 0;
        let mut total_object_size = 0;
        let mut next_start_after: Option<String> = None;
        let filtered = metadata_iter
            .filter_map(|r| {
                let result = match r {
                    Ok(o) => {
                        total_object_count += 1;
                        total_object_size += o.total_size;
                        // Determine if the LIST has been truncated.
                        // A false positives would at most cost one additional LIST later.
                        if let Some(limit) = full_scan_task.limit
                            && limit == total_object_count
                        {
                            next_start_after = Some(o.key.clone());
                            tracing::debug!(next_start_after, "set next start after");
                        }
                        if o.last_modified < full_scan_task.sst_retention_watermark as f64 {
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
            total_object_count,
            total_object_size as u64,
            next_start_after,
        ))
    }

    /// Returns **filtered** object ids, and **unfiltered** total object count and size.
    pub async fn handle_full_scan_task(
        full_scan_task: FullScanTask,
        sstable_store: SstableStoreRef,
    ) -> HummockResult<(Vec<HummockSstableObjectId>, u64, u64, Option<String>)> {
        tracing::info!(
            sst_retention_watermark = full_scan_task.sst_retention_watermark,
            prefix = full_scan_task.prefix.as_ref().unwrap_or(&String::from("")),
            start_after = full_scan_task.start_after,
            limit = full_scan_task.limit,
            "try to full scan SSTs"
        );
        let metadata_iter = sstable_store
            .list_object_metadata_from_object_store(
                full_scan_task.prefix.clone(),
                full_scan_task.start_after.clone(),
                full_scan_task.limit.map(|i| i as usize),
            )
            .await?;
        Vacuum::full_scan_inner(full_scan_task, metadata_iter).await
    }

    pub async fn report_full_scan_task(
        filtered_object_ids: Vec<u64>,
        unfiltered_count: u64,
        unfiltered_size: u64,
        start_after: Option<String>,
        next_start_after: Option<String>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> bool {
        tracing::info!(
            filtered_object_ids_len = filtered_object_ids.len(),
            unfiltered_count,
            unfiltered_size,
            start_after,
            next_start_after,
            "try to report full scan task"
        );
        match hummock_meta_client
            .report_full_scan_task(
                filtered_object_ids,
                unfiltered_count,
                unfiltered_size,
                start_after,
                next_start_after,
            )
            .await
        {
            Ok(_) => {
                tracing::info!("full scan SSTs succeeded");
            }
            Err(e) => {
                tracing::warn!(error = %e.as_report(), "failed to report full scan task");
                return false;
            }
        }
        true
    }
}

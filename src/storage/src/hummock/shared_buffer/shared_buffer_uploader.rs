// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManagerRef;
use risingwave_hummock_sdk::{HummockEpoch, LocalSstableInfo};
use risingwave_rpc_client::HummockMetaClient;

use crate::hummock::compaction_executor::CompactionExecutor;
use crate::hummock::compactor::{Compactor, CompactorContext};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::shared_buffer::OrderSortedUncommittedData;
use crate::hummock::{HummockResult, MemoryLimiter, SstableIdManagerRef, SstableStoreRef};
use crate::monitor::StateStoreMetrics;

pub(crate) type UploadTaskPayload = OrderSortedUncommittedData;
pub(crate) type UploadTaskResult = HummockResult<Vec<LocalSstableInfo>>;

pub struct SharedBufferUploader {
    options: Arc<StorageConfig>,
    write_conflict_detector: Option<Arc<ConflictDetector>>,

    sstable_store: SstableStoreRef,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    stats: Arc<StateStoreMetrics>,
    compaction_executor: Option<Arc<CompactionExecutor>>,
    local_object_store_compactor_context: Arc<CompactorContext>,
    remote_object_store_compactor_context: Arc<CompactorContext>,
}

impl SharedBufferUploader {
    pub fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        stats: Arc<StateStoreMetrics>,
        write_conflict_detector: Option<Arc<ConflictDetector>>,
        sstable_id_manager: SstableIdManagerRef,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> Self {
        let compaction_executor = if options.share_buffer_compaction_worker_threads_number == 0 {
            None
        } else {
            Some(Arc::new(CompactionExecutor::new(Some(
                options.share_buffer_compaction_worker_threads_number as usize,
            ))))
        };
        // not limit memory for uploader
        let memory_limiter = Arc::new(MemoryLimiter::new(u64::MAX - 1));
        let local_object_store_compactor_context = Arc::new(CompactorContext {
            options: options.clone(),
            hummock_meta_client: hummock_meta_client.clone(),
            sstable_store: sstable_store.clone(),
            stats: stats.clone(),
            is_share_buffer_compact: true,
            compaction_executor: compaction_executor.as_ref().cloned(),
            filter_key_extractor_manager: filter_key_extractor_manager.clone(),
            memory_limiter: memory_limiter.clone(),
            sstable_id_manager: sstable_id_manager.clone(),
        });
        let remote_object_store_compactor_context = Arc::new(CompactorContext {
            options: options.clone(),
            hummock_meta_client: hummock_meta_client.clone(),
            sstable_store: sstable_store.clone(),
            stats: stats.clone(),
            is_share_buffer_compact: true,
            compaction_executor: compaction_executor.as_ref().cloned(),
            filter_key_extractor_manager,
            memory_limiter,
            sstable_id_manager,
        });
        Self {
            options,
            write_conflict_detector,
            sstable_store,
            hummock_meta_client,
            stats,
            compaction_executor,
            local_object_store_compactor_context,
            remote_object_store_compactor_context,
        }
    }
}

impl SharedBufferUploader {
    pub async fn flush(
        &self,
        epoch: HummockEpoch,
        is_local: bool,
        payload: UploadTaskPayload,
    ) -> HummockResult<Vec<LocalSstableInfo>> {
        if payload.is_empty() {
            return Ok(vec![]);
        }

        // Compact buffers into SSTs
        let mem_compactor_ctx = if is_local {
            self.local_object_store_compactor_context.clone()
        } else {
            self.remote_object_store_compactor_context.clone()
        };

        // Set a watermark SST id for this epoch to prevent full GC from accidentally deleting SSTs
        // for in-progress write op. The watermark is invalidated when the epoch is
        // committed or cancelled.
        mem_compactor_ctx
            .sstable_id_manager
            .add_watermark_sst_id(Some(epoch))
            .await?;

        let tables =
            Compactor::compact_shared_buffer_by_compaction_group(mem_compactor_ctx, payload)
                .await?;

        let uploaded_sst_info = tables.into_iter().collect();

        // TODO: re-enable conflict detector after we have a better way to determine which actor
        // writes the batch. if let Some(detector) = &self.write_conflict_detector {
        //     for data_list in payload {
        //         for data in data_list {
        //             if let UncommittedData::Batch(batch) = data {
        //                 detector.check_conflict_and_track_write_batch(batch.get_payload(),
        // epoch);             }
        //         }
        //     }
        // }

        Ok(uploaded_sst_info)
    }
}

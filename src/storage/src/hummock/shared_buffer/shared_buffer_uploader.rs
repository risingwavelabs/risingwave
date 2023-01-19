// Copyright 2023 Singularity Data
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

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, LocalSstableInfo};

use crate::hummock::compactor::{compact, CompactorContext};
use crate::hummock::shared_buffer::OrderSortedUncommittedData;
use crate::hummock::HummockResult;

pub(crate) type UploadTaskPayload = OrderSortedUncommittedData;
pub(crate) type UploadTaskResult = HummockResult<Vec<LocalSstableInfo>>;

pub struct SharedBufferUploader {
    compactor_context: Arc<CompactorContext>,
}

impl SharedBufferUploader {
    pub fn new(compactor_context: Arc<CompactorContext>) -> Self {
        Self { compactor_context }
    }
}

impl SharedBufferUploader {
    pub async fn flush(
        &self,
        payload: UploadTaskPayload,
        epoch: HummockEpoch,
        compaction_group_index: Arc<HashMap<TableId, CompactionGroupId>>,
    ) -> HummockResult<Vec<LocalSstableInfo>> {
        if payload.is_empty() {
            return Ok(vec![]);
        }

        // Compact buffers into SSTs
        let mem_compactor_ctx = self.compactor_context.clone();

        // Set a watermark SST id for this epoch to prevent full GC from accidentally deleting SSTs
        // for in-progress write op. The watermark is invalidated when the epoch is
        // committed or cancelled.
        mem_compactor_ctx
            .sstable_id_manager
            .add_watermark_sst_id(Some(epoch))
            .await?;

        let tables = compact(mem_compactor_ctx, payload, compaction_group_index).await?;

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

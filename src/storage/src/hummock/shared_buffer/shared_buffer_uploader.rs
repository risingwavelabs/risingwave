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

use std::collections::BTreeMap;
use std::sync::Arc;

use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::SstableInfo;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use super::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::compactor::{Compactor, CompactorContext};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::{HummockError, HummockResult, SstableStoreRef};
use crate::monitor::StateStoreMetrics;

pub(crate) type UploadTaskId = u64;
pub(crate) type UploadTaskPayload = Vec<SharedBufferBatch>;
pub(crate) type UploadTaskResult =
    BTreeMap<(HummockEpoch, UploadTaskId), HummockResult<Vec<SstableInfo>>>;

#[derive(Debug)]
pub struct UploadTask {
    pub(crate) id: UploadTaskId,
    pub(crate) epoch: HummockEpoch,
    pub(crate) payload: UploadTaskPayload,
}

impl UploadTask {
    pub fn new(id: UploadTaskId, epoch: HummockEpoch, payload: UploadTaskPayload) -> Self {
        Self { id, epoch, payload }
    }
}

pub struct UploadItem {
    pub(crate) tasks: Vec<UploadTask>,
    pub(crate) notifier: oneshot::Sender<UploadTaskResult>,
}

impl UploadItem {
    pub fn new(tasks: Vec<UploadTask>, notifier: oneshot::Sender<UploadTaskResult>) -> Self {
        Self { tasks, notifier }
    }
}

pub struct SharedBufferUploader {
    options: Arc<StorageConfig>,
    write_conflict_detector: Option<Arc<ConflictDetector>>,

    uploader_rx: mpsc::UnboundedReceiver<UploadItem>,

    sstable_store: SstableStoreRef,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    stats: Arc<StateStoreMetrics>,
}

impl SharedBufferUploader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        uploader_rx: mpsc::UnboundedReceiver<UploadItem>,
        stats: Arc<StateStoreMetrics>,
        write_conflict_detector: Option<Arc<ConflictDetector>>,
    ) -> Self {
        Self {
            options,
            write_conflict_detector,
            uploader_rx,
            sstable_store,
            hummock_meta_client,
            stats,
        }
    }

    pub async fn run(&mut self) -> HummockResult<()> {
        loop {
            match self.run_inner().await {
                Ok(()) => return Ok(()),
                Err(e) => error!("error raised in shared buffer uploader: {:?}", e),
            }
        }
    }
}

impl SharedBufferUploader {
    async fn run_inner(&mut self) -> HummockResult<()> {
        while let Some(item) = self.uploader_rx.recv().await {
            let mut task_results = BTreeMap::new();
            let mut failed = false;
            for UploadTask {
                id: task_id,
                epoch,
                payload,
            } in item.tasks
            {
                // If a previous task failed, this task will also fail
                if failed {
                    task_results.insert(
                        (epoch, task_id),
                        Err(HummockError::shared_buffer_error(
                            "failed due to previous failure",
                        )),
                    );
                } else {
                    match self.flush(epoch, &payload).await {
                        Ok(tables) => {
                            task_results.insert((epoch, task_id), Ok(tables));
                        }
                        Err(e) => {
                            failed = true;
                            task_results.insert((epoch, task_id), Err(e));
                        }
                    }
                };
            }
            item.notifier
                .send(task_results)
                .map_err(|_| HummockError::shared_buffer_error("failed to send result"))?;
        }
        Ok(())
    }

    async fn flush(
        &mut self,
        epoch: HummockEpoch,
        batches: &Vec<SharedBufferBatch>,
    ) -> HummockResult<Vec<SstableInfo>> {
        if batches.is_empty() {
            return Ok(vec![]);
        }

        if let Some(detector) = &self.write_conflict_detector {
            for batch in batches {
                detector.check_conflict_and_track_write_batch(batch.get_payload(), epoch);
            }
        }

        // Compact buffers into SSTs
        let mem_compactor_ctx = CompactorContext {
            options: self.options.clone(),
            hummock_meta_client: self.hummock_meta_client.clone(),
            sstable_store: self.sstable_store.clone(),
            stats: self.stats.clone(),
            is_share_buffer_compact: true,
        };

        let tables = Compactor::compact_shared_buffer(
            Arc::new(mem_compactor_ctx),
            batches,
            self.stats.clone(),
        )
        .await?;

        let uploaded_sst_info: Vec<SstableInfo> = tables
            .iter()
            .map(|(sst, vnode_bitmap)| SstableInfo {
                id: sst.id,
                key_range: Some(risingwave_pb::hummock::KeyRange {
                    left: sst.meta.smallest_key.clone(),
                    right: sst.meta.largest_key.clone(),
                    inf: false,
                }),
                file_size: sst.meta.estimated_size as u64,
                vnode_bitmap: vnode_bitmap.clone(),
            })
            .collect();

        // Add all tables at once.
        self.hummock_meta_client
            .add_tables(epoch, uploaded_sst_info.clone())
            .await
            .map_err(HummockError::meta_error)?;

        Ok(uploaded_sst_info)
    }
}

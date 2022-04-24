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

use itertools::Itertools;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::SstableInfo;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use super::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::compactor::{Compactor, CompactorContext};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::{HummockError, HummockResult, SstableStoreRef};
use crate::monitor::StateStoreMetrics;

#[derive(Debug)]
pub enum UploadItem {
    Batch(SharedBufferBatch),
    Flush,
    Sync {
        epoch: Option<HummockEpoch>,
        notifier: oneshot::Sender<()>,
    },
}

pub struct SharedBufferUploader {
    options: Arc<StorageConfig>,

    size: usize,
    threshold: usize,

    /// Serve as for not-uploaded batches.
    batches: BTreeMap<HummockEpoch, Vec<SharedBufferBatch>>,

    /// For conflict key detection. Enabled by setting `write_conflict_detection_enabled` to true
    /// in `StorageConfig`
    write_conflict_detector: Option<Arc<ConflictDetector>>,

    uploader_rx: mpsc::UnboundedReceiver<UploadItem>,

    sstable_store: SstableStoreRef,
    local_version_manager: Arc<LocalVersionManager>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    stats: Arc<StateStoreMetrics>,
}

impl SharedBufferUploader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        options: Arc<StorageConfig>,
        threshold: usize,
        sstable_store: SstableStoreRef,
        local_version_manager: Arc<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        uploader_rx: mpsc::UnboundedReceiver<UploadItem>,
        stats: Arc<StateStoreMetrics>,
    ) -> Self {
        Self {
            threshold,
            write_conflict_detector: if options.write_conflict_detection_enabled {
                Some(Arc::new(ConflictDetector::new()))
            } else {
                None
            },
            uploader_rx,
            batches: BTreeMap::default(),
            size: 0,
            sstable_store,
            local_version_manager,
            hummock_meta_client,
            stats,
            options,
        }
    }

    pub async fn run(&mut self) -> HummockResult<()> {
        loop {
            match self.run_inner().await {
                Ok(()) => return Ok(()),
                Err(e) => error!("error raised in shared buffer uploader: {}", e),
            }
        }
    }
}

impl SharedBufferUploader {
    async fn run_inner(&mut self) -> HummockResult<()> {
        while let Some(item) = self.uploader_rx.recv().await {
            match item {
                UploadItem::Batch(batch) => {
                    if let Some(detector) = &self.write_conflict_detector {
                        detector.check_conflict_and_track_write_batch(&batch.inner, batch.epoch);
                    }

                    self.size += batch.size as usize;
                    self.batches
                        .entry(batch.epoch)
                        .or_insert(vec![])
                        .push(batch);
                    if self.size > self.threshold {
                        self.flush().await?;
                    }
                }
                UploadItem::Flush => self.flush().await?,
                UploadItem::Sync { epoch, notifier } => {
                    match epoch {
                        Some(epoch) => self.flush_epoch(epoch).await?,
                        None => self.flush().await?,
                    }
                    notifier.send(()).map_err(|_| {
                        HummockError::shared_buffer_error(format!(
                            "error raised when noitfy sync epoch {:?}",
                            epoch
                        ))
                    })?;
                }
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> HummockResult<()> {
        let epochs = self.batches.keys().copied().collect_vec();
        for epoch in epochs {
            self.flush_epoch(epoch).await?;
        }
        Ok(())
    }

    async fn flush_epoch(&mut self, epoch: HummockEpoch) -> HummockResult<()> {
        if let Some(detector) = &self.write_conflict_detector {
            detector.archive_epoch(epoch);
        }

        let batches = match self.batches.remove(&epoch) {
            Some(batches) => batches,
            None => return Ok(()),
        };

        let flush_size: usize = batches.iter().map(|batch| batch.size as usize).sum();

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
            batches.clone(),
            self.stats.clone(),
        )
        .await?;

        let uploaded_sst_info: Vec<SstableInfo> = tables
            .iter()
            .map(|sst| SstableInfo {
                id: sst.id,
                key_range: Some(risingwave_pb::hummock::KeyRange {
                    left: sst.meta.smallest_key.clone(),
                    right: sst.meta.largest_key.clone(),
                    inf: false,
                }),
            })
            .collect();

        // Add all tables at once.
        self.hummock_meta_client
            .add_tables(epoch, uploaded_sst_info.clone())
            .await
            .map_err(HummockError::meta_error)?;

        // Ensure the added data is available locally
        self.local_version_manager
            .update_uncommitted_ssts(epoch, uploaded_sst_info, batches);

        self.size -= flush_size;

        Ok(())
    }
}

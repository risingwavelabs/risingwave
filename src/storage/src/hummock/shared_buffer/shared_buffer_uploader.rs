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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use risingwave_common::config::StorageConfig;
use risingwave_pb::hummock::SstableInfo;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::Notify;

use crate::error::StorageResult;
use crate::hummock::compactor::{Compactor, CompactorContext};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::{HummockError, HummockResult, SstableStoreRef};
use crate::monitor::StateStoreMetrics;

#[derive(Debug)]
pub struct SyncItem {
    /// Epoch to sync. None means syncing all epochs.
    pub epoch: Option<u64>,
    /// Notifier to notify on sync finishes
    pub notifier: Option<tokio::sync::oneshot::Sender<HummockResult<usize>>>,
}

#[derive(Debug)]
pub enum SharedBufferUploaderItem {
    Batch(SharedBufferBatch),
    Sync(SyncItem),
    Reset(u64),
}

pub struct SharedBufferUploader {
    /// Batches to upload grouped by epoch
    batches_to_upload: BTreeMap<u64, Vec<SharedBufferBatch>>,
    local_version_manager: Arc<LocalVersionManager>,
    options: Arc<StorageConfig>,

    scheduled_size: Arc<AtomicUsize>,
    notify: Arc<Notify>,
    uploader_rx: tokio::sync::mpsc::UnboundedReceiver<SharedBufferUploaderItem>,

    hummock_meta_client: Arc<dyn HummockMetaClient>,
    sstable_store: SstableStoreRef,

    /// For conflict key detection. Enabled by setting `write_conflict_detection_enabled` to true
    /// in `StorageConfig`.ƒ
    write_conflict_detector: Option<Arc<ConflictDetector>>,

    /// Statistics.
    // TODO: separate `HummockStats` from `StateStoreMetrics`.
    stats: Arc<StateStoreMetrics>,
}

impl SharedBufferUploader {
    pub fn new(
        options: Arc<StorageConfig>,
        local_version_manager: Arc<LocalVersionManager>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        scheduled_size: Arc<AtomicUsize>,
        notify: Arc<Notify>,
        uploader_rx: tokio::sync::mpsc::UnboundedReceiver<SharedBufferUploaderItem>,
    ) -> Self {
        Self {
            batches_to_upload: BTreeMap::new(),
            options: options.clone(),
            local_version_manager,

            stats,
            hummock_meta_client,
            sstable_store,
            write_conflict_detector: if options.write_conflict_detection_enabled {
                Some(Arc::new(ConflictDetector::new()))
            } else {
                None
            },
            scheduled_size,
            notify,
            uploader_rx,
        }
    }

    /// Uploads buffer batches to S3.
    async fn flush(&mut self, epoch: Option<u64>) -> HummockResult<usize> {
        match epoch {
            // Sync a specific epoch
            Some(e) => self.flush_epoch(e).await,
            // Sync all epochs
            None => {
                let epochs = self.batches_to_upload.keys().copied().collect_vec();
                let mut total_size = 0;
                for epoch in epochs {
                    total_size += self.flush_epoch(epoch).await?;
                }
                Ok(total_size)
            }
        }
    }

    async fn flush_epoch(&mut self, epoch: u64) -> HummockResult<usize> {
        if let Some(detector) = &self.write_conflict_detector {
            detector.archive_epoch(epoch);
        }

        let buffers = match self.batches_to_upload.remove(&epoch) {
            Some(m) => m,
            None => return Ok(0),
        };

        let flush_size = buffers.iter().map(|batch| batch.len()).sum();

        self.scheduled_size.fetch_add(flush_size, Ordering::Release);

        // Compact buffers into SSTs
        let mem_compactor_ctx = CompactorContext {
            options: self.options.clone(),
            local_version_manager: self.local_version_manager.clone(),
            hummock_meta_client: self.hummock_meta_client.clone(),
            sstable_store: self.sstable_store.clone(),
            stats: self.stats.clone(),
            is_share_buffer_compact: true,
        };

        let tables = Compactor::compact_shared_buffer(
            Arc::new(mem_compactor_ctx),
            buffers,
            self.stats.clone(),
        )
        .await?;

        // Add all tables at once.
        let version = self
            .hummock_meta_client
            .add_tables(
                epoch,
                tables
                    .iter()
                    .map(|sst| SstableInfo {
                        id: sst.id,
                        key_range: Some(risingwave_pb::hummock::KeyRange {
                            left: sst.meta.smallest_key.clone(),
                            right: sst.meta.largest_key.clone(),
                            inf: false,
                        }),
                    })
                    .collect(),
            )
            .await
            .map_err(HummockError::meta_error)?;

        // Ensure the added data is available locally
        // RIVIEW ME: need assert?
        self.local_version_manager.try_set_version(version);

        Ok(flush_size)
    }

    async fn handle(&mut self, item: SharedBufferUploaderItem) -> StorageResult<()> {
        match item {
            SharedBufferUploaderItem::Batch(batch) => {
                if let Some(detector) = &self.write_conflict_detector {
                    detector.check_conflict_and_track_write_batch(&batch.inner, batch.epoch);
                }

                self.batches_to_upload
                    .entry(batch.epoch())
                    .or_insert(Vec::new())
                    .push(batch);
            }
            SharedBufferUploaderItem::Sync(sync_item) => {
                // Flush buffer to S3.
                let res = self.flush(sync_item.epoch).await;
                // Notify sync notifier.
                if let Some(tx) = sync_item.notifier {
                    tx.send(res).map_err(|_| {
                        HummockError::shared_buffer_error(
                            "Failed to notify shared buffer sync because of send drop",
                        )
                    })?;
                }
            }
            SharedBufferUploaderItem::Reset(epoch) => {
                self.batches_to_upload.remove(&epoch);
            }
        }
        Ok(())
    }

    pub async fn run(mut self) -> StorageResult<()> {
        let mut ticker = tokio::time::interval(Duration::from_millis(
            self.options.shared_buffer_upload_timeout as u64,
        ));
        loop {
            tokio::select! {
                Some(item) = self.uploader_rx.recv() => self.handle(item).await?,
                _ = ticker.tick() => {self.flush(None).await?;}
                _ = self.notify.notified() => {self.flush(None).await?;}
                else => break,
            }
        }
        Ok(())
    }
}

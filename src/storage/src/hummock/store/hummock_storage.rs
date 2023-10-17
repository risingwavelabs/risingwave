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

use std::future::Future;
use std::ops::{Bound, Deref};
use std::sync::atomic::{AtomicU64, Ordering as MemOrdering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use itertools::Itertools;
use more_asserts::assert_gt;
use risingwave_common::catalog::TableId;
use risingwave_common_service::observer_manager::{NotificationClient, ObserverManager};
use risingwave_hummock_sdk::key::{TableKey, TableKeyRange};
use risingwave_hummock_sdk::HummockReadEpoch;
#[cfg(any(test, feature = "test"))]
use risingwave_pb::hummock::HummockVersion;
use risingwave_pb::hummock::{version_update_payload, SstableInfo};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot;
use tracing::log::error;

use super::local_hummock_storage::{HummockStorageIterator, LocalHummockStorage};
use super::version::{CommittedVersion, HummockVersionReader};
use crate::error::StorageResult;
use crate::filter_key_extractor::{FilterKeyExtractorManager, RpcFilterKeyExtractorManager};
use crate::hummock::backup_reader::{BackupReader, BackupReaderRef};
use crate::hummock::compactor::CompactorContext;
use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
use crate::hummock::event_handler::refiller::CacheRefillConfig;
use crate::hummock::event_handler::{HummockEvent, HummockEventHandler, ReadVersionMappingType};
use crate::hummock::local_version::pinned_version::{start_pinned_version_worker, PinnedVersion};
use crate::hummock::observer_manager::HummockObserverNode;
use crate::hummock::store::version::read_filter_for_batch;
use crate::hummock::utils::{validate_safe_epoch, wait_for_epoch};
use crate::hummock::write_limiter::{WriteLimiter, WriteLimiterRef};
use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, MemoryLimiter, SstableObjectIdManager,
    SstableObjectIdManagerRef, SstableStoreRef,
};
use crate::mem_table::ImmutableMemtable;
use crate::monitor::{CompactorMetrics, HummockStateStoreMetrics, StoreLocalStatistic};
use crate::opts::StorageOpts;
use crate::store::*;
use crate::StateStore;

struct HummockStorageShutdownGuard {
    shutdown_sender: UnboundedSender<HummockEvent>,
}

impl Drop for HummockStorageShutdownGuard {
    fn drop(&mut self) {
        let _ = self
            .shutdown_sender
            .send(HummockEvent::Shutdown)
            .inspect_err(|e| error!("unable to send shutdown: {:?}", e));
    }
}

/// `HummockStorage` is the entry point of the Hummock state store backend.
/// It implements the `StateStore` and `StateStoreRead` traits but not the `StateStoreWrite` trait
/// since all writes should be done via `LocalHummockStorage` to ensure the single writer property
/// of hummock. `LocalHummockStorage` instance can be created via `new_local` call.
/// Hummock is the state store backend.
#[derive(Clone)]
pub struct HummockStorage {
    hummock_event_sender: UnboundedSender<HummockEvent>,

    context: CompactorContext,

    filter_key_extractor_manager: FilterKeyExtractorManager,

    sstable_object_id_manager: SstableObjectIdManagerRef,

    buffer_tracker: BufferTracker,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<HummockEpoch>>,

    seal_epoch: Arc<AtomicU64>,

    pinned_version: Arc<ArcSwap<PinnedVersion>>,

    hummock_version_reader: HummockVersionReader,

    _shutdown_guard: Arc<HummockStorageShutdownGuard>,

    read_version_mapping: Arc<ReadVersionMappingType>,

    backup_reader: BackupReaderRef,

    /// current_epoch < min_current_epoch cannot be read.
    min_current_epoch: Arc<AtomicU64>,

    write_limiter: WriteLimiterRef,
}

impl HummockStorage {
    /// Creates a [`HummockStorage`].
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        options: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
        filter_key_extractor_manager: Arc<RpcFilterKeyExtractorManager>,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        compactor_metrics: Arc<CompactorMetrics>,
    ) -> HummockResult<Self> {
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            options.sstable_id_remote_fetch_number,
        ));
        let backup_reader = BackupReader::new(
            &options.backup_storage_url,
            &options.backup_storage_directory,
        )
        .await
        .map_err(HummockError::read_backup_error)?;
        let write_limiter = Arc::new(WriteLimiter::default());
        let (event_tx, mut event_rx) = unbounded_channel();

        let observer_manager = ObserverManager::new(
            notification_client,
            HummockObserverNode::new(
                filter_key_extractor_manager.clone(),
                backup_reader.clone(),
                event_tx.clone(),
                write_limiter.clone(),
            ),
        )
        .await;
        observer_manager.start().await;

        let hummock_version = match event_rx.recv().await {
            Some(HummockEvent::VersionUpdate(version_update_payload::Payload::PinnedVersion(version))) => version,
            _ => unreachable!("the hummock observer manager is the first one to take the event tx. Should be full hummock version")
        };

        let (pin_version_tx, pin_version_rx) = unbounded_channel();
        let pinned_version = PinnedVersion::new(hummock_version, pin_version_tx);
        tokio::spawn(start_pinned_version_worker(
            pin_version_rx,
            hummock_meta_client.clone(),
        ));
        let filter_key_extractor_manager = FilterKeyExtractorManager::RpcFilterKeyExtractorManager(
            filter_key_extractor_manager.clone(),
        );
        let compactor_context = CompactorContext::new_local_compact_context(
            options.clone(),
            sstable_store.clone(),
            compactor_metrics.clone(),
        );

        let seal_epoch = Arc::new(AtomicU64::new(pinned_version.max_committed_epoch()));
        let min_current_epoch = Arc::new(AtomicU64::new(pinned_version.max_committed_epoch()));
        let hummock_event_handler = HummockEventHandler::new(
            event_tx.clone(),
            event_rx,
            pinned_version,
            compactor_context.clone(),
            filter_key_extractor_manager.clone(),
            sstable_object_id_manager.clone(),
            state_store_metrics.clone(),
            CacheRefillConfig {
                timeout: Duration::from_millis(options.cache_refill_timeout_ms),
                data_refill_levels: options
                    .cache_refill_data_refill_levels
                    .iter()
                    .copied()
                    .collect(),
                concurrency: options.cache_refill_concurrency,
                unit: options.cache_refill_unit,
                threshold: options.cache_refill_threshold,
            },
        );

        let instance = Self {
            context: compactor_context,
            filter_key_extractor_manager: filter_key_extractor_manager.clone(),
            sstable_object_id_manager,
            buffer_tracker: hummock_event_handler.buffer_tracker().clone(),
            version_update_notifier_tx: hummock_event_handler.version_update_notifier_tx(),
            seal_epoch,
            hummock_event_sender: event_tx.clone(),
            pinned_version: hummock_event_handler.pinned_version(),
            hummock_version_reader: HummockVersionReader::new(
                sstable_store,
                state_store_metrics.clone(),
            ),
            _shutdown_guard: Arc::new(HummockStorageShutdownGuard {
                shutdown_sender: event_tx,
            }),
            read_version_mapping: hummock_event_handler.read_version_mapping(),
            backup_reader,
            min_current_epoch,
            write_limiter,
        };

        tokio::spawn(hummock_event_handler.start_hummock_event_handler_worker());

        Ok(instance)
    }

    /// Gets the value of a specified `key` in the table specified in `read_options`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    /// if `key` has consistent hash virtual node value, then such value is stored in `value_meta`
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    async fn get_inner(
        &self,
        key: TableKey<Bytes>,
        epoch: HummockEpoch,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        let key_range = (Bound::Included(key.clone()), Bound::Included(key.clone()));

        let read_version_tuple = if read_options.read_version_from_backup {
            self.build_read_version_tuple_from_backup(epoch).await?
        } else {
            self.build_read_version_tuple(epoch, read_options.table_id, &key_range)?
        };

        self.hummock_version_reader
            .get(key, epoch, read_options, read_version_tuple)
            .await
    }

    async fn iter_inner(
        &self,
        key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<StreamTypeOfIter<HummockStorageIterator>> {
        let read_version_tuple = if read_options.read_version_from_backup {
            self.build_read_version_tuple_from_backup(epoch).await?
        } else {
            self.build_read_version_tuple(epoch, read_options.table_id, &key_range)?
        };

        self.hummock_version_reader
            .iter(key_range, epoch, read_options, read_version_tuple)
            .await
    }

    async fn build_read_version_tuple_from_backup(
        &self,
        epoch: u64,
    ) -> StorageResult<(Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion)> {
        match self.backup_reader.try_get_hummock_version(epoch).await {
            Ok(Some(backup_version)) => {
                validate_safe_epoch(backup_version.safe_epoch(), epoch)?;
                Ok((Vec::default(), Vec::default(), backup_version))
            }
            Ok(None) => Err(HummockError::read_backup_error(format!(
                "backup include epoch {} not found",
                epoch
            ))
            .into()),
            Err(e) => Err(e),
        }
    }

    fn build_read_version_tuple(
        &self,
        epoch: u64,
        table_id: TableId,
        key_range: &TableKeyRange,
    ) -> StorageResult<(Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion)> {
        let pinned_version = self.pinned_version.load();
        validate_safe_epoch(pinned_version.safe_epoch(), epoch)?;

        // check epoch if lower mce
        let read_version_tuple: (Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion) =
            if epoch <= pinned_version.max_committed_epoch() {
                // read committed_version directly without build snapshot
                (Vec::default(), Vec::default(), (**pinned_version).clone())
            } else {
                let read_version_vec = {
                    let read_guard = self.read_version_mapping.read();
                    read_guard
                        .get(&table_id)
                        .map(|v| {
                            v.values()
                                .filter(|v| !v.read_arc().is_replicated())
                                .cloned()
                                .collect_vec()
                        })
                        .unwrap_or_default()
                };

                // When the system has just started and no state has been created, the memory state
                // may be empty
                if read_version_vec.is_empty() {
                    (Vec::default(), Vec::default(), (**pinned_version).clone())
                } else {
                    let (imm_vec, sst_vec) =
                        read_filter_for_batch(epoch, table_id, key_range, read_version_vec)?;
                    let committed_version = (**pinned_version).clone();

                    (imm_vec, sst_vec, committed_version)
                }
            };

        Ok(read_version_tuple)
    }

    async fn new_local_inner(&self, option: NewLocalOptions) -> LocalHummockStorage {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.hummock_event_sender
            .send(HummockEvent::RegisterReadVersion {
                table_id: option.table_id,
                new_read_version_sender: tx,
                is_replicated: option.is_replicated,
            })
            .unwrap();

        let (basic_read_version, instance_guard) = rx.await.unwrap();
        let version_update_notifier_tx = self.version_update_notifier_tx.clone();
        LocalHummockStorage::new(
            instance_guard,
            basic_read_version,
            self.hummock_version_reader.clone(),
            self.hummock_event_sender.clone(),
            self.buffer_tracker.get_memory_limiter().clone(),
            self.write_limiter.clone(),
            option,
            version_update_notifier_tx,
        )
    }

    pub fn sstable_store(&self) -> SstableStoreRef {
        self.context.sstable_store.clone()
    }

    pub fn sstable_object_id_manager(&self) -> &SstableObjectIdManagerRef {
        &self.sstable_object_id_manager
    }

    pub fn filter_key_extractor_manager(&self) -> &FilterKeyExtractorManager {
        &self.filter_key_extractor_manager
    }

    pub fn get_memory_limiter(&self) -> Arc<MemoryLimiter> {
        self.buffer_tracker.get_memory_limiter().clone()
    }

    pub fn get_pinned_version(&self) -> PinnedVersion {
        self.pinned_version.load().deref().deref().clone()
    }

    pub fn backup_reader(&self) -> BackupReaderRef {
        self.backup_reader.clone()
    }
}

impl StateStoreRead for HummockStorage {
    type IterStream = StreamTypeOfIter<HummockStorageIterator>;

    fn get(
        &self,
        key: TableKey<Bytes>,
        epoch: u64,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Option<Bytes>>> + '_ {
        self.get_inner(key, epoch, read_options)
    }

    fn iter(
        &self,
        key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::IterStream>> + '_ {
        self.iter_inner(key_range, epoch, read_options)
    }
}

impl StateStore for HummockStorage {
    type Local = LocalHummockStorage;

    /// Waits until the local hummock version contains the epoch. If `wait_epoch` is `Current`,
    /// we will only check whether it is le `sealed_epoch` and won't wait.
    async fn try_wait_epoch(&self, wait_epoch: HummockReadEpoch) -> StorageResult<()> {
        self.validate_read_epoch(wait_epoch)?;
        let wait_epoch = match wait_epoch {
            HummockReadEpoch::Committed(epoch) => {
                assert_ne!(epoch, HummockEpoch::MAX, "epoch should not be u64::MAX");
                epoch
            }
            _ => return Ok(()),
        };
        wait_for_epoch(&self.version_update_notifier_tx, wait_epoch).await
    }

    async fn sync(&self, epoch: u64) -> StorageResult<SyncResult> {
        let (tx, rx) = oneshot::channel();
        self.hummock_event_sender
            .send(HummockEvent::AwaitSyncEpoch {
                new_sync_epoch: epoch,
                sync_result_sender: tx,
            })
            .expect("should send success");
        Ok(rx.await.expect("should wait success")?)
    }

    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
        // Update `seal_epoch` synchronously,
        // as `HummockEvent::SealEpoch` is handled asynchronously.
        let prev_epoch = self.seal_epoch.swap(epoch, MemOrdering::SeqCst);
        assert_gt!(epoch, prev_epoch);

        if is_checkpoint {
            let _ = self.min_current_epoch.compare_exchange(
                HummockEpoch::MAX,
                epoch,
                MemOrdering::SeqCst,
                MemOrdering::SeqCst,
            );
        }
        self.hummock_event_sender
            .send(HummockEvent::SealEpoch {
                epoch,
                is_checkpoint,
            })
            .expect("should send success");
        StoreLocalStatistic::flush_all();
    }

    async fn clear_shared_buffer(&self) -> StorageResult<()> {
        let (tx, rx) = oneshot::channel();
        self.hummock_event_sender
            .send(HummockEvent::Clear(tx))
            .expect("should send success");
        rx.await.expect("should wait success");

        let epoch = self.pinned_version.load().max_committed_epoch();
        self.min_current_epoch
            .store(HummockEpoch::MAX, MemOrdering::SeqCst);
        self.seal_epoch.store(epoch, MemOrdering::SeqCst);

        Ok(())
    }

    fn new_local(&self, option: NewLocalOptions) -> impl Future<Output = Self::Local> + Send + '_ {
        self.new_local_inner(option)
    }

    fn validate_read_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()> {
        if let HummockReadEpoch::Current(read_current_epoch) = epoch {
            assert_ne!(
                read_current_epoch,
                HummockEpoch::MAX,
                "epoch should not be u64::MAX"
            );
            let sealed_epoch = self.seal_epoch.load(MemOrdering::SeqCst);
            if read_current_epoch > sealed_epoch {
                tracing::warn!(
                    "invalid barrier read {} > max seal epoch {}",
                    read_current_epoch,
                    sealed_epoch
                );
                return Err(HummockError::read_current_epoch().into());
            }

            let min_current_epoch = self.min_current_epoch.load(MemOrdering::SeqCst);
            if read_current_epoch < min_current_epoch {
                tracing::warn!(
                    "invalid barrier read {} < min current epoch {}",
                    read_current_epoch,
                    min_current_epoch
                );
                return Err(HummockError::read_current_epoch().into());
            }
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test"))]
impl HummockStorage {
    pub async fn seal_and_sync_epoch(&self, epoch: u64) -> StorageResult<SyncResult> {
        self.seal_epoch(epoch, true);
        self.sync(epoch).await
    }

    /// Used in the compaction test tool
    pub async fn update_version_and_wait(&self, version: HummockVersion) {
        use tokio::task::yield_now;
        let version_id = version.id;
        self.hummock_event_sender
            .send(HummockEvent::VersionUpdate(
                version_update_payload::Payload::PinnedVersion(version),
            ))
            .unwrap();
        loop {
            if self.pinned_version.load().id() >= version_id {
                break;
            }

            yield_now().await
        }
    }

    pub async fn wait_version(&self, version: HummockVersion) {
        use tokio::task::yield_now;
        loop {
            if self.pinned_version.load().id() >= version.id {
                break;
            }

            yield_now().await
        }
    }

    pub fn get_shared_buffer_size(&self) -> usize {
        self.buffer_tracker.get_buffer_size()
    }

    pub async fn try_wait_epoch_for_test(&self, wait_epoch: u64) {
        let mut rx = self.version_update_notifier_tx.subscribe();
        while *(rx.borrow_and_update()) < wait_epoch {
            rx.changed().await.unwrap();
        }
    }

    /// Creates a [`HummockStorage`] with default stats. Should only be used by tests.
    pub async fn for_test(
        options: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
    ) -> HummockResult<Self> {
        Self::new(
            options,
            sstable_store,
            hummock_meta_client,
            notification_client,
            Arc::new(RpcFilterKeyExtractorManager::default()),
            Arc::new(HummockStateStoreMetrics::unused()),
            Arc::new(CompactorMetrics::unused()),
        )
        .await
    }

    pub fn storage_opts(&self) -> &Arc<StorageOpts> {
        &self.context.storage_opts
    }

    pub fn version_reader(&self) -> &HummockVersionReader {
        &self.hummock_version_reader
    }

    pub async fn wait_version_update(&self, old_id: u64) -> u64 {
        use tokio::task::yield_now;
        loop {
            let cur_id = self.pinned_version.load().id();
            if cur_id > old_id {
                return cur_id;
            }
            yield_now().await;
        }
    }
}

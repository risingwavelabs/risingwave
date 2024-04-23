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

use std::collections::{BTreeMap, HashMap};
use std::ops::DerefMut;
use std::pin::pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};

use arc_swap::ArcSwap;
use await_tree::InstrumentAwait;
use futures::FutureExt;
use itertools::Itertools;
use parking_lot::RwLock;
use prometheus::core::{AtomicU64, GenericGauge};
use prometheus::{Histogram, IntGauge};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::{HummockEpoch, SyncResult};
use thiserror_ext::AsReport;
use tokio::spawn;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace, warn};

use super::refiller::{CacheRefillConfig, CacheRefiller};
use super::{LocalInstanceGuard, LocalInstanceId, ReadVersionMappingType};
use crate::filter_key_extractor::FilterKeyExtractorManager;
use crate::hummock::compactor::{await_tree_key, compact, CompactorContext};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::event_handler::refiller::{CacheRefillerEvent, SpawnRefillTask};
use crate::hummock::event_handler::uploader::{
    default_spawn_merging_task, HummockUploader, SpawnMergingTask, SpawnUploadTask, SyncedData,
    UploadTaskInfo, UploadTaskOutput, UploadTaskPayload, UploaderEvent,
};
use crate::hummock::event_handler::{
    HummockEvent, HummockReadVersionRef, HummockVersionUpdate, ReadOnlyReadVersionMapping,
    ReadOnlyRwLockRef,
};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::store::version::{
    HummockReadVersion, StagingData, StagingSstableInfo, VersionUpdate,
};
use crate::hummock::utils::validate_table_key_range;
use crate::hummock::{
    HummockError, HummockResult, MemoryLimiter, SstableObjectIdManager, SstableStoreRef, TrackerId,
};
use crate::monitor::HummockStateStoreMetrics;
use crate::opts::StorageOpts;

#[derive(Clone)]
pub struct BufferTracker {
    flush_threshold: usize,
    global_buffer: Arc<MemoryLimiter>,
    global_upload_task_size: GenericGauge<AtomicU64>,
}

impl BufferTracker {
    pub fn from_storage_opts(
        config: &StorageOpts,
        global_upload_task_size: GenericGauge<AtomicU64>,
    ) -> Self {
        let capacity = config.shared_buffer_capacity_mb * (1 << 20);
        let flush_threshold = (capacity as f32 * config.shared_buffer_flush_ratio) as usize;
        assert!(
            flush_threshold < capacity,
            "flush_threshold {} should be less or equal to capacity {}",
            flush_threshold,
            capacity
        );
        Self::new(capacity, flush_threshold, global_upload_task_size)
    }

    pub fn new(
        capacity: usize,
        flush_threshold: usize,
        global_upload_task_size: GenericGauge<AtomicU64>,
    ) -> Self {
        assert!(capacity >= flush_threshold);
        Self {
            flush_threshold,
            global_buffer: Arc::new(MemoryLimiter::new(capacity as u64)),
            global_upload_task_size,
        }
    }

    pub fn for_test() -> Self {
        Self::from_storage_opts(
            &StorageOpts::default(),
            GenericGauge::new("test", "test").unwrap(),
        )
    }

    pub fn get_buffer_size(&self) -> usize {
        self.global_buffer.get_memory_usage() as usize
    }

    pub fn get_memory_limiter(&self) -> &Arc<MemoryLimiter> {
        &self.global_buffer
    }

    pub fn global_upload_task_size(&self) -> &GenericGauge<AtomicU64> {
        &self.global_upload_task_size
    }

    /// Return true when the buffer size minus current upload task size is still greater than the
    /// flush threshold.
    pub fn need_more_flush(&self) -> bool {
        self.get_buffer_size() > self.flush_threshold + self.global_upload_task_size.get() as usize
    }
}

#[derive(Clone)]
pub struct HummockEventSender {
    inner: UnboundedSender<HummockEvent>,
    event_count: IntGauge,
}

pub fn event_channel(event_count: IntGauge) -> (HummockEventSender, HummockEventReceiver) {
    let (tx, rx) = unbounded_channel();
    (
        HummockEventSender {
            inner: tx,
            event_count: event_count.clone(),
        },
        HummockEventReceiver {
            inner: rx,
            event_count,
        },
    )
}

impl HummockEventSender {
    pub fn send(&self, event: HummockEvent) -> Result<(), SendError<HummockEvent>> {
        self.inner.send(event)?;
        self.event_count.inc();
        Ok(())
    }
}

pub struct HummockEventReceiver {
    inner: UnboundedReceiver<HummockEvent>,
    event_count: IntGauge,
}

impl HummockEventReceiver {
    async fn recv(&mut self) -> Option<HummockEvent> {
        let event = self.inner.recv().await?;
        self.event_count.dec();
        Some(event)
    }
}

struct HummockEventHandlerMetrics {
    event_handler_on_sync_finish_latency: Histogram,
    event_handler_on_spilled_latency: Histogram,
    event_handler_on_apply_version_update: Histogram,
    event_handler_on_recv_version_update: Histogram,
}

pub struct HummockEventHandler {
    hummock_event_tx: HummockEventSender,
    hummock_event_rx: HummockEventReceiver,
    version_update_rx: UnboundedReceiver<HummockVersionUpdate>,
    pending_sync_requests: BTreeMap<HummockEpoch, oneshot::Sender<HummockResult<SyncResult>>>,
    read_version_mapping: Arc<RwLock<ReadVersionMappingType>>,
    /// A copy of `read_version_mapping` but owned by event handler
    local_read_version_mapping: HashMap<LocalInstanceId, HummockReadVersionRef>,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<HummockEpoch>>,
    pinned_version: Arc<ArcSwap<PinnedVersion>>,
    write_conflict_detector: Option<Arc<ConflictDetector>>,

    uploader: HummockUploader,
    refiller: CacheRefiller,

    last_instance_id: LocalInstanceId,

    sstable_object_id_manager: Option<Arc<SstableObjectIdManager>>,
    metrics: HummockEventHandlerMetrics,
}

async fn flush_imms(
    payload: UploadTaskPayload,
    task_info: UploadTaskInfo,
    compactor_context: CompactorContext,
    filter_key_extractor_manager: FilterKeyExtractorManager,
    sstable_object_id_manager: Arc<SstableObjectIdManager>,
) -> HummockResult<UploadTaskOutput> {
    for epoch in &task_info.epochs {
        let _ = sstable_object_id_manager
            .add_watermark_object_id(Some(*epoch))
            .await
            .inspect_err(|e| {
                error!(epoch, error = %e.as_report(), "unable to set watermark sst id");
            });
    }
    compact(
        compactor_context,
        sstable_object_id_manager,
        payload,
        task_info.compaction_group_index,
        filter_key_extractor_manager,
    )
    .verbose_instrument_await("shared_buffer_compact")
    .await
}

impl HummockEventHandler {
    pub fn new(
        version_update_rx: UnboundedReceiver<HummockVersionUpdate>,
        pinned_version: PinnedVersion,
        compactor_context: CompactorContext,
        filter_key_extractor_manager: FilterKeyExtractorManager,
        sstable_object_id_manager: Arc<SstableObjectIdManager>,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
    ) -> Self {
        let upload_compactor_context = compactor_context.clone();
        let cloned_sstable_object_id_manager = sstable_object_id_manager.clone();
        let upload_task_latency = state_store_metrics.uploader_upload_task_latency.clone();
        let wait_poll_latency = state_store_metrics.uploader_wait_poll_latency.clone();
        Self::new_inner(
            version_update_rx,
            pinned_version,
            Some(sstable_object_id_manager),
            compactor_context.sstable_store.clone(),
            state_store_metrics,
            &compactor_context.storage_opts,
            Arc::new(move |payload, task_info| {
                static NEXT_UPLOAD_TASK_ID: LazyLock<AtomicUsize> =
                    LazyLock::new(|| AtomicUsize::new(0));
                let tree_root = upload_compactor_context.await_tree_reg.as_ref().map(|reg| {
                    let upload_task_id = NEXT_UPLOAD_TASK_ID.fetch_add(1, Relaxed);
                    reg.register(
                        await_tree_key::SpawnUploadTask { id: upload_task_id },
                        format!("Spawn Upload Task: {}", task_info),
                    )
                });
                let upload_task_latency = upload_task_latency.clone();
                let wait_poll_latency = wait_poll_latency.clone();
                let upload_compactor_context = upload_compactor_context.clone();
                let filter_key_extractor_manager = filter_key_extractor_manager.clone();
                let sstable_object_id_manager = cloned_sstable_object_id_manager.clone();
                spawn({
                    let future = async move {
                        let _timer = upload_task_latency.start_timer();
                        let mut output = flush_imms(
                            payload,
                            task_info,
                            upload_compactor_context.clone(),
                            filter_key_extractor_manager.clone(),
                            sstable_object_id_manager.clone(),
                        )
                        .await?;
                        assert!(
                            output
                                .wait_poll_timer
                                .replace(wait_poll_latency.start_timer())
                                .is_none(),
                            "should not set timer before"
                        );
                        Ok(output)
                    };
                    if let Some(tree_root) = tree_root {
                        tree_root.instrument(future).left_future()
                    } else {
                        future.right_future()
                    }
                })
            }),
            default_spawn_merging_task(
                compactor_context.compaction_executor.clone(),
                compactor_context.await_tree_reg.clone(),
            ),
            CacheRefiller::default_spawn_refill_task(),
        )
    }

    fn new_inner(
        version_update_rx: UnboundedReceiver<HummockVersionUpdate>,
        pinned_version: PinnedVersion,
        sstable_object_id_manager: Option<Arc<SstableObjectIdManager>>,
        sstable_store: SstableStoreRef,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        storage_opts: &StorageOpts,
        spawn_upload_task: SpawnUploadTask,
        spawn_merging_task: SpawnMergingTask,
        spawn_refill_task: SpawnRefillTask,
    ) -> Self {
        let (hummock_event_tx, hummock_event_rx) =
            event_channel(state_store_metrics.event_handler_pending_event.clone());
        let (version_update_notifier_tx, _) =
            tokio::sync::watch::channel(pinned_version.max_committed_epoch());
        let version_update_notifier_tx = Arc::new(version_update_notifier_tx);
        let read_version_mapping = Arc::new(RwLock::new(HashMap::default()));
        let buffer_tracker = BufferTracker::from_storage_opts(
            storage_opts,
            state_store_metrics.uploader_uploading_task_size.clone(),
        );
        let write_conflict_detector = ConflictDetector::new_from_config(storage_opts);

        let metrics = HummockEventHandlerMetrics {
            event_handler_on_sync_finish_latency: state_store_metrics
                .event_handler_latency
                .with_label_values(&["on_sync_finish"]),
            event_handler_on_spilled_latency: state_store_metrics
                .event_handler_latency
                .with_label_values(&["on_spilled"]),
            event_handler_on_apply_version_update: state_store_metrics
                .event_handler_latency
                .with_label_values(&["apply_version"]),
            event_handler_on_recv_version_update: state_store_metrics
                .event_handler_latency
                .with_label_values(&["recv_version_update"]),
        };

        let uploader = HummockUploader::new(
            state_store_metrics,
            pinned_version.clone(),
            spawn_upload_task,
            spawn_merging_task,
            buffer_tracker,
            storage_opts,
        );
        let refiller = CacheRefiller::new(
            CacheRefillConfig::from_storage_opts(storage_opts),
            sstable_store,
            spawn_refill_task,
        );

        Self {
            hummock_event_tx,
            hummock_event_rx,
            version_update_rx,
            pending_sync_requests: Default::default(),
            version_update_notifier_tx,
            pinned_version: Arc::new(ArcSwap::from_pointee(pinned_version)),
            write_conflict_detector,
            read_version_mapping,
            local_read_version_mapping: Default::default(),
            uploader,
            refiller,
            last_instance_id: 0,
            sstable_object_id_manager,
            metrics,
        }
    }

    pub fn version_update_notifier_tx(&self) -> Arc<tokio::sync::watch::Sender<HummockEpoch>> {
        self.version_update_notifier_tx.clone()
    }

    pub fn pinned_version(&self) -> Arc<ArcSwap<PinnedVersion>> {
        self.pinned_version.clone()
    }

    pub fn read_version_mapping(&self) -> ReadOnlyReadVersionMapping {
        ReadOnlyRwLockRef::new(self.read_version_mapping.clone())
    }

    pub fn event_sender(&self) -> HummockEventSender {
        self.hummock_event_tx.clone()
    }

    pub fn buffer_tracker(&self) -> &BufferTracker {
        self.uploader.buffer_tracker()
    }
}

// Handler for different events
impl HummockEventHandler {
    fn handle_epoch_synced(
        &mut self,
        epoch: HummockEpoch,
        newly_uploaded_sstables: Vec<StagingSstableInfo>,
    ) {
        debug!("epoch has been synced: {}.", epoch);
        if !newly_uploaded_sstables.is_empty() {
            newly_uploaded_sstables
                .into_iter()
                // Take rev because newer data come first in `newly_uploaded_sstables` but we apply
                // older data first
                .rev()
                .for_each(|staging_sstable_info| {
                    let staging_sstable_info_ref = Arc::new(staging_sstable_info);
                    self.for_each_read_version(|read_version| {
                        read_version.update(VersionUpdate::Staging(StagingData::Sst(
                            staging_sstable_info_ref.clone(),
                        )))
                    });
                });
        }
        let result = self
            .uploader
            .get_synced_data(epoch)
            .expect("data just synced. must exist");
        // clear the pending sync epoch that is older than newly synced epoch
        while let Some((smallest_pending_sync_epoch, _)) =
            self.pending_sync_requests.first_key_value()
        {
            if *smallest_pending_sync_epoch > epoch {
                // The smallest pending sync epoch has not synced yet. Wait later
                break;
            }
            let (pending_sync_epoch, result_sender) =
                self.pending_sync_requests.pop_first().expect("must exist");
            if pending_sync_epoch == epoch {
                send_sync_result(result_sender, to_sync_result(result));
                break;
            } else {
                send_sync_result(
                    result_sender,
                    Err(HummockError::other(format!(
                        "epoch {} is not a checkpoint epoch",
                        pending_sync_epoch
                    ))),
                );
            }
        }
    }

    /// This function will be performed under the protection of the `read_version_mapping` read
    /// lock, and add write lock on each `read_version` operation
    fn for_each_read_version(&self, mut f: impl FnMut(&mut HummockReadVersion)) {
        self.local_read_version_mapping
            .values()
            .for_each(|read_version: &HummockReadVersionRef| f(read_version.write().deref_mut()));
    }

    fn handle_data_spilled(&mut self, staging_sstable_info: StagingSstableInfo) {
        // todo: do some prune for version update
        let staging_sstable_info = Arc::new(staging_sstable_info);
        self.for_each_read_version(|read_version| {
            trace!("data_spilled. SST size {}", staging_sstable_info.imm_size());
            read_version.update(VersionUpdate::Staging(StagingData::Sst(
                staging_sstable_info.clone(),
            )))
        })
    }

    fn handle_await_sync_epoch(
        &mut self,
        new_sync_epoch: HummockEpoch,
        sync_result_sender: oneshot::Sender<HummockResult<SyncResult>>,
    ) {
        debug!("receive await sync epoch: {}", new_sync_epoch);
        // The epoch to sync has been committed already.
        if new_sync_epoch <= self.uploader.max_committed_epoch() {
            send_sync_result(
                sync_result_sender,
                Err(HummockError::other(format!(
                    "epoch {} has been committed. {}",
                    new_sync_epoch,
                    self.uploader.max_committed_epoch()
                ))),
            );
            return;
        }
        // The epoch has been synced
        if new_sync_epoch <= self.uploader.max_synced_epoch() {
            debug!(
                "epoch {} has been synced. Current max_sync_epoch {}",
                new_sync_epoch,
                self.uploader.max_synced_epoch()
            );
            if let Some(result) = self.uploader.get_synced_data(new_sync_epoch) {
                let result = to_sync_result(result);
                send_sync_result(sync_result_sender, result);
            } else {
                send_sync_result(
                    sync_result_sender,
                    Err(HummockError::other(
                        "the requested sync epoch is not a checkpoint epoch",
                    )),
                );
            }
            return;
        }

        debug!(
            "awaiting for epoch to be synced: {}, max_synced_epoch: {}",
            new_sync_epoch,
            self.uploader.max_synced_epoch()
        );

        // If the epoch is not synced, we add to the `pending_sync_requests` anyway. If the epoch is
        // not a checkpoint epoch, it will be clear with the max synced epoch bumps up.
        if let Some(old_sync_result_sender) = self
            .pending_sync_requests
            .insert(new_sync_epoch, sync_result_sender)
        {
            let _ = old_sync_result_sender
                .send(Err(HummockError::other(
                    "the sync rx is overwritten by an new rx",
                )))
                .inspect_err(|e| {
                    error!(
                        "unable to send sync result: {}. Err: {:?}",
                        new_sync_epoch, e
                    );
                });
        }
    }

    async fn handle_clear(&mut self, notifier: oneshot::Sender<()>, prev_epoch: u64) {
        info!(
            prev_epoch,
            max_committed_epoch = self.uploader.max_committed_epoch(),
            max_synced_epoch = self.uploader.max_synced_epoch(),
            max_sealed_epoch = self.uploader.max_sealed_epoch(),
            "handle clear event"
        );

        self.uploader.clear();

        let current_version = self.uploader.hummock_version();

        if current_version.max_committed_epoch() < prev_epoch {
            let mut latest_version = if let Some(CacheRefillerEvent {
                pinned_version,
                new_pinned_version,
            }) = self.refiller.clear()
            {
                assert_eq!(
                    current_version.id(),
                    pinned_version.id(),
                    "refiller earliest version {:?} not equal to current version {:?}",
                    pinned_version.version(),
                    current_version.version()
                );

                info!(
                    prev_epoch,
                    current_mce = current_version.max_committed_epoch(),
                    refiller_mce = new_pinned_version.max_committed_epoch(),
                    "refiller is clear in recovery"
                );

                Some(new_pinned_version)
            } else {
                None
            };

            while let latest_version_ref = latest_version.as_ref().unwrap_or(current_version)
                && latest_version_ref.max_committed_epoch() < prev_epoch
            {
                let version_update = self
                    .version_update_rx
                    .recv()
                    .await
                    .expect("should not be empty");
                latest_version = Some(Self::resolve_version_update_info(
                    latest_version_ref.clone(),
                    version_update,
                    None,
                ));
            }

            self.apply_version_update(
                current_version.clone(),
                latest_version.expect("must have some version update to raise the mce"),
            );
        }

        assert!(self.uploader.max_committed_epoch() >= prev_epoch);
        if self.uploader.max_committed_epoch() > prev_epoch {
            warn!(
                mce = self.uploader.max_committed_epoch(),
                prev_epoch, "mce higher than clear prev_epoch"
            );
        }

        for (epoch, result_sender) in self.pending_sync_requests.extract_if(|_, _| true) {
            send_sync_result(
                result_sender,
                Err(HummockError::other(format!(
                    "the sync epoch {} has been cleared",
                    epoch
                ))),
            );
        }

        assert!(
            self.local_read_version_mapping.is_empty(),
            "read version mapping not empty when clear. remaining tables: {:?}",
            self.local_read_version_mapping
                .values()
                .map(|read_version| read_version.read().table_id())
                .collect_vec()
        );

        if let Some(sstable_object_id_manager) = &self.sstable_object_id_manager {
            sstable_object_id_manager
                .remove_watermark_object_id(TrackerId::Epoch(HummockEpoch::MAX));
        }

        // Notify completion of the Clear event.
        let _ = notifier.send(()).inspect_err(|e| {
            error!("failed to notify completion of clear event: {:?}", e);
        });

        info!(prev_epoch, "clear finished");
    }

    fn handle_version_update(&mut self, version_payload: HummockVersionUpdate) {
        let _timer = self
            .metrics
            .event_handler_on_recv_version_update
            .start_timer();
        let pinned_version = self
            .refiller
            .last_new_pinned_version()
            .cloned()
            .unwrap_or_else(|| self.uploader.hummock_version().clone());

        let mut sst_delta_infos = vec![];
        let new_pinned_version = Self::resolve_version_update_info(
            pinned_version.clone(),
            version_payload,
            Some(&mut sst_delta_infos),
        );

        self.refiller
            .start_cache_refill(sst_delta_infos, pinned_version, new_pinned_version);
    }

    fn resolve_version_update_info(
        pinned_version: PinnedVersion,
        version_payload: HummockVersionUpdate,
        mut sst_delta_infos: Option<&mut Vec<SstDeltaInfo>>,
    ) -> PinnedVersion {
        let newly_pinned_version = match version_payload {
            HummockVersionUpdate::VersionDeltas(version_deltas) => {
                let mut version_to_apply = pinned_version.version().clone();
                for version_delta in &version_deltas {
                    assert_eq!(version_to_apply.id, version_delta.prev_id);
                    if version_to_apply.max_committed_epoch == version_delta.max_committed_epoch {
                        if let Some(sst_delta_infos) = &mut sst_delta_infos {
                            **sst_delta_infos =
                                version_to_apply.build_sst_delta_infos(version_delta);
                        }
                    }
                    version_to_apply.apply_version_delta(version_delta);
                }

                version_to_apply
            }
            HummockVersionUpdate::PinnedVersion(version) => version,
        };

        validate_table_key_range(&newly_pinned_version);

        pinned_version.new_pin_version(newly_pinned_version)
    }

    fn apply_version_update(
        &mut self,
        pinned_version: PinnedVersion,
        new_pinned_version: PinnedVersion,
    ) {
        let _timer = self
            .metrics
            .event_handler_on_apply_version_update
            .start_timer();
        self.pinned_version
            .store(Arc::new(new_pinned_version.clone()));

        {
            self.for_each_read_version(|read_version| {
                read_version.update(VersionUpdate::CommittedSnapshot(new_pinned_version.clone()))
            });
        }

        let prev_max_committed_epoch = pinned_version.max_committed_epoch();
        let max_committed_epoch = new_pinned_version.max_committed_epoch();

        // only notify local_version_manager when MCE change
        self.version_update_notifier_tx.send_if_modified(|state| {
            assert_eq!(prev_max_committed_epoch, *state);
            if max_committed_epoch > *state {
                *state = max_committed_epoch;
                true
            } else {
                false
            }
        });

        if let Some(conflict_detector) = self.write_conflict_detector.as_ref() {
            conflict_detector.set_watermark(max_committed_epoch);
        }

        if let Some(sstable_object_id_manager) = &self.sstable_object_id_manager {
            sstable_object_id_manager.remove_watermark_object_id(TrackerId::Epoch(
                self.pinned_version.load().max_committed_epoch(),
            ));
        }

        debug!(
            "update to hummock version: {}, epoch: {}",
            new_pinned_version.id(),
            new_pinned_version.max_committed_epoch()
        );

        self.uploader.update_pinned_version(new_pinned_version);
    }
}

impl HummockEventHandler {
    pub async fn start_hummock_event_handler_worker(mut self) {
        loop {
            tokio::select! {
                event = self.uploader.next_event() => {
                    self.handle_uploader_event(event);
                }
                event = self.refiller.next_event() => {
                    let CacheRefillerEvent {pinned_version, new_pinned_version } = event;
                    self.apply_version_update(pinned_version, new_pinned_version);
                }
                event = pin!(self.hummock_event_rx.recv()) => {
                    let Some(event) = event else { break };
                    match event {
                        HummockEvent::Clear(notifier, prev_epoch) => {
                            self.handle_clear(notifier, prev_epoch).await
                        },
                        HummockEvent::Shutdown => {
                            info!("event handler shutdown");
                            return;
                        },
                        event => {
                            self.handle_hummock_event(event);
                        }
                    }
                }
                version_update = pin!(self.version_update_rx.recv()) => {
                    let Some(version_update) = version_update else {
                        warn!("version update stream ends. event handle shutdown");
                        return;
                    };
                    self.handle_version_update(version_update);
                }
            }
        }
    }

    fn handle_uploader_event(&mut self, event: UploaderEvent) {
        match event {
            UploaderEvent::SyncFinish(epoch, newly_uploaded_sstables) => {
                let _timer = self
                    .metrics
                    .event_handler_on_sync_finish_latency
                    .start_timer();
                self.handle_epoch_synced(epoch, newly_uploaded_sstables);
            }

            UploaderEvent::DataSpilled(staging_sstable_info) => {
                let _timer = self.metrics.event_handler_on_spilled_latency.start_timer();
                self.handle_data_spilled(staging_sstable_info);
            }

            UploaderEvent::ImmMerged(merge_output) => {
                // update read version for corresponding table shards
                if let Some(read_version) = self
                    .local_read_version_mapping
                    .get(&merge_output.instance_id)
                {
                    read_version
                        .write()
                        .update(VersionUpdate::Staging(StagingData::MergedImmMem(
                            merge_output.merged_imm,
                            merge_output.imm_ids,
                        )));
                } else {
                    warn!(
                        "handle ImmMerged: table instance not found. table {:?}, instance {}",
                        &merge_output.table_id, &merge_output.instance_id
                    )
                }
            }
        }
    }

    /// Gracefully shutdown if returns `true`.
    fn handle_hummock_event(&mut self, event: HummockEvent) {
        match event {
            HummockEvent::BufferMayFlush => {
                self.uploader.may_flush();
            }
            HummockEvent::AwaitSyncEpoch {
                new_sync_epoch,
                sync_result_sender,
            } => {
                self.handle_await_sync_epoch(new_sync_epoch, sync_result_sender);
            }
            HummockEvent::Clear(_, _) => {
                unreachable!("clear is handled in separated async context")
            }
            HummockEvent::Shutdown => {
                unreachable!("shutdown is handled specially")
            }
            HummockEvent::ImmToUploader(imm) => {
                assert!(
                    self.local_read_version_mapping
                        .contains_key(&imm.instance_id),
                    "add imm from non-existing read version instance: instance_id: {}, table_id {}",
                    imm.instance_id,
                    imm.table_id,
                );
                self.uploader.add_imm(imm);
                self.uploader.may_flush();
            }

            HummockEvent::SealEpoch {
                epoch,
                is_checkpoint,
            } => {
                self.uploader.seal_epoch(epoch);

                if is_checkpoint {
                    self.uploader.start_sync_epoch(epoch);
                } else {
                    // start merging task on non-checkpoint epochs sealed
                    self.uploader.start_merge_imms(epoch);
                }
            }

            HummockEvent::LocalSealEpoch {
                epoch,
                opts,
                table_id,
                instance_id,
            } => {
                assert!(
                    self.local_read_version_mapping
                        .contains_key(&instance_id),
                    "seal epoch from non-existing read version instance: instance_id: {}, table_id: {}, epoch: {}",
                    instance_id, table_id, epoch,
                );
                if let Some((direction, watermarks)) = opts.table_watermarks {
                    self.uploader
                        .add_table_watermarks(epoch, table_id, watermarks, direction)
                }
            }

            #[cfg(any(test, feature = "test"))]
            HummockEvent::FlushEvent(sender) => {
                let _ = sender.send(()).inspect_err(|e| {
                    error!("unable to send flush result: {:?}", e);
                });
            }

            HummockEvent::RegisterReadVersion {
                table_id,
                new_read_version_sender,
                is_replicated,
                vnodes,
            } => {
                let pinned_version = self.pinned_version.load();
                let basic_read_version = Arc::new(RwLock::new(
                    HummockReadVersion::new_with_replication_option(
                        table_id,
                        (**pinned_version).clone(),
                        is_replicated,
                        vnodes,
                    ),
                ));

                let instance_id = self.generate_instance_id();

                debug!(
                    "new read version registered: table_id: {}, instance_id: {}",
                    table_id, instance_id
                );

                {
                    self.local_read_version_mapping
                        .insert(instance_id, basic_read_version.clone());
                    let mut read_version_mapping_guard = self.read_version_mapping.write();

                    read_version_mapping_guard
                        .entry(table_id)
                        .or_default()
                        .insert(instance_id, basic_read_version.clone());
                }

                match new_read_version_sender.send((
                    basic_read_version,
                    LocalInstanceGuard {
                        table_id,
                        instance_id,
                        event_sender: Some(self.hummock_event_tx.clone()),
                    },
                )) {
                    Ok(_) => {}
                    Err((_, mut guard)) => {
                        warn!(
                            "RegisterReadVersion send fail table_id {:?} instance_is {:?}",
                            table_id, instance_id
                        );
                        guard.event_sender.take().expect("sender is just set");
                        self.destroy_read_version(table_id, instance_id);
                    }
                }
            }

            HummockEvent::DestroyReadVersion {
                table_id,
                instance_id,
            } => {
                self.destroy_read_version(table_id, instance_id);
            }
        }
    }

    fn destroy_read_version(&mut self, table_id: TableId, instance_id: LocalInstanceId) {
        {
            {
                debug!(
                    "read version deregister: table_id: {}, instance_id: {}",
                    table_id, instance_id
                );
                self.local_read_version_mapping
                    .remove(&instance_id)
                    .unwrap_or_else(|| {
                        panic!(
                            "DestroyHummockInstance inexist instance table_id {} instance_id {}",
                            table_id, instance_id
                        )
                    });
                let mut read_version_mapping_guard = self.read_version_mapping.write();
                let entry = read_version_mapping_guard
                    .get_mut(&table_id)
                    .unwrap_or_else(|| {
                        panic!(
                            "DestroyHummockInstance table_id {} instance_id {} fail",
                            table_id, instance_id
                        )
                    });
                entry.remove(&instance_id).unwrap_or_else(|| {
                    panic!(
                        "DestroyHummockInstance inexist instance table_id {} instance_id {}",
                        table_id, instance_id
                    )
                });
                if entry.is_empty() {
                    read_version_mapping_guard.remove(&table_id);
                }
            }
        }
    }

    fn generate_instance_id(&mut self) -> LocalInstanceId {
        self.last_instance_id += 1;
        self.last_instance_id
    }
}

fn send_sync_result(
    sender: oneshot::Sender<HummockResult<SyncResult>>,
    result: HummockResult<SyncResult>,
) {
    let _ = sender.send(result).inspect_err(|e| {
        error!("unable to send sync result. Err: {:?}", e);
    });
}

fn to_sync_result(result: &HummockResult<SyncedData>) -> HummockResult<SyncResult> {
    match result {
        Ok(sync_data) => {
            let sync_size = sync_data
                .staging_ssts
                .iter()
                .map(StagingSstableInfo::imm_size)
                .sum();
            Ok(SyncResult {
                sync_size,
                uncommitted_ssts: sync_data
                    .staging_ssts
                    .iter()
                    .flat_map(|staging_sstable_info| staging_sstable_info.sstable_infos().clone())
                    .collect(),
                table_watermarks: sync_data.table_watermarks.clone(),
                old_value_ssts: sync_data
                    .staging_ssts
                    .iter()
                    .flat_map(|staging_sstable_info| {
                        staging_sstable_info.old_value_sstable_infos().clone()
                    })
                    .collect(),
            })
        }
        Err(e) => Err(HummockError::other(format!(
            "sync task failed: {}",
            e.as_report()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::future::{poll_fn, Future};
    use std::iter::once;
    use std::sync::Arc;
    use std::task::Poll;

    use bytes::Bytes;
    use futures::FutureExt;
    use itertools::Itertools;
    use risingwave_common::buffer::Bitmap;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::{test_epoch, EpochExt};
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_hummock_sdk::key::TableKey;
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_pb::hummock::PbHummockVersion;
    use tokio::spawn;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot;
    use tokio::task::yield_now;

    use crate::hummock::event_handler::refiller::CacheRefiller;
    use crate::hummock::event_handler::{HummockEvent, HummockEventHandler, HummockVersionUpdate};
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::shared_buffer::shared_buffer_batch::{
        SharedBufferBatch, SharedBufferValue,
    };
    use crate::hummock::store::version::{StagingData, VersionUpdate};
    use crate::hummock::test_utils::default_opts_for_test;
    use crate::hummock::HummockError;
    use crate::monitor::HummockStateStoreMetrics;

    #[tokio::test]
    async fn test_event_handler_merging_task() {
        let table_id = TableId::new(123);
        let epoch0 = test_epoch(233);
        let pinned_version = PinnedVersion::new(
            HummockVersion::from_rpc_protobuf(&PbHummockVersion {
                id: 1,
                max_committed_epoch: epoch0,
                ..Default::default()
            }),
            unbounded_channel().0,
        );

        let mut storage_opts = default_opts_for_test();
        storage_opts.imm_merge_threshold = 5;

        let (_version_update_tx, version_update_rx) = unbounded_channel();

        let (spawn_upload_task_tx, mut spawn_upload_task_rx) = unbounded_channel();
        let (spawn_merging_task_tx, mut spawn_merging_task_rx) = unbounded_channel();
        let event_handler = HummockEventHandler::new_inner(
            version_update_rx,
            pinned_version,
            None,
            mock_sstable_store(),
            Arc::new(HummockStateStoreMetrics::unused()),
            &storage_opts,
            Arc::new(move |_, _| {
                let (tx, rx) = oneshot::channel::<()>();
                spawn_upload_task_tx.send(tx).unwrap();
                spawn(async move {
                    // wait for main thread to notify returning error
                    rx.await.unwrap();
                    Err(HummockError::other("".to_string()))
                })
            }),
            Arc::new(move |_, _, imms, _| {
                let (tx, rx) = oneshot::channel::<()>();
                let (finish_tx, finish_rx) = oneshot::channel::<()>();
                spawn_merging_task_tx.send((tx, finish_rx)).unwrap();
                spawn(async move {
                    rx.await.unwrap();
                    finish_tx.send(()).unwrap();
                    imms[0].clone()
                })
            }),
            CacheRefiller::default_spawn_refill_task(),
        );

        let tx = event_handler.event_sender();

        let _join_handle = spawn(event_handler.start_hummock_event_handler_worker());

        let (read_version_tx, read_version_rx) = oneshot::channel();

        tx.send(HummockEvent::RegisterReadVersion {
            table_id,
            new_read_version_sender: read_version_tx,
            is_replicated: false,
            vnodes: Arc::new(Bitmap::ones(VirtualNode::COUNT)),
        })
        .unwrap();
        let (read_version, guard) = read_version_rx.await.unwrap();
        let instance_id = guard.instance_id;

        let build_batch = |epoch, spill_offset| {
            SharedBufferBatch::build_shared_buffer_batch(
                epoch,
                spill_offset,
                vec![(TableKey(Bytes::from("key")), SharedBufferValue::Delete)],
                None,
                10,
                table_id,
                instance_id,
                None,
            )
        };

        let epoch1 = epoch0.next_epoch();
        let imm1 = build_batch(epoch1, 0);
        read_version
            .write()
            .update(VersionUpdate::Staging(StagingData::ImmMem(imm1.clone())));
        tx.send(HummockEvent::ImmToUploader(imm1.clone())).unwrap();
        tx.send(HummockEvent::SealEpoch {
            epoch: epoch1,
            is_checkpoint: true,
        })
        .unwrap();
        let (sync_tx, mut sync_rx) = oneshot::channel();
        tx.send(HummockEvent::AwaitSyncEpoch {
            new_sync_epoch: epoch1,
            sync_result_sender: sync_tx,
        })
        .unwrap();

        let upload_finish_tx = spawn_upload_task_rx.recv().await.unwrap();
        assert!(poll_fn(|cx| Poll::Ready(sync_rx.poll_unpin(cx)))
            .await
            .is_pending());

        let epoch2 = epoch1.next_epoch();
        let mut imm_ids = Vec::new();
        for i in 0..10 {
            let imm = build_batch(epoch2, i);
            imm_ids.push(imm.batch_id());
            read_version
                .write()
                .update(VersionUpdate::Staging(StagingData::ImmMem(imm.clone())));
            tx.send(HummockEvent::ImmToUploader(imm)).unwrap();
        }

        for (staging_imm, imm_id) in read_version
            .read()
            .staging()
            .imm
            .iter()
            .zip_eq_debug(imm_ids.iter().copied().rev().chain(once(imm1.batch_id())))
        {
            assert_eq!(staging_imm.batch_id(), imm_id);
        }

        // should start merging task
        tx.send(HummockEvent::SealEpoch {
            epoch: epoch2,
            is_checkpoint: false,
        })
        .unwrap();

        println!("before wait spawn merging task");

        let (merging_start_tx, merging_finish_rx) = spawn_merging_task_rx.recv().await.unwrap();
        merging_start_tx.send(()).unwrap();

        println!("after wait spawn merging task");

        // yield to possibly poll the merging task, though it shouldn't poll it because there is unfinished syncing task
        yield_now().await;

        for (staging_imm, imm_id) in read_version
            .read()
            .staging()
            .imm
            .iter()
            .zip_eq_debug(imm_ids.iter().copied().rev().chain(once(imm1.batch_id())))
        {
            assert_eq!(staging_imm.batch_id(), imm_id);
        }

        upload_finish_tx.send(()).unwrap();
        assert!(sync_rx.await.unwrap().is_err());

        merging_finish_rx.await.unwrap();

        // yield to poll the merging task, and then it should have finished.
        for _ in 0..10 {
            yield_now().await;
        }

        assert_eq!(
            read_version
                .read()
                .staging()
                .imm
                .iter()
                .map(|imm| imm.batch_id())
                .collect_vec(),
            vec![*imm_ids.last().unwrap(), imm1.batch_id()]
        );
    }

    #[tokio::test]
    async fn test_clear_shared_buffer() {
        let epoch0 = 233;
        let mut next_version_id = 1;
        let mut make_new_version = |max_committed_epoch| {
            let id = next_version_id;
            next_version_id += 1;
            HummockVersion::from_rpc_protobuf(&PbHummockVersion {
                id,
                max_committed_epoch,
                ..Default::default()
            })
        };

        let initial_version = PinnedVersion::new(make_new_version(epoch0), unbounded_channel().0);

        let (version_update_tx, version_update_rx) = unbounded_channel();
        let (refill_task_tx, mut refill_task_rx) = unbounded_channel();

        let refill_task_tx_clone = refill_task_tx.clone();

        let event_handler = HummockEventHandler::new_inner(
            version_update_rx,
            initial_version.clone(),
            None,
            mock_sstable_store(),
            Arc::new(HummockStateStoreMetrics::unused()),
            &default_opts_for_test(),
            Arc::new(|_, _| unreachable!("should not spawn upload task")),
            Arc::new(|_, _, _, _| unreachable!("should not spawn merging task")),
            Arc::new(move |_, _, old_version, new_version| {
                let (tx, rx) = oneshot::channel();
                refill_task_tx_clone
                    .send((old_version, new_version, tx))
                    .unwrap();
                spawn(async move {
                    let _ = rx.await;
                })
            }),
        );

        let event_tx = event_handler.event_sender();
        let latest_version = event_handler.pinned_version.clone();
        let latest_version_update_tx = event_handler.version_update_notifier_tx.clone();

        let send_clear = |epoch| {
            let (tx, rx) = oneshot::channel();
            event_tx.send(HummockEvent::Clear(tx, epoch)).unwrap();
            rx
        };

        let _join_handle = spawn(event_handler.start_hummock_event_handler_worker());

        // test normal recovery
        send_clear(epoch0).await.unwrap();

        // test normal refill finish
        let epoch1 = epoch0 + 1;
        let version1 = make_new_version(epoch1);
        {
            version_update_tx
                .send(HummockVersionUpdate::PinnedVersion(version1.clone()))
                .unwrap();
            let (old_version, new_version, refill_finish_tx) = refill_task_rx.recv().await.unwrap();
            assert_eq!(old_version.version(), initial_version.version());
            assert_eq!(new_version.version(), &version1);
            assert_eq!(latest_version.load().version(), initial_version.version());

            let mut changed = latest_version_update_tx.subscribe();
            refill_finish_tx.send(()).unwrap();
            changed.changed().await.unwrap();
            assert_eq!(latest_version.load().version(), &version1);
        }

        // test recovery with pending refill task
        let epoch2 = epoch1 + 1;
        let version2 = make_new_version(epoch2);
        let epoch3 = epoch2 + 1;
        let version3 = make_new_version(epoch3);
        {
            version_update_tx
                .send(HummockVersionUpdate::PinnedVersion(version2.clone()))
                .unwrap();
            version_update_tx
                .send(HummockVersionUpdate::PinnedVersion(version3.clone()))
                .unwrap();
            let (old_version2, new_version2, _refill_finish_tx2) =
                refill_task_rx.recv().await.unwrap();
            assert_eq!(old_version2.version(), &version1);
            assert_eq!(new_version2.version(), &version2);
            let (old_version3, new_version3, _refill_finish_tx3) =
                refill_task_rx.recv().await.unwrap();
            assert_eq!(old_version3.version(), &version2);
            assert_eq!(new_version3.version(), &version3);
            assert_eq!(latest_version.load().version(), &version1);

            let rx = send_clear(epoch3);
            rx.await.unwrap();
            assert_eq!(latest_version.load().version(), &version3);
        }

        async fn assert_pending(fut: &mut (impl Future + Unpin)) {
            assert!(poll_fn(|cx| Poll::Ready(fut.poll_unpin(cx).is_pending())).await);
        }

        // test recovery with later arriving version update
        let epoch4 = epoch3 + 1;
        let version4 = make_new_version(epoch4);
        let epoch5 = epoch4 + 1;
        let version5 = make_new_version(epoch5);
        {
            let mut rx = send_clear(epoch5);
            assert_pending(&mut rx).await;
            version_update_tx
                .send(HummockVersionUpdate::PinnedVersion(version4.clone()))
                .unwrap();
            assert_pending(&mut rx).await;
            version_update_tx
                .send(HummockVersionUpdate::PinnedVersion(version5.clone()))
                .unwrap();
            rx.await.unwrap();
            assert_eq!(latest_version.load().version(), &version5);
        }
    }
}

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
use std::sync::{Arc, LazyLock};

use arc_swap::ArcSwap;
use await_tree::InstrumentAwait;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use prometheus::core::{AtomicU64, GenericGauge};
// use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{HummockEpoch, LocalSstableInfo};
use risingwave_pb::stream_plan::StreamActor;
use thiserror_ext::AsReport;
use tokio::spawn;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, trace, warn};

use super::refiller::{CacheRefillConfig, CacheRefiller};
use super::{LocalInstanceGuard, LocalInstanceId, ReadVersionMappingType};
use crate::filter_key_extractor::FilterKeyExtractorManager;
use crate::hummock::compactor::{compact, CompactorContext};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::event_handler::refiller::CacheRefillerEvent;
use crate::hummock::event_handler::uploader::{
    default_spawn_merging_task, HummockUploader, SpawnMergingTask, SpawnUploadTask, SyncedData,
    UploadTaskInfo, UploadTaskPayload, UploaderEvent,
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
use crate::store::SyncResult;

struct ActorTracker {
    running_actors: HashMap<usize, StreamActor>,
    next_tracking_id: usize,
}

pub struct ActorTrackerGuard {
    tracking_id: usize,
}

impl Drop for ActorTrackerGuard {
    fn drop(&mut self) {
        deregister_running_actor(self.tracking_id);
    }
}

static ACTOR_HOOK: LazyLock<Mutex<ActorTracker>> = LazyLock::new(|| {
    Mutex::new(ActorTracker {
        next_tracking_id: 0,
        running_actors: HashMap::new(),
    })
});

fn deregister_running_actor(tracking_id: usize) {
    let actor = ACTOR_HOOK
        .lock()
        .running_actors
        .remove(&tracking_id)
        .expect("should exist");
    info!(tracking_id, actor_id = actor.actor_id, "deregister actor");
}

pub fn register_new_running_actor(actor: StreamActor) -> ActorTrackerGuard {
    let mut lock = ACTOR_HOOK.lock();
    lock.next_tracking_id += 1;
    let tracking_id = lock.next_tracking_id;
    info!(tracking_id, actor_id = actor.actor_id, "register new actor");
    lock.running_actors.insert(tracking_id, actor);
    ActorTrackerGuard { tracking_id }
}

fn running_actors() -> Vec<StreamActor> {
    ACTOR_HOOK
        .lock()
        .running_actors
        .values()
        .cloned()
        .collect_vec()
}

#[derive(Clone)]
pub struct BufferTracker {
    flush_threshold: usize,
    global_buffer: Arc<MemoryLimiter>,
    global_upload_task_size: GenericGauge<AtomicU64>,
}

// pub static VERSION_STORE: LazyLock<Mutex<HashMap<u64, HummockVersion>>> =
//     LazyLock::new(|| Mutex::new(HashMap::new()));
//
// fn store_version(version: HummockVersion) {
//     VERSION_STORE.lock().insert(version.id, version);
// }
//
// pub fn get_version(id: u64) -> HummockVersion {
//     VERSION_STORE.lock().get(&id).unwrap().clone()
// }

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

pub struct HummockEventHandler {
    hummock_event_tx: mpsc::UnboundedSender<HummockEvent>,
    hummock_event_rx: mpsc::UnboundedReceiver<HummockEvent>,
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
}

async fn flush_imms(
    payload: UploadTaskPayload,
    task_info: UploadTaskInfo,
    compactor_context: CompactorContext,
    filter_key_extractor_manager: FilterKeyExtractorManager,
    sstable_object_id_manager: Arc<SstableObjectIdManager>,
) -> HummockResult<Vec<LocalSstableInfo>> {
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
        hummock_event_tx: mpsc::UnboundedSender<HummockEvent>,
        hummock_event_rx: mpsc::UnboundedReceiver<HummockEvent>,
        pinned_version: PinnedVersion,
        compactor_context: CompactorContext,
        filter_key_extractor_manager: FilterKeyExtractorManager,
        sstable_object_id_manager: Arc<SstableObjectIdManager>,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
    ) -> Self {
        let upload_compactor_context = compactor_context.clone();
        let cloned_sstable_object_id_manager = sstable_object_id_manager.clone();
        Self::new_inner(
            hummock_event_tx,
            hummock_event_rx,
            pinned_version,
            Some(sstable_object_id_manager),
            compactor_context.sstable_store.clone(),
            state_store_metrics,
            &compactor_context.storage_opts,
            Arc::new(move |payload, task_info| {
                spawn(flush_imms(
                    payload,
                    task_info,
                    upload_compactor_context.clone(),
                    filter_key_extractor_manager.clone(),
                    cloned_sstable_object_id_manager.clone(),
                ))
            }),
            default_spawn_merging_task(compactor_context.compaction_executor.clone()),
        )
    }

    fn new_inner(
        hummock_event_tx: mpsc::UnboundedSender<HummockEvent>,
        hummock_event_rx: mpsc::UnboundedReceiver<HummockEvent>,
        pinned_version: PinnedVersion,
        sstable_object_id_manager: Option<Arc<SstableObjectIdManager>>,
        sstable_store: SstableStoreRef,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        storage_opts: &StorageOpts,
        spawn_upload_task: SpawnUploadTask,
        spawn_merging_task: SpawnMergingTask,
    ) -> Self {
        let (version_update_notifier_tx, _) =
            tokio::sync::watch::channel(pinned_version.max_committed_epoch());
        let version_update_notifier_tx = Arc::new(version_update_notifier_tx);
        let read_version_mapping = Arc::new(RwLock::new(HashMap::default()));
        let buffer_tracker = BufferTracker::from_storage_opts(
            storage_opts,
            state_store_metrics.uploader_uploading_task_size.clone(),
        );
        let write_conflict_detector = ConflictDetector::new_from_config(storage_opts);

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
        );

        Self {
            hummock_event_tx,
            hummock_event_rx,
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
                    self.for_each_read_version(|read_version| {
                        read_version.update(VersionUpdate::Staging(StagingData::Sst(
                            staging_sstable_info.clone(),
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

    fn handle_clear(&mut self, notifier: oneshot::Sender<()>) {
        info!(
            "handle clear event. max_committed_epoch: {}, max_synced_epoch: {}, max_sealed_epoch: {}",
            self.uploader.max_committed_epoch(),
            self.uploader.max_synced_epoch(),
            self.uploader.max_sealed_epoch(),
        );

        self.uploader.clear();

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
            "read version mapping not empty when clear. remaining tables: {:?}, remaining actors: {:?}",
            self.local_read_version_mapping
                .values()
                .map(|read_version| read_version.read().table_id())
                .collect_vec(),
            running_actors(),
        );

        if let Some(sstable_object_id_manager) = &self.sstable_object_id_manager {
            sstable_object_id_manager
                .remove_watermark_object_id(TrackerId::Epoch(HummockEpoch::MAX));
        }

        // Notify completion of the Clear event.
        let _ = notifier.send(()).inspect_err(|e| {
            error!("failed to notify completion of clear event: {:?}", e);
        });
    }

    fn handle_version_update(&mut self, version_payload: HummockVersionUpdate) {
        let pinned_version = self
            .refiller
            .last_new_pinned_version()
            .cloned()
            .map(Arc::new)
            .unwrap_or_else(|| self.pinned_version.load().clone());

        if pinned_version.id() == 48311 {
            warn!(?version_payload, "update on target version");
        }

        let mut sst_delta_infos = vec![];
        let newly_pinned_version = match version_payload {
            HummockVersionUpdate::VersionDeltas(version_deltas) => {
                let mut version_to_apply = pinned_version.version().clone();
                for version_delta in &version_deltas {
                    assert_eq!(version_to_apply.id, version_delta.prev_id);
                    if version_to_apply.max_committed_epoch == version_delta.max_committed_epoch {
                        sst_delta_infos = version_to_apply.build_sst_delta_infos(version_delta);
                    }
                    version_to_apply.apply_version_delta(version_delta);
                }

                version_to_apply
            }
            HummockVersionUpdate::PinnedVersion(version) => version,
        };

        validate_table_key_range(&newly_pinned_version);

        let new_pinned_version = pinned_version.new_pin_version(newly_pinned_version);

        self.refiller
            .start_cache_refill(sst_delta_infos, pinned_version, new_pinned_version);
    }

    fn apply_version_update(
        &mut self,
        pinned_version: Arc<PinnedVersion>,
        new_pinned_version: PinnedVersion,
    ) {
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

        info!(
            new_version_id = new_pinned_version.id(),
            new_mce = new_pinned_version.max_committed_epoch(),
            prev_version_id = pinned_version.id(),
            prev_mce = pinned_version.max_committed_epoch(),
            "update  hummock version"
        );

        // store_version(new_pinned_version.version().clone());

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
                    if self.handle_hummock_event(event) {
                        break;
                    }
                }
            }
        }
    }

    fn handle_uploader_event(&mut self, event: UploaderEvent) {
        match event {
            UploaderEvent::SyncFinish(epoch, newly_uploaded_sstables) => {
                self.handle_epoch_synced(epoch, newly_uploaded_sstables);
            }

            UploaderEvent::DataSpilled(staging_sstable_info) => {
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
    fn handle_hummock_event(&mut self, event: HummockEvent) -> bool {
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
            HummockEvent::Clear(notifier) => {
                self.handle_clear(notifier);
            }
            HummockEvent::Shutdown => {
                info!("buffer tracker shutdown");
                return true;
            }

            HummockEvent::VersionUpdate(version_payload) => {
                self.handle_version_update(version_payload);
            }

            HummockEvent::ImmToUploader(imm) => {
                if imm.table_id.table_id == 338022 {
                    warn!(epochs = ?imm.epochs(), instance_id = imm.instance_id, imm_id = imm.batch_id(), "write imm");
                }
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
                info!(epoch, is_checkpoint, "seal epoch");
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
                if table_id.table_id == 338022 {
                    warn!(epoch, instance_id, "local seal epoch");
                }
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
            } => {
                let pinned_version = self.pinned_version.load();
                let basic_read_version = Arc::new(RwLock::new(
                    HummockReadVersion::new_with_replication_option(
                        table_id,
                        (**pinned_version).clone(),
                        is_replicated,
                    ),
                ));

                let instance_id = self.generate_instance_id();

                if table_id.table_id == 338022 {
                    warn!(
                        mce = pinned_version.max_committed_epoch(),
                        instance_id, "read version register"
                    );
                }
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
                        event_sender: self.hummock_event_tx.clone(),
                    },
                )) {
                    Ok(_) => {}
                    Err(_) => {
                        warn!(
                            "RegisterReadVersion send fail table_id {:?} instance_is {:?}",
                            table_id, instance_id
                        )
                    }
                }
            }

            HummockEvent::DestroyReadVersion {
                table_id,
                instance_id,
            } => {
                if table_id.table_id == 338022 {
                    warn!(instance_id, "read version deregister");
                }
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
        false
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
    use std::future::poll_fn;
    use std::iter::once;
    use std::sync::Arc;
    use std::task::Poll;

    use bytes::Bytes;
    use futures::FutureExt;
    use itertools::Itertools;
    use risingwave_common::catalog::TableId;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_hummock_sdk::key::TableKey;
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_pb::hummock::PbHummockVersion;
    use tokio::spawn;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot;
    use tokio::task::yield_now;

    use crate::hummock::event_handler::{HummockEvent, HummockEventHandler};
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::shared_buffer::shared_buffer_batch::{
        SharedBufferBatch, SharedBufferBatchInner,
    };
    use crate::hummock::store::version::{StagingData, VersionUpdate};
    use crate::hummock::test_utils::default_opts_for_test;
    use crate::hummock::value::HummockValue;
    use crate::hummock::HummockError;
    use crate::monitor::HummockStateStoreMetrics;

    #[tokio::test]
    async fn test_event_handler() {
        let (tx, rx) = unbounded_channel();
        let table_id = TableId::new(123);
        let epoch0 = 233;
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

        let (spawn_upload_task_tx, mut spawn_upload_task_rx) = unbounded_channel();
        let (spawn_merging_task_tx, mut spawn_merging_task_rx) = unbounded_channel();
        let event_handler = HummockEventHandler::new_inner(
            tx.clone(),
            rx,
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
            Arc::new(move |table_id, instance_id, imms, _| {
                let (tx, rx) = oneshot::channel::<()>();
                let (finish_tx, finish_rx) = oneshot::channel::<()>();
                spawn_merging_task_tx.send((tx, finish_rx)).unwrap();
                spawn(async move {
                    rx.await.unwrap();
                    finish_tx.send(()).unwrap();
                    let first_imm = &imms[0];
                    Ok(SharedBufferBatch {
                        inner: Arc::new(SharedBufferBatchInner::new_with_multi_epoch_batches(
                            first_imm.epochs().clone(),
                            first_imm.get_payload().iter().cloned().collect_vec(),
                            100,
                            imms.iter().map(|imm| imm.batch_id()).collect_vec(),
                            100,
                            None,
                        )),
                        table_id,
                        instance_id,
                    })
                })
            }),
        );

        let _join_handle = spawn(event_handler.start_hummock_event_handler_worker());

        let (read_version_tx, read_version_rx) = oneshot::channel();

        tx.send(HummockEvent::RegisterReadVersion {
            table_id,
            new_read_version_sender: read_version_tx,
            is_replicated: false,
        })
        .unwrap();
        let (read_version, guard) = read_version_rx.await.unwrap();
        let instance_id = guard.instance_id;

        let build_batch = |epoch, spill_offset| {
            SharedBufferBatch::build_shared_buffer_batch(
                epoch,
                spill_offset,
                vec![(TableKey(Bytes::from("key")), HummockValue::Delete)],
                10,
                table_id,
                instance_id,
                None,
            )
        };

        let epoch1 = epoch0 + 1;
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

        let epoch2 = epoch1 + 1;
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
}

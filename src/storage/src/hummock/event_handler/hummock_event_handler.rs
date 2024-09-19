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

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use await_tree::InstrumentAwait;
use futures::FutureExt;
use itertools::Itertools;
use parking_lot::RwLock;
use prometheus::core::{AtomicU64, GenericGauge};
use prometheus::{Histogram, IntGauge};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::{HummockEpoch, HummockVersionId, SyncResult};
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
    HummockUploader, SpawnUploadTask, SyncedData, UploadTaskInfo, UploadTaskOutput,
};
use crate::hummock::event_handler::{
    HummockEvent, HummockReadVersionRef, HummockVersionUpdate, ReadOnlyReadVersionMapping,
    ReadOnlyRwLockRef,
};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::local_version::recent_versions::RecentVersions;
use crate::hummock::store::version::{
    HummockReadVersion, StagingData, StagingSstableInfo, VersionUpdate,
};
use crate::hummock::{
    HummockResult, MemoryLimiter, SstableObjectIdManager, SstableStoreRef, TrackerId,
};
use crate::mem_table::ImmutableMemtable;
use crate::monitor::HummockStateStoreMetrics;
use crate::opts::StorageOpts;

#[derive(Clone)]
pub struct BufferTracker {
    flush_threshold: usize,
    min_batch_flush_size: usize,
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
        let shared_buffer_min_batch_flush_size =
            config.shared_buffer_min_batch_flush_size_mb * (1 << 20);
        assert!(
            flush_threshold < capacity,
            "flush_threshold {} should be less or equal to capacity {}",
            flush_threshold,
            capacity
        );
        Self::new(
            capacity,
            flush_threshold,
            global_upload_task_size,
            shared_buffer_min_batch_flush_size,
        )
    }

    fn new(
        capacity: usize,
        flush_threshold: usize,
        global_upload_task_size: GenericGauge<AtomicU64>,
        min_batch_flush_size: usize,
    ) -> Self {
        assert!(capacity >= flush_threshold);
        Self {
            flush_threshold,
            global_buffer: Arc::new(MemoryLimiter::new(capacity as u64)),
            global_upload_task_size,
            min_batch_flush_size,
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
    pub fn need_flush(&self) -> bool {
        self.get_buffer_size() > self.flush_threshold + self.global_upload_task_size.get() as usize
    }

    pub fn need_more_flush(&self, curr_batch_flush_size: usize) -> bool {
        curr_batch_flush_size < self.min_batch_flush_size || self.need_flush()
    }

    #[cfg(test)]
    pub(crate) fn flush_threshold(&self) -> usize {
        self.flush_threshold
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
    event_handler_on_upload_finish_latency: Histogram,
    event_handler_on_apply_version_update: Histogram,
    event_handler_on_recv_version_update: Histogram,
}

pub struct HummockEventHandler {
    hummock_event_tx: HummockEventSender,
    hummock_event_rx: HummockEventReceiver,
    version_update_rx: UnboundedReceiver<HummockVersionUpdate>,
    read_version_mapping: Arc<RwLock<ReadVersionMappingType>>,
    /// A copy of `read_version_mapping` but owned by event handler
    local_read_version_mapping: HashMap<LocalInstanceId, (TableId, HummockReadVersionRef)>,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<HummockEpoch>>,
    recent_versions: Arc<ArcSwap<RecentVersions>>,
    write_conflict_detector: Option<Arc<ConflictDetector>>,

    uploader: HummockUploader,
    refiller: CacheRefiller,

    last_instance_id: LocalInstanceId,

    sstable_object_id_manager: Option<Arc<SstableObjectIdManager>>,
    metrics: HummockEventHandlerMetrics,
}

async fn flush_imms(
    payload: Vec<ImmutableMemtable>,
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
                            payload
                                .into_values()
                                .flat_map(|imms| imms.into_iter())
                                .collect(),
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
        spawn_refill_task: SpawnRefillTask,
    ) -> Self {
        let (hummock_event_tx, hummock_event_rx) =
            event_channel(state_store_metrics.event_handler_pending_event.clone());
        let (version_update_notifier_tx, _) =
            tokio::sync::watch::channel(pinned_version.visible_table_committed_epoch());
        let version_update_notifier_tx = Arc::new(version_update_notifier_tx);
        let read_version_mapping = Arc::new(RwLock::new(HashMap::default()));
        let buffer_tracker = BufferTracker::from_storage_opts(
            storage_opts,
            state_store_metrics.uploader_uploading_task_size.clone(),
        );
        let write_conflict_detector = ConflictDetector::new_from_config(storage_opts);

        let metrics = HummockEventHandlerMetrics {
            event_handler_on_upload_finish_latency: state_store_metrics
                .event_handler_latency
                .with_label_values(&["on_upload_finish"]),
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
            version_update_notifier_tx,
            recent_versions: Arc::new(ArcSwap::from_pointee(RecentVersions::new(
                pinned_version,
                storage_opts.max_cached_recent_versions_number,
            ))),
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

    pub fn recent_versions(&self) -> Arc<ArcSwap<RecentVersions>> {
        self.recent_versions.clone()
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
    /// This function will be performed under the protection of the `read_version_mapping` read
    /// lock, and add write lock on each `read_version` operation
    fn for_each_read_version(
        &self,
        instances: impl IntoIterator<Item = LocalInstanceId>,
        mut f: impl FnMut(LocalInstanceId, &mut HummockReadVersion),
    ) {
        let instances = {
            #[cfg(debug_assertions)]
            {
                // check duplication on debug_mode
                let mut id_set = std::collections::HashSet::new();
                for instance in instances {
                    assert!(id_set.insert(instance));
                }
                id_set
            }
            #[cfg(not(debug_assertions))]
            {
                instances
            }
        };
        let mut pending = VecDeque::new();
        let mut total_count = 0;
        for instance_id in instances {
            let Some((_, read_version)) = self.local_read_version_mapping.get(&instance_id) else {
                continue;
            };
            total_count += 1;
            if let Some(mut write_guard) = read_version.try_write() {
                f(instance_id, &mut write_guard);
            } else {
                pending.push_back(instance_id);
            }
        }
        if !pending.is_empty() {
            if pending.len() * 10 > total_count {
                // Only print warn log when failed to acquire more than 10%
                warn!(
                    pending_count = pending.len(),
                    total_count, "cannot acquire lock for all read version"
                );
            } else {
                debug!(
                    pending_count = pending.len(),
                    total_count, "cannot acquire lock for all read version"
                );
            }
        }

        const TRY_LOCK_TIMEOUT: Duration = Duration::from_millis(1);

        while let Some(instance_id) = pending.pop_front() {
            let (_, read_version) = self
                .local_read_version_mapping
                .get(&instance_id)
                .expect("have checked exist before");
            if let Some(mut write_guard) = read_version.try_write_for(TRY_LOCK_TIMEOUT) {
                f(instance_id, &mut write_guard);
            } else {
                warn!(instance_id, "failed to get lock again for instance");
                pending.push_back(instance_id);
            }
        }
    }

    fn handle_uploaded_sst_inner(&mut self, staging_sstable_info: Arc<StagingSstableInfo>) {
        trace!("data_flushed. SST size {}", staging_sstable_info.imm_size());
        self.for_each_read_version(
            staging_sstable_info.imm_ids().keys().cloned(),
            |_, read_version| {
                read_version.update(VersionUpdate::Staging(StagingData::Sst(
                    staging_sstable_info.clone(),
                )))
            },
        )
    }

    fn handle_sync_epoch(
        &mut self,
        new_sync_epoch: HummockEpoch,
        sync_result_sender: oneshot::Sender<HummockResult<SyncedData>>,
        table_ids: HashSet<TableId>,
    ) {
        debug!(
            new_sync_epoch,
            ?table_ids,
            "awaiting for epoch to be synced",
        );
        self.uploader
            .start_sync_epoch(new_sync_epoch, sync_result_sender, table_ids);
    }

    async fn handle_clear(&mut self, notifier: oneshot::Sender<()>, version_id: HummockVersionId) {
        info!(
            ?version_id,
            current_version_id = ?self.uploader.hummock_version().id(),
            "handle clear event"
        );

        self.uploader.clear();

        let current_version = self.uploader.hummock_version();

        if current_version.version().id < version_id {
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
                    ?version_id,
                    current_mce = current_version.visible_table_committed_epoch(),
                    refiller_mce = new_pinned_version.visible_table_committed_epoch(),
                    "refiller is clear in recovery"
                );

                Some(new_pinned_version)
            } else {
                None
            };

            while let latest_version_ref = latest_version.as_ref().unwrap_or(current_version)
                && latest_version_ref.version().id < version_id
            {
                let version_update = self
                    .version_update_rx
                    .recv()
                    .await
                    .expect("should not be empty");
                let prev_version_id = latest_version_ref.id();
                if let Some(new_version) = Self::resolve_version_update_info(
                    latest_version_ref.clone(),
                    version_update,
                    None,
                ) {
                    info!(
                        ?prev_version_id,
                        new_version_id = ?new_version.id(),
                        "recv new version"
                    );
                    latest_version = Some(new_version);
                }
            }

            self.apply_version_update(
                current_version.clone(),
                latest_version.expect("must have some version update to raise the mce"),
            );
        }

        assert!(
            self.local_read_version_mapping.is_empty(),
            "read version mapping not empty when clear. remaining tables: {:?}",
            self.local_read_version_mapping
                .values()
                .map(|(_, read_version)| read_version.read().table_id())
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

        info!(?version_id, "clear finished");
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
        if let Some(new_pinned_version) = Self::resolve_version_update_info(
            pinned_version.clone(),
            version_payload,
            Some(&mut sst_delta_infos),
        ) {
            self.refiller
                .start_cache_refill(sst_delta_infos, pinned_version, new_pinned_version);
        }
    }

    fn resolve_version_update_info(
        pinned_version: PinnedVersion,
        version_payload: HummockVersionUpdate,
        mut sst_delta_infos: Option<&mut Vec<SstDeltaInfo>>,
    ) -> Option<PinnedVersion> {
        let newly_pinned_version = match version_payload {
            HummockVersionUpdate::VersionDeltas(version_deltas) => {
                let mut version_to_apply = pinned_version.version().clone();
                for version_delta in &version_deltas {
                    assert_eq!(version_to_apply.id, version_delta.prev_id);
                    if let Some(sst_delta_infos) = &mut sst_delta_infos {
                        sst_delta_infos.extend(
                            version_to_apply
                                .build_sst_delta_infos(version_delta)
                                .into_iter(),
                        );
                    }
                    version_to_apply.apply_version_delta(version_delta);
                }

                version_to_apply
            }
            HummockVersionUpdate::PinnedVersion(version) => *version,
        };

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
        self.recent_versions.rcu(|prev_recent_versions| {
            prev_recent_versions.with_new_version(new_pinned_version.clone())
        });

        {
            self.for_each_read_version(
                self.local_read_version_mapping.keys().cloned(),
                |_, read_version| {
                    read_version
                        .update(VersionUpdate::CommittedSnapshot(new_pinned_version.clone()))
                },
            );
        }

        let prev_max_committed_epoch = pinned_version.visible_table_committed_epoch();
        let max_committed_epoch = new_pinned_version.visible_table_committed_epoch();

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

        // TODO: should we change the logic when supporting partial ckpt?
        if let Some(sstable_object_id_manager) = &self.sstable_object_id_manager {
            sstable_object_id_manager.remove_watermark_object_id(TrackerId::Epoch(
                self.recent_versions
                    .load()
                    .latest_version()
                    .visible_table_committed_epoch(),
            ));
        }

        debug!(
            "update to hummock version: {}, epoch: {}",
            new_pinned_version.id(),
            new_pinned_version.visible_table_committed_epoch()
        );

        self.uploader.update_pinned_version(new_pinned_version);
    }
}

impl HummockEventHandler {
    pub async fn start_hummock_event_handler_worker(mut self) {
        loop {
            tokio::select! {
                sst = self.uploader.next_uploaded_sst() => {
                    self.handle_uploaded_sst(sst);
                }
                event = self.refiller.next_event() => {
                    let CacheRefillerEvent {pinned_version, new_pinned_version } = event;
                    self.apply_version_update(pinned_version, new_pinned_version);
                }
                event = pin!(self.hummock_event_rx.recv()) => {
                    let Some(event) = event else { break };
                    match event {
                        HummockEvent::Clear(notifier, version_id) => {
                            self.handle_clear(notifier, version_id).await
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

    fn handle_uploaded_sst(&mut self, sst: Arc<StagingSstableInfo>) {
        let _timer = self
            .metrics
            .event_handler_on_upload_finish_latency
            .start_timer();
        self.handle_uploaded_sst_inner(sst);
    }

    /// Gracefully shutdown if returns `true`.
    fn handle_hummock_event(&mut self, event: HummockEvent) {
        match event {
            HummockEvent::BufferMayFlush => {
                self.uploader.may_flush();
            }
            HummockEvent::SyncEpoch {
                new_sync_epoch,
                sync_result_sender,
                table_ids,
            } => {
                self.handle_sync_epoch(new_sync_epoch, sync_result_sender, table_ids);
            }
            HummockEvent::Clear(_, _) => {
                unreachable!("clear is handled in separated async context")
            }
            HummockEvent::Shutdown => {
                unreachable!("shutdown is handled specially")
            }
            HummockEvent::StartEpoch { epoch, table_ids } => {
                self.uploader.start_epoch(epoch, table_ids);
            }
            HummockEvent::InitEpoch {
                instance_id,
                init_epoch,
            } => {
                let table_id = self
                    .local_read_version_mapping
                    .get(&instance_id)
                    .expect("should exist")
                    .0;
                self.uploader
                    .init_instance(instance_id, table_id, init_epoch);
            }
            HummockEvent::ImmToUploader { instance_id, imm } => {
                assert!(
                    self.local_read_version_mapping.contains_key(&instance_id),
                    "add imm from non-existing read version instance: instance_id: {}, table_id {}",
                    instance_id,
                    imm.table_id,
                );
                self.uploader.add_imm(instance_id, imm);
                self.uploader.may_flush();
            }

            HummockEvent::LocalSealEpoch {
                next_epoch,
                opts,
                instance_id,
            } => {
                self.uploader
                    .local_seal_epoch(instance_id, next_epoch, opts);
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
                let pinned_version = self.recent_versions.load().latest_version().clone();
                let instance_id = self.generate_instance_id();
                let basic_read_version = Arc::new(RwLock::new(
                    HummockReadVersion::new_with_replication_option(
                        table_id,
                        instance_id,
                        pinned_version,
                        is_replicated,
                        vnodes,
                    ),
                ));

                debug!(
                    "new read version registered: table_id: {}, instance_id: {}",
                    table_id, instance_id
                );

                {
                    self.local_read_version_mapping
                        .insert(instance_id, (table_id, basic_read_version.clone()));
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
                        self.destroy_read_version(instance_id);
                    }
                }
            }

            HummockEvent::DestroyReadVersion { instance_id } => {
                self.uploader.may_destroy_instance(instance_id);
                self.destroy_read_version(instance_id);
            }
        }
    }

    fn destroy_read_version(&mut self, instance_id: LocalInstanceId) {
        {
            {
                debug!("read version deregister: instance_id: {}", instance_id);
                let (table_id, _) = self
                    .local_read_version_mapping
                    .remove(&instance_id)
                    .unwrap_or_else(|| {
                        panic!(
                            "DestroyHummockInstance inexist instance instance_id {}",
                            instance_id
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

pub(super) fn send_sync_result(
    sender: oneshot::Sender<HummockResult<SyncedData>>,
    result: HummockResult<SyncedData>,
) {
    let _ = sender.send(result).inspect_err(|e| {
        error!("unable to send sync result. Err: {:?}", e);
    });
}

impl SyncedData {
    pub fn into_sync_result(self) -> SyncResult {
        {
            let SyncedData {
                uploaded_ssts,
                table_watermarks,
            } = self;
            let mut sync_size = 0;
            let mut uncommitted_ssts = Vec::new();
            let mut old_value_ssts = Vec::new();
            // The newly uploaded `sstable_infos` contains newer data. Therefore,
            // `newly_upload_ssts` at the front
            for sst in uploaded_ssts {
                sync_size += sst.imm_size();
                uncommitted_ssts.extend(sst.sstable_infos().iter().cloned());
                old_value_ssts.extend(sst.old_value_sstable_infos().iter().cloned());
            }
            SyncResult {
                sync_size,
                uncommitted_ssts,
                table_watermarks: table_watermarks.clone(),
                old_value_ssts,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::future::{poll_fn, Future};
    use std::sync::Arc;
    use std::task::Poll;

    use futures::FutureExt;
    use parking_lot::Mutex;
    use risingwave_common::bitmap::BitmapBuilder;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::{test_epoch, EpochExt};
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_pb::hummock::PbHummockVersion;
    use tokio::spawn;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot;

    use crate::hummock::event_handler::refiller::CacheRefiller;
    use crate::hummock::event_handler::uploader::test_utils::{gen_imm, TEST_TABLE_ID};
    use crate::hummock::event_handler::uploader::UploadTaskOutput;
    use crate::hummock::event_handler::{HummockEvent, HummockEventHandler, HummockVersionUpdate};
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::store::version::{StagingData, VersionUpdate};
    use crate::hummock::test_utils::default_opts_for_test;
    use crate::hummock::HummockError;
    use crate::monitor::HummockStateStoreMetrics;
    use crate::store::SealCurrentEpochOptions;

    #[tokio::test]
    async fn test_clear_shared_buffer() {
        let epoch0 = test_epoch(233);
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
            mock_sstable_store().await,
            Arc::new(HummockStateStoreMetrics::unused()),
            &default_opts_for_test(),
            Arc::new(|_, _| unreachable!("should not spawn upload task")),
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
        let latest_version = event_handler.recent_versions.clone();
        let latest_version_update_tx = event_handler.version_update_notifier_tx.clone();

        let send_clear = |version_id| {
            let (tx, rx) = oneshot::channel();
            event_tx.send(HummockEvent::Clear(tx, version_id)).unwrap();
            rx
        };

        let _join_handle = spawn(event_handler.start_hummock_event_handler_worker());

        // test normal recovery
        send_clear(initial_version.id()).await.unwrap();

        // test normal refill finish
        let epoch1 = epoch0 + 1;
        let version1 = make_new_version(epoch1);
        {
            version_update_tx
                .send(HummockVersionUpdate::PinnedVersion(Box::new(
                    version1.clone(),
                )))
                .unwrap();
            let (old_version, new_version, refill_finish_tx) = refill_task_rx.recv().await.unwrap();
            assert_eq!(old_version.version(), initial_version.version());
            assert_eq!(new_version.version(), &version1);
            assert_eq!(
                latest_version.load().latest_version().version(),
                initial_version.version()
            );

            let mut changed = latest_version_update_tx.subscribe();
            refill_finish_tx.send(()).unwrap();
            changed.changed().await.unwrap();
            assert_eq!(latest_version.load().latest_version().version(), &version1);
        }

        // test recovery with pending refill task
        let epoch2 = epoch1 + 1;
        let version2 = make_new_version(epoch2);
        let epoch3 = epoch2 + 1;
        let version3 = make_new_version(epoch3);
        {
            version_update_tx
                .send(HummockVersionUpdate::PinnedVersion(Box::new(
                    version2.clone(),
                )))
                .unwrap();
            version_update_tx
                .send(HummockVersionUpdate::PinnedVersion(Box::new(
                    version3.clone(),
                )))
                .unwrap();
            let (old_version2, new_version2, _refill_finish_tx2) =
                refill_task_rx.recv().await.unwrap();
            assert_eq!(old_version2.version(), &version1);
            assert_eq!(new_version2.version(), &version2);
            let (old_version3, new_version3, _refill_finish_tx3) =
                refill_task_rx.recv().await.unwrap();
            assert_eq!(old_version3.version(), &version2);
            assert_eq!(new_version3.version(), &version3);
            assert_eq!(latest_version.load().latest_version().version(), &version1);

            let rx = send_clear(version3.id);
            rx.await.unwrap();
            assert_eq!(latest_version.load().latest_version().version(), &version3);
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
            let mut rx = send_clear(version5.id);
            assert_pending(&mut rx).await;
            version_update_tx
                .send(HummockVersionUpdate::PinnedVersion(Box::new(
                    version4.clone(),
                )))
                .unwrap();
            assert_pending(&mut rx).await;
            version_update_tx
                .send(HummockVersionUpdate::PinnedVersion(Box::new(
                    version5.clone(),
                )))
                .unwrap();
            rx.await.unwrap();
            assert_eq!(latest_version.load().latest_version().version(), &version5);
        }
    }

    #[tokio::test]
    async fn test_old_epoch_sync_fail() {
        let epoch0 = test_epoch(233);

        let initial_version = PinnedVersion::new(
            HummockVersion::from_rpc_protobuf(&PbHummockVersion {
                id: 1,
                max_committed_epoch: epoch0,
                ..Default::default()
            }),
            unbounded_channel().0,
        );

        let (_version_update_tx, version_update_rx) = unbounded_channel();

        let epoch1 = epoch0.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let (tx, rx) = oneshot::channel();
        let rx = Arc::new(Mutex::new(Some(rx)));

        let event_handler = HummockEventHandler::new_inner(
            version_update_rx,
            initial_version.clone(),
            None,
            mock_sstable_store().await,
            Arc::new(HummockStateStoreMetrics::unused()),
            &default_opts_for_test(),
            Arc::new(move |_, info| {
                assert_eq!(info.epochs.len(), 1);
                let epoch = info.epochs[0];
                match epoch {
                    epoch if epoch == epoch1 => {
                        let rx = rx.lock().take().unwrap();
                        spawn(async move {
                            rx.await.unwrap();
                            Err(HummockError::other("fail"))
                        })
                    }
                    epoch if epoch == epoch2 => spawn(async move {
                        Ok(UploadTaskOutput {
                            new_value_ssts: vec![],
                            old_value_ssts: vec![],
                            wait_poll_timer: None,
                        })
                    }),
                    _ => unreachable!(),
                }
            }),
            CacheRefiller::default_spawn_refill_task(),
        );

        let event_tx = event_handler.event_sender();

        let send_event = |event| event_tx.send(event).unwrap();

        let _join_handle = spawn(event_handler.start_hummock_event_handler_worker());

        let (read_version, guard) = {
            let (tx, rx) = oneshot::channel();
            send_event(HummockEvent::RegisterReadVersion {
                table_id: TEST_TABLE_ID,
                new_read_version_sender: tx,
                is_replicated: false,
                vnodes: Arc::new(BitmapBuilder::filled(VirtualNode::COUNT).finish()),
            });
            rx.await.unwrap()
        };

        send_event(HummockEvent::StartEpoch {
            epoch: epoch1,
            table_ids: HashSet::from_iter([TEST_TABLE_ID]),
        });

        send_event(HummockEvent::InitEpoch {
            instance_id: guard.instance_id,
            init_epoch: epoch1,
        });

        let imm1 = gen_imm(epoch1).await;
        read_version
            .write()
            .update(VersionUpdate::Staging(StagingData::ImmMem(imm1.clone())));

        send_event(HummockEvent::ImmToUploader {
            instance_id: guard.instance_id,
            imm: imm1,
        });

        send_event(HummockEvent::StartEpoch {
            epoch: epoch2,
            table_ids: HashSet::from_iter([TEST_TABLE_ID]),
        });

        send_event(HummockEvent::LocalSealEpoch {
            instance_id: guard.instance_id,
            next_epoch: epoch2,
            opts: SealCurrentEpochOptions::for_test(),
        });

        let imm2 = gen_imm(epoch2).await;
        read_version
            .write()
            .update(VersionUpdate::Staging(StagingData::ImmMem(imm2.clone())));

        send_event(HummockEvent::ImmToUploader {
            instance_id: guard.instance_id,
            imm: imm2,
        });

        let epoch3 = epoch2.next_epoch();
        send_event(HummockEvent::StartEpoch {
            epoch: epoch3,
            table_ids: HashSet::from_iter([TEST_TABLE_ID]),
        });
        send_event(HummockEvent::LocalSealEpoch {
            instance_id: guard.instance_id,
            next_epoch: epoch3,
            opts: SealCurrentEpochOptions::for_test(),
        });

        let (tx1, mut rx1) = oneshot::channel();
        send_event(HummockEvent::SyncEpoch {
            new_sync_epoch: epoch1,
            sync_result_sender: tx1,
            table_ids: HashSet::from_iter([TEST_TABLE_ID]),
        });
        assert!(poll_fn(|cx| Poll::Ready(rx1.poll_unpin(cx).is_pending())).await);
        let (tx2, mut rx2) = oneshot::channel();
        send_event(HummockEvent::SyncEpoch {
            new_sync_epoch: epoch2,
            sync_result_sender: tx2,
            table_ids: HashSet::from_iter([TEST_TABLE_ID]),
        });
        assert!(poll_fn(|cx| Poll::Ready(rx2.poll_unpin(cx).is_pending())).await);

        tx.send(()).unwrap();
        rx1.await.unwrap().unwrap_err();
        rx2.await.unwrap().unwrap_err();
    }
}

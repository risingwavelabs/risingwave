// Copyright 2025 RisingWave Labs
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
use await_tree::{InstrumentAwait, SpanExt};
use futures::FutureExt;
use itertools::Itertools;
use parking_lot::RwLock;
use prometheus::{Histogram, IntGauge};
use risingwave_common::catalog::TableId;
use risingwave_common::metrics::UintGauge;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::version::{HummockVersionCommon, LocalHummockVersionDelta};
use risingwave_hummock_sdk::{HummockEpoch, SyncResult};
use tokio::spawn;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace, warn};

use super::refiller::{CacheRefillConfig, CacheRefiller};
use super::{LocalInstanceGuard, LocalInstanceId, ReadVersionMappingType};
use crate::compaction_catalog_manager::CompactionCatalogManagerRef;
use crate::hummock::compactor::{CompactorContext, await_tree_key, compact};
use crate::hummock::event_handler::refiller::{CacheRefillerEvent, SpawnRefillTask};
use crate::hummock::event_handler::uploader::{
    HummockUploader, SpawnUploadTask, SyncedData, UploadTaskOutput,
};
use crate::hummock::event_handler::{
    HummockEvent, HummockReadVersionRef, HummockVersionUpdate, ReadOnlyReadVersionMapping,
    ReadOnlyRwLockRef,
};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::local_version::recent_versions::RecentVersions;
use crate::hummock::store::version::{HummockReadVersion, StagingSstableInfo, VersionUpdate};
use crate::hummock::{HummockResult, MemoryLimiter, ObjectIdManager, SstableStoreRef};
use crate::mem_table::ImmutableMemtable;
use crate::monitor::HummockStateStoreMetrics;
use crate::opts::StorageOpts;

#[derive(Clone)]
pub(crate) struct BufferTracker {
    flush_threshold: usize,
    min_batch_flush_size: usize,
    global_uploading_memory_limiter: Arc<MemoryLimiter>,
    uploader_imm_size: UintGauge,
    uploader_uploading_task_size: UintGauge,
}

impl BufferTracker {
    pub fn from_storage_opts(config: &StorageOpts, metrics: &HummockStateStoreMetrics) -> Self {
        let capacity = config.shared_buffer_capacity_mb * (1 << 20);
        let flush_threshold = (capacity as f32 * config.shared_buffer_flush_ratio) as usize;
        let min_batch_flush_size = config.shared_buffer_min_batch_flush_size_mb * (1 << 20);
        assert!(
            flush_threshold < capacity,
            "flush_threshold {} should be less or equal to capacity {}",
            flush_threshold,
            capacity
        );
        Self {
            flush_threshold,
            min_batch_flush_size,
            global_uploading_memory_limiter: Arc::new(MemoryLimiter::new(capacity as u64)),
            uploader_imm_size: metrics.uploader_imm_size.clone(),
            uploader_uploading_task_size: metrics.uploader_uploading_task_size.clone(),
        }
    }

    #[cfg(test)]
    fn for_test_with_config(flush_threshold: usize, min_batch_flush_size: usize) -> Self {
        Self {
            flush_threshold,
            min_batch_flush_size,
            ..Self::for_test()
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::from_storage_opts(&StorageOpts::default(), &HummockStateStoreMetrics::unused())
    }

    pub fn get_memory_limiter(&self) -> &Arc<MemoryLimiter> {
        &self.global_uploading_memory_limiter
    }

    pub fn global_upload_task_size(&self) -> &UintGauge {
        &self.uploader_uploading_task_size
    }

    /// Return true when the buffer size minus current upload task size is still greater than the
    /// flush threshold.
    pub fn need_flush(&self) -> bool {
        self.uploader_imm_size.get()
            > self.flush_threshold as u64 + self.uploader_uploading_task_size.get()
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

pub(crate) struct HummockEventHandler {
    hummock_event_tx: HummockEventSender,
    hummock_event_rx: HummockEventReceiver,
    version_update_rx: UnboundedReceiver<HummockVersionUpdate>,
    read_version_mapping: Arc<RwLock<ReadVersionMappingType>>,
    /// A copy of `read_version_mapping` but owned by event handler
    local_read_version_mapping: HashMap<LocalInstanceId, (TableId, HummockReadVersionRef)>,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<PinnedVersion>>,
    recent_versions: Arc<ArcSwap<RecentVersions>>,

    uploader: HummockUploader,
    refiller: CacheRefiller,

    last_instance_id: LocalInstanceId,

    metrics: HummockEventHandlerMetrics,
}

async fn flush_imms(
    payload: Vec<ImmutableMemtable>,
    compactor_context: CompactorContext,
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
    object_id_manager: Arc<ObjectIdManager>,
) -> HummockResult<UploadTaskOutput> {
    compact(
        compactor_context,
        object_id_manager,
        payload,
        compaction_catalog_manager_ref,
    )
    .instrument_await("shared_buffer_compact".verbose())
    .await
}

impl HummockEventHandler {
    pub fn new(
        version_update_rx: UnboundedReceiver<HummockVersionUpdate>,
        pinned_version: PinnedVersion,
        compactor_context: CompactorContext,
        compaction_catalog_manager_ref: CompactionCatalogManagerRef,
        object_id_manager: Arc<ObjectIdManager>,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
    ) -> Self {
        let upload_compactor_context = compactor_context.clone();
        let upload_task_latency = state_store_metrics.uploader_upload_task_latency.clone();
        let wait_poll_latency = state_store_metrics.uploader_wait_poll_latency.clone();
        let recent_versions = RecentVersions::new(
            pinned_version,
            compactor_context
                .storage_opts
                .max_cached_recent_versions_number,
            state_store_metrics.clone(),
        );
        let buffer_tracker =
            BufferTracker::from_storage_opts(&compactor_context.storage_opts, &state_store_metrics);
        Self::new_inner(
            version_update_rx,
            compactor_context.sstable_store.clone(),
            state_store_metrics,
            CacheRefillConfig::from_storage_opts(&compactor_context.storage_opts),
            recent_versions,
            buffer_tracker,
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
                let compaction_catalog_manager_ref = compaction_catalog_manager_ref.clone();
                let object_id_manager = object_id_manager.clone();
                spawn({
                    let future = async move {
                        let _timer = upload_task_latency.start_timer();
                        let mut output = flush_imms(
                            payload
                                .into_values()
                                .flat_map(|imms| imms.into_iter())
                                .collect(),
                            upload_compactor_context.clone(),
                            compaction_catalog_manager_ref.clone(),
                            object_id_manager.clone(),
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
        sstable_store: SstableStoreRef,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        refill_config: CacheRefillConfig,
        recent_versions: RecentVersions,
        buffer_tracker: BufferTracker,
        spawn_upload_task: SpawnUploadTask,
        spawn_refill_task: SpawnRefillTask,
    ) -> Self {
        let (hummock_event_tx, hummock_event_rx) =
            event_channel(state_store_metrics.event_handler_pending_event.clone());
        let (version_update_notifier_tx, _) =
            tokio::sync::watch::channel(recent_versions.latest_version().clone());
        let version_update_notifier_tx = Arc::new(version_update_notifier_tx);
        let read_version_mapping = Arc::new(RwLock::new(HashMap::default()));

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
            recent_versions.latest_version().clone(),
            spawn_upload_task,
            buffer_tracker,
        );
        let refiller = CacheRefiller::new(refill_config, sstable_store, spawn_refill_task);

        Self {
            hummock_event_tx,
            hummock_event_rx,
            version_update_rx,
            version_update_notifier_tx,
            recent_versions: Arc::new(ArcSwap::from_pointee(recent_versions)),
            read_version_mapping,
            local_read_version_mapping: Default::default(),
            uploader,
            refiller,
            last_instance_id: 0,
            metrics,
        }
    }

    pub fn version_update_notifier_tx(&self) -> Arc<tokio::sync::watch::Sender<PinnedVersion>> {
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
            match read_version.try_write() {
                Some(mut write_guard) => {
                    f(instance_id, &mut write_guard);
                }
                _ => {
                    pending.push_back(instance_id);
                }
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
            match read_version.try_write_for(TRY_LOCK_TIMEOUT) {
                Some(mut write_guard) => {
                    f(instance_id, &mut write_guard);
                }
                _ => {
                    warn!(instance_id, "failed to get lock again for instance");
                    pending.push_back(instance_id);
                }
            }
        }
    }

    fn handle_uploaded_sst_inner(&mut self, staging_sstable_info: Arc<StagingSstableInfo>) {
        trace!("data_flushed. SST size {}", staging_sstable_info.imm_size());
        self.for_each_read_version(
            staging_sstable_info.imm_ids().keys().cloned(),
            |_, read_version| read_version.update(VersionUpdate::Sst(staging_sstable_info.clone())),
        )
    }

    fn handle_sync_epoch(
        &mut self,
        sync_table_epochs: Vec<(HummockEpoch, HashSet<TableId>)>,
        sync_result_sender: oneshot::Sender<HummockResult<SyncedData>>,
    ) {
        debug!(?sync_table_epochs, "awaiting for epoch to be synced",);
        self.uploader
            .start_sync_epoch(sync_result_sender, sync_table_epochs);
    }

    fn handle_clear(&mut self, notifier: oneshot::Sender<()>, table_ids: Option<HashSet<TableId>>) {
        info!(
            current_version_id = ?self.uploader.hummock_version().id(),
            "handle clear event"
        );

        self.uploader.clear(table_ids.clone());

        if table_ids.is_none() {
            assert!(
                self.local_read_version_mapping.is_empty(),
                "read version mapping not empty when clear. remaining tables: {:?}",
                self.local_read_version_mapping
                    .values()
                    .map(|(_, read_version)| read_version.read().table_id())
                    .collect_vec()
            );
        }

        // Notify completion of the Clear event.
        let _ = notifier.send(()).inspect_err(|e| {
            error!("failed to notify completion of clear event: {:?}", e);
        });

        info!("clear finished");
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
            &pinned_version,
            version_payload,
            Some(&mut sst_delta_infos),
        ) {
            self.refiller
                .start_cache_refill(sst_delta_infos, pinned_version, new_pinned_version);
        }
    }

    fn resolve_version_update_info(
        pinned_version: &PinnedVersion,
        version_payload: HummockVersionUpdate,
        mut sst_delta_infos: Option<&mut Vec<SstDeltaInfo>>,
    ) -> Option<PinnedVersion> {
        match version_payload {
            HummockVersionUpdate::VersionDeltas(version_deltas) => {
                let mut version_to_apply = (**pinned_version).clone();
                {
                    let mut table_change_log_to_apply_guard =
                        pinned_version.table_change_log_write_lock();
                    for version_delta in version_deltas {
                        assert_eq!(version_to_apply.id, version_delta.prev_id);

                        // apply change-log-delta
                        {
                            let mut state_table_info = version_to_apply.state_table_info.clone();
                            let (changed_table_info, _is_commit_epoch) = state_table_info
                                .apply_delta(
                                    &version_delta.state_table_info_delta,
                                    &version_delta.removed_table_ids,
                                );

                            HummockVersionCommon::<SstableInfo>::apply_change_log_delta(
                                &mut *table_change_log_to_apply_guard,
                                &version_delta.change_log_delta,
                                &version_delta.removed_table_ids,
                                &version_delta.state_table_info_delta,
                                &changed_table_info,
                            );
                        }

                        let local_hummock_version_delta =
                            LocalHummockVersionDelta::from(version_delta);
                        if let Some(sst_delta_infos) = &mut sst_delta_infos {
                            sst_delta_infos.extend(
                                version_to_apply
                                    .build_sst_delta_infos(&local_hummock_version_delta)
                                    .into_iter(),
                            );
                        }

                        version_to_apply.apply_version_delta(&local_hummock_version_delta);
                    }
                }

                pinned_version.new_with_local_version(version_to_apply)
            }
            HummockVersionUpdate::PinnedVersion(version) => {
                pinned_version.new_pin_version(*version)
            }
        }
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

        self.version_update_notifier_tx.send_if_modified(|state| {
            assert_eq!(pinned_version.id(), state.id());
            if state.id() == new_pinned_version.id() {
                return false;
            }
            assert!(new_pinned_version.id() > state.id());
            *state = new_pinned_version.clone();
            true
        });

        debug!("update to hummock version: {}", new_pinned_version.id(),);

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
                sync_result_sender,
                sync_table_epochs,
            } => {
                self.handle_sync_epoch(sync_table_epochs, sync_result_sender);
            }
            HummockEvent::Clear(notifier, table_ids) => {
                self.handle_clear(notifier, table_ids);
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
            HummockEvent::ImmToUploader { instance_id, imms } => {
                assert!(
                    self.local_read_version_mapping.contains_key(&instance_id),
                    "add imm from non-existing read version instance: instance_id: {}, table_id {:?}",
                    instance_id,
                    imms.first().map(|(imm, _)| imm.table_id),
                );
                self.uploader.add_imms(instance_id, imms);
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
            HummockEvent::GetMinUncommittedObjectId { result_tx } => {
                let _ = result_tx
                    .send(self.uploader.min_uncommitted_object_id())
                    .inspect_err(|e| {
                        error!("unable to send get_min_uncommitted_sst_id result: {:?}", e);
                    });
            }
            HummockEvent::RegisterVectorWriter {
                table_id,
                init_epoch,
            } => self.uploader.register_vector_writer(table_id, init_epoch),
            HummockEvent::VectorWriterSealEpoch {
                table_id,
                next_epoch,
                add,
            } => {
                self.uploader
                    .vector_writer_seal_epoch(table_id, next_epoch, add);
            }
            HummockEvent::DropVectorWriter { table_id } => {
                self.uploader.drop_vector_writer(table_id);
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
                vector_index_adds,
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
                table_watermarks,
                old_value_ssts,
                vector_index_adds,
            }
        }
    }
}

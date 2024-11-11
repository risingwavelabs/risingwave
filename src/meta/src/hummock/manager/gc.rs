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

use std::cmp;
use std::collections::HashSet;
use std::ops::Bound::{Excluded, Included};
use std::ops::DerefMut;
use std::time::{Duration, SystemTime};

use chrono::DateTime;
use futures::future::try_join_all;
use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::{get_sst_data_path, HummockSstableObjectId};
use risingwave_meta_model::hummock_sequence::HUMMOCK_NOW;
use risingwave_meta_model::{hummock_gc_history, hummock_sequence};
use risingwave_meta_model_migration::OnConflict;
use risingwave_object_store::object::ObjectStoreRef;
use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;
use risingwave_pb::hummock::FullScanTask;
use risingwave_pb::stream_service::GetMinUncommittedSstIdRequest;
use risingwave_rpc_client::StreamClientPool;
use sea_orm::{ActiveValue, ColumnTrait, EntityTrait, QueryFilter, Set};

use crate::hummock::error::{Error, Result};
use crate::hummock::manager::commit_multi_var;
use crate::hummock::HummockManager;
use crate::manager::MetadataManager;
use crate::model::BTreeMapTransaction;
use crate::MetaResult;

#[derive(Default)]
pub(super) struct DeleteObjectTracker {
    /// Objects that waits to be deleted from object store. It comes from either compaction, or
    /// full GC (listing object store).
    objects_to_delete: Mutex<HashSet<HummockSstableObjectId>>,
}

impl DeleteObjectTracker {
    pub(super) fn add(&self, objects: impl Iterator<Item = HummockSstableObjectId>) {
        self.objects_to_delete.lock().extend(objects)
    }

    pub(super) fn current(&self) -> HashSet<HummockSstableObjectId> {
        self.objects_to_delete.lock().clone()
    }

    pub(super) fn clear(&self) {
        self.objects_to_delete.lock().clear();
    }

    pub(super) fn ack<'a>(&self, objects: impl Iterator<Item = &'a HummockSstableObjectId>) {
        let mut lock = self.objects_to_delete.lock();
        for object in objects {
            lock.remove(object);
        }
    }
}

pub(crate) struct GcManager {
    store: ObjectStoreRef,
    path_prefix: String,
    use_new_object_prefix_strategy: bool,
}

impl GcManager {
    pub fn new(
        store: ObjectStoreRef,
        path_prefix: &str,
        use_new_object_prefix_strategy: bool,
    ) -> Self {
        Self {
            store,
            path_prefix: path_prefix.to_owned(),
            use_new_object_prefix_strategy,
        }
    }

    /// Deletes all SSTs specified in the given list of IDs from storage.
    pub async fn delete_list(&self, object_id_list: &[HummockSstableObjectId]) -> Result<()> {
        let mut paths = Vec::with_capacity(object_id_list.len() * 2);
        for &object_id in object_id_list {
            let obj_prefix = self
                .store
                .get_object_prefix(object_id, self.use_new_object_prefix_strategy);
            paths.push(get_sst_data_path(&obj_prefix, &self.path_prefix, object_id));
        }
        self.store.delete_objects(&paths).await?;
        Ok(())
    }
}

impl HummockManager {
    /// Gets SST objects that is safe to be deleted from object store.
    pub fn get_objects_to_delete(&self) -> Vec<HummockSstableObjectId> {
        self.delete_object_tracker
            .current()
            .iter()
            .cloned()
            .collect_vec()
    }

    /// Acknowledges SSTs have been deleted from object store.
    pub async fn ack_deleted_objects(&self, object_ids: &[HummockSstableObjectId]) -> Result<()> {
        self.delete_object_tracker.ack(object_ids.iter());
        let mut versioning_guard = self.versioning.write().await;
        for stale_objects in versioning_guard.checkpoint.stale_objects.values_mut() {
            stale_objects.id.retain(|id| !object_ids.contains(id));
        }
        versioning_guard
            .checkpoint
            .stale_objects
            .retain(|_, stale_objects| !stale_objects.id.is_empty());
        drop(versioning_guard);
        Ok(())
    }

    /// Deletes at most `batch_size` deltas.
    ///
    /// Returns (number of deleted deltas, number of remain `deltas_to_delete`).
    pub async fn delete_version_deltas(&self, batch_size: usize) -> Result<(usize, usize)> {
        let mut versioning_guard = self.versioning.write().await;
        let versioning = versioning_guard.deref_mut();
        let context_info = self.context_info.read().await;
        let deltas_to_delete = versioning
            .hummock_version_deltas
            .range(..=versioning.checkpoint.version.id)
            .map(|(k, _)| *k)
            .collect_vec();
        // If there is any safe point, skip this to ensure meta backup has required delta logs to
        // replay version.
        if !context_info.version_safe_points.is_empty() {
            return Ok((0, deltas_to_delete.len()));
        }
        let mut hummock_version_deltas =
            BTreeMapTransaction::new(&mut versioning.hummock_version_deltas);
        let batch = deltas_to_delete
            .iter()
            .take(batch_size)
            .cloned()
            .collect_vec();
        if batch.is_empty() {
            return Ok((0, 0));
        }
        for delta_id in &batch {
            hummock_version_deltas.remove(*delta_id);
        }
        commit_multi_var!(self.meta_store_ref(), hummock_version_deltas)?;
        #[cfg(test)]
        {
            drop(context_info);
            drop(versioning_guard);
            self.check_state_consistency().await;
        }
        Ok((batch.len(), deltas_to_delete.len() - batch.len()))
    }

    /// Extends `objects_to_delete` according to object store full scan result.
    /// Caller should ensure `object_ids` doesn't include any SST objects belong to a on-going
    /// version write. That's to say, these `object_ids` won't appear in either `commit_epoch` or
    /// `report_compact_task`.
    pub async fn extend_objects_to_delete_from_scan(
        &self,
        object_ids: &[HummockSstableObjectId],
    ) -> Result<usize> {
        let tracked_object_ids: HashSet<HummockSstableObjectId> = {
            let versioning = self.versioning.read().await;
            let context_info = self.context_info.read().await;

            // object ids in checkpoint version
            let mut tracked_object_ids = versioning.checkpoint.version.get_object_ids();
            // add object ids added between checkpoint version and current version
            for (_, delta) in versioning.hummock_version_deltas.range((
                Excluded(versioning.checkpoint.version.id),
                Included(versioning.current_version.id),
            )) {
                tracked_object_ids.extend(delta.newly_added_object_ids());
            }
            // add stale object ids before the checkpoint version
            let min_pinned_version_id = context_info.min_pinned_version_id();
            tracked_object_ids.extend(
                versioning
                    .checkpoint
                    .stale_objects
                    .iter()
                    .filter(|(version_id, _)| **version_id >= min_pinned_version_id)
                    .flat_map(|(_, objects)| objects.id.iter())
                    .cloned(),
            );
            tracked_object_ids
        };
        let to_delete = object_ids
            .iter()
            .filter(|object_id| !tracked_object_ids.contains(object_id))
            .copied()
            .collect_vec();
        let to_delete_num = to_delete.len();
        self.write_gc_history(to_delete.iter().cloned()).await?;
        // This lock ensures that during commit_epoch or report_compact_tasks, where versioning lock is held,
        // no new objects will be marked for deletion here.
        let _versioning = self.versioning.read().await;
        self.delete_object_tracker.add(to_delete.into_iter());
        Ok(to_delete_num)
    }

    /// Starts a full GC.
    /// 1. Meta node sends a `FullScanTask` to a compactor in this method.
    /// 2. The compactor returns scan result of object store to meta node. See
    ///    `HummockManager::full_scan_inner` in storage crate.
    /// 3. Meta node decides which SSTs to delete. See `HummockManager::complete_full_gc`.
    ///
    /// Returns Ok(false) if there is no worker available.
    pub async fn start_full_gc(
        &self,
        sst_retention_time: Duration,
        prefix: Option<String>,
    ) -> Result<bool> {
        self.metrics.full_gc_trigger_count.inc();
        // Set a minimum sst_retention_time.
        let sst_retention_time = cmp::max(
            sst_retention_time,
            Duration::from_secs(self.env.opts.min_sst_retention_time_sec),
        );
        let start_after = self.full_gc_state.next_start_after();
        let limit = self.full_gc_state.limit;
        tracing::info!(
            retention_sec = sst_retention_time.as_secs(),
            prefix = prefix.as_ref().unwrap_or(&String::from("")),
            start_after,
            limit,
            "run full GC"
        );

        let compactor = match self.compactor_manager.next_compactor() {
            None => {
                tracing::warn!("full GC attempt but no available idle worker");
                return Ok(false);
            }
            Some(compactor) => compactor,
        };
        let sst_retention_watermark = self
            .now()
            .await?
            .saturating_sub(sst_retention_time.as_secs());
        compactor
            .send_event(ResponseEvent::FullScanTask(FullScanTask {
                sst_retention_watermark,
                prefix,
                start_after,
                limit,
            }))
            .map_err(|_| Error::CompactorUnreachable(compactor.context_id()))?;
        Ok(true)
    }

    /// Given candidate SSTs to GC, filter out false positive.
    /// Returns number of SSTs to GC.
    pub async fn complete_full_gc(
        &self,
        object_ids: Vec<HummockSstableObjectId>,
        next_start_after: Option<String>,
        pinned_by_metadata_backup: HashSet<HummockSstableObjectId>,
    ) -> Result<usize> {
        // It's crucial to collect_min_uncommitted_sst_id (i.e. `min_sst_id`) only after LIST object store (i.e. `object_ids`).
        // Because after getting `min_sst_id`, new compute nodes may join and generate new uncommitted SSTs that are not covered by `min_sst_id`.
        // By getting `min_sst_id` after `object_ids`, it's ensured `object_ids` won't include any SSTs from those new compute nodes.
        let min_sst_id =
            collect_min_uncommitted_sst_id(&self.metadata_manager, self.env.stream_client_pool())
                .await?;
        self.full_gc_state.set_next_start_after(next_start_after);
        if object_ids.is_empty() {
            tracing::info!("SST full scan returns no SSTs.");
            return Ok(0);
        }
        let metrics = &self.metrics;
        let candidate_object_number = object_ids.len();
        metrics
            .full_gc_candidate_object_count
            .observe(candidate_object_number as _);
        let pinned_object_ids = self
            .all_object_ids_in_time_travel()
            .await?
            .collect::<HashSet<_>>();
        self.metrics
            .time_travel_object_count
            .set(pinned_object_ids.len() as _);
        // filter by SST id watermark, i.e. minimum id of uncommitted SSTs reported by compute nodes.
        let object_ids = object_ids
            .into_iter()
            .filter(|id| *id < min_sst_id)
            .collect_vec();
        let after_min_sst_id = object_ids.len();
        // filter by time travel archive
        let object_ids = object_ids
            .into_iter()
            .filter(|s| !pinned_object_ids.contains(s))
            .collect_vec();
        let after_time_travel = object_ids.len();
        // filter by metadata backup
        let object_ids = object_ids
            .into_iter()
            .filter(|s| !pinned_by_metadata_backup.contains(s))
            .collect_vec();
        let after_metadata_backup = object_ids.len();
        // filter by version
        let after_version = self.extend_objects_to_delete_from_scan(&object_ids).await?;
        metrics
            .full_gc_selected_object_count
            .observe(after_version as _);
        tracing::info!(
            candidate_object_number,
            after_min_sst_id,
            after_time_travel,
            after_metadata_backup,
            after_version,
            "complete full gc"
        );
        Ok(after_version)
    }

    pub async fn now(&self) -> Result<u64> {
        let mut guard = self.now.lock().await;
        let new_now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        if new_now < *guard {
            return Err(anyhow::anyhow!(format!(
                "unexpected decreasing now, old={}, new={}",
                *guard, new_now
            ))
            .into());
        }
        *guard = new_now;
        drop(guard);
        // Persist now to maintain non-decreasing even after a meta node reboot.
        let m = hummock_sequence::ActiveModel {
            name: ActiveValue::Set(HUMMOCK_NOW.into()),
            seq: ActiveValue::Set(new_now.try_into().unwrap()),
        };
        hummock_sequence::Entity::insert(m)
            .on_conflict(
                OnConflict::column(hummock_sequence::Column::Name)
                    .update_column(hummock_sequence::Column::Seq)
                    .to_owned(),
            )
            .exec(&self.env.meta_store_ref().conn)
            .await?;
        Ok(new_now)
    }

    pub(crate) async fn load_now(&self) -> Result<Option<u64>> {
        let now = hummock_sequence::Entity::find_by_id(HUMMOCK_NOW.to_string())
            .one(&self.env.meta_store_ref().conn)
            .await?
            .map(|m| m.seq.try_into().unwrap());
        Ok(now)
    }

    pub fn update_paged_metrics(
        &self,
        start_after: Option<String>,
        next_start_after: Option<String>,
        total_object_count_in_page: u64,
        total_object_size_in_page: u64,
    ) {
        let mut paged_metrics = self.paged_metrics.lock();
        paged_metrics.total_object_size.update(
            start_after.clone(),
            next_start_after.clone(),
            total_object_size_in_page,
        );
        paged_metrics.total_object_count.update(
            start_after,
            next_start_after,
            total_object_count_in_page,
        );
        if let Some(total_object_size) = paged_metrics.total_object_size.take() {
            self.metrics.total_object_size.set(total_object_size as _);
        }
        if let Some(total_object_count) = paged_metrics.total_object_count.take() {
            self.metrics.total_object_count.set(total_object_count as _);
        }
    }

    async fn write_gc_history(
        &self,
        object_ids: impl Iterator<Item = HummockSstableObjectId>,
    ) -> Result<()> {
        if self.env.opts.gc_history_retention_time_sec == 0 {
            return Ok(());
        }
        let now = self.now().await?;
        let dt = DateTime::from_timestamp(now.try_into().unwrap(), 0).unwrap();
        let models = object_ids.map(|o| hummock_gc_history::ActiveModel {
            object_id: Set(o.try_into().unwrap()),
            mark_delete_at: Set(dt.naive_utc()),
        });
        let db = &self.meta_store_ref().conn;
        let gc_history_low_watermark = DateTime::from_timestamp(
            now.saturating_sub(self.env.opts.gc_history_retention_time_sec)
                .try_into()
                .unwrap(),
            0,
        )
        .unwrap();
        hummock_gc_history::Entity::delete_many()
            .filter(hummock_gc_history::Column::MarkDeleteAt.lt(gc_history_low_watermark))
            .exec(db)
            .await?;
        hummock_gc_history::Entity::insert_many(models)
            .on_conflict_do_nothing()
            .exec(db)
            .await?;
        Ok(())
    }

    pub async fn delete_list(&self, object_id_list: &[HummockSstableObjectId]) -> Result<()> {
        self.gc_manager.delete_list(object_id_list).await
    }

    /// Tries to delete stale Hummock metadata.
    ///
    /// Returns number of deleted deltas
    pub async fn delete_metadata(&self) -> MetaResult<usize> {
        let batch_size = 64usize;
        let mut total_deleted = 0;
        loop {
            if total_deleted != 0 && self.env.opts.vacuum_spin_interval_ms != 0 {
                tokio::time::sleep(Duration::from_millis(self.env.opts.vacuum_spin_interval_ms))
                    .await;
            }
            let (deleted, remain) = self.delete_version_deltas(batch_size).await?;
            total_deleted += deleted;
            if total_deleted == 0 || remain < batch_size {
                break;
            }
        }

        let current_epoch_time = Epoch::now().physical_time();
        let epoch_watermark = Epoch::from_physical_time(
            current_epoch_time.saturating_sub(
                self.env
                    .system_params_reader()
                    .await
                    .time_travel_retention_ms(),
            ),
        )
        .0;
        self.truncate_time_travel_metadata(epoch_watermark).await?;

        Ok(total_deleted)
    }

    /// Deletes SST objects from object store.
    ///
    /// Returns the total count of deleted SST objects.
    pub async fn delete_object(&self) -> MetaResult<usize> {
        // Select SST objects to delete.
        let mut objects_to_delete = self.get_objects_to_delete();
        if objects_to_delete.is_empty() {
            return Ok(0);
        }
        tracing::debug!(?objects_to_delete, "Attempt to delete objects.");
        let deleted_object_ids = objects_to_delete.clone();
        let total = objects_to_delete.len();
        let mut batch_size = 1000usize;
        while !objects_to_delete.is_empty() {
            if self.env.opts.vacuum_spin_interval_ms != 0 {
                tokio::time::sleep(Duration::from_millis(self.env.opts.vacuum_spin_interval_ms))
                    .await;
            }
            batch_size = cmp::min(objects_to_delete.len(), batch_size);
            if batch_size == 0 {
                break;
            }
            let delete_batch = objects_to_delete.drain(..batch_size).collect_vec();
            self.delete_list(&delete_batch).await?;
        }
        self.ack_deleted_objects(&deleted_object_ids).await?;
        tracing::info!(?deleted_object_ids, "Finish deleting objects.");
        Ok(total)
    }
}

async fn collect_min_uncommitted_sst_id(
    metadata_manager: &MetadataManager,
    client_pool: &StreamClientPool,
) -> Result<HummockSstableObjectId> {
    let futures = metadata_manager
        .list_active_streaming_compute_nodes()
        .await
        .map_err(|err| Error::MetaStore(err.into()))?
        .into_iter()
        .map(|worker_node| async move {
            let client = client_pool.get(&worker_node).await?;
            let request = GetMinUncommittedSstIdRequest {};
            client.get_min_uncommitted_sst_id(request).await
        });
    let min_watermark = try_join_all(futures)
        .await
        .map_err(|err| Error::Internal(err.into()))?
        .into_iter()
        .map(|resp| resp.min_uncommitted_sst_id)
        .min()
        .unwrap_or(HummockSstableObjectId::MAX);
    Ok(min_watermark)
}

pub struct FullGcState {
    next_start_after: Mutex<Option<String>>,
    limit: Option<u64>,
}

impl FullGcState {
    pub fn new(limit: Option<u64>) -> Self {
        Self {
            next_start_after: Mutex::new(None),
            limit,
        }
    }

    pub fn set_next_start_after(&self, next_start_after: Option<String>) {
        *self.next_start_after.lock() = next_start_after;
    }

    pub fn next_start_after(&self) -> Option<String> {
        self.next_start_after.lock().clone()
    }
}

pub struct PagedMetrics {
    total_object_count: PagedMetric,
    total_object_size: PagedMetric,
}

impl PagedMetrics {
    pub fn new() -> Self {
        Self {
            total_object_count: PagedMetric::new(),
            total_object_size: PagedMetric::new(),
        }
    }
}

/// The metrics should be accumulated on a per-page basis and then finalized at the end.
pub struct PagedMetric {
    /// identifier of a page
    expect_start_key: Option<String>,
    /// accumulated metric value of pages seen so far
    running_value: u64,
    /// final metric value
    sealed_value: Option<u64>,
}

impl PagedMetric {
    fn new() -> Self {
        Self {
            expect_start_key: None,
            running_value: 0,
            sealed_value: None,
        }
    }

    fn update(
        &mut self,
        current_start_key: Option<String>,
        next_start_key: Option<String>,
        value: u64,
    ) {
        // Encounter an update without pagination, replace current state.
        if current_start_key.is_none() && next_start_key.is_none() {
            self.running_value = value;
            self.seal();
            return;
        }
        // Encounter an update from unexpected page, reset current state.
        if current_start_key != self.expect_start_key {
            self.reset();
            return;
        }
        self.running_value += value;
        // There are more pages to add.
        if next_start_key.is_some() {
            self.expect_start_key = next_start_key;
            return;
        }
        // This is the last page, seal the metric value.
        self.seal();
    }

    fn seal(&mut self) {
        self.sealed_value = Some(self.running_value);
        self.reset();
    }

    fn reset(&mut self) {
        self.running_value = 0;
        self.expect_start_key = None;
    }

    fn take(&mut self) -> Option<u64> {
        if self.sealed_value.is_some() {
            self.reset();
        }
        self.sealed_value.take()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use itertools::Itertools;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_rpc_client::HummockMetaClient;

    use super::{PagedMetric, ResponseEvent};
    use crate::hummock::test_utils::{add_test_tables, setup_compute_env};
    use crate::hummock::MockHummockMetaClient;

    #[tokio::test]
    async fn test_full_gc() {
        let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager.clone(),
            worker_id as _,
        ));
        let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
        let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
        // No task scheduled because no available worker.
        assert!(!hummock_manager
            .start_full_gc(
                Duration::from_secs(hummock_manager.env.opts.min_sst_retention_time_sec - 1,),
                None
            )
            .await
            .unwrap());

        let mut receiver = compactor_manager.add_compactor(worker_id as _);

        assert!(hummock_manager
            .start_full_gc(
                Duration::from_secs(hummock_manager.env.opts.min_sst_retention_time_sec - 1),
                None
            )
            .await
            .unwrap());
        let _full_scan_task = match receiver.recv().await.unwrap().unwrap().event.unwrap() {
            ResponseEvent::FullScanTask(task) => task,
            _ => {
                panic!()
            }
        };

        assert!(hummock_manager
            .start_full_gc(
                Duration::from_secs(hummock_manager.env.opts.min_sst_retention_time_sec + 1),
                None
            )
            .await
            .unwrap());
        let _full_scan_task = match receiver.recv().await.unwrap().unwrap().event.unwrap() {
            ResponseEvent::FullScanTask(task) => task,
            _ => {
                panic!()
            }
        };

        // Empty input results immediate return, without waiting heartbeat.
        hummock_manager
            .complete_full_gc(vec![], None, HashSet::default())
            .await
            .unwrap();

        // LSMtree is empty. All input SST ids should be treated as garbage.
        // Use fake object ids, because they'll be written to GC history and they shouldn't affect later commit.
        assert_eq!(
            3,
            hummock_manager
                .complete_full_gc(
                    vec![i64::MAX as u64 - 2, i64::MAX as u64 - 1, i64::MAX as u64],
                    None,
                    HashSet::new(),
                )
                .await
                .unwrap()
        );

        // All committed SST ids should be excluded from GC.
        let sst_infos = add_test_tables(
            hummock_manager.as_ref(),
            hummock_meta_client.clone(),
            compaction_group_id,
        )
        .await;
        let committed_object_ids = sst_infos
            .into_iter()
            .flatten()
            .map(|s| s.object_id)
            .sorted()
            .collect_vec();
        assert!(!committed_object_ids.is_empty());
        let max_committed_object_id = *committed_object_ids.iter().max().unwrap();
        assert_eq!(
            1,
            hummock_manager
                .complete_full_gc(
                    [committed_object_ids, vec![max_committed_object_id + 1]].concat(),
                    None,
                    HashSet::default(),
                )
                .await
                .unwrap()
        );
    }

    #[test]
    fn test_paged_metric() {
        let mut metric = PagedMetric::new();
        fn assert_empty_state(metric: &mut PagedMetric) {
            assert_eq!(metric.running_value, 0);
            assert!(metric.expect_start_key.is_none());
        }
        assert!(metric.sealed_value.is_none());
        assert_empty_state(&mut metric);

        metric.update(None, None, 100);
        assert_eq!(metric.take().unwrap(), 100);
        assert!(metric.take().is_none());
        assert_empty_state(&mut metric);

        // "start" is not a legal identifier for the first page
        metric.update(Some("start".into()), Some("end".into()), 100);
        assert!(metric.take().is_none());
        assert_empty_state(&mut metric);

        metric.update(None, Some("middle".into()), 100);
        assert!(metric.take().is_none());
        assert_eq!(metric.running_value, 100);
        assert_eq!(metric.expect_start_key, Some("middle".into()));

        metric.update(Some("middle".into()), None, 50);
        assert_eq!(metric.take().unwrap(), 150);
        assert!(metric.take().is_none());
        assert_empty_state(&mut metric);
    }
}

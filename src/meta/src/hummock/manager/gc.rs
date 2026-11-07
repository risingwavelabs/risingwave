// Copyright 2022 RisingWave Labs
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
use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use chrono::DateTime;
use futures::future::try_join_all;
use futures::{StreamExt, TryStreamExt, future};
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::{
    HummockEpoch, HummockObjectId, HummockRawObjectId, VALID_OBJECT_ID_SUFFIXES,
    get_object_data_path, get_object_id_from_path,
};
use risingwave_meta_model::hummock_sequence::HUMMOCK_NOW;
use risingwave_meta_model::{hummock_gc_history, hummock_sequence, hummock_version_delta};
use risingwave_meta_model_migration::OnConflict;
use risingwave_object_store::object::{ObjectMetadataIter, ObjectStoreRef};
use risingwave_pb::stream_service::GetMinUncommittedObjectIdRequest;
use risingwave_rpc_client::StreamClientPool;
use sea_orm::{ActiveValue, ColumnTrait, EntityTrait, QueryFilter, Set};

use crate::MetaResult;
use crate::backup_restore::BackupManagerRef;
use crate::hummock::HummockManager;
use crate::hummock::error::{Error, Result};
use crate::manager::MetadataManager;

pub(crate) struct GcManager {
    store: ObjectStoreRef,
    path_prefix: String,
    use_new_object_prefix_strategy: bool,
    /// These objects may still be used by backup or time travel.
    may_delete_object_ids: parking_lot::Mutex<HashSet<HummockObjectId>>,
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
            may_delete_object_ids: Default::default(),
        }
    }

    /// Deletes all objects specified in the given list of IDs from storage.
    pub async fn delete_objects(
        &self,
        object_id_list: impl Iterator<Item = HummockObjectId>,
    ) -> Result<()> {
        let mut paths = Vec::with_capacity(1000);
        for object_id in object_id_list {
            let obj_prefix = self.store.get_object_prefix(
                object_id.as_raw().as_raw_id(),
                self.use_new_object_prefix_strategy,
            );
            paths.push(get_object_data_path(
                &obj_prefix,
                &self.path_prefix,
                object_id,
            ));
        }
        self.store.delete_objects(&paths).await?;
        Ok(())
    }

    async fn list_object_metadata_from_object_store(
        &self,
        prefix: Option<String>,
    ) -> Result<ObjectMetadataIter> {
        let list_path = format!("{}/{}", self.path_prefix, prefix.unwrap_or("".into()));
        let raw_iter = self.store.list(&list_path, None, None).await?;
        let valid_suffixes = VALID_OBJECT_ID_SUFFIXES.map(|suffix| format!(".{}", suffix));
        let iter = raw_iter.filter(move |r| match r {
            Ok(i) => future::ready(valid_suffixes.iter().any(|suffix| i.key.ends_with(suffix))),
            Err(_) => future::ready(true),
        });
        Ok(Box::pin(iter))
    }

    /// Consumes a batch from the same listing, preserving the backend's pagination state.
    /// Returns object ids filtered by retention, and the batch's count and size before that filter.
    async fn list_objects_batch(
        metadata_iter: &mut ObjectMetadataIter,
        object_retention_watermark: u64,
        limit: usize,
    ) -> Result<(HashSet<HummockObjectId>, u64, u64)> {
        let mut total_object_count = 0;
        let mut total_object_size = 0;
        let filtered = metadata_iter
            .by_ref()
            .take(limit)
            .filter_map(|r| {
                let result = match r {
                    Ok(o) => {
                        total_object_count += 1;
                        total_object_size += o.total_size;
                        if o.last_modified < object_retention_watermark as f64 {
                            Some(Ok(get_object_id_from_path(&o.key)))
                        } else {
                            None
                        }
                    }
                    Err(e) => Some(Err(Error::ObjectStore(e))),
                };
                async move { result }
            })
            .try_collect::<HashSet<HummockObjectId>>()
            .await?;
        Ok((filtered, total_object_count, total_object_size as u64))
    }

    pub fn add_may_delete_object_ids(
        &self,
        may_delete_object_ids: impl Iterator<Item = HummockObjectId>,
    ) {
        self.may_delete_object_ids
            .lock()
            .extend(may_delete_object_ids);
    }

    /// Takes if `least_count` elements available.
    pub fn try_take_may_delete_object_ids(
        &self,
        least_count: usize,
    ) -> Option<HashSet<HummockObjectId>> {
        let mut guard = self.may_delete_object_ids.lock();
        if guard.len() < least_count {
            None
        } else {
            Some(std::mem::take(&mut *guard))
        }
    }
}

impl HummockManager {
    /// Deletes version deltas.
    ///
    /// Returns number of deleted deltas
    pub async fn delete_version_deltas(&self) -> Result<usize> {
        let mut versioning_guard = self
            .versioning
            .write_with_process_name("delete_version_deltas")
            .await;
        let versioning = versioning_guard.deref_mut();
        let context_info = self
            .context_info
            .read_with_process_name("delete_version_deltas")
            .await;
        // If there is any safe point, skip this to ensure meta backup has required delta logs to
        // replay version.
        if !context_info.version_safe_points.is_empty() {
            return Ok(0);
        }
        // The context_info lock must be held to prevent any potential metadata backup.
        // The lock order requires version lock to be held as well.
        let version_id = versioning.checkpoint.version.id;
        let res = hummock_version_delta::Entity::delete_many()
            .filter(hummock_version_delta::Column::Id.lte(version_id))
            .exec(&self.env.meta_store_ref().conn)
            .await?;
        tracing::debug!(rows_affected = res.rows_affected, "Deleted version deltas");
        versioning
            .hummock_version_deltas
            .retain(|id, _| *id > version_id);
        #[cfg(test)]
        {
            drop(context_info);
            drop(versioning_guard);
            self.check_state_consistency().await;
        }
        Ok(res.rows_affected as usize)
    }

    /// Filters by Hummock version and Writes GC history.
    pub async fn finalize_objects_to_delete(
        &self,
        object_ids: impl Iterator<Item = HummockObjectId>,
    ) -> Result<Vec<HummockObjectId>> {
        // This lock ensures `commit_epoch` and `report_compat_task` can see the latest GC history during sanity check.
        let versioning = self
            .versioning
            .read_with_process_name("finalize_objects_to_delete")
            .await;
        let min_pinned_version_id = self
            .context_info
            .read_with_process_name("finalize_objects_to_delete")
            .await
            .min_pinned_version_id();
        let tracked_object_ids: HashSet<HummockObjectId> =
            versioning.get_tracked_object_ids(min_pinned_version_id);
        let to_delete = object_ids
            .filter(|object_id| !tracked_object_ids.contains(object_id))
            .collect_vec();
        // Even an empty batch must advance the persisted GC clock and expire old history
        // when GC history is enabled.
        self.write_gc_history(to_delete.iter().copied()).await?;
        Ok(to_delete)
    }

    /// LIST object store and DELETE stale objects, in batches.
    /// GC can be very slow. Spawn a dedicated tokio task for it.
    pub async fn start_full_gc(
        &self,
        object_retention_time: Duration,
        prefix: Option<String>,
        backup_manager: Option<BackupManagerRef>,
    ) -> Result<()> {
        if !self.full_gc_state.try_start() {
            return Err(anyhow::anyhow!("failed to start GC due to an ongoing process").into());
        }
        let _guard = scopeguard::guard(self.full_gc_state.clone(), |full_gc_state| {
            full_gc_state.stop()
        });
        self.metrics.full_gc_trigger_count.inc();
        let object_retention_time = cmp::max(
            object_retention_time,
            Duration::from_secs(self.env.opts.min_sst_retention_time_sec),
        );
        let limit = usize::try_from(self.env.opts.full_gc_object_limit)
            .map_err(|_| anyhow::anyhow!("full_gc_object_limit exceeds usize::MAX"))?;
        if limit == 0 {
            return Err(anyhow::anyhow!("full_gc_object_limit must be greater than zero").into());
        }
        let object_retention_watermark = self
            .now()
            .await?
            .saturating_sub(object_retention_time.as_secs());
        let mut total_object_count = 0;
        let mut total_object_size = 0;
        tracing::info!(
            retention_sec = object_retention_time.as_secs(),
            prefix,
            limit,
            "Start GC."
        );
        // Keep one lister across batches so backends such as Azure Blob can use their
        // continuation tokens without requiring support for listing after an object key.
        let mut metadata_iter = self
            .gc_manager
            .list_object_metadata_from_object_store(prefix.clone())
            .await?;
        loop {
            tracing::debug!(
                retention_sec = object_retention_time.as_secs(),
                prefix,
                limit,
                "Start a GC batch."
            );
            let (object_ids, batch_object_count, batch_object_size) =
                GcManager::list_objects_batch(
                    &mut metadata_iter,
                    object_retention_watermark,
                    limit,
                )
                .await?;
            total_object_count += batch_object_count;
            total_object_size += batch_object_size;
            tracing::debug!(
                ?object_ids,
                batch_object_count,
                batch_object_size,
                "Finish listing a GC batch."
            );
            self.complete_gc_batch(object_ids, backup_manager.clone())
                .await?;
            if batch_object_count < limit as u64 {
                break;
            }
        }
        tracing::info!(total_object_count, total_object_size, "Finish GC");
        self.metrics.total_object_size.set(total_object_size as _);
        self.metrics.total_object_count.set(total_object_count as _);
        match self.time_travel_pinned_object_count().await {
            Ok(count) => {
                self.metrics.time_travel_object_count.set(count as _);
            }
            Err(err) => {
                use thiserror_ext::AsReport;
                tracing::warn!(error = %err.as_report(), "Failed to count time travel objects.");
            }
        }
        Ok(())
    }

    /// Given candidate objects to delete, filter out false positive.
    /// Returns number of objects to delete.
    pub(crate) async fn complete_gc_batch(
        &self,
        object_ids: HashSet<HummockObjectId>,
        backup_manager: Option<BackupManagerRef>,
    ) -> Result<usize> {
        if object_ids.is_empty() {
            return Ok(0);
        }
        let pinned_by_metadata_backup = match backup_manager.as_ref() {
            Some(b) => b.list_pinned_object_ids().await,
            None => HashSet::default(),
        };
        // It's crucial to collect_min_uncommitted_object_id (i.e. `min_object_id`) only after LIST object store (i.e. `object_ids`).
        // Because after getting `min_object_id`, new compute nodes may join and generate new uncommitted objects that are not covered by `min_sst_id`.
        // By getting `min_object_id` after `object_ids`, it's ensured `object_ids` won't include any objects from those new compute nodes.
        let min_object_id = collect_min_uncommitted_object_id(
            &self.metadata_manager,
            self.env.stream_client_pool(),
        )
        .await?;
        let metrics = &self.metrics;
        let candidate_object_number = object_ids.len();
        metrics
            .full_gc_candidate_object_count
            .observe(candidate_object_number as _);
        // filter by metadata backup
        let object_ids = object_ids
            .into_iter()
            .filter(|s| !pinned_by_metadata_backup.contains(&s.as_raw()))
            .collect_vec();
        let after_metadata_backup = object_ids.len();
        // filter by time travel archive
        let filter_by_time_travel_start_time = Instant::now();
        let object_ids = self
            .filter_out_objects_by_time_travel(object_ids.into_iter())
            .await?;
        tracing::info!(elapsed = ?filter_by_time_travel_start_time.elapsed(), "filter out objects by time travel in full GC");
        let after_time_travel = object_ids.len();
        // filter by object id watermark, i.e. minimum id of uncommitted objects reported by compute nodes.
        let object_ids = object_ids
            .into_iter()
            .filter(|id| id.as_raw() < min_object_id)
            .collect_vec();
        let after_min_object_id = object_ids.len();
        // filter by version
        let after_version = self
            .finalize_objects_to_delete(object_ids.into_iter())
            .await?;
        let after_version_count = after_version.len();
        metrics
            .full_gc_selected_object_count
            .observe(after_version_count as _);
        tracing::info!(
            candidate_object_number,
            after_metadata_backup,
            after_time_travel,
            after_min_object_id,
            after_version_count,
            "complete gc batch"
        );
        self.delete_objects(after_version).await?;
        Ok(after_version_count)
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
        let now = hummock_sequence::Entity::find_by_id(HUMMOCK_NOW.to_owned())
            .one(&self.env.meta_store_ref().conn)
            .await?
            .map(|m| m.seq.try_into().unwrap());
        Ok(now)
    }

    async fn write_gc_history(
        &self,
        object_ids: impl Iterator<Item = HummockObjectId>,
    ) -> Result<()> {
        if self.env.opts.gc_history_retention_time_sec == 0 {
            return Ok(());
        }
        let now = self.now().await?;
        let dt = DateTime::from_timestamp(now.try_into().unwrap(), 0).unwrap();
        let mut models = object_ids.map(|o| hummock_gc_history::ActiveModel {
            object_id: Set(o.as_raw().into()),
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
        let mut is_finished = false;
        while !is_finished {
            let mut batch = vec![];
            let mut count: usize = self.env.opts.hummock_gc_history_insert_batch_size;
            while count > 0 {
                let Some(m) = models.next() else {
                    is_finished = true;
                    break;
                };
                count -= 1;
                batch.push(m);
            }
            if batch.is_empty() {
                break;
            }
            hummock_gc_history::Entity::insert_many(batch)
                .on_conflict_do_nothing()
                .exec(db)
                .await?;
        }
        Ok(())
    }

    pub async fn delete_time_travel_metadata(
        &self,
        pinned_snapshot_epochs: HashMap<TableId, HashSet<HummockEpoch>>,
    ) -> MetaResult<()> {
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
        self.truncate_time_travel_metadata(epoch_watermark, pinned_snapshot_epochs)
            .await?;
        Ok(())
    }

    /// Deletes stale objects from object store.
    ///
    /// Deduplicates within each batch of at most 1,000 input IDs. On success, returns the input
    /// count, including duplicates; deletion errors stop subsequent batches.
    pub async fn delete_objects(&self, objects_to_delete: Vec<HummockObjectId>) -> Result<usize> {
        let total = objects_to_delete.len();
        for objects in objects_to_delete.chunks(1000) {
            if self.env.opts.vacuum_spin_interval_ms != 0 {
                tokio::time::sleep(Duration::from_millis(self.env.opts.vacuum_spin_interval_ms))
                    .await;
            }
            let delete_batch: HashSet<_> = objects.iter().copied().collect();
            tracing::info!(?delete_batch, "Attempt to delete objects.");
            self.gc_manager
                .delete_objects(delete_batch.iter().copied())
                .await?;
            tracing::debug!(deleted_object_ids = ?delete_batch, "Finish deleting objects.");
        }
        Ok(total)
    }

    /// Minor GC attempts to delete objects that were part of Hummock version but are no longer in use.
    pub async fn try_start_minor_gc(&self, backup_manager: BackupManagerRef) -> Result<()> {
        const MIN_MINOR_GC_OBJECT_COUNT: usize = 1000;
        let Some(object_ids) = self
            .gc_manager
            .try_take_may_delete_object_ids(MIN_MINOR_GC_OBJECT_COUNT)
        else {
            return Ok(());
        };
        // Objects pinned by either meta backup or time travel should be filtered out.
        let backup_pinned: HashSet<_> = backup_manager.list_pinned_object_ids().await;
        // The version_pinned is obtained after the candidate object_ids for deletion, which is new enough for filtering purpose.
        let version_pinned = {
            let versioning = self
                .versioning
                .read_with_process_name("try_start_minor_gc")
                .await;
            let min_pinned_version_id = self
                .context_info
                .read_with_process_name("try_start_minor_gc")
                .await
                .min_pinned_version_id();
            versioning.get_tracked_object_ids(min_pinned_version_id)
        };
        let object_ids = object_ids
            .into_iter()
            .filter(|s| !version_pinned.contains(s) && !backup_pinned.contains(&s.as_raw()));
        let filter_by_time_travel_start_time = Instant::now();
        let object_ids = self.filter_out_objects_by_time_travel(object_ids).await?;
        tracing::info!(elapsed = ?filter_by_time_travel_start_time.elapsed(), "filter out objects by time travel in minor GC");
        // Retry is not necessary. Full GC will handle these objects eventually.
        self.delete_objects(object_ids.into_iter().collect())
            .await?;
        Ok(())
    }
}

async fn collect_min_uncommitted_object_id(
    metadata_manager: &MetadataManager,
    client_pool: &StreamClientPool,
) -> Result<HummockRawObjectId> {
    let futures = metadata_manager
        .list_active_streaming_compute_nodes()
        .await
        .map_err(|err| Error::MetaStore(err.into()))?
        .into_iter()
        .map(|worker_node| async move {
            let client = client_pool.get(&worker_node).await?;
            let request = GetMinUncommittedObjectIdRequest {};
            client.get_min_uncommitted_object_id(request).await
        });
    let min_watermark = try_join_all(futures)
        .await
        .map_err(|err| Error::Internal(err.into()))?
        .into_iter()
        .map(|resp| resp.min_uncommitted_object_id)
        .min()
        .unwrap_or(u64::MAX.into());
    Ok(min_watermark)
}

pub struct FullGcState {
    is_started: AtomicBool,
}

impl FullGcState {
    pub fn new() -> Self {
        Self {
            is_started: AtomicBool::new(false),
        }
    }

    pub fn try_start(&self) -> bool {
        self.is_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn stop(&self) {
        self.is_started.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use itertools::Itertools;
    use risingwave_hummock_sdk::HummockObjectId;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_object_store::object::{ObjectError, ObjectMetadata, ObjectMetadataIter};
    use risingwave_rpc_client::HummockMetaClient;

    use super::GcManager;
    use crate::hummock::MockHummockMetaClient;
    use crate::hummock::test_utils::{add_test_tables, setup_compute_env};

    #[tokio::test]
    async fn test_list_objects_batch() {
        // Deliberately unordered keys: batching must not depend on lexicographic cursors.
        let mut objects: ObjectMetadataIter =
            Box::pin(futures::stream::iter([3, 1, 2].into_iter().map(|id| {
                Ok(ObjectMetadata {
                    key: format!("test/{id}.data"),
                    last_modified: if id == 2 { 9.0 } else { 10.0 },
                    total_size: id as usize,
                })
            })));
        // A full batch containing only retained objects must still advance the iterator.
        let (ids, count, size) = GcManager::list_objects_batch(&mut objects, 10, 2)
            .await
            .unwrap();
        assert!(ids.is_empty());
        assert_eq!((count, size), (2, 4));
        let (ids, count, size) = GcManager::list_objects_batch(&mut objects, 10, 2)
            .await
            .unwrap();
        assert_eq!(ids, [HummockObjectId::Sstable(2.into())].into());
        assert_eq!((count, size), (1, 2));
        let (ids, count, size) = GcManager::list_objects_batch(&mut objects, 10, 2)
            .await
            .unwrap();
        assert!(ids.is_empty());
        assert_eq!((count, size), (0, 0));
    }

    #[tokio::test]
    async fn test_list_objects_batch_propagates_error() {
        let mut objects: ObjectMetadataIter = Box::pin(futures::stream::iter([
            Ok(ObjectMetadata {
                key: "test/1.data".to_owned(),
                last_modified: 10.0,
                total_size: 1,
            }),
            Err(ObjectError::internal("listing failed")),
        ]));
        assert!(
            GcManager::list_objects_batch(&mut objects, 10, 1)
                .await
                .is_ok()
        );
        // An error on a later page must not be mistaken for the end of the listing.
        assert!(
            GcManager::list_objects_batch(&mut objects, 10, 1)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_full_gc_rejects_zero_batch_limit() {
        use super::super::tests::setup_compute_env_with_meta_opts;
        use crate::manager::MetaOpts;

        let mut opts = MetaOpts::test(false);
        opts.full_gc_object_limit = 0;
        let (_, manager, _, _) = setup_compute_env_with_meta_opts(80, opts).await;
        for _ in 0..2 {
            let error = manager
                .start_full_gc(Duration::ZERO, None, None)
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("full_gc_object_limit must be greater than zero")
            );
        }
    }

    #[cfg(not(madsim))] // OpenDAL FS uses real filesystem I/O.
    #[tokio::test]
    async fn test_full_gc_without_start_after() {
        use bytes::Bytes;
        use risingwave_object_store::object::ObjectStoreImpl;

        use crate::controller::catalog::CatalogController;
        use crate::controller::cluster::ClusterController;
        use crate::hummock::{CompactorManager, HummockManager};
        use crate::manager::{MetaOpts, MetaSrvEnv, MetadataManager};
        use crate::rpc::metrics::MetaMetrics;

        let dir = tempfile::tempdir().unwrap();
        let mut opts = MetaOpts::test(false);
        opts.full_gc_object_limit = 2;
        opts.object_store_config.req_concurrency_limit = 1;
        let env = MetaSrvEnv::for_test_opts(opts, |params| {
            params.state_store = Some(format!("hummock+fs://{}", dir.path().display()));
        })
        .await;
        let cluster = Arc::new(
            ClusterController::new(env.clone(), Duration::from_secs(1))
                .await
                .unwrap(),
        );
        let catalog = Arc::new(CatalogController::new(env.clone()).await.unwrap());
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let manager = HummockManager::new(
            env,
            MetadataManager::new(cluster, catalog),
            Arc::new(MetaMetrics::default()),
            Arc::new(CompactorManager::for_test()),
            tx,
        )
        .await
        .unwrap();
        assert!(matches!(
            manager.object_store.as_ref(),
            ObjectStoreImpl::Opendal(_)
        ));
        let prefix = format!("{}/gc-test", manager.gc_manager.path_prefix);
        // Include unrelated files. They must not cause a short batch to end the scan early.
        for name in ["0.tmp", "1.data", "2.data", "3.data", "4.data", "5.tmp"] {
            manager
                .object_store
                .upload(&format!("{prefix}/{name}"), Bytes::from_static(b"data"))
                .await
                .unwrap();
        }
        // FS, like Azure Blob, cannot resume a fresh listing with start_after.
        assert!(
            manager
                .object_store
                .list(&prefix, Some(format!("{prefix}/1.data")), None)
                .await
                .is_err()
        );
        // The watermark is zero, so every object survives both full batches. Repeat the
        // run to also check that completion releases the in-progress guard.
        for _ in 0..2 {
            tokio::time::timeout(
                Duration::from_secs(10),
                manager.start_full_gc(
                    Duration::from_secs(u64::MAX),
                    Some("gc-test/".to_owned()),
                    None,
                ),
            )
            .await
            .expect("full GC must finish without restarting the listing")
            .unwrap();
            assert_eq!(manager.metrics.total_object_count.get(), 4);
            assert_eq!(manager.metrics.total_object_size.get(), 16);
        }
        for id in 1..=4 {
            manager
                .object_store
                .metadata(&format!("{prefix}/{id}.data"))
                .await
                .unwrap();
        }

        // Stale objects use the canonical paths so complete_gc_batch really deletes them.
        // Multiple batches must release the sole operation permit before deletion.
        let mut stale_paths = Vec::new();
        for id in 100..106 {
            let object_id = HummockObjectId::Sstable(id.into());
            let object_prefix = manager
                .object_store
                .get_object_prefix(id, manager.gc_manager.use_new_object_prefix_strategy);
            let path = risingwave_hummock_sdk::get_object_data_path(
                &object_prefix,
                &manager.gc_manager.path_prefix,
                object_id,
            );
            manager
                .object_store
                .upload(&path, Bytes::from_static(b"data"))
                .await
                .unwrap();
            std::fs::File::open(dir.path().join(&path))
                .unwrap()
                .set_modified(std::time::SystemTime::UNIX_EPOCH)
                .unwrap();
            stale_paths.push(path);
        }
        tokio::time::timeout(
            Duration::from_secs(10),
            manager.start_full_gc(Duration::ZERO, None, None),
        )
        .await
        .expect("full GC must delete batches with req_concurrency_limit = 1")
        .unwrap();
        assert_eq!(manager.metrics.total_object_count.get(), 10);
        assert_eq!(manager.metrics.total_object_size.get(), 40);
        for path in stale_paths {
            assert!(
                manager
                    .object_store
                    .metadata(&path)
                    .await
                    .unwrap_err()
                    .is_object_not_found_error()
            );
        }
        for id in 1..=4 {
            manager
                .object_store
                .metadata(&format!("{prefix}/{id}.data"))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_full_gc() {
        let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager.clone(),
            worker_id as _,
        ));
        let compaction_group_id = StaticCompactionGroupId::StateDefault;
        hummock_manager
            .start_full_gc(
                Duration::from_secs(hummock_manager.env.opts.min_sst_retention_time_sec + 1),
                None,
                None,
            )
            .await
            .unwrap();

        // Empty input results immediate return, without waiting heartbeat.
        hummock_manager
            .complete_gc_batch(vec![].into_iter().collect(), None)
            .await
            .unwrap();

        // LSMtree is empty. All input object ids should be treated as garbage.
        // Use fake object ids, because they'll be written to GC history and they shouldn't affect later commit.
        assert_eq!(
            3,
            hummock_manager
                .complete_gc_batch(
                    [i64::MAX as u64 - 2, i64::MAX as u64 - 1, i64::MAX as u64]
                        .into_iter()
                        .map(|id| HummockObjectId::Sstable(id.into()))
                        .collect(),
                    None,
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
                .complete_gc_batch(
                    [committed_object_ids, vec![max_committed_object_id + 1]]
                        .concat()
                        .into_iter()
                        .map(HummockObjectId::Sstable)
                        .collect(),
                    None,
                )
                .await
                .unwrap()
        );
    }
}

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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use fail::fail_point;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{
    HummockContextId, HummockObjectId, HummockSstableObjectId, HummockVersionId,
    INVALID_VERSION_ID, LocalSstableInfo,
};
use risingwave_meta_model::hummock_gc_history;
use risingwave_pb::hummock::{HummockPinnedVersion, ValidationTask};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};

use crate::controller::SqlMetaStore;
use crate::hummock::HummockManager;
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::commit_multi_var;
use crate::hummock::manager::worker::{HummockManagerEvent, HummockManagerEventSender};
use crate::hummock::metrics_utils::trigger_pin_unpin_version_state;
use crate::manager::{META_NODE_ID, MetadataManager};
use crate::model::BTreeMapTransaction;
use crate::rpc::metrics::MetaMetrics;

const GC_HISTORY_QUERY_BATCH_SIZE: usize = 1000;

/// `HummockVersionSafePoint` prevents hummock versions GE than it from being GC.
/// It's used by meta node itself to temporarily pin versions and retain changelog-only
/// objects whose live references are removed while the safe point is registered.
pub struct HummockVersionSafePoint {
    pub id: HummockVersionId,
    event_sender: HummockManagerEventSender,
}

impl Drop for HummockVersionSafePoint {
    fn drop(&mut self) {
        if self
            .event_sender
            .send(HummockManagerEvent::DropSafePoint(self.id))
            .is_err()
        {
            tracing::debug!("failed to drop hummock version safe point {}", self.id);
        }
    }
}

#[derive(Default)]
pub(super) struct ContextInfo {
    pub pinned_versions: BTreeMap<HummockContextId, HummockPinnedVersion>,
}

impl ContextInfo {
    /// Release resources pinned by these contexts, including:
    /// - Version
    /// - Snapshot
    async fn release_contexts(
        &mut self,
        context_ids: impl AsRef<[HummockContextId]>,
        meta_store_ref: SqlMetaStore,
    ) -> Result<()> {
        fail_point!("release_contexts_metastore_err", |_| Err(Error::MetaStore(
            anyhow::anyhow!("failpoint metastore error")
        )));
        fail_point!("release_contexts_internal_err", |_| Err(Error::Internal(
            anyhow::anyhow!("failpoint internal error")
        )));

        let mut pinned_versions = BTreeMapTransaction::new(&mut self.pinned_versions);
        for context_id in context_ids.as_ref() {
            pinned_versions.remove(*context_id);
        }
        commit_multi_var!(meta_store_ref, pinned_versions)?;

        Ok(())
    }
}

impl HummockManager {
    pub async fn release_contexts(
        &self,
        context_ids: impl AsRef<[HummockContextId]>,
    ) -> Result<()> {
        let mut context_info = self
            .context_info
            .write_with_process_name("release_contexts")
            .await;
        context_info
            .release_contexts(context_ids, self.env.meta_store())
            .await?;
        #[cfg(test)]
        {
            drop(context_info);
            self.check_state_consistency().await;
        }
        Ok(())
    }

    /// Checks whether `context_id` is valid.
    pub async fn check_context(&self, context_id: HummockContextId) -> Result<bool> {
        Ok(self
            .context_info
            .read_with_process_name("check_context")
            .await
            .check_context(context_id, &self.metadata_manager)
            .await)
    }

    async fn check_context_with_meta_node(
        &self,
        context_id: HummockContextId,
        context_info: &ContextInfo,
    ) -> Result<()> {
        if context_id == META_NODE_ID {
            // Using the preserved meta id is allowed.
        } else if !context_info
            .check_context(context_id, &self.metadata_manager)
            .await
        {
            // The worker is not found in cluster.
            return Err(Error::InvalidContext(context_id));
        }
        Ok(())
    }

    pub async fn get_min_pinned_version_id(&self) -> HummockVersionId {
        let versioning = self
            .versioning
            .read_with_process_name("get_min_pinned_version_id")
            .await;
        self.context_info
            .read_with_process_name("get_min_pinned_version_id")
            .await
            .min_pinned_version_id(&versioning.safe_points.lock())
    }
}

impl ContextInfo {
    /// Checks whether `context_id` is valid.
    ///
    /// Need `&self` to sync with `release_context`
    pub(super) async fn check_context(
        &self,
        context_id: HummockContextId,
        metadata_manager: &MetadataManager,
    ) -> bool {
        // Worker existence is kept in memory and updated under the cluster controller lock, so
        // checking it does not need a meta store query.
        metadata_manager
            .get_worker_info_by_id(context_id)
            .await
            .is_some()
    }
}

impl HummockManager {
    /// Release invalid contexts, aka worker node ids which are no longer valid in `ClusterManager`.
    pub(super) async fn release_invalid_contexts(&self) -> Result<Vec<HummockContextId>> {
        let (active_context_ids, mut context_info) = {
            let compaction_guard = self
                .compaction
                .read_with_process_name("release_invalid_contexts")
                .await;
            let context_info = self
                .context_info
                .write_with_process_name("release_invalid_contexts")
                .await;
            let mut active_context_ids = HashSet::new();
            active_context_ids.extend(
                compaction_guard
                    .compact_task_assignment
                    .values()
                    .map(|c| c.context_id),
            );
            active_context_ids.extend(context_info.pinned_versions.keys());
            (active_context_ids, context_info)
        };

        let mut invalid_context_ids = vec![];
        for active_context_id in &active_context_ids {
            if !context_info
                .check_context(*active_context_id, &self.metadata_manager)
                .await
            {
                invalid_context_ids.push(*active_context_id);
            }
        }

        context_info
            .release_contexts(&invalid_context_ids, self.env.meta_store())
            .await?;

        Ok(invalid_context_ids)
    }

    pub async fn commit_epoch_sanity_check(
        &self,
        tables_to_commit: &HashMap<TableId, u64>,
        sstables: &[LocalSstableInfo],
        sst_to_context: &HashMap<HummockSstableObjectId, HummockContextId>,
        current_version: &HummockVersion,
    ) -> Result<()> {
        use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;

        let context_info = self
            .context_info
            .read_with_process_name("commit_epoch_sanity_check")
            .await;
        let mut checked_contexts = HashSet::new();
        for (sst_id, context_id) in sst_to_context {
            if !checked_contexts.insert(*context_id) {
                continue;
            }
            #[cfg(test)]
            {
                if *context_id == crate::manager::META_NODE_ID {
                    continue;
                }
            }
            if !context_info
                .check_context(*context_id, &self.metadata_manager)
                .await
            {
                return Err(Error::InvalidSst(*sst_id));
            }
        }
        drop(context_info);

        // sanity check on monotonically increasing table committed epoch
        for (table_id, committed_epoch) in tables_to_commit {
            if let Some(info) = current_version.state_table_info.info().get(table_id)
                && *committed_epoch <= info.committed_epoch
            {
                return Err(anyhow::anyhow!(
                    "table {} Epoch {} <= committed_epoch {}",
                    table_id,
                    committed_epoch,
                    info.committed_epoch,
                )
                .into());
            }
        }

        // HummockManager::now requires a write to the meta store. Thus, it should be avoided whenever feasible.
        if !sstables.is_empty() {
            // Sanity check to ensure SSTs to commit have not been full GCed yet.
            let now = self.now().await?;
            check_sst_retention(
                now,
                self.env.opts.min_sst_retention_time_sec,
                sstables
                    .iter()
                    .map(|s| (s.sst_info.object_id, s.created_at)),
            )?;
            if self.env.opts.gc_history_retention_time_sec != 0 {
                let ids = sstables.iter().map(|s| s.sst_info.object_id).collect_vec();
                check_gc_history(&self.meta_store_ref().conn, ids).await?;
            }
        }

        async {
            if !self.env.opts.enable_committed_sst_sanity_check {
                return;
            }
            if sstables.is_empty() {
                return;
            }
            let compactor = match self.compactor_manager.next_compactor() {
                None => {
                    tracing::warn!("Skip committed SST sanity check due to no available worker");
                    return;
                }
                Some(compactor) => compactor,
            };
            let sst_infos = sstables
                .iter()
                .map(|LocalSstableInfo { sst_info, .. }| sst_info.clone())
                .collect_vec();
            if compactor
                .send_event(
                    #[expect(deprecated)]
                    ResponseEvent::ValidationTask(ValidationTask {
                        sst_infos: sst_infos.into_iter().map(|sst| sst.into()).collect_vec(),
                        sst_id_to_worker_id: sst_to_context
                            .iter()
                            .map(|(object_id, worker_id)| (*object_id, *worker_id))
                            .collect(),
                    }),
                )
                .is_err()
            {
                tracing::warn!("Skip committed SST sanity check due to send failure");
            }
        }
        .await;
        Ok(())
    }

    pub async fn release_meta_context(&self) -> Result<()> {
        self.release_contexts([META_NODE_ID]).await
    }

    pub(crate) async fn report_compaction_sanity_check(
        &self,
        object_timestamps: &HashMap<HummockSstableObjectId, u64>,
    ) -> Result<()> {
        // HummockManager::now requires a write to the meta store. Thus, it should be avoided whenever feasible.
        if object_timestamps.is_empty() {
            return Ok(());
        }
        let now = self.now().await?;
        check_sst_retention(
            now,
            self.env.opts.min_sst_retention_time_sec,
            object_timestamps.iter().map(|(k, v)| (*k, *v)),
        )?;
        if self.env.opts.gc_history_retention_time_sec != 0 {
            let ids = object_timestamps.keys().copied().collect_vec();
            check_gc_history(&self.meta_store_ref().conn, ids).await?;
        }
        Ok(())
    }
}

fn check_sst_retention(
    now: u64,
    retention_sec: u64,
    sst_infos: impl Iterator<Item = (HummockSstableObjectId, u64)>,
) -> Result<()> {
    let sst_retention_watermark = now.saturating_sub(retention_sec);
    for (object_id, created_at) in sst_infos {
        if created_at < sst_retention_watermark {
            return Err(anyhow::anyhow!("object {object_id} is rejected from being committed since it's below watermark: object timestamp {created_at}, meta node timestamp {now}, retention_sec {retention_sec}, watermark {sst_retention_watermark}").into());
        }
    }
    Ok(())
}

async fn check_gc_history(
    db: &DatabaseConnection,
    object_ids: impl IntoIterator<Item = HummockSstableObjectId>,
) -> Result<()> {
    let object_ids = object_ids.into_iter().collect_vec();
    let mut expired_object_ids = Vec::new();
    for object_ids in object_ids.chunks(GC_HISTORY_QUERY_BATCH_SIZE) {
        expired_object_ids.extend(
            hummock_gc_history::Entity::find()
                .filter(hummock_gc_history::Column::ObjectId.is_in(object_ids.iter().copied()))
                .all(db)
                .await?,
        );
    }
    if expired_object_ids.is_empty() {
        return Ok(());
    }
    tracing::error!(
        ?expired_object_ids,
        "new SSTs are rejected because they have already been GCed"
    );
    Err(Error::InvalidSst(expired_object_ids[0].object_id))
}

// pin and unpin method
impl HummockManager {
    /// Pin the current greatest hummock version. The pin belongs to `context_id`
    /// and will be unpinned when `context_id` is invalidated.
    pub async fn pin_version(&self, context_id: HummockContextId) -> Result<Arc<HummockVersion>> {
        let versioning = self.versioning.read_with_process_name("pin_version").await;
        let mut context_info = self
            .context_info
            .write_with_process_name("pin_version")
            .await;
        self.check_context_with_meta_node(context_id, &context_info)
            .await?;
        let mut pinned_versions = BTreeMapTransaction::new(&mut context_info.pinned_versions);
        let mut context_pinned_version = pinned_versions.new_entry_txn_or_default(
            context_id,
            HummockPinnedVersion {
                context_id,
                min_pinned_id: INVALID_VERSION_ID,
            },
        );
        let version_id = versioning.current_version.id;
        let ret = versioning.current_version.clone();
        if context_pinned_version.min_pinned_id == INVALID_VERSION_ID
            || context_pinned_version.min_pinned_id > version_id
        {
            context_pinned_version.min_pinned_id = version_id;
            commit_multi_var!(self.meta_store_ref(), context_pinned_version)?;
            trigger_pin_unpin_version_state(&self.metrics, &context_info.pinned_versions);
        }

        #[cfg(test)]
        {
            drop(context_info);
            drop(versioning);
            self.check_state_consistency().await;
        }

        Ok(ret)
    }

    /// Unpin all pins which belongs to `context_id` and has an id which is older than
    /// `unpin_before`. All versions >= `unpin_before` will be treated as if they are all pinned by
    /// this `context_id` so they will not be vacuumed.
    pub async fn unpin_version_before(
        &self,
        context_id: HummockContextId,
        unpin_before: HummockVersionId,
    ) -> Result<()> {
        let mut context_info = self
            .context_info
            .write_with_process_name("unpin_version_before")
            .await;
        self.check_context_with_meta_node(context_id, &context_info)
            .await?;
        let mut pinned_versions = BTreeMapTransaction::new(&mut context_info.pinned_versions);
        let mut context_pinned_version = pinned_versions.new_entry_txn_or_default(
            context_id,
            HummockPinnedVersion {
                context_id,
                min_pinned_id: HummockVersionId::default(),
            },
        );
        assert!(
            context_pinned_version.min_pinned_id <= unpin_before,
            "val must be monotonically non-decreasing. old = {}, new = {}.",
            context_pinned_version.min_pinned_id,
            unpin_before
        );
        context_pinned_version.min_pinned_id = unpin_before;
        commit_multi_var!(self.meta_store_ref(), context_pinned_version)?;
        trigger_pin_unpin_version_state(&self.metrics, &context_info.pinned_versions);

        #[cfg(test)]
        {
            drop(context_info);
            self.check_state_consistency().await;
        }

        Ok(())
    }
}

/// Transient retention for version references and changelog-only objects.
///
/// Registration takes a versioning read lock; changelog removal takes its write lock.
/// A short inner mutex lets registration proceed alongside GC's final version read and
/// history write. Never hold the inner mutex across an await or object-store I/O.
#[derive(Default)]
pub(super) struct VersionSafePoints {
    pub version_ids: Vec<HummockVersionId>,
    // A batch is tagged with the largest pinned version when its references were removed.
    // Every older safe point may also need it, including a GC pin inheriting an older pin.
    // These are ownership boundaries, not changelog truncation version numbers: truncation
    // can happen repeatedly without advancing the Hummock version.
    retired_change_log_objects: BTreeMap<HummockVersionId, Arc<HashSet<HummockObjectId>>>,
}

impl VersionSafePoints {
    pub fn retain_change_log_objects(&mut self, objects: impl Iterator<Item = HummockObjectId>) {
        if let Some(id) = self.version_ids.iter().max().copied() {
            Arc::make_mut(self.retired_change_log_objects.entry(id).or_default()).extend(objects);
        }
    }

    /// Snapshot only the batch handles under the mutex; scan object IDs after unlocking.
    pub fn retained_change_log_objects(&self) -> Vec<Arc<HashSet<HummockObjectId>>> {
        self.retired_change_log_objects.values().cloned().collect()
    }

    fn unregister(&mut self, id: HummockVersionId) -> Vec<Arc<HashSet<HummockObjectId>>> {
        if let Some(pos) = self.version_ids.iter().position(|sp| *sp == id) {
            self.version_ids.remove(pos);
        }
        let min_id = self.version_ids.iter().min().copied();
        let mut released = Vec::new();
        while let Some(entry) = self.retired_change_log_objects.first_entry() {
            if min_id.is_some_and(|min_id| min_id <= *entry.key()) {
                break;
            }
            released.push(entry.remove());
        }
        released
    }
}

impl HummockManager {
    pub async fn register_safe_point(&self) -> HummockVersionSafePoint {
        let versioning = self
            .versioning
            .read_with_process_name("register_safe_point")
            .await;
        let safe_point = HummockVersionSafePoint {
            id: versioning.current_version.id,
            event_sender: self.event_sender.clone(),
        };
        let mut safe_points = versioning.safe_points.lock();
        safe_points.version_ids.push(safe_point.id);
        trigger_safepoint_stat(&self.metrics, &safe_points.version_ids);
        safe_point
    }

    /// Preserve references that may move to a backup or time-travel snapshot during GC's
    /// reference scans. Inherit older pins before their original owners can release them.
    pub(super) async fn register_gc_safe_point(&self) -> HummockVersionSafePoint {
        let versioning = self
            .versioning
            .read_with_process_name("register_gc_safe_point")
            .await;
        let context = self
            .context_info
            .read_with_process_name("register_gc_safe_point")
            .await;
        let mut safe_points = versioning.safe_points.lock();
        let safe_point = HummockVersionSafePoint {
            // Objects already removed in the current version must remain reclaimable in
            // an idle cluster. Future removals are protected, including changelog-only SSTs.
            id: context
                .min_pinned_version_id(&safe_points)
                .min(versioning.current_version.id + 1),
            event_sender: self.event_sender.clone(),
        };
        safe_points.version_ids.push(safe_point.id);
        trigger_safepoint_stat(&self.metrics, &safe_points.version_ids);
        safe_point
    }

    pub async fn unregister_safe_point(&self, safe_point: HummockVersionId) {
        let versioning = self
            .versioning
            .read_with_process_name("unregister_safe_point")
            .await;
        let released = versioning.safe_points.lock().unregister(safe_point);
        trigger_safepoint_stat(&self.metrics, &versioning.safe_points.lock().version_ids);
        // Minor GC may already have consumed these candidates while they were pinned.
        // Its normal reference checks still protect published backups and time travel.
        self.gc_manager
            .add_may_delete_object_ids(released.iter().flat_map(|objects| objects.iter().copied()));
    }
}

fn trigger_safepoint_stat(metrics: &MetaMetrics, safepoints: &[HummockVersionId]) {
    if let Some(sp) = safepoints.iter().min() {
        metrics.min_safepoint_version_id.set(sp.as_i64_id());
    } else {
        metrics.min_safepoint_version_id.set(u64::MAX as _);
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::ActiveModelTrait;
    use sea_orm::ActiveValue::Set;

    use super::*;
    use crate::hummock::test_utils::setup_compute_env;

    #[test]
    fn test_changelog_retention_with_overlapping_and_duplicate_safe_points() {
        let mut pins = VersionSafePoints::default();
        let first = HummockObjectId::Sstable(1.into());
        let second = HummockObjectId::Sstable(2.into());
        pins.retain_change_log_objects([first].into_iter());
        assert_eq!(pins.retained_change_log_objects().len(), 0);
        pins.version_ids.extend([HummockVersionId::new(10); 2]);
        pins.retain_change_log_objects([first].into_iter());
        // A newer pin does not need an object retired before its registration.
        pins.version_ids.push(20.into());
        pins.retain_change_log_objects([second].into_iter());
        assert!(pins.unregister(10.into()).is_empty());
        assert_eq!(
            pins.unregister(10.into()),
            vec![Arc::new(HashSet::from([first]))]
        );
        assert_eq!(
            pins.retained_change_log_objects(),
            vec![Arc::new(HashSet::from([second]))]
        );
        assert_eq!(
            pins.unregister(20.into()),
            vec![Arc::new(HashSet::from([second]))]
        );
        assert_eq!(pins.retained_change_log_objects().len(), 0);
    }

    #[tokio::test]
    async fn test_gc_safe_point_inherits_older_pin() {
        let (_, manager, ..) = setup_compute_env(80).await;
        let backup_pin = manager.register_safe_point().await;
        let old_version = backup_pin.id;
        crate::hummock::test_utils::register_table_ids_to_compaction_group(
            &manager,
            &[1],
            risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId::StateDefault,
        )
        .await;
        assert!(manager.get_current_version().await.id > old_version);
        let gc_pin = manager.register_gc_safe_point().await;
        assert_eq!(gc_pin.id, old_version);
        drop(backup_pin);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while manager
                .versioning
                .read()
                .await
                .safe_points
                .lock()
                .version_ids
                .len()
                != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(manager.get_min_pinned_version_id().await, old_version);
        drop(gc_pin);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while manager.get_min_pinned_version_id().await
                != risingwave_hummock_sdk::version::MAX_HUMMOCK_VERSION_ID
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_check_gc_history_in_batches() {
        let (env, ..) = setup_compute_env(80).await;
        let object_ids = (1..=GC_HISTORY_QUERY_BATCH_SIZE as u64 + 1)
            .map(Into::into)
            .collect_vec();
        let expired_object_id = *object_ids.last().unwrap();
        hummock_gc_history::ActiveModel {
            object_id: Set(expired_object_id),
            mark_delete_at: Set(Default::default()),
        }
        .insert(&env.meta_store_ref().conn)
        .await
        .unwrap();

        let error = check_gc_history(&env.meta_store_ref().conn, object_ids)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidSst(id) if id == expired_object_id));
    }
}

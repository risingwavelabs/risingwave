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

use std::collections::{BTreeMap, HashMap, HashSet};

use fail::fail_point;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{
    HummockContextId, HummockEpoch, HummockSstableObjectId, HummockVersionId, LocalSstableInfo,
    INVALID_VERSION_ID,
};
use risingwave_pb::hummock::{HummockPinnedVersion, ValidationTask};

use crate::hummock::error::{Error, Result};
use crate::hummock::manager::worker::{HummockManagerEvent, HummockManagerEventSender};
use crate::hummock::manager::{commit_multi_var, start_measure_real_process_timer};
use crate::hummock::metrics_utils::trigger_pin_unpin_version_state;
use crate::hummock::HummockManager;
use crate::manager::{MetaStoreImpl, MetadataManager, META_NODE_ID};
use crate::model::BTreeMapTransaction;
use crate::rpc::metrics::MetaMetrics;

/// `HummockVersionSafePoint` prevents hummock versions GE than it from being GC.
/// It's used by meta node itself to temporarily pin versions.
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
    /// `version_safe_points` is similar to `pinned_versions` expect for being a transient state.
    pub version_safe_points: Vec<HummockVersionId>,
}

impl ContextInfo {
    /// Release resources pinned by these contexts, including:
    /// - Version
    /// - Snapshot
    async fn release_contexts(
        &mut self,
        context_ids: impl AsRef<[HummockContextId]>,
        meta_store_ref: MetaStoreImpl,
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
        let mut context_info = self.context_info.write().await;
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
        self.context_info
            .read()
            .await
            .check_context(context_id, &self.metadata_manager)
            .await
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
            .await?
        {
            // The worker is not found in cluster.
            return Err(Error::InvalidContext(context_id));
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn get_min_pinned_version_id(&self) -> HummockVersionId {
        self.context_info.read().await.min_pinned_version_id()
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
    ) -> Result<bool> {
        Ok(metadata_manager
            .get_worker_by_id(context_id)
            .await
            .map_err(|err| Error::MetaStore(err.into()))?
            .is_some())
    }
}

impl HummockManager {
    /// Release invalid contexts, aka worker node ids which are no longer valid in `ClusterManager`.
    pub(super) async fn release_invalid_contexts(&self) -> Result<Vec<HummockContextId>> {
        let (active_context_ids, mut context_info) = {
            let compaction_guard = self.compaction.read().await;
            let context_info = self.context_info.write().await;
            let _timer = start_measure_real_process_timer!(self, "release_invalid_contexts");
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
                .await?
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
        committed_epoch: HummockEpoch,
        tables_to_commit: &HashSet<TableId>,
        sstables: &[LocalSstableInfo],
        sst_to_context: &HashMap<HummockSstableObjectId, HummockContextId>,
        current_version: &HummockVersion,
    ) -> Result<()> {
        use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;

        for (sst_id, context_id) in sst_to_context {
            #[cfg(test)]
            {
                if *context_id == crate::manager::META_NODE_ID {
                    continue;
                }
            }
            if !self
                .context_info
                .read()
                .await
                .check_context(*context_id, &self.metadata_manager)
                .await?
            {
                return Err(Error::InvalidSst(*sst_id));
            }
        }

        // sanity check on monotonically increasing table committed epoch
        for table_id in tables_to_commit {
            if let Some(info) = current_version.state_table_info.info().get(table_id) {
                if committed_epoch <= info.committed_epoch {
                    return Err(anyhow::anyhow!(
                        "table {} Epoch {} <= committed_epoch {}",
                        table_id,
                        committed_epoch,
                        info.committed_epoch,
                    )
                    .into());
                }
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
                .send_event(ResponseEvent::ValidationTask(ValidationTask {
                    sst_infos: sst_infos.into_iter().map(|sst| sst.into()).collect_vec(),
                    sst_id_to_worker_id: sst_to_context.clone(),
                    epoch: committed_epoch,
                }))
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
}

// pin and unpin method
impl HummockManager {
    /// Pin the current greatest hummock version. The pin belongs to `context_id`
    /// and will be unpinned when `context_id` is invalidated.
    pub async fn pin_version(&self, context_id: HummockContextId) -> Result<HummockVersion> {
        let versioning = self.versioning.read().await;
        let mut context_info = self.context_info.write().await;
        self.check_context_with_meta_node(context_id, &context_info)
            .await?;
        let _timer = start_measure_real_process_timer!(self, "pin_version");
        let mut pinned_versions = BTreeMapTransaction::new(&mut context_info.pinned_versions);
        let mut context_pinned_version = pinned_versions.new_entry_txn_or_default(
            context_id,
            HummockPinnedVersion {
                context_id,
                min_pinned_id: INVALID_VERSION_ID.to_u64(),
            },
        );
        let version_id = versioning.current_version.id;
        let ret = versioning.current_version.clone();
        if HummockVersionId::new(context_pinned_version.min_pinned_id) == INVALID_VERSION_ID
            || HummockVersionId::new(context_pinned_version.min_pinned_id) > version_id
        {
            context_pinned_version.min_pinned_id = version_id.to_u64();
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
        let mut context_info = self.context_info.write().await;
        self.check_context_with_meta_node(context_id, &context_info)
            .await?;
        let _timer = start_measure_real_process_timer!(self, "unpin_version_before");
        let mut pinned_versions = BTreeMapTransaction::new(&mut context_info.pinned_versions);
        let mut context_pinned_version = pinned_versions.new_entry_txn_or_default(
            context_id,
            HummockPinnedVersion {
                context_id,
                min_pinned_id: 0,
            },
        );
        assert!(
            context_pinned_version.min_pinned_id <= unpin_before.to_u64(),
            "val must be monotonically non-decreasing. old = {}, new = {}.",
            context_pinned_version.min_pinned_id,
            unpin_before
        );
        context_pinned_version.min_pinned_id = unpin_before.to_u64();
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

// safe point
impl HummockManager {
    pub async fn register_safe_point(&self) -> HummockVersionSafePoint {
        let versioning = self.versioning.read().await;
        let mut wl = self.context_info.write().await;
        let safe_point = HummockVersionSafePoint {
            id: versioning.current_version.id,
            event_sender: self.event_sender.clone(),
        };
        wl.version_safe_points.push(safe_point.id);
        trigger_safepoint_stat(&self.metrics, &wl.version_safe_points);
        safe_point
    }

    pub async fn unregister_safe_point(&self, safe_point: HummockVersionId) {
        let mut wl = self.context_info.write().await;
        let version_safe_points = &mut wl.version_safe_points;
        if let Some(pos) = version_safe_points.iter().position(|sp| *sp == safe_point) {
            version_safe_points.remove(pos);
        }
        trigger_safepoint_stat(&self.metrics, &wl.version_safe_points);
    }
}

fn trigger_safepoint_stat(metrics: &MetaMetrics, safepoints: &[HummockVersionId]) {
    if let Some(sp) = safepoints.iter().min() {
        metrics.min_safepoint_version_id.set(sp.to_u64() as _);
    } else {
        metrics
            .min_safepoint_version_id
            .set(HummockVersionId::MAX.to_u64() as _);
    }
}

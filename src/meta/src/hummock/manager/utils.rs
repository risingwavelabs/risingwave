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

/// Commit multiple `ValTransaction`s to state store and upon success update the local in-mem state
/// by the way
/// After called, the `ValTransaction` will be dropped.
macro_rules! commit_multi_var {
    ($meta_store:expr, $($val_txn:expr),*) => {
        {
            async {
                use crate::model::{MetadataModelError, InMemValTransaction, ValTransaction};
                use sea_orm::TransactionTrait;
                let mut txn = $meta_store.conn.begin().await.map_err(MetadataModelError::from)?;
                $(
                    $val_txn.apply_to_txn(&mut txn).await?;
                )*
                txn.commit().await.map_err(MetadataModelError::from)?;
                $(
                    $val_txn.commit();
                )*
                Result::Ok(())
            }.await
        }
    };
}

macro_rules! commit_multi_var_with_provided_txn {
    ($txn:expr, $($val_txn:expr),*) => {
        {
            async {
                use crate::model::{InMemValTransaction, ValTransaction};
                use crate::model::MetadataModelError;
                $(
                    $val_txn.apply_to_txn(&mut $txn).await?;
                )*
                $txn.commit().await.map_err(MetadataModelError::from)?;
                $(
                    $val_txn.commit();
                )*
                Result::Ok(())
            }.await
        }
    };
}

use risingwave_hummock_sdk::ObjectIdRange;
pub(crate) use {commit_multi_var, commit_multi_var_with_provided_txn};

use crate::hummock::HummockManager;
use crate::hummock::error::Result;
use crate::hummock::sequence::next_raw_object_id;

impl HummockManager {
    #[cfg(test)]
    pub(super) async fn check_state_consistency(&self) {
        // We don't check `checkpoint` because it's allowed to update its in memory state without
        // persisting to object store.
        let get_state =
            |compact_statuses_copy, compact_task_assignment_copy, non_compaction_state| {
                ((
                    compact_statuses_copy,
                    compact_task_assignment_copy,
                    non_compaction_state,
                ),)
            };
        let get_non_compaction_state =
            |pinned_versions_copy, hummock_version_deltas_copy, version_stats_copy| {
                ((
                    pinned_versions_copy,
                    hummock_version_deltas_copy,
                    version_stats_copy,
                ),)
            };

        let mem_compact_statuses = self.compaction.snapshot_statuses().await;
        let mem_compact_task_assignment = self.compaction.snapshot_assignments().await;
        let (mem_pinned_versions, mem_hummock_version_deltas, mem_version_stats) = {
            let versioning_guard = self.versioning.read().await;
            let context_info_guard = self.context_info.read().await;
            (
                context_info_guard.pinned_versions.clone(),
                versioning_guard.hummock_version_deltas.clone(),
                versioning_guard.version_stats.clone(),
            )
        };
        let mem_state = get_state(
            mem_compact_statuses,
            mem_compact_task_assignment,
            get_non_compaction_state(
                mem_pinned_versions,
                mem_hummock_version_deltas,
                mem_version_stats,
            ),
        );

        self.load_meta_store_state()
            .await
            .expect("Failed to load state from meta store");

        let loaded_compact_statuses = self.compaction.snapshot_statuses().await;
        let loaded_compact_task_assignment = self.compaction.snapshot_assignments().await;
        let (loaded_pinned_versions, loaded_hummock_version_deltas, loaded_version_stats) = {
            let versioning_guard = self.versioning.read().await;
            let context_info_guard = self.context_info.read().await;
            (
                context_info_guard.pinned_versions.clone(),
                versioning_guard.hummock_version_deltas.clone(),
                versioning_guard.version_stats.clone(),
            )
        };
        let loaded_state = get_state(
            loaded_compact_statuses,
            loaded_compact_task_assignment,
            get_non_compaction_state(
                loaded_pinned_versions,
                loaded_hummock_version_deltas,
                loaded_version_stats,
            ),
        );
        assert_eq!(
            mem_state, loaded_state,
            "hummock in-mem state is inconsistent with meta store state",
        );
    }

    pub async fn get_new_object_ids(&self, number: u32) -> Result<ObjectIdRange> {
        let start_id = next_raw_object_id(&self.env, number).await?;
        Ok(ObjectIdRange::new(start_id, start_id + number as u64))
    }
}

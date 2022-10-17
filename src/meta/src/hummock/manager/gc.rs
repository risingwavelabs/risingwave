// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp;
use std::collections::HashSet;
use std::ops::DerefMut;

use function_name::named;
use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::{HummockSstableId, HummockVersionId, INVALID_VERSION_ID};

use crate::hummock::error::Result;
use crate::hummock::manager::{commit_multi_var, read_lock, write_lock};
use crate::hummock::HummockManager;
use crate::model::{BTreeMapTransaction, ValTransaction};
use crate::storage::{MetaStore, Transaction};

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    /// Gets SSTs that is safe to be deleted from object store.
    #[named]
    pub async fn get_ssts_to_delete(&self) -> Vec<HummockSstableId> {
        read_lock!(self, versioning)
            .await
            .ssts_to_delete
            .keys()
            .cloned()
            .collect_vec()
    }

    /// Acknowledges SSTs have been deleted from object store.
    ///
    /// Possibly extends deltas_to_delete.
    #[named]
    pub async fn ack_deleted_ssts(&self, sst_ids: &[HummockSstableId]) -> Result<()> {
        let mut deltas_to_delete = HashSet::new();
        let mut versioning_guard = write_lock!(self, versioning).await;
        for sst_id in sst_ids {
            if let Some(version_id) = versioning_guard.ssts_to_delete.remove(sst_id) && version_id != INVALID_VERSION_ID{
                // Orphan SST is mapped to INVALID_VERSION_ID
                deltas_to_delete.insert(version_id);
            }
        }
        let remain_deltas: HashSet<HummockVersionId> =
            HashSet::from_iter(versioning_guard.ssts_to_delete.values().cloned());
        deltas_to_delete.retain(|id| !remain_deltas.contains(id));
        versioning_guard.deltas_to_delete.extend(deltas_to_delete);
        Ok(())
    }

    /// Deletes at most `batch_size` deltas.
    ///
    /// Returns (number of deleted deltas, number of remain `deltas_to_delete`).
    #[named]
    pub async fn delete_version_deltas(&self, batch_size: usize) -> Result<(usize, usize)> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        if versioning_guard.deltas_to_delete.is_empty() {
            return Ok((0, 0));
        }
        let versioning = versioning_guard.deref_mut();
        let mut hummock_version_deltas =
            BTreeMapTransaction::new(&mut versioning.hummock_version_deltas);
        for delta_id in versioning.deltas_to_delete.iter().take(batch_size) {
            hummock_version_deltas.remove(*delta_id);
        }
        commit_multi_var!(self, None, hummock_version_deltas)?;
        let deleted = cmp::min(batch_size, versioning.deltas_to_delete.len());
        versioning.deltas_to_delete.drain(..deleted);
        let remain = versioning.deltas_to_delete.len();
        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }
        Ok((deleted, remain))
    }

    /// Extends `ssts_to_delete` according to object store full scan result.
    /// Caller should ensure `sst_ids` doesn't include any SSTs belong to a on-going version write.
    /// That's to say, these sst_ids won't appear in either `commit_epoch` or
    /// `report_compact_task`.
    #[named]
    pub async fn extend_ssts_to_delete_from_scan(&self, sst_ids: &[HummockSstableId]) -> usize {
        let tracked_sst_ids: HashSet<HummockSstableId> = {
            let versioning_guard = read_lock!(self, versioning).await;
            let mut tracked_sst_ids =
                HashSet::from_iter(versioning_guard.current_version.get_sst_ids());
            for delta in versioning_guard.hummock_version_deltas.values() {
                tracked_sst_ids.extend(delta.get_gc_sst_ids());
            }
            tracked_sst_ids
        };
        let to_delete = sst_ids
            .iter()
            .filter(|sst_id| !tracked_sst_ids.contains(sst_id))
            .collect_vec();
        tracing::info!("SST to delete in full GC: {:#?}", to_delete);
        write_lock!(self, versioning).await.ssts_to_delete.extend(
            to_delete
                .iter()
                .map(|sst_id| (**sst_id, INVALID_VERSION_ID)),
        );
        to_delete.len()
    }
}

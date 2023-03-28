// Copyright 2023 RisingWave Labs
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

use std::ops::Bound::{Excluded, Included};
use std::ops::{Deref, DerefMut};

use function_name::named;
use itertools::Itertools;
use risingwave_backup::checkpoint_dir;
use risingwave_pb::hummock::hummock_version_checkpoint::StaleObjects;
use risingwave_pb::hummock::HummockVersionCheckpoint;

use crate::hummock::error::Result;
use crate::hummock::manager::{read_lock, write_lock};
use crate::hummock::metrics_utils::trigger_stale_ssts_stat;
use crate::hummock::HummockManager;
use crate::storage::MetaStore;

/// A hummock version checkpoint compacts previous hummock version delta logs, and stores stale
/// objects from those delta logs.
impl<S> HummockManager<S>
where
    S: MetaStore,
{
    pub(crate) async fn read_checkpoint(&self) -> Result<Option<HummockVersionCheckpoint>> {
        // We `list` then `read`. Because from `read`'s error, we cannot tell whether it's "object
        // not found" or other kind of error.
        use prost::Message;
        let metadata = self
            .object_store
            .list(&checkpoint_dir(&self.checkpoint_path))
            .await?
            .into_iter()
            .filter(|o| o.key == self.checkpoint_path)
            .collect_vec();
        assert!(metadata.len() <= 1);
        if metadata.is_empty() {
            return Ok(None);
        }
        let data = self.object_store.read(&self.checkpoint_path, None).await?;
        let ckpt = HummockVersionCheckpoint::decode(data).map_err(|e| anyhow::anyhow!(e))?;
        Ok(Some(ckpt))
    }

    pub(super) async fn write_checkpoint(
        &self,
        checkpoint: &HummockVersionCheckpoint,
    ) -> Result<()> {
        use prost::Message;
        let buf = checkpoint.encode_to_vec();
        self.object_store
            .upload(&self.checkpoint_path, buf.into())
            .await?;
        Ok(())
    }

    /// Creates a hummock version checkpoint.
    /// Returns the diff between new and old checkpoint id.
    /// Note that this method must not be called concurrently, because internally it doesn't hold
    /// lock throughout the method.
    #[named]
    pub async fn create_version_checkpoint(&self, min_delta_log_num: u64) -> Result<u64> {
        // 1. hold read lock and create new checkpoint
        let versioning_guard = read_lock!(self, versioning).await;
        let versioning = versioning_guard.deref();
        let current_version = &versioning.current_version;
        let old_checkpoint = &versioning.checkpoint;
        let new_checkpoint_id = current_version.id;
        let old_checkpoint_id = old_checkpoint.checkpoint.as_ref().unwrap().id;
        if new_checkpoint_id < old_checkpoint_id + min_delta_log_num {
            return Ok(0);
        }
        let mut stale_objects = old_checkpoint.stale_objects.clone();
        for (_, version_delta) in versioning
            .hummock_version_deltas
            .range((Excluded(old_checkpoint_id), Included(new_checkpoint_id)))
        {
            let removed_object_ids = version_delta.gc_object_ids.clone();
            if removed_object_ids.is_empty() {
                continue;
            }
            stale_objects.insert(
                version_delta.id,
                StaleObjects {
                    id: removed_object_ids,
                },
            );
        }
        let new_checkpoint = HummockVersionCheckpoint {
            checkpoint: Some(current_version.clone()),
            stale_objects,
        };
        drop(versioning_guard);
        // 2. persist the new checkpoint without holding lock
        self.write_checkpoint(&new_checkpoint).await?;
        // 3. hold write lock and update in memory state
        let mut versioning_guard = write_lock!(self, versioning).await;
        let mut versioning = versioning_guard.deref_mut();
        assert!(
            versioning.checkpoint.checkpoint.is_none()
                || new_checkpoint.checkpoint.as_ref().unwrap().id
                    >= versioning.checkpoint.checkpoint.as_ref().unwrap().id
        );
        versioning.checkpoint = new_checkpoint;
        versioning.mark_objects_for_deletion();
        let remain = versioning.objects_to_delete.len();
        drop(versioning_guard);
        self.metrics
            .checkpoint_version_id
            .set(new_checkpoint_id as i64);
        trigger_stale_ssts_stat(&self.metrics, remain);

        Ok(new_checkpoint_id - old_checkpoint_id)
    }
}

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

use std::collections::HashMap;
use std::ops::Bound::{Excluded, Included};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;

use function_name::named;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    object_size_map, summarize_group_deltas,
};
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::HummockVersionId;
use risingwave_pb::hummock::hummock_version_checkpoint::{PbStaleObjects, StaleObjects};
use risingwave_pb::hummock::PbHummockVersionCheckpoint;

use crate::hummock::error::Result;
use crate::hummock::manager::{read_lock, write_lock};
use crate::hummock::metrics_utils::trigger_gc_stat;
use crate::hummock::HummockManager;

#[derive(Default)]
pub struct HummockVersionCheckpoint {
    pub version: HummockVersion,
    pub stale_objects: HashMap<HummockVersionId, PbStaleObjects>,
}

impl HummockVersionCheckpoint {
    pub fn from_protobuf(checkpoint: &PbHummockVersionCheckpoint) -> Self {
        Self {
            version: HummockVersion::from_persisted_protobuf(checkpoint.version.as_ref().unwrap()),
            stale_objects: checkpoint
                .stale_objects
                .iter()
                .map(|(version_id, objects)| (*version_id as HummockVersionId, objects.clone()))
                .collect(),
        }
    }

    pub fn to_protobuf(&self) -> PbHummockVersionCheckpoint {
        PbHummockVersionCheckpoint {
            version: Some(self.version.to_protobuf()),
            stale_objects: self.stale_objects.clone(),
        }
    }
}

/// A hummock version checkpoint compacts previous hummock version delta logs, and stores stale
/// objects from those delta logs.
impl HummockManager {
    /// Returns Ok(None) if not found.
    pub async fn try_read_checkpoint(&self) -> Result<Option<HummockVersionCheckpoint>> {
        use prost::Message;
        let data = match self
            .object_store
            .read(&self.version_checkpoint_path, ..)
            .await
        {
            Ok(data) => data,
            Err(e) => {
                if e.is_object_not_found_error() {
                    return Ok(None);
                }
                return Err(e.into());
            }
        };
        let ckpt = PbHummockVersionCheckpoint::decode(data).map_err(|e| anyhow::anyhow!(e))?;
        Ok(Some(HummockVersionCheckpoint::from_protobuf(&ckpt)))
    }

    pub(super) async fn write_checkpoint(
        &self,
        checkpoint: &HummockVersionCheckpoint,
    ) -> Result<()> {
        use prost::Message;
        let buf = checkpoint.to_protobuf().encode_to_vec();
        self.object_store
            .upload(&self.version_checkpoint_path, buf.into())
            .await?;
        Ok(())
    }

    /// Creates a hummock version checkpoint.
    /// Returns the diff between new and old checkpoint id.
    /// Note that this method must not be called concurrently, because internally it doesn't hold
    /// lock throughout the method.
    #[named]
    pub async fn create_version_checkpoint(&self, min_delta_log_num: u64) -> Result<u64> {
        let timer = self.metrics.version_checkpoint_latency.start_timer();
        // 1. hold read lock and create new checkpoint
        let versioning_guard = read_lock!(self, versioning).await;
        let versioning = versioning_guard.deref();
        let current_version = &versioning.current_version;
        let old_checkpoint = &versioning.checkpoint;
        let new_checkpoint_id = current_version.id;
        let old_checkpoint_id = old_checkpoint.version.id;
        if new_checkpoint_id < old_checkpoint_id + min_delta_log_num {
            return Ok(0);
        }
        let mut stale_objects = old_checkpoint.stale_objects.clone();
        // `object_sizes` is used to calculate size of stale objects.
        let mut object_sizes = object_size_map(&old_checkpoint.version);
        for (_, version_delta) in versioning
            .hummock_version_deltas
            .range((Excluded(old_checkpoint_id), Included(new_checkpoint_id)))
        {
            for group_deltas in version_delta.group_deltas.values() {
                let summary = summarize_group_deltas(group_deltas);
                object_sizes.extend(
                    summary
                        .insert_table_infos
                        .iter()
                        .map(|t| (t.object_id, t.file_size)),
                );
            }
            let removed_object_ids = version_delta.gc_object_ids.clone();
            if removed_object_ids.is_empty() {
                continue;
            }
            let total_file_size = removed_object_ids
                .iter()
                .map(|t| object_sizes.get(t).copied().unwrap())
                .sum::<u64>();
            stale_objects.insert(
                version_delta.id,
                StaleObjects {
                    id: removed_object_ids,
                    total_file_size,
                },
            );
        }
        let new_checkpoint = HummockVersionCheckpoint {
            version: current_version.clone(),
            stale_objects,
        };
        drop(versioning_guard);
        // 2. persist the new checkpoint without holding lock
        self.write_checkpoint(&new_checkpoint).await?;
        // 3. hold write lock and update in memory state
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        assert!(new_checkpoint.version.id >= versioning.checkpoint.version.id);
        versioning.checkpoint = new_checkpoint;
        versioning.mark_objects_for_deletion();

        let min_pinned_version_id = versioning.min_pinned_version_id();
        trigger_gc_stat(&self.metrics, &versioning.checkpoint, min_pinned_version_id);
        drop(versioning_guard);
        timer.observe_duration();
        self.metrics
            .checkpoint_version_id
            .set(new_checkpoint_id as i64);

        Ok(new_checkpoint_id - old_checkpoint_id)
    }

    pub fn pause_version_checkpoint(&self) {
        self.pause_version_checkpoint.store(true, Ordering::Relaxed);
        tracing::info!("hummock version checkpoint is paused.");
    }

    pub fn resume_version_checkpoint(&self) {
        self.pause_version_checkpoint
            .store(false, Ordering::Relaxed);
        tracing::info!("hummock version checkpoint is resumed.");
    }

    pub fn is_version_checkpoint_paused(&self) -> bool {
        self.pause_version_checkpoint.load(Ordering::Relaxed)
    }

    #[named]
    pub async fn get_checkpoint_version(&self) -> HummockVersion {
        let versioning_guard = read_lock!(self, versioning).await;
        versioning_guard.checkpoint.version.clone()
    }
}

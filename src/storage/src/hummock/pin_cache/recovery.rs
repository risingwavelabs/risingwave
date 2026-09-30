// Copyright 2026 RisingWave Labs
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

use std::sync::Arc;

use futures::StreamExt;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{ObjectMetadataIter, ObjectResult};

use super::{PinCache, PinCacheFile, PinCacheObject, PinCacheShard};

impl PinCache {
    pub(super) fn parse_object_id(path: &str) -> Option<HummockSstableObjectId> {
        if path.contains('/') {
            return None;
        }
        let (object_id, path_id) = path.strip_suffix(".sst")?.split_once('-')?;
        path_id.parse::<u64>().ok()?;
        object_id.parse::<u64>().ok().map(Into::into)
    }

    /// Recovers only existing local files matching the initial membership and expected size.
    /// This runs before sharing the cache, so no shard locks or runtime recovery state are needed.
    /// On any inventory error, construction fails without reclaiming a partial inventory.
    /// No remote SST metadata is read for vnode pruning; unpin/version removal withdraws stale routes.
    /// There is no persisted refill watermark: objects missed during downtime are not backfilled.
    /// Reads of missing objects use the normal fallback; subsequent version deltas drive refill.
    pub(super) async fn recover_local_files(
        &mut self,
        objects: ObjectResult<ObjectMetadataIter>,
    ) -> ObjectResult<()> {
        let mut objects = objects?;
        let mut stale_objects = Vec::new();
        while let Some(metadata) = objects.next().await {
            let metadata = metadata?;
            if metadata.key.is_empty() || metadata.key.ends_with('/') {
                continue;
            }
            let entry = Arc::new(PinCacheFile::new(
                metadata.key,
                metadata.total_size as u64,
                Arc::clone(&self.gc),
            ));
            self.gc.account_existing(&entry);
            if let Some(object_id) = Self::parse_object_id(&entry.path) {
                let shard_index = Self::shard_index(object_id, self.shards.len());
                let state = self.shards[shard_index].get_mut();
                if let Some(object) = state.recovery_target(object_id, &entry) {
                    object.publish(entry);
                    continue;
                }
            } else {
                tracing::warn!(path = %entry.path, "skipping pin cache file with invalid name during recovery");
            }
            stale_objects.push(entry);
        }
        // Do not reclaim anything from an incomplete inventory.
        self.gc.reclaim(stale_objects);
        Ok(())
    }
}

impl PinCacheShard {
    fn recovery_target(
        &mut self,
        object_id: HummockSstableObjectId,
        entry: &PinCacheFile,
    ) -> Option<&mut PinCacheObject> {
        let Some(object) = self.objects.get_mut(&object_id) else {
            tracing::debug!(path = %entry.path, "skipping pin cache file outside current membership during recovery");
            return None;
        };
        let expected_size = object.size();
        if entry.size != expected_size {
            tracing::warn!(
                path = %entry.path,
                actual_size = entry.size,
                expected_size,
                "skipping pin cache file with unexpected size during recovery"
            );
            return None;
        }
        if object.published().is_some() {
            tracing::debug!(path = %entry.path, "skipping duplicate pin cache file during recovery");
            return None;
        }
        Some(object)
    }
}

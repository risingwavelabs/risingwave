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

use futures::TryStreamExt;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{ObjectMetadataIter, ObjectResult};

use super::PinCache;

#[derive(Debug, Default)]
pub(super) struct RecoveryStats {
    pub objects: u64,
    pub bytes: u64,
}

impl PinCache {
    pub(super) fn parse_object_id(path: &str) -> Option<HummockSstableObjectId> {
        if path.contains('/') {
            return None;
        }
        let (object_id, path_id) = path.strip_suffix(".sst")?.split_once('-')?;
        path_id.parse::<u64>().ok()?;
        object_id.parse::<u64>().ok().map(Into::into)
    }

    /// Restores local routes matching the initial membership and accounts their actual sizes.
    /// Reads validate content lazily and invalidate a failed publication before falling back.
    /// The cache remains private until the complete inventory succeeds. Rejected files stay
    /// on disk for GC; failure or cancellation drops the private index without deleting files.
    /// No remote SST metadata is read, and missing objects are not backfilled.
    pub(super) async fn recover_local_files(
        &mut self,
        objects: ObjectResult<ObjectMetadataIter>,
    ) -> ObjectResult<RecoveryStats> {
        let mut stats = RecoveryStats::default();
        let mut objects = objects?;
        while let Some(metadata) = objects.try_next().await? {
            // An in-memory or buffered listing may stay ready for the entire inventory.
            tokio::task::consume_budget().await;
            if metadata.key.is_empty() || metadata.key.ends_with('/') {
                continue;
            }
            let Some(object_id) = Self::parse_object_id(&metadata.key) else {
                // Temporary and malformed files are discovered independently by full GC.
                tracing::warn!(path = %metadata.key, "skipping pin cache file with invalid name during recovery");
                continue;
            };
            let entry = self.account_existing(metadata.key, metadata.total_size as u64);
            let shard_index = Self::shard_index(object_id, self.shards.len());
            let state = self.shards[shard_index].get_mut();
            let Some(object) = state.objects.get_mut(&object_id) else {
                continue;
            };
            if object.published.is_some() {
                continue;
            }
            stats.objects += 1;
            stats.bytes += entry.size;
            object.published = Some(entry);
        }
        Ok(stats)
    }
}

#[cfg(test)]
mod tests;

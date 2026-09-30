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

use futures::future::join_all;
use futures::stream::TryChunksError;
use futures::{StreamExt, TryStreamExt};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{
    ObjectError, ObjectMetadata, ObjectMetadataIter, ObjectResult,
};

use super::{PinCache, PinCacheFile, PinCacheObject, PinCacheObjectState, PinCacheShard};

const RECOVERY_BATCH_SIZE: usize = 4096;

#[derive(Debug, Default)]
pub(super) struct RecoveryStats {
    pub objects: u64,
    pub bytes: u64,
}

impl RecoveryStats {
    fn merge(&mut self, other: Self) {
        self.objects += other.objects;
        self.bytes += other.bytes;
    }
}

struct ShardRecovery {
    index: usize,
    state: PinCacheShard,
    files: Vec<(HummockSstableObjectId, Arc<PinCacheFile>)>,
}

fn recover_group(mut shards: Vec<ShardRecovery>) -> (Vec<ShardRecovery>, RecoveryStats) {
    let mut stats = RecoveryStats::default();
    for shard in &mut shards {
        for (object_id, entry) in shard.files.drain(..) {
            if let Some(object) = shard.state.recovery_target(object_id, &entry) {
                stats.objects += 1;
                stats.bytes += entry.size;
                // Workers own these shards exclusively and never update global metrics,
                // including after cancellation of the construction future.
                object.state = PinCacheObjectState::Published(entry);
            }
        }
    }
    (shards, stats)
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

    /// Recovers only existing local files matching the initial membership and expected size.
    /// This runs before sharing the cache. Each task exclusively owns its shards, without locks.
    /// Metadata listing remains serial; only index construction is parallel, in bounded batches.
    /// Any inventory error fails construction. Unselected files remain on disk until GC is added.
    /// No remote SST metadata is read for vnode pruning; unpin/version removal withdraws stale routes.
    /// There is no persisted refill watermark: objects missed during downtime are not backfilled.
    /// Reads of missing objects use the normal fallback; subsequent version deltas drive refill.
    pub(super) async fn recover_local_files(
        &mut self,
        objects: ObjectResult<ObjectMetadataIter>,
        concurrency: usize,
    ) -> ObjectResult<RecoveryStats> {
        let mut batches = objects?.try_chunks(RECOVERY_BATCH_SIZE);
        let mut stats = RecoveryStats::default();
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(|TryChunksError(_, error)| error)?;
            stats.merge(
                self.recover_batch(batch, concurrency, recover_group)
                    .await?,
            );
        }
        Ok(stats)
    }

    async fn recover_batch(
        &mut self,
        batch: Vec<ObjectMetadata>,
        concurrency: usize,
        recover: impl Fn(Vec<ShardRecovery>) -> (Vec<ShardRecovery>, RecoveryStats)
        + Clone
        + Send
        + 'static,
    ) -> ObjectResult<RecoveryStats> {
        let mut files: Vec<Vec<_>> = (0..self.shards.len()).map(|_| Vec::new()).collect();
        for metadata in batch {
            if metadata.key.is_empty() || metadata.key.ends_with('/') {
                continue;
            }
            let entry = Arc::new(PinCacheFile {
                path: metadata.key,
                size: metadata.total_size as u64,
            });
            if let Some(object_id) = Self::parse_object_id(&entry.path) {
                let shard_index = Self::shard_index(object_id, self.shards.len());
                files[shard_index].push((object_id, entry));
            } else {
                tracing::warn!(path = %entry.path, "skipping pin cache file with invalid name during recovery");
            }
        }
        let concurrency = concurrency.min(self.shards.len());
        let mut groups: Vec<Vec<_>> = (0..concurrency).map(|_| Vec::new()).collect();
        for (group, (index, files)) in files
            .into_iter()
            .enumerate()
            .filter(|(_, files)| !files.is_empty())
            .enumerate()
        {
            groups[group % concurrency].push(ShardRecovery {
                index,
                state: std::mem::take(self.shards[index].get_mut()),
                files,
            });
        }
        let tasks = groups
            .into_iter()
            .filter(|group| !group.is_empty())
            .map(|group| {
                let recover = recover.clone();
                tokio::task::spawn_blocking(move || recover(group))
            });
        // Join every started task even if one panics. Cancellation can leave at most this batch's
        // local CPU work running; tasks never access the store, shared cache, or global metrics.
        let results = join_all(tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                ObjectError::internal(format!("pin cache recovery task failed: {err}"))
            })?;
        let mut stats = RecoveryStats::default();
        for (shards, recovered) in results {
            for shard in shards {
                *self.shards[shard.index].get_mut() = shard.state;
            }
            stats.merge(recovered);
        }
        Ok(stats)
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

#[cfg(test)]
mod tests;

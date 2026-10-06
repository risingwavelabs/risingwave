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

use futures::{StreamExt, TryStreamExt, stream};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{ObjectError, ObjectMetadataIter, ObjectResult};

use super::{PinCache, PinCacheFile, PinCacheObjectState, PinCacheShard};

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

    /// Recovers only existing local files matching the initial membership and expected size.
    /// This runs before sharing the cache. Each task exclusively owns one shard, without locks.
    /// Lists and partitions all file metadata first, then recovers shards with bounded concurrency.
    /// Any inventory error fails construction. Unselected files remain on disk until GC is added.
    /// No remote SST metadata is read for vnode pruning; unpin/version removal withdraws stale routes.
    /// There is no persisted refill watermark: objects missed during downtime are not backfilled.
    /// Reads of missing objects use the normal fallback; subsequent version deltas drive refill.
    pub(super) async fn recover_local_files(
        &mut self,
        objects: ObjectResult<ObjectMetadataIter>,
        concurrency: usize,
    ) -> ObjectResult<RecoveryStats> {
        let mut files: Vec<Vec<_>> = (0..self.shards.len()).map(|_| Vec::new()).collect();
        let mut objects = objects?;
        while let Some(metadata) = objects.try_next().await? {
            if metadata.key.is_empty() || metadata.key.ends_with('/') {
                continue;
            }
            let entry = PinCacheFile {
                path: metadata.key,
                size: metadata.total_size as u64,
            };
            if let Some(object_id) = Self::parse_object_id(&entry.path) {
                let shard_index = Self::shard_index(object_id, self.shards.len());
                files[shard_index].push((object_id, entry));
            } else {
                tracing::warn!(path = %entry.path, "skipping pin cache file with invalid name during recovery");
            }
        }
        let shards = self
            .shards
            .iter_mut()
            .zip_eq_fast(files)
            .enumerate()
            .filter(|(_, (_, files))| !files.is_empty());
        let results = stream::iter(shards)
            .map(|(index, (shard, files))| {
                let mut state = std::mem::take(shard.get_mut());
                tokio::task::spawn_blocking(move || {
                    let stats = state.recover(files);
                    (index, state, stats)
                })
            })
            .buffer_unordered(concurrency)
            // Join all tasks before propagating a panic. On cancellation, running tasks own
            // only private shard state and never access the store or global metrics.
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                ObjectError::internal(format!("pin cache recovery task failed: {err}"))
            })?;
        let mut stats = RecoveryStats::default();
        for (index, state, recovered) in results {
            *self.shards[index].get_mut() = state;
            stats.objects += recovered.objects;
            stats.bytes += recovered.bytes;
        }
        Ok(stats)
    }
}

impl PinCacheShard {
    fn recover(&mut self, files: Vec<(HummockSstableObjectId, PinCacheFile)>) -> RecoveryStats {
        let mut stats = RecoveryStats::default();
        for (object_id, entry) in files {
            let Some(object) = self.objects.get_mut(&object_id) else {
                tracing::debug!(path = %entry.path, "skipping pin cache file outside current membership during recovery");
                continue;
            };
            let expected_size = object.size();
            if entry.size != expected_size {
                tracing::warn!(
                    path = %entry.path,
                    actual_size = entry.size,
                    expected_size,
                    "skipping pin cache file with unexpected size during recovery"
                );
                continue;
            }
            if object.published().is_some() {
                tracing::debug!(path = %entry.path, "skipping duplicate pin cache file during recovery");
                continue;
            }
            stats.objects += 1;
            stats.bytes += entry.size;
            object.state = PinCacheObjectState::Published(Arc::new(entry));
        }
        stats
    }
}

#[cfg(test)]
mod tests;

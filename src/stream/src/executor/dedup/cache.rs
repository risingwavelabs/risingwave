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

use std::fmt::Debug;
use std::hash::Hash;

use risingwave_common::array::Op;
use risingwave_common::estimate_size::EstimateSize;

use crate::cache::{new_unbounded, ManagedLruCache};
use crate::common::metrics::MetricsInfo;
use crate::task::AtomicU64Ref;

/// [`DedupCache`] is used for key deduplication. Currently, the cache behaves like a set that only
/// accepts a key without a value. This could be refined in the future to support k-v pairs.
pub struct DedupCache<K: Hash + Eq + EstimateSize> {
    inner: ManagedLruCache<K, i64>,
}

impl<K: Hash + Eq + EstimateSize + Debug> DedupCache<K> {
    pub fn new(watermark_epoch: AtomicU64Ref, metrics_info: MetricsInfo) -> Self {
        let cache = new_unbounded(watermark_epoch, metrics_info);
        Self { inner: cache }
    }

    /// insert a `key` into the cache and return whether the key is duplicated.
    /// If duplicated, the cache will update the row count. If row count is 0, will return true.
    pub fn dedup_insert(&mut self, op: &Op, key: K) -> bool {
        let row_cnt = self.inner.get(&key);
        match (row_cnt.cloned(), op) {
            (Some(row_cnt), Op::Insert | Op::UpdateInsert) => {
                self.inner.put(key, row_cnt + 1);
                if row_cnt == 0 {
                    // row_cnt 0 -> 1
                    true
                } else {
                    false
                }
            }
            (Some(row_cnt), Op::Delete | Op::UpdateDelete) => {
                if row_cnt == 1 {
                    // row_cnt 1 -> 0
                    self.inner.put(key, row_cnt - 1);
                    true
                } else if row_cnt == 0 {
                    tracing::debug!("trying to delete a non-existing key from cache: {:?}", key);
                    false
                } else {
                    self.inner.put(key, row_cnt - 1);
                    false
                }
            }
            (None, Op::Insert | Op::UpdateInsert) => {
                // row_cnt 0 -> 1
                self.inner.put(key, 1).is_none()
            }
            (None, Op::Delete | Op::UpdateDelete) => {
                tracing::debug!("trying to delete a non-existing key from cache: {:?}", key);
                false
            }
        };
        false
    }

    /// Insert a `key` into the cache without checking for duplication.
    pub fn insert(&mut self, key: K) {
        self.inner.push(key, 1);
    }

    /// Check whether the given key is in the cache.
    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains(key)
    }

    /// Evict the inner LRU cache according to the watermark epoch.
    pub fn evict(&mut self) {
        self.inner.evict()
    }

    pub fn update_epoch(&mut self, epoch: u64) {
        // Update the current epoch in `ManagedLruCache`
        self.inner.update_epoch(epoch)
    }

    /// Clear everything in the cache.
    pub fn clear(&mut self) {
        self.inner.clear()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    use risingwave_common::array::Op;

    use super::DedupCache;
    use crate::common::metrics::MetricsInfo;

    #[test]
    fn test_dedup_cache() {
        let mut cache = DedupCache::new(Arc::new(AtomicU64::new(10000)), MetricsInfo::for_test());

        cache.insert(10);
        assert!(cache.contains(&10));
        assert!(!cache.dedup_insert(&Op::Insert, 10));

        assert!(cache.dedup_insert(&Op::Insert, 20));
        assert!(cache.contains(&20));
        assert!(!cache.dedup_insert(&Op::Insert, 20));

        cache.clear();
        assert!(!cache.contains(&10));
        assert!(!cache.contains(&20));
    }
}

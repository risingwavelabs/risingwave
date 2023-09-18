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
    /// Also, returns the `dup_count` of the key as i64 after inserting the record.
    pub fn dedup_insert(&mut self, op: &Op, key: K) -> (bool, i64) {
        let row_cnt = self.inner.get(&key);
        match (row_cnt.cloned(), op) {
            (Some(row_cnt), Op::Insert | Op::UpdateInsert) => {
                self.inner.put(key, row_cnt + 1);
                if row_cnt == 0 {
                    // row_cnt 0 -> 1
                    (true, 1)
                } else {
                    (false, row_cnt + 1)
                }
            }
            (Some(row_cnt), Op::Delete | Op::UpdateDelete) => {
                if row_cnt == 1 {
                    // row_cnt 1 -> 0
                    self.inner.put(key, row_cnt - 1);
                    (true, row_cnt - 1)
                } else if row_cnt == 0 {
                    tracing::debug!("trying to delete a non-existing key from cache: {:?}", key);
                    (false, 0)
                } else {
                    self.inner.put(key, row_cnt - 1);
                    (false, row_cnt - 1)
                }
            }
            (None, Op::Insert | Op::UpdateInsert) => {
                // row_cnt 0 -> 1
                (self.inner.put(key, 1).is_none(), 1)
            }
            (None, Op::Delete | Op::UpdateDelete) => {
                tracing::debug!("trying to delete a non-existing key from cache: {:?}", key);
                (false, 0)
            }
        }
    }

    /// Insert a `key` into the cache without checking for duplication.
    pub fn insert(&mut self, key: K, dup_count: i64) {
        self.inner.push(key, dup_count);
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

        cache.insert(10, 1);
        assert!(cache.contains(&10));
        assert!(!cache.dedup_insert(&Op::Insert, 10).0);

        assert!(cache.dedup_insert(&Op::Insert, 20).0);
        assert!(cache.contains(&20));
        assert!(!cache.dedup_insert(&Op::Insert, 20).0);

        cache.clear();
        assert!(!cache.contains(&10));
        assert!(!cache.contains(&20));
    }
}

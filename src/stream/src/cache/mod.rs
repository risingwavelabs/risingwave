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

use std::alloc::{Allocator, Global};
use std::hash::{BuildHasher, Hash};
use std::ops::{Deref, DerefMut};

use itertools::Itertools;
use lru::{DefaultHasher, LruCache};

mod evictable;
mod lru_manager;
mod managed_lru;
pub use evictable::*;
pub use lru_manager::*;
pub use managed_lru::*;
use risingwave_common::buffer::Bitmap;

pub enum ExecutorCache<K, V, S = DefaultHasher, A: Clone + Allocator = Global> {
    /// An managed cache. Eviction depends on the node memory usage.
    Managed(ManagedLruCache<K, V, S, A>),
    /// An local cache. Eviction depends on local executor cache limit setting.
    Local(EvictableHashMap<K, V, S, A>),
}

impl<K: Hash + Eq, V, S: BuildHasher, A: Clone + Allocator> ExecutorCache<K, V, S, A> {
    /// Evict epochs lower than the watermark
    pub fn evict(&mut self) {
        match self {
            ExecutorCache::Managed(cache) => cache.evict(),
            ExecutorCache::Local(cache) => cache.evict_to_target_cap(),
        }
    }

    /// Update the current epoch for cache. Only effective when using [`ManagedLruCache`]
    pub fn update_epoch(&mut self, epoch: u64) {
        if let ExecutorCache::Managed(cache) = self {
            cache.update_epoch(epoch)
        }
    }

    /// An iterator visiting all values in most-recently used order. The iterator element type is
    /// &V.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        let get_val = |(_k, v)| v;
        match self {
            ExecutorCache::Managed(cache) => cache.iter().map(get_val),
            ExecutorCache::Local(cache) => cache.iter().map(get_val),
        }
    }

    /// An iterator visiting all values mutably in most-recently used order. The iterator element
    /// type is &mut V.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        let get_val = |(_k, v)| v;
        match self {
            ExecutorCache::Managed(cache) => cache.iter_mut().map(get_val),
            ExecutorCache::Local(cache) => cache.iter_mut().map(get_val),
        }
    }
}

impl<K, V, S, A: Clone + Allocator> Deref for ExecutorCache<K, V, S, A> {
    type Target = LruCache<K, V, S, A>;

    fn deref(&self) -> &Self::Target {
        match self {
            ExecutorCache::Managed(cache) => &cache.inner,
            ExecutorCache::Local(cache) => &cache.inner,
        }
    }
}

impl<K, V, S, A: Clone + Allocator> DerefMut for ExecutorCache<K, V, S, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            ExecutorCache::Managed(cache) => &mut cache.inner,
            ExecutorCache::Local(cache) => &mut cache.inner,
        }
    }
}

/// Returns whether we're unsure about the fressness of the cache after the scaling from the
/// previous partition to the current one, denoted by vnode bitmaps. If the value is `true`, we must
/// evict the cache entries that does not belong to the previous partition before further
/// processing.
///
/// TODO: currently most executors simply clear all the cache entries if `true`, we can make the
/// cache aware of the consistent hashing to avoid unnecessary eviction in the future.
/// https://github.com/risingwavelabs/risingwave/issues/5567
///
/// TODO: may encapsulate the logic into [`ExecutorCache`] when ready.
///
/// # Explanation
/// We use a lazy manner to manipulate the cache. When scaling out, the partition of the existing
/// executors will likely shrink and becomes a subset of the previous one (to ensure the best
/// locality). In this case, this function will return `false` and the cache entries that're not in
/// the current partition anymore are still kept. This achieves the best performance as we won't
/// touch and valiate the cache at all when scaling-out, which is the common case and the critical
/// path.
///
/// This brings a problem when scaling in after a while. Some partitions may be reassigned back to
/// the current executor, while the cache entries of these partitions are still unevicted. So it's
/// possible that these entries have been updated by other executors on other parallel units, and
/// the content is now stale! The executor must evict these entries which are not in the
/// **previous** partition before further processing.
pub(super) fn cache_may_stale(
    previous_vnode_bitmap: &Bitmap,
    current_vnode_bitmap: &Bitmap,
) -> bool {
    let current_is_subset = previous_vnode_bitmap
        .iter()
        .zip_eq(current_vnode_bitmap.iter())
        .all(|(p, c)| p >= c);

    !current_is_subset
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_cache_may_stale() {
        let p123 = Bitmap::from_bytes(Bytes::from_static(&[0b_0000_0111_u8]));
        let p1234 = Bitmap::from_bytes(Bytes::from_static(&[0b_0000_1111_u8]));
        let p1245 = Bitmap::from_bytes(Bytes::from_static(&[0b_0001_1011_u8]));

        assert_eq!(cache_may_stale(&p123, &p123), false); // unchanged
        assert_eq!(cache_may_stale(&p1234, &p123), false); // scale-out
        assert_eq!(cache_may_stale(&p123, &p1234), true); // scale-in
        assert_eq!(cache_may_stale(&p123, &p1245), true); // scale-in
    }
}

use std::hash::Hash;

use crate::cache::{new_unbounded, ExecutorCache};
use crate::task::AtomicU64Ref;

/// [`DedupCache`] is used for key deduplication. Currently, the cache behaves like a set that only
/// accepts a key without a value. This could be refined in the future to support k-v pairs.
pub struct DedupCache<K: Hash + Eq> {
    inner: ExecutorCache<K, ()>,
}

impl<K: Hash + Eq> DedupCache<K> {
    pub fn new(watermark_epoch: AtomicU64Ref) -> Self {
        let cache = ExecutorCache::new(new_unbounded(watermark_epoch));
        Self { inner: cache }
    }

    /// Insert a `key` into the cache only if the `key` doesn't exist in the cache before. Return
    /// whether the `key` is successfully inserted.
    pub fn dedup_insert(&mut self, key: K) -> bool {
        if !self.inner.contains(&key) {
            self.inner.push(key, ());
            true
        } else {
            false
        }
    }

    /// Insert a `key` into the cache without checking for duplication.
    pub fn insert(&mut self, key: K) {
        self.inner.push(key, ());
    }

    /// Check whether the given key is in the cache.
    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains(key)
    }

    /// Evict the inner LRU cache according to the watermark epoch.
    pub fn evict(&mut self) {
        self.inner.evict()
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

    use super::DedupCache;

    #[test]
    fn test_dedup_cache() {
        let mut cache = DedupCache::new(Arc::new(AtomicU64::new(10000)));

        cache.insert(10);
        assert!(cache.contains(&10));
        assert!(!cache.dedup_insert(10));

        assert!(cache.dedup_insert(20));
        assert!(cache.contains(&20));
        assert!(!cache.dedup_insert(20));

        cache.clear();
        assert!(!cache.contains(&10));
        assert!(!cache.contains(&20));
    }
}

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

use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_common::cache::LruCache;

use super::LRU_SHARD_BITS;
use crate::hummock::{TieredCacheEntryHolder, TieredCacheKey, TieredCacheValue};

pub type Buffer<K, V> = Arc<LruCache<K, V>>;

struct TwoLevelBufferCore<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    active_buffer: Buffer<K, V>,
    frozen_buffer: Buffer<K, V>,
}

impl<K, V> TwoLevelBufferCore<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    fn swap(&mut self) {
        // Swap fields of `&mut self` to avoid the borrow checker complaining.
        std::mem::swap(&mut self.active_buffer, &mut self.frozen_buffer);
    }
}

pub struct TwoLevelBuffer<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    capacity: usize,
    core: Arc<RwLock<TwoLevelBufferCore<K, V>>>,
}

impl<K, V> Clone for TwoLevelBuffer<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    fn clone(&self) -> Self {
        Self {
            capacity: self.capacity,
            core: Arc::clone(&self.core),
        }
    }
}

impl<K, V> TwoLevelBuffer<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            core: Arc::new(RwLock::new(TwoLevelBufferCore {
                active_buffer: Arc::new(LruCache::new(LRU_SHARD_BITS, capacity, 0)),
                frozen_buffer: Arc::new(LruCache::new(LRU_SHARD_BITS, capacity, 0)),
            })),
        }
    }

    pub fn insert(&self, hash: u64, key: K, charge: usize, value: V) {
        let core = self.core.read();
        core.active_buffer.insert(key, hash, charge, value, true);
    }

    pub fn get(&self, hash: u64, key: &K) -> Option<TieredCacheEntryHolder<K, V>> {
        let core = self.core.read();
        if let Some(entry) = core.active_buffer.lookup(hash, key) {
            return Some(TieredCacheEntryHolder::from_cached_value(entry));
        }
        if let Some(entry) = core.frozen_buffer.lookup(hash, key) {
            return Some(TieredCacheEntryHolder::from_cached_value(entry));
        }
        None
    }

    pub fn erase(&self, hash: u64, key: &K) {
        let core = self.core.read();
        core.active_buffer.erase(hash, key);
        core.frozen_buffer.erase(hash, key);
    }

    pub fn active(&self) -> Buffer<K, V> {
        self.core.read().active_buffer.clone()
    }

    pub fn frozen(&self) -> Buffer<K, V> {
        self.core.read().frozen_buffer.clone()
    }

    pub fn swap(&self) {
        self.core.write().swap();
    }

    pub fn rotate(&self) -> Buffer<K, V> {
        let mut buffer = Arc::new(LruCache::new(LRU_SHARD_BITS, self.capacity, 0));
        let mut core = self.core.write();
        std::mem::swap(&mut buffer, &mut core.active_buffer);
        std::mem::swap(&mut buffer, &mut core.frozen_buffer);
        buffer
    }
}

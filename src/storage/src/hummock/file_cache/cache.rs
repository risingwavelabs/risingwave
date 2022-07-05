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

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::sync::Arc;
use std::u8;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::cache::LruCache;
use tokio::sync::Notify;

use super::buffer::TwoLevelBuffer;
use super::coding::CacheKey;
use super::error::Result;
use super::filter::Filter;
use super::meta::SlotId;
use super::store::{Store, StoreOptions, StoreRef};

const INDICES_LRU_SHARD_BITS: usize = 6;

pub struct FileCacheOptions {
    pub dir: String,
    pub capacity: usize,
    pub total_buffer_capacity: usize,
    pub cache_file_fallocate_unit: usize,
    pub filters: Vec<Arc<dyn Filter>>,

    pub flush_buffer_hooks: Vec<Arc<dyn FlushBufferHook>>,
}

#[async_trait]
pub trait FlushBufferHook: Send + Sync + 'static {
    async fn pre_flush(&self) -> Result<()> {
        Ok(())
    }

    async fn post_flush(&self) -> Result<()> {
        Ok(())
    }
}

struct BufferFlusher<K>
where
    K: CacheKey,
{
    buffer: TwoLevelBuffer<K>,
    store: StoreRef<K>,
    indices: Arc<LruCache<K, SlotId>>,
    notifier: Arc<Notify>,

    hooks: Vec<Arc<dyn FlushBufferHook>>,
}

impl<K> BufferFlusher<K>
where
    K: CacheKey,
{
    async fn run(&self) -> Result<()> {
        loop {
            self.notifier.notified().await;

            for hook in &self.hooks {
                hook.pre_flush().await?;
            }

            let frozen = self.buffer.frozen();

            let mut batch = Vec::new();
            // TODO(MrCroxx): Avoid clone here?
            frozen.fill_with(|key, value| batch.push((key.clone(), value.clone())));

            if batch.is_empty() {
                // Avoid allocate a new buffer.
                self.buffer.swap();
            } else {
                let slots = self.store.insert(&batch).await?;
                for ((key, value), slot) in batch.into_iter().zip_eq(slots.into_iter()) {
                    let mut hasher = DefaultHasher::new();
                    key.hash(&mut hasher);
                    let hash = hasher.finish();
                    self.indices.insert(key, hash, value.len(), slot);
                }
                self.buffer.rotate();
            }

            for hook in &self.hooks {
                hook.post_flush().await?;
            }
        }
    }
}

#[derive(Clone)]
pub struct FileCache<K>
where
    K: CacheKey,
{
    _filters: Vec<Arc<dyn Filter>>,

    indices: Arc<LruCache<K, SlotId>>,

    store: StoreRef<K>,

    buffer: TwoLevelBuffer<K>,
    buffer_flusher_notifier: Arc<Notify>,
}

impl<K> FileCache<K>
where
    K: CacheKey,
{
    pub async fn open(options: FileCacheOptions) -> Result<Self> {
        let buffer_capacity = options.total_buffer_capacity / 2;

        let store = Store::open(StoreOptions {
            dir: options.dir,
            capacity: options.capacity,
            buffer_capacity,
            cache_file_fallocate_unit: options.cache_file_fallocate_unit,
        })
        .await?;
        let store = Arc::new(store);

        // TODO: Restore indices.
        let indices = LruCache::with_event_listeners(
            INDICES_LRU_SHARD_BITS,
            options.capacity,
            vec![store.clone()],
        );
        store.restore(&indices).await?;
        let indices = Arc::new(indices);

        let buffer = TwoLevelBuffer::new(buffer_capacity);
        let buffer_flusher_notifier = Arc::new(Notify::new());

        let buffer_flusher = BufferFlusher {
            buffer: buffer.clone(),
            store: store.clone(),
            indices: indices.clone(),
            notifier: buffer_flusher_notifier.clone(),

            hooks: options.flush_buffer_hooks,
        };
        // TODO(MrCroxx): Graceful shutdown.
        let _handle = tokio::task::spawn(async move {
            if let Err(e) = buffer_flusher.run().await {
                tracing::error!("error raised within file cache buffer flusher: {}", e);
            }
        });

        Ok(Self {
            _filters: options.filters,

            indices,

            store,

            buffer,
            buffer_flusher_notifier,
        })
    }

    pub fn insert(&self, key: K, value: Vec<u8>) -> Result<()> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        self.buffer.insert(hash, key, value.len(), value);

        self.buffer_flusher_notifier.notify_one();
        Ok(())
    }

    // TODO(MrCroxx): Return Arc<..> or ..? Based on use cases?
    pub async fn get(&self, key: &K) -> Result<Option<Vec<u8>>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        if let Some(value) = self.buffer.get(hash, key) {
            return Ok(Some(value));
        }

        if let Some(entry) = self.indices.lookup(hash, key) {
            let slot = *entry.value();
            let value = self.store.get(slot).await?;
            return Ok(Some(value));
        }

        Ok(None)
    }

    pub fn earse(&self, key: &K) -> Result<()> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        self.buffer.erase(hash, key);
        // No need to manually remove data from store. `LruCacheEventListener` on `indices` will
        // free the slot.
        self.indices.erase(hash, key);

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;
    use std::path::Path;

    use super::super::test_utils::{key, TestCacheKey};
    use super::super::utils;
    use super::*;
    use crate::hummock::file_cache::test_utils::FlushHolder;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<FileCache<TestCacheKey>>();
    }

    fn tempdir() -> tempfile::TempDir {
        let ci: bool = std::env::var("RISINGWAVE_CI")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .expect("env $RISINGWAVE_CI must be 'true' or 'false'");

        if ci {
            tempfile::Builder::new().tempdir_in("/risingwave").unwrap()
        } else {
            tempfile::tempdir().unwrap()
        }
    }

    async fn create_file_cache_manager_for_test(
        dir: impl AsRef<Path>,
        flush_buffer_hooks: Vec<Arc<dyn FlushBufferHook>>,
    ) -> FileCache<TestCacheKey> {
        let options = FileCacheOptions {
            dir: dir.as_ref().to_str().unwrap().to_string(),
            capacity: 128 * 4 * 1024,
            total_buffer_capacity: 32 * 2 * 4 * 1024,
            cache_file_fallocate_unit: 64 * 4 * 1024,
            filters: vec![],

            flush_buffer_hooks,
        };
        FileCache::open(options).await.unwrap()
    }

    #[tokio::test]
    async fn test_cache_curd() {
        let dir = tempdir();

        let holder = Arc::new(FlushHolder::default());
        let cache = create_file_cache_manager_for_test(dir.path(), vec![holder.clone()]).await;

        cache.insert(key(1), vec![b'1'; 1234]).unwrap();

        // active
        assert_eq!(cache.get(&key(1)).await.unwrap(), Some(vec![b'1'; 1234]));
        // frozen
        holder.trigger();
        holder.wait().await;
        assert_eq!(cache.get(&key(1)).await.unwrap(), Some(vec![b'1'; 1234]));
        // cache file
        cache.buffer_flusher_notifier.notify_one();
        holder.trigger();
        holder.wait().await;
        assert_eq!(cache.get(&key(1)).await.unwrap(), Some(vec![b'1'; 1234]));

        cache.earse(&key(1)).unwrap();
        assert_eq!(cache.get(&key(1)).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_cache_grow() {
        let dir = tempdir();

        let holder = Arc::new(FlushHolder::default());
        let cache = create_file_cache_manager_for_test(dir.path(), vec![holder.clone()]).await;
        let bs = cache.store.block_size();

        for i in 1..=64 {
            cache.insert(key(i), vec![b'x'; bs]).unwrap();
            assert_eq!(cache.get(&key(i)).await.unwrap(), Some(vec![b'x'; bs]));
        }

        let mut in_cache = HashSet::new();
        for i in 1..=64 {
            if cache.get(&key(i)).await.unwrap().is_some() {
                in_cache.insert(i);
            }
        }
        assert!(in_cache.len() <= 32);

        assert_eq!(cache.store.cache_file_len(), 0);

        holder.trigger();
        holder.wait().await;
        cache.buffer_flusher_notifier.notify_one();
        holder.trigger();
        holder.wait().await;

        assert_eq!(
            cache.store.cache_file_len(),
            utils::usize::align_up(cache.store.block_size(), in_cache.len() * bs)
        );
        for i in 1..=64 {
            assert_eq!(
                cache.get(&key(i)).await.unwrap(),
                if in_cache.get(&i).is_some() {
                    Some(vec![b'x'; bs])
                } else {
                    None
                }
            );
        }

        in_cache.clear();
        // Insert 4 times of capacity data to make sure old data in all shards are evicted.
        let mut buffer_len = 0;
        for i in 65..=65 + 128 * 4 {
            cache.insert(key(i), vec![b'x'; bs]).unwrap();
            assert_eq!(cache.get(&key(i)).await.unwrap(), Some(vec![b'x'; bs]));

            buffer_len += 1;

            if buffer_len == 16 {
                holder.trigger();
                holder.wait().await;
                cache.buffer_flusher_notifier.notify_one();
                holder.trigger();
                holder.wait().await;
                buffer_len = 0;
            }
        }

        for i in 1..=64 {
            assert_eq!(cache.get(&key(i)).await.unwrap(), None, "i: {}", i);
        }
        for i in 65..=65 + 128 * 4 {
            if cache.get(&key(i)).await.unwrap().is_some() {
                in_cache.insert(i);
            }
        }
        assert!(in_cache.len() * bs <= 128 * 4 * 1024);
    }
}

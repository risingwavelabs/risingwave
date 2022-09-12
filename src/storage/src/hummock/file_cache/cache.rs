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

use std::collections::hash_map::RandomState;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::cache::LruCache;
use tokio::sync::Notify;

use super::buffer::TwoLevelBuffer;
use super::error::Result;
use super::meta::SlotId;
use super::metrics::FileCacheMetricsRef;
use super::store::{Store, StoreOptions, StoreRef};
use super::{utils, LRU_SHARD_BITS};
use crate::hummock::{HashBuilder, TieredCacheEntryHolder, TieredCacheKey, TieredCacheValue};

pub struct FileCacheOptions {
    pub dir: String,
    pub capacity: usize,
    pub total_buffer_capacity: usize,
    pub cache_file_fallocate_unit: usize,

    pub flush_buffer_hooks: Vec<Arc<dyn FlushBufferHook>>,
}

#[async_trait]
pub trait FlushBufferHook: Send + Sync + 'static {
    async fn pre_flush(&self) -> Result<()> {
        Ok(())
    }

    async fn post_flush(&self, _bytes: usize) -> Result<()> {
        Ok(())
    }
}

struct BufferFlusher<K, V, S>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
    S: HashBuilder,
{
    buffer: TwoLevelBuffer<K, V>,
    store: StoreRef<K, V>,
    indices: Arc<LruCache<K, SlotId>>,
    notifier: Arc<Notify>,

    hash_builder: S,

    hooks: Vec<Arc<dyn FlushBufferHook>>,
}

impl<K, V, S> BufferFlusher<K, V, S>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
    S: HashBuilder,
{
    async fn run(&self) -> Result<()> {
        loop {
            self.notifier.notified().await;

            for hook in &self.hooks {
                hook.pre_flush().await?;
            }

            let frozen = self.buffer.frozen();

            let mut encoded_value_lens = Vec::with_capacity(64);
            let mut batch = self.store.start_batch_writer(64);

            frozen.for_all(|key, value| {
                batch.append(key.clone(), value);
                encoded_value_lens.push(value.encoded_len());
            });

            let mut bytes = 0;
            if batch.is_empty() {
                // Avoid allocate a new buffer.
                self.buffer.swap();
                // Trigger clear free list.
                batch.finish().await?;
            } else {
                let (keys, slots) = batch.finish().await?;

                for ((key, encoded_value_len), slot) in keys
                    .into_iter()
                    .zip_eq(encoded_value_lens.into_iter())
                    .zip_eq(slots.into_iter())
                {
                    let hash = self.hash_builder.hash_one(&key);
                    self.indices.insert(
                        key,
                        hash,
                        utils::align_up(self.store.block_size(), encoded_value_len),
                        slot,
                    );
                    bytes += utils::align_up(self.store.block_size(), encoded_value_len);
                }
                self.buffer.rotate();
            }

            for hook in &self.hooks {
                hook.post_flush(bytes).await?;
            }
        }
    }
}

pub struct FileCache<K, V, S = RandomState>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
    S: HashBuilder,
{
    hash_builder: S,

    indices: Arc<LruCache<K, SlotId>>,

    store: StoreRef<K, V>,

    buffer: TwoLevelBuffer<K, V>,
    buffer_flusher_notifier: Arc<Notify>,

    metrics: FileCacheMetricsRef,
}

impl<K, V, S> Clone for FileCache<K, V, S>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
    S: HashBuilder,
{
    fn clone(&self) -> Self {
        Self {
            hash_builder: self.hash_builder.clone(),
            indices: self.indices.clone(),
            store: self.store.clone(),
            buffer: self.buffer.clone(),
            buffer_flusher_notifier: self.buffer_flusher_notifier.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<K, V> FileCache<K, V, RandomState>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    pub async fn open(options: FileCacheOptions, metrics: FileCacheMetricsRef) -> Result<Self> {
        let hash_builder = RandomState::new();
        Self::open_with_hasher(options, hash_builder, metrics).await
    }
}

impl<K, V, S> FileCache<K, V, S>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
    S: HashBuilder,
{
    pub async fn open_with_hasher(
        options: FileCacheOptions,
        hash_builder: S,
        metrics: FileCacheMetricsRef,
    ) -> Result<Self> {
        let buffer_capacity = options.total_buffer_capacity / 2;

        let store = Store::open(StoreOptions {
            dir: options.dir,
            capacity: options.capacity,
            buffer_capacity,
            cache_file_fallocate_unit: options.cache_file_fallocate_unit,
            metrics: metrics.clone(),
        })
        .await?;
        let store = Arc::new(store);

        let indices = Arc::new(LruCache::with_event_listener(
            LRU_SHARD_BITS,
            options.capacity,
            store.clone(),
        ));
        store.restore(&indices, &hash_builder).await?;

        let buffer = TwoLevelBuffer::new(buffer_capacity);
        let buffer_flusher_notifier = Arc::new(Notify::new());

        let buffer_flusher = BufferFlusher {
            buffer: buffer.clone(),
            store: store.clone(),
            indices: indices.clone(),
            notifier: buffer_flusher_notifier.clone(),

            hash_builder: hash_builder.clone(),

            hooks: options.flush_buffer_hooks,
        };
        // TODO(MrCroxx): Graceful shutdown.
        let _handle = tokio::task::spawn(async move {
            if let Err(e) = buffer_flusher.run().await {
                panic!("error raised within file cache buffer flusher: {}", e);
            }
        });

        Ok(Self {
            hash_builder,

            indices,

            store,

            buffer,
            buffer_flusher_notifier,

            metrics,
        })
    }

    pub fn insert(&self, key: K, value: V) -> Result<()> {
        let timer = self.metrics.insert_latency.start_timer();

        let hash = self.hash_builder.hash_one(&key);
        self.buffer.insert(hash, key, value.len(), value);

        self.buffer_flusher_notifier.notify_one();

        timer.observe_duration();

        Ok(())
    }

    pub async fn get(&self, key: &K) -> Result<Option<TieredCacheEntryHolder<K, V>>> {
        let timer = self.metrics.get_latency.start_timer();

        let hash = self.hash_builder.hash_one(key);
        if let Some(holder) = self.buffer.get(hash, key) {
            timer.observe_duration();
            return Ok(Some(holder));
        }

        if let Some(entry) = self.indices.lookup(hash, key) {
            let slot = *entry.value();
            let raw = self.store.get(slot).await?;
            let value = V::decode(raw);

            timer.observe_duration();

            return Ok(Some(TieredCacheEntryHolder::from_owned_value(value)));
        }

        timer.observe_duration();
        self.metrics.cache_miss.inc();

        Ok(None)
    }

    pub fn erase(&self, key: &K) -> Result<()> {
        let timer = self.metrics.erase_latency.start_timer();

        let hash = self.hash_builder.hash_one(key);
        self.buffer.erase(hash, key);

        if let Some(entry) = self.indices.lookup(hash, key) {
            let slot = *entry.value();
            self.indices.erase(hash, key);
            self.store.erase(slot).unwrap();
        }

        timer.observe_duration();

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::path::Path;

    use prometheus::Registry;

    use super::super::test_utils::{datasize, key, FlushHolder, ModuloHasherBuilder, TestCacheKey};
    use super::super::utils;
    use super::*;
    use crate::hummock::file_cache::metrics::FileCacheMetrics;
    use crate::hummock::file_cache::test_utils::TestCacheValue;

    const SHARDS: usize = 1 << LRU_SHARD_BITS;
    const SHARDSU8: u8 = SHARDS as u8;
    const SHARDSU64: u64 = SHARDS as u64;

    const BS: usize = 4096 * 4;
    const CAPACITY: usize = 4 * SHARDS * BS;
    const BUFFER_CAPACITY: usize = SHARDS * BS;
    const FALLOCATE_UNIT: usize = 2 * SHARDS * BS;

    const LOOPS: usize = 8;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<FileCache<TestCacheKey, Vec<u8>>>();
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
    ) -> FileCache<TestCacheKey, TestCacheValue, ModuloHasherBuilder<SHARDSU8>> {
        let options = FileCacheOptions {
            dir: dir.as_ref().to_str().unwrap().to_string(),
            capacity: CAPACITY,
            total_buffer_capacity: 2 * BUFFER_CAPACITY,
            cache_file_fallocate_unit: FALLOCATE_UNIT,

            flush_buffer_hooks,
        };
        FileCache::open_with_hasher(
            options,
            ModuloHasherBuilder,
            Arc::new(FileCacheMetrics::new(Registry::new())),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_cache_curd() {
        let dir = tempdir();

        let holder = Arc::new(FlushHolder::default());
        let cache = create_file_cache_manager_for_test(dir.path(), vec![holder.clone()]).await;

        cache.insert(key(1), vec![b'1'; 1234]).unwrap();

        // active
        assert_eq!(
            cache.get(&key(1)).await.unwrap().as_deref(),
            Some(&vec![b'1'; 1234])
        );

        // frozen
        holder.trigger();
        holder.wait().await;
        assert_eq!(
            cache.get(&key(1)).await.unwrap().as_deref(),
            Some(&vec![b'1'; 1234])
        );

        // cache file
        cache.buffer_flusher_notifier.notify_one();
        holder.trigger();
        holder.wait().await;
        assert_eq!(
            cache.get(&key(1)).await.unwrap().as_deref(),
            Some(&vec![b'1'; 1234])
        );

        cache.erase(&key(1)).unwrap();
        assert_eq!(cache.get(&key(1)).await.unwrap().as_deref(), None);
    }

    #[tokio::test]
    async fn test_cache_grow() {
        let dir = tempdir();

        let holder = Arc::new(FlushHolder::default());
        let cache = create_file_cache_manager_for_test(dir.path(), vec![holder.clone()]).await;

        for i in 0..SHARDSU64 * 2 {
            cache.insert(key(i), vec![b'x'; BS]).unwrap();
            assert_eq!(
                cache.get(&key(i)).await.unwrap().as_deref(),
                Some(&vec![b'x'; BS])
            );
        }

        for i in 0..SHARDSU64 {
            assert_eq!(
                cache.get(&key(i)).await.unwrap().as_deref(),
                None,
                "i: {}",
                i
            );
        }
        for i in SHARDSU64..SHARDSU64 * 2 {
            assert_eq!(
                cache.get(&key(i)).await.unwrap().as_deref(),
                Some(&vec![b'x'; BS]),
                "i: {}",
                i
            );
        }

        assert_eq!(cache.store.cache_file_len(), 0);

        holder.trigger();
        holder.wait().await;
        cache.buffer_flusher_notifier.notify_one();
        holder.trigger();
        holder.wait().await;

        assert_eq!(cache.store.cache_file_len(), SHARDS * BS);
        assert_eq!(
            datasize(cache.store.cache_file_path()).unwrap(),
            utils::align_up(FALLOCATE_UNIT, SHARDS * BS)
        );

        for i in 0..SHARDSU64 {
            assert_eq!(cache.get(&key(i)).await.unwrap().as_deref(), None);
        }
        for i in SHARDSU64..SHARDSU64 * 2 {
            assert_eq!(
                cache.get(&key(i)).await.unwrap().as_deref(),
                Some(&vec![b'x'; BS])
            );
        }

        for l in 0..LOOPS as u64 {
            for i in (l + 2) * SHARDSU64..(l + 3) * SHARDSU64 {
                cache.insert(key(i), vec![b'x'; BS]).unwrap();
                assert_eq!(
                    cache.get(&key(i)).await.unwrap().as_deref(),
                    Some(&vec![b'x'; BS])
                );
            }
            holder.trigger();
            holder.wait().await;
            cache.buffer_flusher_notifier.notify_one();
            holder.trigger();
            holder.wait().await;
        }

        // Trigger free last free list.
        cache.buffer_flusher_notifier.notify_one();
        holder.trigger();
        holder.wait().await;

        assert_eq!(cache.store.cache_file_len(), 9 * SHARDS * BS);
        assert_eq!(
            datasize(cache.store.cache_file_path()).unwrap(),
            // TODO(MrCroxx): For inserting performs "append -> insert indices & punch hole",
            // the maximum file size may exceed the capacity by at most an alloc unit.
            // May refactor it later.
            utils::align_up(FALLOCATE_UNIT, CAPACITY) + FALLOCATE_UNIT - BUFFER_CAPACITY
        );

        for l in 0..2 + LOOPS as u64 - 4 {
            for i in l * SHARDSU64..(l + 1) * SHARDSU64 {
                assert_eq!(
                    cache.get(&key(i)).await.unwrap().as_deref(),
                    None,
                    "i: {}",
                    i
                );
            }
        }

        for l in 2 + LOOPS as u64 - 4..2 + LOOPS as u64 {
            for i in l * SHARDSU64..(l + 1) * SHARDSU64 {
                assert_eq!(
                    cache.get(&key(i)).await.unwrap().as_deref(),
                    Some(&vec![b'x'; BS])
                );
            }
        }
    }

    #[tokio::test]
    async fn test_recovery() {
        let dir = tempdir();

        let holder = Arc::new(FlushHolder::default());
        let cache = create_file_cache_manager_for_test(dir.path(), vec![holder.clone()]).await;

        for l in 0..LOOPS as u64 {
            for i in l * SHARDSU64..(l + 1) * SHARDSU64 {
                cache.insert(key(i), vec![b'x'; BS]).unwrap();
                assert_eq!(
                    cache.get(&key(i)).await.unwrap().as_deref(),
                    Some(&vec![b'x'; BS])
                );
            }
            holder.trigger();
            holder.wait().await;
            cache.buffer_flusher_notifier.notify_one();
            holder.trigger();
            holder.wait().await;
        }

        let mut map = HashMap::new();
        for l in 0..LOOPS as u64 {
            for i in l * SHARDSU64..(l + 1) * SHARDSU64 {
                map.insert(key(i), cache.get(&key(i)).await.unwrap());
            }
        }

        drop(cache);

        let cache = create_file_cache_manager_for_test(dir.path(), vec![holder.clone()]).await;
        for (key, slot) in map {
            assert_eq!(cache.get(&key).await.unwrap().as_deref(), slot.as_deref());
        }
    }
}

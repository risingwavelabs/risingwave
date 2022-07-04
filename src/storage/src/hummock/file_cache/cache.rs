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

use parking_lot::RwLock;
use risingwave_common::cache::LruCache;
use tokio::sync::Notify;

use super::coding::CacheKey;
use super::error::Result;
use super::filter::Filter;
use super::meta::SlotId;
use super::store::{Store, StoreOptions, StoreRef};

const INDICES_LRU_SHARD_BITS: usize = 6;
const BUFFER_LRU_SHARD_BITS: usize = 5;

pub struct FileCacheOptions {
    pub dir: String,
    pub capacity: usize,
    pub total_buffer_capacity: usize,
    pub cache_file_fallocate_unit: usize,
    pub filters: Vec<Arc<dyn Filter>>,
}

#[derive(Clone)]
struct Buffer<K>
where
    K: CacheKey,
{
    active_buffer: Arc<LruCache<K, Vec<u8>>>,
    frozen_buffer: Arc<LruCache<K, Vec<u8>>>,
}

struct BufferFlusher<K>
where
    K: CacheKey,
{
    buffer_capacity: usize,
    buffer: Arc<RwLock<Buffer<K>>>,
    _store: StoreRef<K>,
    _indices: Arc<LruCache<K, SlotId>>,
    notifier: Arc<Notify>,
}

impl<K> BufferFlusher<K>
where
    K: CacheKey,
{
    async fn run(&self) -> Result<()> {
        loop {
            self.notifier.notified().await;

            let _frozen = self.buffer.read().frozen_buffer.clone();

            // TODO: Drain `buf`, inseret store, update `indices`.
            // let slots = self.store.insert(batch).await?;

            // rotate buffer
            let mut buf = Arc::new(LruCache::new(BUFFER_LRU_SHARD_BITS, self.buffer_capacity));
            let mut buffer = self.buffer.write();
            std::mem::swap(&mut buf, &mut buffer.active_buffer);
            std::mem::swap(&mut buf, &mut buffer.frozen_buffer);
            drop(buffer);
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

    _block_size: usize,

    buffer: Arc<RwLock<Buffer<K>>>,
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
        let block_size = store.block_size();
        let store = Arc::new(store);

        // TODO: Restore indices.
        let indices = LruCache::with_event_listeners(
            INDICES_LRU_SHARD_BITS,
            options.capacity,
            vec![store.clone()],
        );
        store.restore(&indices).await?;
        let indices = Arc::new(indices);

        let buffer = Arc::new(RwLock::new(Buffer {
            active_buffer: Arc::new(LruCache::new(BUFFER_LRU_SHARD_BITS, buffer_capacity)),
            frozen_buffer: Arc::new(LruCache::new(BUFFER_LRU_SHARD_BITS, buffer_capacity)),
        }));
        let buffer_flusher_notifier = Arc::new(Notify::new());

        let buffer_flusher = BufferFlusher {
            buffer_capacity,
            buffer: buffer.clone(),
            _store: store.clone(),
            _indices: indices.clone(),
            notifier: buffer_flusher_notifier.clone(),
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
            _block_size: block_size,

            buffer,
            buffer_flusher_notifier,
        })
    }

    pub fn insert(&self, key: K, value: Vec<u8>) -> Result<()> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        let buffer = self.buffer.read();

        buffer.active_buffer.insert(key, hash, value.len(), value);

        self.buffer_flusher_notifier.notify_one();
        Ok(())
    }

    // TODO(MrCroxx): Return Arc<..> or ..? Based on use cases?
    pub async fn get(&self, key: &K) -> Result<Option<Vec<u8>>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        // Use a block to wrap the `RwLockGuard`, or clippy will raise a warning.
        {
            let buffer = self.buffer.read();
            if let Some(entry) = buffer.active_buffer.lookup(hash, key) {
                return Ok(Some(entry.value().clone()));
            }
            if let Some(entry) = buffer.frozen_buffer.lookup(hash, key) {
                return Ok(Some(entry.value().clone()));
            }
            drop(buffer);
        }

        if let Some(entry) = self.indices.lookup(hash, key) {
            let slot = *entry.value();
            let value = self.store.get(slot).await?;
            return Ok(Some(value));
        }

        Ok(None)
    }

    pub fn earse(&self, _key: &K) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use super::super::test_utils::TestCacheKey;
    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<FileCache<TestCacheKey>>();
    }

    #[tokio::test]
    async fn test_file_cache_manager() {
        let ci: bool = std::env::var("RISINGWAVE_CI")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .expect("env $RISINGWAVE_CI must be 'true' or 'false'");

        let tempdir = if ci {
            tempfile::Builder::new().tempdir_in("/risingwave").unwrap()
        } else {
            tempfile::tempdir().unwrap()
        };

        let options = FileCacheOptions {
            dir: tempdir.path().to_str().unwrap().to_string(),
            capacity: 256 * 1024 * 1024,
            total_buffer_capacity: 128 * 1024,
            cache_file_fallocate_unit: 64 * 1024 * 1024,
            filters: vec![],
        };
        let _cache: FileCache<TestCacheKey> = FileCache::open(options).await.unwrap();
    }
}

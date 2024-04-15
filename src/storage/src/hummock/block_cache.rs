// Copyright 2024 RisingWave Labs
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

use std::ops::Deref;
use std::sync::Arc;

use ahash::RandomState;
use await_tree::InstrumentAwait;
use foyer::memory::{
    Cache, CacheContext, CacheEntry, Entry, EntryState, LfuCacheConfig, LruCacheConfig,
    S3FifoCacheConfig,
};
use futures::Future;
use risingwave_common::config::EvictionConfig;
use risingwave_hummock_sdk::HummockSstableObjectId;

use super::{Block, BlockCacheEventListener, HummockResult};
use crate::hummock::HummockError;

type CachedBlockEntry =
    CacheEntry<(HummockSstableObjectId, u64), Box<Block>, BlockCacheEventListener>;

enum BlockEntry {
    Cache(#[allow(dead_code)] CachedBlockEntry),
    Owned(#[allow(dead_code)] Box<Block>),
    RefEntry(#[allow(dead_code)] Arc<Block>),
}

pub struct BlockHolder {
    _handle: BlockEntry,
    pub block: *const Block,
}

impl BlockHolder {
    pub fn from_ref_block(block: Arc<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::RefEntry(block),
            block: ptr,
        }
    }

    pub fn from_owned_block(block: Box<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::Owned(block),
            block: ptr,
        }
    }

    pub fn from_cached_block(entry: CachedBlockEntry) -> Self {
        let ptr = entry.deref().as_ref() as *const _;
        Self {
            _handle: BlockEntry::Cache(entry),
            block: ptr,
        }
    }
}

impl Deref for BlockHolder {
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.block) }
    }
}

unsafe impl Send for BlockHolder {}
unsafe impl Sync for BlockHolder {}

#[derive(Debug)]
pub struct BlockCacheConfig {
    pub capacity: usize,
    pub shard_num: usize,
    pub eviction: EvictionConfig,
    pub listener: BlockCacheEventListener,
}

#[derive(Clone)]
pub struct BlockCache {
    inner: Cache<(HummockSstableObjectId, u64), Box<Block>, BlockCacheEventListener>,
}

pub enum BlockResponse {
    Block(BlockHolder),
    Entry(Entry<(HummockSstableObjectId, u64), Box<Block>, HummockError, BlockCacheEventListener>),
}

impl BlockResponse {
    pub async fn wait(self) -> HummockResult<BlockHolder> {
        let entry = match self {
            BlockResponse::Block(block) => return Ok(block),
            BlockResponse::Entry(entry) => entry,
        };
        match entry.state() {
            EntryState::Hit => entry.await.map(BlockHolder::from_cached_block),
            EntryState::Wait => entry
                .verbose_instrument_await("wait_pending_fetch_block")
                .await
                .map(BlockHolder::from_cached_block),
            EntryState::Miss => entry
                .verbose_instrument_await("fetch_block")
                .await
                .map(BlockHolder::from_cached_block),
        }
    }
}

impl BlockCache {
    pub fn new(config: BlockCacheConfig) -> Self {
        assert!(
            config.capacity > 0,
            "Block cache capacity must be positive."
        );

        let capacity = config.capacity;
        let shards = config.shard_num;
        let object_pool_capacity = shards * 1024;
        let hash_builder = RandomState::default();
        let event_listener = config.listener;

        let inner = match config.eviction {
            EvictionConfig::Lru(eviction_config) => Cache::lru(LruCacheConfig {
                capacity,
                shards,
                eviction_config,
                object_pool_capacity,
                hash_builder,
                event_listener,
            }),
            EvictionConfig::Lfu(eviction_config) => Cache::lfu(LfuCacheConfig {
                capacity,
                shards,
                eviction_config,
                object_pool_capacity,
                hash_builder,
                event_listener,
            }),
            EvictionConfig::S3Fifo(eviction_config) => Cache::s3fifo(S3FifoCacheConfig {
                capacity,
                shards,
                eviction_config,
                object_pool_capacity,
                hash_builder,
                event_listener,
            }),
        };

        Self { inner }
    }

    pub fn get(&self, object_id: HummockSstableObjectId, block_idx: u64) -> Option<BlockHolder> {
        self.inner
            .get(&(object_id, block_idx))
            .map(BlockHolder::from_cached_block)
    }

    pub fn exists_block(&self, sst_id: HummockSstableObjectId, block_idx: u64) -> bool {
        // TODO(MrCroxx): optimize me
        self.get(sst_id, block_idx).is_some()
    }

    pub fn insert(
        &self,
        object_id: HummockSstableObjectId,
        block_idx: u64,
        block: Box<Block>,
        context: CacheContext,
    ) -> BlockHolder {
        let charge = block.capacity();
        BlockHolder::from_cached_block(self.inner.insert_with_context(
            (object_id, block_idx),
            block,
            charge,
            context,
        ))
    }

    pub fn get_or_insert_with<F, Fut>(
        &self,
        object_id: HummockSstableObjectId,
        block_idx: u64,
        context: CacheContext,
        mut fetch_block: F,
    ) -> BlockResponse
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = HummockResult<Box<Block>>> + Send + 'static,
    {
        let key = (object_id, block_idx);

        let entry = self.inner.entry(key, || {
            let f = fetch_block();
            async move {
                let block = f.await?;
                let len = block.capacity();
                Ok::<_, HummockError>((block, len, context))
            }
        });

        BlockResponse::Entry(entry)
    }

    pub fn size(&self) -> usize {
        self.inner.usage()
    }

    #[cfg(any(test, feature = "test"))]
    pub fn clear(&self) {
        // This is only a method for test. Therefore it should be safe to call the unsafe method.
        self.inner.clear();
    }
}

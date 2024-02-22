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
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use futures::Future;
use risingwave_common::cache::{CachePriority, CacheableEntry, LruCacheEventListener};
use risingwave_common::fifo_cache::{FifoCache, LookupResponse};
use risingwave_hummock_sdk::HummockSstableObjectId;
use tokio::sync::oneshot::Receiver;
use tokio::task::JoinHandle;

use super::{Block, HummockResult};
use crate::hummock::HummockError;

const MIN_BUFFER_SIZE_PER_SHARD: usize = 256 * 1024 * 1024;

type CachedBlockEntry = CacheableEntry<(HummockSstableObjectId, u64), Box<Block>>;

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
        let ptr = entry.as_ref() as *const _;
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

type BlockCacheEventListener =
    Arc<dyn LruCacheEventListener<K = (HummockSstableObjectId, u64), T = Box<Block>>>;

#[derive(Clone)]
pub struct BlockCache {
    inner: Arc<FifoCache<(HummockSstableObjectId, u64), Arc<Block>>>,
    cache_miss_times: Arc<AtomicUsize>,
}

pub enum BlockResponse {
    Block(BlockHolder),
    WaitPendingRequest(Receiver<Arc<Block>>),
    Miss(JoinHandle<Result<Arc<Block>, HummockError>>),
}

impl BlockResponse {
    pub async fn wait(self) -> HummockResult<BlockHolder> {
        match self {
            BlockResponse::Block(block_holder) => Ok(block_holder),
            BlockResponse::WaitPendingRequest(receiver) => {
                receiver
                    .verbose_instrument_await("wait_pending_fetch_block")
                    .await
                    .map_err(|recv_error| recv_error.into())
                    .map(BlockHolder::from_ref_block)
            },
            BlockResponse::Miss(join_handle) => {
               join_handle
                    .verbose_instrument_await("fetch_block")
                    .await
                    .unwrap()
                .map(BlockHolder::from_ref_block)
            },
        }
    }
}

impl BlockCache {
    pub fn new(capacity: usize, max_shard_bits: usize, high_priority_ratio: usize) -> Self {
        Self::new_inner(capacity, max_shard_bits, high_priority_ratio, None)
    }

    pub fn with_event_listener(
        capacity: usize,
        max_shard_bits: usize,
        high_priority_ratio: usize,
        listener: BlockCacheEventListener,
    ) -> Self {
        Self::new_inner(
            capacity,
            max_shard_bits,
            high_priority_ratio,
            Some(listener),
        )
    }

    fn new_inner(
        capacity: usize,
        mut max_shard_bits: usize,
        _high_priority_ratio: usize,
        _listener: Option<BlockCacheEventListener>,
    ) -> Self {
        if capacity == 0 {
            panic!("block cache capacity == 0");
        }
        while (capacity >> max_shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && max_shard_bits > 0 {
            max_shard_bits -= 1;
        }

        let cache = FifoCache::new(max_shard_bits, capacity);
        Self {
            inner: Arc::new(cache),
            cache_miss_times: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get(&self, object_id: HummockSstableObjectId, block_idx: u64) -> Option<BlockHolder> {
        self.inner
            .lookup(&(object_id, block_idx))
            .map(BlockHolder::from_ref_block)
    }

    pub fn exists_block(&self, sst_id: HummockSstableObjectId, block_idx: u64) -> bool {
        self.inner.contains(&(sst_id, block_idx))
    }

    pub fn insert(
        &self,
        object_id: HummockSstableObjectId,
        block_idx: u64,
        block: Block,
        _priority: CachePriority,
    ) -> BlockHolder {
        let block = Arc::new(block);
        self.inner
            .insert((object_id, block_idx), block.clone(), block.capacity());
        BlockHolder::from_ref_block(block)
    }

    pub fn get_or_insert_with<F, Fut>(
        &self,
        object_id: HummockSstableObjectId,
        block_idx: u64,
        _priority: CachePriority,
        mut fetch_block: F,
    ) -> BlockResponse
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = HummockResult<Block>> + Send + 'static,
    {
        let key = (object_id, block_idx);
        let lookup_response =
            self.inner
                .lookup_or_insert_with::<_, HummockError, _>(key, || {
                    let f = fetch_block();
                    async move {
                        let block = f.await?;
                        let len = block.capacity();
                        Ok((Arc::new(block), len))
                    }
                });
        match lookup_response {
            LookupResponse::Invalid => unreachable!(),
            LookupResponse::Cached(entry) => {
                BlockResponse::Block(BlockHolder::from_ref_block(entry))
            }
            LookupResponse::WaitPendingRequest(receiver) => {
                BlockResponse::WaitPendingRequest(receiver)
            }
            LookupResponse::Miss(join_handle) => {
                let last_miss_count = self
                    .cache_miss_times
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if last_miss_count % 30000 == 0 {
                    let debug_info = self.inner.debug_print();
                    tracing::info!("cache debug info: {:?}", debug_info);
                }
                BlockResponse::Miss(join_handle)
            },
        }
    }

    // fn hash(object_id: HummockSstableObjectId, block_idx: u64) -> u64 {
    //     let mut hasher = DefaultHasher::default();
    //     object_id.hash(&mut hasher);
    //     block_idx.hash(&mut hasher);
    //     hasher.finish()
    // }

    pub fn size(&self) -> usize {
        self.inner.get_memory_usage()
    }

    #[cfg(any(test, feature = "test"))]
    pub fn clear(&self) {
        // This is only a method for test. Therefore it should be safe to call the unsafe method.
        self.inner.clear();
    }
}

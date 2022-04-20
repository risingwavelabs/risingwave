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
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use futures::channel::oneshot::{channel, Sender};
use futures::Future;
use spin::Mutex;

use super::cache::{CachableEntry, LruCache};
use super::{Block, HummockError, HummockResult};
pub type BlockCacheEntry = CachableEntry<(u64, u64), Box<Block>>;

const CACHE_SHARD_BITS: usize = 6; // It means that there will be 64 shards lru-cache to avoid lock conflict.
const DEFAULT_OBJECT_POOL_SIZE: usize = 1024; // we only need a small object pool because when the cache reach the limit of capacity, it will
                                              // always release some object after insert a new block.

enum BlockEntry {
    Cache(BlockCacheEntry),
    Owned(Box<Block>),
}

pub struct BlockHolder {
    _handle: BlockEntry,
    block: *const Block,
}

impl BlockHolder {
    pub fn from_owned_block(block: Box<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::Owned(block),
            block: ptr,
        }
    }

    pub fn from_cached_block(entry: BlockCacheEntry) -> Self {
        let ptr = entry.value().as_ref() as *const _;
        Self {
            _handle: BlockEntry::Cache(entry),
            block: ptr,
        }
    }
}

impl AsRef<Block> for BlockHolder {
    fn as_ref(&self) -> &Block {
        unsafe { &(*self.block) }
    }
}

unsafe impl Send for BlockHolder {}
unsafe impl Sync for BlockHolder {}

type RequestQueue = Vec<Sender<BlockCacheEntry>>;

pub struct BlockCache {
    inner: Arc<LruCache<(u64, u64), Box<Block>>>,
    wait_request_queue: Vec<Mutex<HashMap<(u64, u64), RequestQueue>>>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let cache = LruCache::new(CACHE_SHARD_BITS, capacity, DEFAULT_OBJECT_POOL_SIZE, false);
        let mut wait_request_queue = vec![];
        let wait_request_queue_size = 1 << CACHE_SHARD_BITS;
        for _ in 0..wait_request_queue_size {
            wait_request_queue.push(Mutex::new(HashMap::default()));
        }
        Self {
            inner: Arc::new(cache),
            wait_request_queue,
        }
    }

    pub fn get(&self, sst_id: u64, block_idx: u64) -> Option<BlockHolder> {
        self.inner
            .lookup(Self::hash(sst_id, block_idx), &(sst_id, block_idx))
            .map(BlockHolder::from_cached_block)
    }

    pub fn insert(&self, sst_id: u64, block_idx: u64, block: Box<Block>) {
        self.inner.insert(
            (sst_id, block_idx),
            Self::hash(sst_id, block_idx),
            block.len(),
            block,
        );
    }

    pub async fn get_or_insert_with<F>(
        &self,
        sst_id: u64,
        block_idx: u64,
        f: F,
    ) -> HummockResult<BlockHolder>
    where
        F: Future<Output = HummockResult<Box<Block>>>,
    {
        let h = Self::hash(sst_id, block_idx);
        let key = (sst_id, block_idx);
        if let Some(e) = self.inner.lookup(h, &key) {
            return Ok(BlockHolder::from_cached_block(e));
        }
        {
            let mut request_que =
                self.wait_request_queue[h as usize % (self.wait_request_queue.len())].lock();
            if let Some(que) = request_que.get_mut(&key) {
                let (tx, rc) = channel();
                que.push(tx);
                drop(request_que);
                let entry = rc.await.map_err(HummockError::other)?;
                return Ok(BlockHolder::from_cached_block(entry));
            }
            request_que.insert(key, vec![]);
            drop(request_que);
            let ret = f.await?;
            let handle = self.inner.insert(key, h, ret.len(), ret).unwrap();
            let mut request_que =
                self.wait_request_queue[h as usize % (self.wait_request_queue.len())].lock();
            let que = request_que.remove(&key).unwrap();
            for sender in que {
                let _ = sender.send(handle.clone());
            }
            Ok(BlockHolder::from_cached_block(handle))
        }
    }

    fn hash(sst_id: u64, block_idx: u64) -> u64 {
        let mut hasher = DefaultHasher::default();
        sst_id.hash(&mut hasher);
        block_idx.hash(&mut hasher);
        hasher.finish()
    }
}

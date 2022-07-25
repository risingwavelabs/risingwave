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
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};
use futures::Future;
use risingwave_common::cache::{CachableEntry, LruCache, LruCacheEventListener};
use risingwave_hummock_sdk::HummockSstableId;

use crate::hummock::{
    Block, HummockError, HummockResult, TieredCache, TieredCacheKey, TieredCacheOptions,
};

const MIN_BUFFER_SIZE_PER_SHARD: usize = 32 * 1024 * 1024;

enum BlockEntry {
    Cache(CachableEntry<(HummockSstableId, u64), Box<Block>>),
    Owned(Box<Block>),
    RefEntry(Arc<Block>),
}

pub struct BlockHolder {
    _handle: BlockEntry,
    block: *const Block,
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

    pub fn from_cached_block(entry: CachableEntry<(HummockSstableId, u64), Box<Block>>) -> Self {
        let ptr = entry.value().as_ref() as *const _;
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

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct CacheKey {
    sst: u64,
    // TODO: To be compatible, use u64 for now. u32 is enough for idx.
    idx: u64,
}

impl TieredCacheKey for CacheKey {
    fn encoded_len() -> usize {
        16
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.sst);
        buf.put_u64(self.idx);
    }

    fn decode(mut buf: &[u8]) -> Self {
        let sst = buf.get_u64();
        let idx = buf.get_u64();
        Self { sst, idx }
    }
}

#[derive(Clone)]
pub struct BlockCache {
    // TODO: replace `(HummockSstableId, u64)` with CacheKey.
    inner: Arc<LruCache<(HummockSstableId, u64), Box<Block>>>,

    tiered_cache: TieredCache<CacheKey>,
}

// N (4B) + N * restart point len (N * 4B) + data len
fn encode_block(block: &Block) -> Vec<u8> {
    let mut buf = Vec::with_capacity(block.len() + 4 + block.restart_point_len() * 4);
    buf.put_u32(block.restart_point_len() as u32);
    for i in 0..block.restart_point_len() {
        buf.put_u32(block.restart_point(i));
    }
    buf.extend_from_slice(block.data());
    buf
}

fn decode_block(buf: &[u8]) -> Block {
    let mut cursor = 0;

    let restart_points_len = (&buf[cursor..cursor + 4]).get_u32() as usize;
    cursor += 4;

    let mut restart_points = Vec::with_capacity(restart_points_len);
    for _ in 0..restart_points_len {
        let restart_point = (&buf[cursor..cursor + 4]).get_u32();
        cursor += 4;
        restart_points.push(restart_point);
    }

    let data = Bytes::copy_from_slice(&buf[cursor..]);

    Block::new(data, restart_points)
}

pub struct MemoryBlockCacheEventListener {
    tiered_cache: TieredCache<CacheKey>,
}

impl LruCacheEventListener for MemoryBlockCacheEventListener {
    type K = (HummockSstableId, u64);
    type T = Box<Block>;

    fn on_evict(&self, key: &Self::K, value: &Self::T) {
        let tiered_cache_key = CacheKey {
            sst: key.0,
            idx: key.1,
        };
        let tiered_cache_value = encode_block(value);
        // TODO(MrCroxx): handle error?
        self.tiered_cache
            .insert(tiered_cache_key, tiered_cache_value)
            .unwrap();
    }
}

impl BlockCache {
    pub async fn new(capacity: usize, mut max_shard_bits: usize) -> Self {
        let tiered_cache_options = TieredCacheOptions::NoneCache;
        let tiered_cache = TieredCache::open(tiered_cache_options).await.unwrap();

        let listener = Arc::new(MemoryBlockCacheEventListener {
            tiered_cache: tiered_cache.clone(),
        });

        if capacity == 0 {
            panic!("block cache capacity == 0");
        }
        while (capacity >> max_shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && max_shard_bits > 0 {
            max_shard_bits -= 1;
        }
        let cache = LruCache::with_event_listeners(max_shard_bits, capacity, vec![listener]);

        Self {
            inner: Arc::new(cache),

            tiered_cache,
        }
    }

    pub async fn get(&self, sst_id: HummockSstableId, block_idx: u64) -> Option<BlockHolder> {
        if let Some(block) = self
            .inner
            .lookup(Self::hash(sst_id, block_idx), &(sst_id, block_idx))
            .map(BlockHolder::from_cached_block)
        {
            return Some(block);
        }

        // TODO(MrCroxx): handle error
        if let Some(data) = self
            .tiered_cache
            .get(&CacheKey {
                sst: sst_id,
                idx: block_idx,
            })
            .await
            .unwrap()
        {
            let block = Box::new(decode_block(&data));
            return Some(BlockHolder::from_owned_block(block));
        }

        None
    }

    pub fn insert(
        &self,
        sst_id: HummockSstableId,
        block_idx: u64,
        block: Box<Block>,
    ) -> BlockHolder {
        BlockHolder::from_cached_block(self.inner.insert(
            (sst_id, block_idx),
            Self::hash(sst_id, block_idx),
            block.len(),
            block,
        ))
    }

    pub async fn get_or_insert_with<F, Fut>(
        &self,
        sst_id: HummockSstableId,
        block_idx: u64,
        f: F,
    ) -> HummockResult<BlockHolder>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = HummockResult<Box<Block>>> + Send + 'static,
    {
        let h = Self::hash(sst_id, block_idx);
        let key = (sst_id, block_idx);
        let entry = self
            .inner
            .lookup_with_request_dedup::<_, HummockError, _>(h, key, || {
                let tiered_cache = self.tiered_cache.clone();
                let f = f();
                async move {
                    if let Some(data) = tiered_cache
                        .get(&CacheKey {
                            sst: key.0,
                            idx: key.1,
                        })
                        .await
                        .map_err(HummockError::tiered_cache)?
                    {
                        let block = Box::new(decode_block(&data));
                        let len = block.len();
                        return Ok((block, len));
                    }

                    let block = f.await?;
                    let len = block.len();
                    Ok((block, len))
                }
            })
            .await
            .map_err(|e| {
                HummockError::other(format!(
                    "block cache lookup request dedup get cancel: {:?}",
                    e,
                ))
            })??;
        Ok(BlockHolder::from_cached_block(entry))
    }

    fn hash(sst_id: HummockSstableId, block_idx: u64) -> u64 {
        let mut hasher = DefaultHasher::default();
        sst_id.hash(&mut hasher);
        block_idx.hash(&mut hasher);
        hasher.finish()
    }

    pub fn size(&self) -> usize {
        self.inner.get_memory_usage()
    }

    #[cfg(any(test, feature = "test"))]
    pub fn clear(&self) {
        // This is only a method for test. Therefore it should be safe to call the unsafe method.
        unsafe {
            self.inner.clear();
        }
    }
}
